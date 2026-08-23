# Decisions

One paragraph per non-obvious choice. Newest at the bottom.

Related: `docs/issues.md` records bugs and their fixes; `docs/conventions.md`
records the operational rules (git identities, verification commands).

## 2026-08-07 - Go, and the real Kafka wire protocol

Chose Go over Java/Rust/C++ for this project. Neither of us had used Go before; we picked it for goroutines
mapping naturally onto "one goroutine per client connection," and for the infra-ecosystem resume signal.
We chose to implement the real Kafka wire protocol rather than a custom one so that official clients
(`kafka-console-producer`, `kafka-python`, `franz-go`) connect without modification - that's the project's
core differentiator versus most "Kafka clone" repos, which stop at an in-memory queue behind a JSON API.

## 2026-08-07 - Building both tracks together, not split by person

The build plan's default is to split work into Track A (protocol/networking: `internal/protocol`,
`internal/broker`) and Track B (storage/state: `internal/storage`, `internal/group`), one person per track,
working in parallel. We're deviating from that: both of us are building both tracks together in the same
session. The track labels still matter for two reasons - they mark the seam where the `Log` interface
decouples protocol code from storage code, and they're still useful for resume framing (being able to speak
to "the protocol half" and "the storage half" separately). Every piece of work will still be identified as
belonging to Track A or Track B as it's built, even though there's no per-person ownership split.

## 2026-08-16 - `Log.Append` takes a record count (the frozen interface's first change)

The `Log` interface was frozen deliberately so the two tracks could develop against a stable seam. This is
the first change to it, and it was forced by a real bug (`docs/issues.md` entry 5): a Kafka record batch can
hold many records and therefore consume many offsets, but the storage engine had no way to know how many.
It advanced the log by one offset per append, which made `LatestOffset` under-report, which made the next
`Produce` assign a `baseOffset` colliding with records already written.

Only the protocol layer can read a record count, because it lives in the Kafka record batch header, and
parsing that header is exactly what the storage layer must never do - that separation is what keeps
`internal/storage` independently testable and what makes the zero-copy "store batch bytes verbatim" design
possible. So the count has to cross the boundary explicitly:
`Append(topic, partition, batch []byte, recordCount int32)`.

Alternatives rejected: (a) letting `DiskLog` parse the batch header itself - avoids the interface change but
collapses the protocol/storage boundary the whole two-track split depends on; (b) having `Produce` split a
batch into one append per record - destroys the store-verbatim principle and would mean re-encoding record
batches, which the build plan calls the single most important design insight to preserve. `Read`,
`EarliestOffset`, and `LatestOffset` were left untouched, so `Fetch` and `ListOffsets` needed no changes.

## 2026-08-16 - Segment records carry their own offset span

Fixing the same bug needed the offset count to survive a restart, because `OpenPartition` rebuilds
`nextOffset` by scanning the active segment. Rather than persist a separate bookkeeping file (extra file,
extra crash-consistency problem), we widened the segment's own per-blob header from 4 bytes to 8:
a payload length followed by an offset span.

This keeps the segment file self-describing - recovery needs nothing but the file itself - and it also
serves the read path, since `FindRecord` needs each blob's span to skip whole batches while scanning
forward. The storage layer never learns that the number originated as a Kafka `RecordCount`; it just stores
the span it was handed. One consequence worth naming: this is an on-disk format change, so any existing
`data/` directory is unreadable by the new code. That's free right now because nothing persistent exists
yet, and it would be a migration problem later - a reason to get the format right early rather than late.

## 2026-08-18 - `CreateTopics`/`DeleteTopics` provision storage eagerly, for real

`CreateTopics` extends the frozen `Log` interface a second time -
`CreatePartition`/`DeletePartition` - rather than staying registry-only.
The alternative (only writing to `TopicRegistry`, leaving storage to keep
being created lazily on first `Produce`, exactly as it already was) was
rejected because it would leave a real gap from real Kafka: a freshly
created, never-produced-to topic would error on `Fetch`/`ListOffsets`
instead of reading back as empty at offset 0. `DiskLog.CreatePartition`
needed no new logic - it's just a new exported entry point onto the
already-existing `openPartition`, which Append already used lazily.

`CreateTopics` also honors the client's requested partition count for
real, rather than clamping to 1 the way most other APIs here simplify to a
single version/case. The storage layer already keys everything by
`(topic, partition)` and `Metadata` already returns a partition list per
topic; honoring the real count only means driving what already existed
with a real number instead of always 1, not new complexity.

`DeleteTopics` does real deletion - closing open file handles, then
removing the partition directory from disk - rather than a registry-only
soft delete that would leave orphaned files behind. This is what
surfaced issue 9 (the Windows `os.RemoveAll` race): real deletion is more
work and more risk than a soft delete, but a soft delete would have been
dishonest about what `DeleteTopics` means, and Kafka on Windows needing a
retry loop here is exactly the kind of gap the manual live-testing
discipline exists to catch before it looks like the feature works.

## 2026-08-18 - `Metadata` bumped from v0 to v1 for `ControllerId`

Every other API in this broker deliberately advertises the lowest version
that does what's needed, to avoid flexible-version (KIP-482) encoding.
`Metadata` was v0 until this changed it to v1 - discovered as a real,
not hypothetical, gap: `kafka-python`'s actual `KafkaAdminClient` failed
outright (`NodeNotReadyError: controller`) because `ControllerId` (which
tells a client which broker to send `CreateTopics`/`DeleteTopics` to)
doesn't exist before Metadata v1. Since this broker is single-node,
`ControllerId` is always the one broker there is - not a new piece of
state to track, just one more field in an already-known value. v1's other
two new fields (`Rack` per broker, `IsInternal` per topic) are encoded as
their "not applicable" value and don't add any state either. Rejected:
staying at v0 and hand-crafting `CreateTopics`/`DeleteTopics` requests in
tests only - that would have "tested" the feature without ever proving a
real client could use it, exactly the gap issue 8 was about.

## 2026-08-16 - `ListOffsets` v0 resolves only the two timestamp sentinels

Kafka's `ListOffsets` asks "what offset holds the first record at or after timestamp T", where `-1` and `-2`
are reserved to mean latest and earliest. Real Kafka also resolves arbitrary timestamps by searching the
time index. We implement only the two sentinels and return an error for anything else, because those two
are what `seek_to_beginning()` / `seek_to_end()` send and therefore what unblocks real consumers. Arbitrary
timestamp lookup is deferred rather than faked - `Partition.LookupOffsetByTimestamp` already exists in the
storage layer, so wiring it up later is small, and returning a wrong answer now would be worse than
returning an honest "not implemented".

## 2026-08-19 - `OffsetStore` is a new interface, not an extension of `Log`

Phase 4's offset storage (`FindCoordinator`, `OffsetCommit`, `OffsetFetch`) needed somewhere to persist
`(group, topic, partition) -> offset`. We added a dedicated `OffsetStore` interface in `internal/group/`
rather than extending `storage.Log` a third time, because the shape is fundamentally different: `Log` stores
opaque byte batches spanning many offsets each; an offset commit is one int64 plus a metadata string, keyed
by group as well as topic-partition. Forcing that through `Log`'s `Append`/`Read` methods would mean
inventing a fake batch format for a single integer, purely to reuse an interface that doesn't fit.

The first (only, for now) implementation, `InMemoryOffsetStore`, is a plain mutex-guarded map - matching the
plan's own two-step guidance (simple store first, `__consumer_offsets`-backed later, once Track A's
rebalance flow exists to actually exercise commit-then-resume end to end). `Commit` still returns an `error`
even though this implementation can never produce one, the same reasoning `Log`'s methods all return `error`
even where `FakeLog` trivially can't fail: the interface is designed for the implementation that replaces
this one, not just today's.

`internal/group` depends on nothing else in this project - no import of `protocol` or `storage`. That keeps
the dependency graph a one-way fan-out (`broker` -> `protocol` -> `{storage, group}`), matching the build
plan's rule to keep `protocol` and `storage` free of dependencies on each other, now extended to `group` too.

One discovery from live-testing worth recording: `kafka-python`'s `KafkaAdminClient.list_group_offsets`
accepts `None` as "fetch all committed offsets for a group" - but that requires `OffsetFetch` v2+, which adds
a nullable topics array. This broker only implements v0, where the topics array is never nullable and the
client must always name explicit partitions. Passing `None` against this broker raises
`UnsupportedVersionError` client-side, not a broker error - the client checks its own negotiated version
before ever sending the request. Verification always passes an explicit `[TopicPartition(...)]` list instead.

## 2026-08-22 - `group.Coordinator`'s concurrency model is a channel-close broadcast

Track A's rebalance state machine (`JoinGroup`/`SyncGroup`/`Heartbeat`/`LeaveGroup`) needed a way to hold
several goroutines - each a different client connection - blocked together until a rebalance window closes,
then release them all at once with a consistent outcome. `Coordinator` does this with a plain `chan
struct{}` per group: every joining goroutine registers itself under the group's mutex, then blocks reading
from the group's current `joinBarrier`; the window's timer closes that channel, which is a one-shot broadcast
to every blocked reader with no polling and no missed-wakeup races. `SyncGroup`'s leader-to-followers handoff
uses the identical pattern with its own `syncBarrier`. `sync.Cond` was the alternative considered and
rejected - it does the same job but is easy to get wrong (a signal sent before a waiter calls `Wait()` under
the right lock is simply lost), where closing a channel is safe regardless of ordering between the close and
any given reader arriving at its receive.

Rejected alternative for the wait itself: polling `Coordinator` state on a short sleep loop from each
goroutine. Works, but wastes CPU and adds latency jitter for no benefit when a real broadcast primitive is
available.

## 2026-08-22 - The rebalance window's timeout is broker-configured, not client-supplied

`Coordinator.JoinGroup` originally used the request's own `SessionTimeoutMs` as the join window's duration -
seemed reasonable, since that's the only timeout-shaped field in the request. Live testing found this was
wrong (`docs/issues.md` issue 11): `kafka-python` sends a 30-second default session timeout, and no real
client's own poll loop waits anywhere near that long for a response, so `JoinGroup` looked like it hung.

Real Kafka's actual design keeps these genuinely separate: `SessionTimeoutMs` is how long a member can go
quiet before being considered dead (governs `Heartbeat` expiry), while the join window for a freshly-forming
group is a broker-side setting, `group.initial.rebalance.delay.ms` (3 seconds by default) - not something the
client requests at all. `Coordinator` now takes that delay as a constructor parameter, matching how
`storage.NewDiskLog` already takes `segmentMaxBytes`/`indexEvery` rather than hardcoding them: a real default
in `main.go`, short values in tests so the suite doesn't spend real wall-clock seconds waiting on windows that
exist to test timing behavior, not to be fast.

## 2026-08-22 - Protocol selection picks the first name common to every member, not full voting

Real Kafka's actual algorithm for choosing which assignment protocol (e.g. `range`, `roundrobin`) a group
uses is a cross-member voting scheme over each member's ranked preference list. `Coordinator` instead takes
the intersection of every joined member's supported protocol names and picks whichever name appears earliest
in the first joiner's list. This is correct for every scenario this project's clients actually produce - every
member in a test run proposes the same protocol name(s) - and full voting would be real complexity with
nothing here to exercise the cases where it would differ from the simplification. `SyncGroup`'s
follower-wait is bounded by the member's own session timeout rather than waiting forever, a real (if narrow)
gap real Kafka closes with more machinery: if the leader crashes between `JoinGroup` and `SyncGroup`, a
follower here times out (`ErrSyncTimedOut`, mapped to `REBALANCE_IN_PROGRESS`) rather than hanging - not
handled today is *automatically retriggering* a fresh rebalance in that case, left for whoever hits it.
