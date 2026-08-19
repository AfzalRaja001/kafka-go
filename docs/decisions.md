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
