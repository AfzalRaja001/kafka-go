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

## 2026-08-24 - `__consumer_offsets` persistence lives in a new `internal/offsets` package, not `internal/group`

`InMemoryOffsetStore` (the previous entry) was always step one of a deliberate two-step build: get the
rebalance flow working first, then persist commits for real once that flow existed to exercise them. Track
A's rebalance machinery is merged now, so this is that second step: `LogBackedStore`, an `OffsetStore` that
appends every commit to a real `storage.Log`-managed internal topic, `__consumer_offsets`, instead of a plain
map - committed offsets now survive a broker restart, not just the lifetime of the Go process holding them.

It doesn't live in `internal/group` alongside the interface it implements, because `internal/group`
deliberately imports nothing else in this project (see the OffsetStore entry above and the Coordinator
entries below) - importing `storage.Log` would break that. Rather than relax the rule, `LogBackedStore` lives
in a new `internal/offsets` package that's allowed to depend on both `group` (for the interface) and
`storage` (for `Log`), the same way `broker` already depends on both without either of them depending on each
other.

Three simplifications, all deliberate, all because nothing outside this broker ever reads
`__consumer_offsets` directly:

- **Not a real Kafka record batch on disk.** Every other topic's data is stored as the client's own record
  batch bytes, verbatim, because a real client later `Fetch`es those exact bytes back. Nothing ever `Fetch`es
  this topic, so there's no reason to pay for a CRC and a full v2 batch header nobody parses. Instead each
  commit is one small length-prefixed record (`internal/offsets/record.go`) - the length prefix matters
  because `Log.Read` concatenates raw batch bytes with no boundaries of its own, so replaying a stretch of the
  log back into individual commits needs each record to say its own length.
- **One partition, not real Kafka's default of 50.** 50 partitions exists purely so multiple brokers can share
  the load of this topic - meaningless on a single-node broker, so it would only add a hashing scheme and N
  logs to replay at startup for no benefit anything here would exercise.
- **Provisioned directly by the broker at startup, not through `TopicRegistry`.** `NewLogBackedStore` calls
  `Log.CreatePartition` itself before replaying, and never touches the registry - so `__consumer_offsets`
  never appears in a `Metadata` response, can't be listed, and can't be `DeleteTopics`'d by a client. Matches
  what the topic actually is here: an internal implementation detail, not something clients are meant to know
  about.

**Read path**: a plain `map[key]latestValue` in memory, rebuilt once at construction by replaying the whole
topic from offset 0 (folding each record in so the last write for a key wins), then kept warm by every
`Commit` afterward - `Fetch` is always just a map read, never disk I/O. This mirrors the same
replay-to-rebuild-state pattern `DiskLog.OpenPartition` already uses to recover `nextOffset` on restart, so
it's a familiar shape applied to a new kind of state, not a new concept.

**No real (space-reclaiming) compaction yet.** The plan lists log compaction as its own Phase 5 deliverable,
separate from this piece. What this step needs is only correctness - replay-and-fold always resolves to the
latest value per key, which is what "compacted" means for a *reader*. The underlying log grows unbounded for
now; when Phase 5 builds real segment compaction for the general `Log` interface, `__consumer_offsets` gets
it for free rather than needing a second bespoke implementation.

A malformed record during replay makes `NewLogBackedStore` return an error rather than skip it silently -
this topic's only job is to make restart recovery correct, so failing loudly on data it can't make sense of
is safer than starting up with a wrong picture of what every group has committed.

Verified against a real broker process, not just unit tests: committed an offset via a hand-crafted
`OffsetCommit` v0 request, killed the broker process, started a fresh one against the same `data/` directory,
and fetched the same offset back over a brand-new connection with no error - the actual property this piece
exists for.

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

## 2026-08-26 - `OffsetFetch` bumped from v0 to v2, adding `OffsetStore.FetchAll`

The OffsetStore design entry above named a real, not hypothetical, gap: `kafka-python`'s
`KafkaAdminClient.list_group_offsets()` - the actual method an admin tool reaches for to answer "what has this
group committed" - sends a null topics array, which requires `OffsetFetch` v2. v0 only lets a client ask about
topic-partitions it already knows to name, which defeats the point of an admin/inspection call. Verified
against Apache Kafka's own `OffsetFetchRequest.json`/`OffsetFetchResponse.json` schemas (branch 2.5) rather
than assumed from memory: v2's only wire differences from v0 are that the top-level `topics` array can be `-1`
(null) instead of always present, and the response gains a top-level `error_code` after the topics array.
Jumped straight from v0 to v2, skipping v1 entirely, matching the schema's own comment that "version 1 is the
same as version 0" - there's nothing v1 offers this broker needs.

Answering "everything this group has committed" needed a capability neither `Commit` nor `Fetch` has - both
are single-key operations. Added `FetchAll(group string) []GroupOffset` to `OffsetStore`, returning a flat,
unordered slice rather than anything shaped like Kafka's nested per-topic response: `OffsetStore` deliberately
knows nothing about wire format for any of its other methods either, so `HandleOffsetFetch` groups the flat
result by topic and sorts it (for a deterministic response) itself, the same way it already builds per-topic
structure from an explicit request. Both `InMemoryOffsetStore` and `internal/offsets.LogBackedStore`
implement it as a straightforward "filter my existing map by group" loop - no new storage, no new state, just
a new way to query what was already being kept.

Verified against a real broker process with the actual client method this exists for: committed offsets for
three topic-partitions via a hand-crafted `OffsetCommit` request, then called `KafkaAdminClient(api_version=
(2, 5, 0)).list_group_offsets(group)` from `kafka-python` and got all three back correctly - the exact call
that failed with `UnsupportedVersionError` before this change.

## 2026-08-27 - Prometheus metrics: request-path only for now, explicit DI, no global registry

Phase 5 Track A's metrics list has two different flavors: request-path metrics (bytes in/out, request counts,
latency per API - all available by wrapping the one function every request already funnels through) and
storage-introspection metrics (partition sizes, consumer group lag - need new capabilities on `storage.Log`
and `OffsetStore`, computed periodically rather than per-request). This piece covers only the first kind;
partition sizes and lag are a clearly-scoped follow-up, matching the "extend a frozen interface deliberately,
one reason at a time" pattern this project already used twice for `Log`.

`prometheus/client_golang` is this project's first external dependency (`go.mod` had zero `require`s before
this). The new `internal/metrics` package builds its own `prometheus.Registry` per `Recorder` rather than
using the client library's global `DefaultRegisterer` via `promauto` - the more common idiom in the wider
Prometheus-Go ecosystem, but it would be this project's first piece of global mutable state, when every other
stateful component (`storage.Log`, `group.Coordinator`, `group.OffsetStore`) is constructed once in `main.go`
and threaded through explicitly. A `Recorder` is constructed the same way and passed into `broker.Serve` ->
`handleConn` -> `dispatch`, so this stays consistent, and multiple `Recorder`s can coexist in one test binary
without a "duplicate metrics collector registration attempted" panic.

`RecordRequest` takes an already-resolved label string, not a raw `api_key` - `internal/metrics` has no
notion of the Kafka protocol's api_key scheme, matching `internal/group`'s own zero-dependency stance applied
to a new package. The int16 -> name resolution (`protocol.ApiKeyName`) lives in `internal/protocol` next to
the `ApiKeyProduce` etc. constants it names; `dispatch` resolves it once before calling in. Unknown api_key
values map to a fixed `"Unknown"` string rather than the raw number - `api_key` is client-controlled input,
and echoing an arbitrary number into a metric label would let garbage input create unlimited distinct label
combinations, a real Prometheus failure mode called cardinality explosion.

`dispatch` gained one parameter and zero changes to any of its 14 `switch` cases: it now has named return
values (`resp, err`) so a single `defer` after the shared request header is decoded can observe whichever
case actually ran and record its outcome once. Header-decode failures (too short to contain a valid
`api_key`) aren't recorded at all - `handleConn` already closes the connection immediately in that case, so
there's no confidently-known `api_key` to label and no ongoing signal worth reporting.

"Messages/sec" in the plan's wording really means records/sec, not requests/sec - one `Produce` request can
carry a batch of many records. Getting the literal count would mean changing `HandleProduce`'s signature to
report it, which is Produce-specific work that breaks the "every API key gets identical generic treatment"
property the rest of this piece relies on. For now, `kafkago_requests_total{api_key="Produce"}` through
`rate()` is a real, useful throughput signal - just "Produce requests/sec" - and true per-record counting is
deferred to the same follow-up as partition sizes and consumer lag.

The metrics HTTP server (`:9101/metrics`, separate from the Kafka wire protocol's `:9092`) has no graceful
shutdown machinery, matching this project's current level of polish everywhere else - a scrape mid-shutdown
just looks like a failed scrape to Prometheus, not data loss.

Verified against a real broker process, not just unit tests: scraped `/metrics` before any traffic (empty,
200 OK - Prometheus vectors don't export a series until first observed), sent real `CreateTopics`/`Metadata`/
`Produce` traffic via `kafka-python`, then scraped again and saw real counts, byte totals, and latency
histograms for exactly the APIs actually called.

## 2026-08-29 - Partition sizes and consumer group lag: the deferred half of Phase 5 Track A metrics

The previous entry deferred two metrics that need periodic background collection rather than per-request
instrumentation. This closes that gap.

`storage.Log` gains its third deliberate extension: `Size(topic string, partition int32) (int64, error)`.
`DiskLog.Size` sums each segment's already-in-memory `size` field (updated on every `Append`), so it costs no
disk I/O - the same "cheap, always-tracked bookkeeping" style the offset machinery already uses.
It reports genuine on-disk footprint, including the per-record 8-byte header this segment format writes
(payload length + offset span) - a TDD-caught correction: the first draft of the disk-backed tests assumed
payload-only byte counts, and the real numbers came back larger until the tests were fixed to expect
`payload + recordHeaderSize` per record, matching what `Segment.Append` actually writes. `FakeLog.Size`
reports payload bytes only, since it has no segment file format to mirror - consistent with `FakeLog` never
having claimed to be byte-exact with `DiskLog`, only offset-exact.

`group.OffsetStore` gains `Groups() []string` - the enumeration lag reporting needs ("which groups even have
lag to compute"), sourced from committed-offset history rather than `group.Coordinator`'s live membership map.
This was a real design choice, not the only option: `Coordinator` already tracks every group ID in memory and
could have exposed a getter with less code. `OffsetStore` won because it's strictly more correct for what lag
means operationally - a group whose members have all crashed still has real lag worth alerting on, and
`Coordinator`'s map is pure in-memory (never persisted), so it would report zero known groups immediately
after a broker restart even while a persistent `OffsetStore` still has real replayed history to compute lag
against.

`internal/metrics.Recorder` gains two plain setters, `SetPartitionBytes`/`SetConsumerGroupLag`, backed by
`GaugeVec`s rather than `CounterVec`s - both values can legitimately decrease (retention/compaction shrink a
partition; a group catching up reduces its own lag), so each call replaces the previous value. Per the
decision already made for `RecordRequest`, `internal/metrics` still doesn't know what a "partition" or a
"consumer group" is - it takes labels and a number. The actual correlation logic lives in a new
`collectMetrics` function in `cmd/broker`: walks `TopicRegistry.All()` calling `Log.Size()` per partition, and
`OffsetStore.Groups()` -> `FetchAll` -> `Log.LatestOffset()` minus committed, per group. It's a plain function
with no ticker, mirroring `group.Coordinator.ReapExpiredMembers`'s own shape exactly - the interesting logic
is independently unit-testable with fakes, and a thin `runMetricsCollector` just wraps it in a ticker, living
in `main.go` next to `runReaper` and `runMetricsServer` (no new convention for where background goroutines
live). A per-partition error (the registry knows about a partition the log doesn't) is skipped, not fatal -
this runs forever on a ticker, so one bad partition must never take the whole loop down. This also produced
this project's first test file for `cmd/broker` - `collectMetrics` being a pure function is exactly what made
that possible; `main()` itself remains untested glue, same as it always has been.

Collection interval is 15s, deliberately slower than the reaper's 1s - this is observability, not
correctness-critical, and each tick does real work (iterates every known topic-partition and every known
group).

Verified against a real broker: produced 10 real records via `kafka-python`, committed offset 4 for a test
group via a hand-crafted `OffsetCommit`, waited for a real collector tick, then scraped `/metrics` and saw
`kafkago_consumer_group_lag{...} 6` (10 - 4, correct) and a real, non-trivial `kafkago_partition_bytes` value
reflecting genuine on-disk bytes including real Kafka record batch framing overhead, not just message
payloads.

## 2026-08-31 - Grafana dashboard: Prometheus+Grafana in Docker, the broker stays native

The plan lists Track A's Phase 5 items in order: Metrics, Grafana, Docker, benchmarks. Docker - a real
Dockerfile for the broker itself, bundled into a docker-compose alongside Prometheus and Grafana - is
explicitly the item *after* this one, not part of it. So this piece needed its own answer to "how does someone
actually see the dashboard right now": `deploy/docker-compose.yml` runs only `prometheus` and `grafana` as
containers; the broker keeps running exactly as it always has (`go run ./cmd/broker`, or the built binary),
with Prometheus's scrape config (`deploy/prometheus/prometheus.yml`) pointing at `host.docker.internal:9101`.

This isn't a throwaway shortcut - when the Docker item lands next, it extends this same compose file by adding
a third `broker` service (with its own new Dockerfile), not starting from a blank file. `host.docker.internal`
resolves out of the box on Docker Desktop (Windows/Mac, this project's own dev platform); an `extra_hosts:
host.docker.internal:host-gateway` entry was added defensively so the same file also works on Linux, where it
doesn't resolve by default - a no-op on Docker Desktop, but keeps this reproducible for anyone else who tries
it, which matters for a resume artifact more than it would for internal tooling.

Grafana is provisioned, not manually configured: `grafana/provisioning/datasources/datasource.yml` auto-wires
the Prometheus datasource, and `grafana/provisioning/dashboards/dashboard-provider.yml` points Grafana at
`grafana/dashboards/kafka-go.json` on startup. The alternative - click "Add data source," then "Import
dashboard," paste JSON - works but turns "one command reproduces the screenshot" (the plan's own stated reason
this piece exists) into a multi-step manual process nobody but the original author will ever bother repeating.
Anonymous admin access is enabled in the Grafana container (`GF_AUTH_ANONYMOUS_ENABLED`) - this is a local dev
dashboard with no real users, so the default login flow is pure friction with no real security benefit here.

Panel set follows directly from what the two metrics PRs already export: request rate and error rate by API,
bytes in/out rate, p50/p95/p99 latency (aggregated across every API rather than split by api_key, since 14
APIs times 3 percentiles would be an unreadable number of lines on one panel), partition sizes, and consumer
group lag - the last one deliberately given real estate as its own panel, since "is anything falling behind"
is usually the first thing anyone actually checks on a real Kafka dashboard.

**Not live-verified against a real Grafana instance** - unlike every other piece this session, this one
couldn't be, because Docker isn't installed in the environment this was built in. JSON/YAML syntax was
validated directly (`python -m json.tool`, `yaml.safe_load`), and every cross-reference between files (mount
paths, service names, ports) was checked by hand for consistency, but actually running `docker compose up` and
confirming the dashboard renders real data is still outstanding, to be done wherever Docker is actually
available.
