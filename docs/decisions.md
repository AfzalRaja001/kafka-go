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

Docker wasn't installed in the environment this was built in, so JSON/YAML syntax was validated directly
(`python -m json.tool`, `yaml.safe_load`) and every cross-reference between files (mount paths, service names,
ports) was checked by hand, without a real `docker compose up` to confirm it. That run happened separately, on
a machine with Docker: `docker compose up` (foreground) needs its own terminal, since interrupting it to run
diagnostic commands elsewhere stops the whole stack cleanly (exit code 0, no error - this tripped up the first
attempt, resolved by using `docker compose up -d` instead) - otherwise the compose file, the provisioning, and
the dashboard queries all worked exactly as designed on the first real run. Confirmed against real broker
traffic: 30 real `Produce` requests and a committed offset deliberately short of the latest showed up correctly
on every panel - request rate, bytes in/out, partition size, and consumer group lag (`10`, matching `30
produced - 20 committed`) all rendering real numbers, not just structurally valid config.

## 2026-09-07 - Throughput/latency benchmark, and a real ListOffsets v0->v1 bump it forced

The last item on Phase 5 Track A's list: `cmd/benchmark`, a new command that drives real, concurrent Produce
and Fetch load against a running broker over the actual Kafka wire protocol - via `github.com/twmb/franz-go`
(`pkg/kgo` + `pkg/kadm`), a real client, not calls into this project's own internal packages - and prints
throughput and latency percentiles for each phase. This is this project's second external dependency (the
first was `prometheus/client_golang`), scoped narrowly: only `cmd/benchmark` imports it.

Design: `producers` goroutines (default 4), each its own client connection, produce fixed-size records as
fast as possible for a configurable `duration`. Then `consumers` goroutines (default 4), each its own client
connection, independently read the *entire* topic from the beginning - simulating that many separate
consuming applications, not splitting the work between them. Every knob (broker address, topic, record size,
concurrency, duration) is a CLI flag, not a hardcoded const like `cmd/broker/main.go` - a benchmark's whole
purpose is answering "how does this behave under different load shapes," so hardcoding those would defeat the
point. Percentile/throughput math (`cmd/benchmark/stats.go`'s `Summarize`) is a pure, TDD-covered function;
the actual load generation is untested glue, verified by running it against a real broker and confirming the
numbers make sense - the same pure/impure split `collectMetrics`/`runMetricsCollector` used in the metrics
work.

Three real bugs surfaced by actually running this against a real broker, not just getting it to compile:

1. **`ensureTopic`'s "already exists is fine" check was checking the wrong thing.** `kadm.Client.CreateTopic`
   returns `(CreateTopicResponse, error)` where the second return value *is* `response.Err` - not two
   independent signals. The first draft checked `err != nil` (returning early) before ever reaching the
   explicit `errors.Is(resp.Err, kerr.TopicAlreadyExists)` tolerance check below it, so re-running the
   benchmark against an already-created topic always failed. Fixed by checking `errors.Is` against the single
   error `CreateTopic` actually returns.

2. **`ListOffsets` needed a real v0->v1 bump - a genuine compatibility gap, not a benchmark-tool bug.** The
   fetch phase needs to know the topic's true latest offset to know when it's caught up; `kadm.ListEndOffsets`
   is the obvious way to ask. It kept coming back with `Offset: -1, Err: nil` - not an error, just silently
   wrong. Tracked down using two things: a hand-rolled raw-socket request (bypassing kmsg entirely) proved the
   broker's own v0 response bytes were correct and decodable by hand, and `kgo.WithLogger` at debug level
   showed franz-go genuinely sending and receiving a well-formed `ListOffsets v0` request/response with no
   error. Reading kmsg's own generated decoder (`ListOffsetsResponse.readFrom`) settled it: v0's response
   shape is an *array* of offsets per partition (`OldStyleOffsets` in kmsg's naming); the scalar `.Offset`
   field kadm's convenience API actually reads is only populated for v1+, where each partition resolves to
   exactly one offset instead of an array. This broker deliberately only ever implemented v0 (real Kafka's own
   history: v1 simplified the array away specifically because no client ever asked for more than one offset
   per partition) - so any client whose high-level admin API reads the modern scalar field, not just clients
   built specifically to expect the old array, cannot get a real answer from this broker as it stood. Fixed
   the same way `Metadata` went v0->v1 and `OffsetFetch` went v0->v2: bumped `ListOffsets` to v1 outright (not
   dual-maintained), matching this project's standing rule of advertising exactly one version, the lowest one
   that does what's needed. `internal/protocol/listoffsets.go`'s request decode dropped v0's now-removed
   `max_num_offsets` field; the response now writes one `(timestamp, offset)` pair per partition instead of an
   array - `timestamp` is always encoded as -1 ("not applicable"), the same treatment `Metadata` v1 gave
   `Rack`/`IsInternal`, since this broker has nowhere to look up when a given offset was actually written.

3. **The fetch phase's first live run was nearly useless as a benchmark, for two compounding reasons.**
   First measurement: a single Fetch call took 5.68 real seconds and returned ~1MB (5295 records) in one
   shot - `kgo`'s default `FetchMaxPartitionBytes` is 1MB, the same default real Kafka clients (Java, Python)
   ship with, so this isn't a franz-go quirk, it's a genuine characteristic of `DiskLog.Read`
   (`internal/storage/disklog.go`): it accumulates a response one record at a time via `Partition.Read`, each
   call a separate read against the segment file, so filling a ~1MB response out of 128-byte records means
   several thousand individual reads - real, measurable per-call overhead that adds up. That's a legitimate
   finding about current read-path performance, worth a real follow-up, but not something to silently paper
   over inside a benchmark tool - flagged separately rather than "fixed" here, since optimizing
   `DiskLog.Read`'s batching is real, separate work with its own design questions, not an incidental fix. What
   *did* belong in this piece: two giant, multi-second fetches produced only two latency samples - technically
   correct, useless as a distribution. Second bug, in `cmd/benchmark/stats.go` itself: `Summarize`'s `Records`
   field was `len(latencies)`, which is exactly the record count for Produce (one record per request) but
   wildly wrong for Fetch (one request can carry thousands of records) - a live run reported "132 records,
   6.48 MB," an internally-inconsistent result once max-partition-bytes was reduced enough to produce more
   samples. Fixed by decoupling them: `Summarize` now takes an explicit `records` count separate from
   `len(latencies)` - percentiles still come from the per-request latency samples, throughput from the real
   record count. Combined with a new `-fetch-max-bytes` flag (default 64KB, well under `kgo`'s 1MB default) to
   get enough real round-trips for percentiles to mean something, this is what turned "one measurement,
   basically meaningless" into "a real, honest latency distribution."

Verified against a real broker end to end, several times: produce phase numbers looked sane immediately
(~8-10k records/sec on this machine); fetch phase, after all three fixes, produced internally-consistent
results (e.g. one run: 2 consumers each independently re-reading a freshly-produced 3856-record topic ->
`Fetch: 7712 records` reported, exactly `2 x 3856`) and correctly picked up pre-existing data on a re-run
against an already-populated topic (a topic left over from a previous run with 3856 records, re-run adding
1869 more, correctly reported `Fetch: 5725 records` = `3856 + 1869`). The Fetch phase's own throughput numbers
honestly reflect a broker whose read path is currently far slower than its write path for many-small-records
workloads - a real, reportable characteristic of this broker today, not a bug in the benchmark that measured
it.

This closes out Phase 5 Track A - Metrics, Grafana, Docker, and now Benchmarks are all shipped and
live-verified.
## 2026-09-03 - Docker: the broker joins the compose stack as its own service

The last piece the 2026-08-31 entry deferred: a real Dockerfile for the broker, added as a third service to
`deploy/docker-compose.yml` alongside `prometheus` and `grafana`. `docker compose up` is now fully
self-contained - no `go run ./cmd/broker` needed first.

`deploy/Dockerfile` is a two-stage build. The builder stage uses `golang:1.26` (matching `go.mod`'s `go
1.26.5`) and compiles with `CGO_ENABLED=0 GOOS=linux` - this project has no cgo dependencies
(`prometheus/client_golang` is pure Go), so a static binary is both possible and required for the final stage.
That final stage is `gcr.io/distroless/static-debian12`: no shell, no package manager, nothing but the
compiled binary. Smallest attack surface and image size, at the cost of not being able to `docker exec` in to
poke around - debugging a running container relies on its logs and the `/metrics` endpoint instead, which is
how a real deployment would need to work anyway.

The native workflow isn't going away: `docker-compose.yml`'s `broker` service is the default now, but running
the broker yourself with `go run ./cmd/broker` and only using Docker for Prometheus + Grafana still works.
`prometheus.yml`'s `static_configs` lists two targets - `broker:9101` (the compose service, resolved via
Docker's internal DNS) and `host.docker.internal:9101` (a natively-run broker) - and whichever one is actually
running answers scrapes normally; the other just shows as a down target in Prometheus's own target list
(`http://localhost:9090/targets`), which is harmless. This avoids any extra machinery (env var substitution,
compose profiles, a second compose file) for what's really a one-line difference in scrape target.

The broker's data directory is a named volume (`broker-data`), not a bind mount and not ephemeral: topics,
records, and committed consumer offsets survive `docker compose down`/`up` and container restarts, matching
how the native workflow already behaves (its own `./data` directory persists on disk between runs). `docker
compose down -v` wipes it if a clean slate is ever needed.

Docker's daemon wasn't reachable from the environment this was built in (the CLI is present - Docker Desktop
is installed - but the daemon socket isn't reachable from that shell), so JSON/YAML syntax was validated by
hand first (`yaml.safe_load` on both compose files; `go build`/`vet`/`test` all green, unaffected since no Go
code changed for this piece). Real verification happened separately, on a machine with a running daemon:
`docker compose up --build` built the broker image and brought up all three services successfully, confirming
the multi-stage build actually produces a working static binary inside `distroless/static-debian12` (a base
image with no shell to fall back on if the binary were missing something at runtime), and that the compose
wiring (ports, the named `broker-data` volume, Prometheus's two-target scrape config) all resolves correctly
end to end.

## 2026-09-07 - Real compaction for __consumer_offsets, scoped away from general Kafka compaction

Phase 5 Track B's last item: log compaction, deferred since PR #19's design entry promised
`__consumer_offsets` would "get it for free" once this landed. It didn't turn out to be free, and it isn't
general - both discovered before writing any code.

Real Kafka compaction operates on individual *records* inside a partition, and a single record batch can hold
many records under different keys - compacting one means decomposing and reassembling batches, recomputing
CRCs and offsets for whatever survives. This project has one foundational, repeated principle - store batch
bytes verbatim, never re-encode - that real compaction would break for the first time. Checking what
`__consumer_offsets` (`internal/offsets/store.go`) actually needs changed the picture entirely: it doesn't use
real Kafka record batches at all, only its own custom one-record-per-blob encoding, and `LogBackedStore`
already tracks the latest value per key in memory (`s.latest`) as a side effect of just existing. The hard
part general compaction usually requires - figuring out what's still needed - was already solved; what was
missing was only the mechanism to physically reclaim the space. So this piece scopes to exactly that:
internal-only, `__consumer_offsets`-specific, not a general `cleanup.policy=compact` feature for client topics.

A second discovery, digging into the segment format before writing anything: a blob's offset is never stored
on disk, only derived by summing each blob's span forward from a known starting point (0, or a sparse index
entry). There's no way to represent a *gap* where a compacted-away record used to be - which is exactly what
real Kafka's compacted topics have (a Fetch at a removed offset returns the next surviving record). Supporting
that would mean a real on-disk format change. Instead, since `__consumer_offsets` is only ever replayed
sequentially from offset 0 at startup (`internal/offsets`'s own doc comment: nothing outside this broker
Fetches it over the wire, ever), `Compact` sidesteps the whole problem: it fully rewrites a partition from
scratch, renumbering offsets from 0. `storage.Log` gains its fourth deliberate extension,
`Compact(topic, partition, records)`, documented plainly as unsafe for anything a real client might Fetch by
offset - true today of nothing but this one internal topic.

`Partition.Compact` (`internal/storage/partition.go`) is the real mechanism: close and delete every existing
segment's three files (`.log`, `.index`, `.timeindex` - actual deletion, the actual space reclamation this
exists for, not just an in-memory swap), open a fresh segment at base offset 0, and re-append the given records
through the same `appendLocked` path normal `Append` already uses (split out from `Append` specifically so
`Compact` can reuse it without re-entering the partition's own mutex). One real gotcha: `OpenSegment`/
`OpenIndex`/`OpenTimeindex` all open with `O_APPEND`, not `O_TRUNC` - reopening base offset 0 without first
deleting its old files would silently append the fresh records after the stale ones instead of replacing them,
since a segment's offset numbering is entirely position-derived, not stored. `FakeLog.Compact` does the
equivalent in-memory swap, keeping `FakeLog`/`DiskLog` symmetric the same way every prior `Log` extension has.

`LogBackedStore.Compact()` snapshots `s.latest`, re-encodes each entry with the existing `encodeCommit`, and
hands the result to `log.Compact` - genuinely trivial, since the "what's still needed" computation was already
being done. What wasn't trivial: `Commit` previously appended to the log *before* taking `s.mu`, only locking
around the in-memory update - a real race, where a `Commit` running concurrently with `Compact`'s
snapshot-then-rewrite could append to the log being replaced and have that commit silently vanish once the
rewrite finished. Fixed by having `Commit` hold `s.mu` around its entire body (append and apply both), the same
lock `Compact` holds around its entire operation - fully serializing the two against each other. A real,
minor behavior change (commits now briefly contend with compaction, not just with each other's in-memory
update), worth naming explicitly, verified clean under `go test -race` with a test that runs 200 concurrent
commits against 20 concurrent compactions on the same store.

Trigger is a background ticker (`runOffsetsCompactor` in `cmd/broker/main.go`), same shape as `runReaper`/
`runMetricsCollector` - 5 minutes, deliberately much slower than either of those, since this is disk-space
housekeeping on a low-traffic internal topic, not correctness-critical or latency-sensitive. A failed
compaction is logged, not fatal - `s.latest` already holds the correct answer regardless of whether the
on-disk rewrite succeeded, so a failure just means trying again next tick, not any loss of correctness.

Verified against a real running broker, with the interval temporarily shortened to 3s for the verification run
(reverted to 5 minutes before this was written up): 100 real `OffsetCommit` requests for the same key grew the
partition's segment file from 202 to 6902 bytes; waiting for the ticker to fire brought it straight back down
to 202 bytes. A real `OffsetFetch` afterward correctly returned the latest committed offset (100), and - the
property this whole feature exists for - a full broker restart against the compacted, renumbered log replayed
it correctly, still reporting offset 100 with the file still at its compacted 202-byte size, not re-bloated.

This closes out Phase 5 Track B's log compaction item and, with it, everything Phase 5 originally scoped for
both tracks.

## 2026-09-08 - `Partition.ReadBatch`: fixing a real O(n * indexEvery) Fetch bug

Flagged as a follow-up during the benchmark work (PR #26): a live run showed a single Fetch pulling ~1MB out of
small (128-byte) records took ~5.68 real seconds - franz-go's default per-partition fetch size, so any real
client hitting this broker with enough small records would hit the same wall.

Traced before writing any code, not assumed: the actual cause wasn't syscall count, it was redundant
re-scanning. `DiskLog.Read` used to call `Partition.Read(offset)` once per blob in a loop, and `Partition.Read`
-> `FindRecord` does a sparse-index lookup then scans forward blob-by-blob from that indexed entry to reach the
target. Since the loop restarted this from scratch for every single blob rather than continuing from where the
previous call left off, reading blob *k* within an `indexEvery`-sized window rescanned all *k* blobs before it
from the window's start. Summed across a whole window that's roughly `indexEvery^2 / 2` redundant blob-reads to
walk through `indexEvery` real blobs - O(n * indexEvery) total instead of O(n). With production's real
`indexEvery=100` and the benchmark's ~8000 small records (comfortably inside one segment, so this was entirely
a within-segment effect), that amplification alone plausibly explains the ~5.68s.

The fix consolidates the whole multi-blob accumulation into one new method, `Partition.ReadBatch(offset,
maxBytes)`, replacing `DiskLog.Read`'s old external loop entirely (`DiskLog.Read` is now a one-line delegation).
`ReadBatch` does exactly one sparse-index lookup - for the segment containing the starting offset - then keeps
scanning forward from wherever the previous blob left off, crossing into the next segment (position 0, no index
lookup needed for a fresh segment's first blob) if `maxBytes` isn't hit before the current one runs out.
`Partition.Read` (the single-blob method) is untouched - it's still exactly right for the point-lookup tests
that use it directly, and was never the slow path; only the pattern of calling it in a loop was. Added
`findSegmentIndex` (returns the segment's position in `p.segments`, not just the segment itself) since
`ReadBatch` needs to advance to `segIdx+1` on segment rollover; `findSegment` is now a thin wrapper over it.

`ReadBatch` preserves `DiskLog.Read`'s exact existing observable contract, which mattered as much as the speed
fix itself: `DiskLog.Read` has never returned a non-nil error, even for an out-of-range offset - any failure
just meant "return whatever was collected so far," because Fetch's long-polling logic treats "nothing new yet"
as an empty response, not a failure. `ReadBatch` replicates this exactly (offset unreachable, or a mid-scan
read failure, both resolve to empty bytes and a nil error) - this is a pure speed fix, verified byte-for-byte
against the old behavior by every existing `DiskLog.Read` test passing unchanged.

Verified against a real broker, reproducing the original bug's exact scenario: producing enough small (128-byte)
records to fill ~1MB, then fetching with franz-go's real default fetch size. The same ~8000-record/~1MB Fetch
request that used to take ~5.68s now completes in roughly 200ms - about a 28x improvement, matching what
eliminating the `indexEvery`-factor amplification predicts.

## 2026-09-11 - Retention (`cleanup.policy=delete`), and why it needed less than compaction did

Confirmed gap #3 in `docs/plan.md`: `DiskLog.EarliestOffset` was hardcoded to `0`, meaning every record ever
appended to any topic stayed on disk forever. Distinct from compaction (2026-09-07 entry): compaction keeps the
latest value per key for `__consumer_offsets` only; retention drops records past an age or size limit
regardless of key, for every topic.

Broker-wide, not per-topic: `CreateTopics` already decodes and discards per-topic configs (its own comment
anticipates "retention" as one), but nothing in this project actually needs different retention per topic, and
every other tunable here (`segmentMaxBytes`, `indexEvery`, every background ticker's interval) is already a
broker-wide constant. Real per-topic retention is a genuine future extension - the wire format already
anticipates it - just not one anything currently exercises.

Turned out to need much less new machinery than compaction did, for one structural reason: retention never
renumbers anything. Deleting old segments from the front of a partition just advances `EarliestOffset` and
leaves a real gap before it - which is normal, expected Kafka behavior, not a problem to route around the way
compaction had to route around this segment format's inability to represent gaps at all. `Partition.
ApplyRetention` (`internal/storage/partition.go`) is a straightforward linear scan from the front, oldest
segment first, deleting whole segments (reusing `removeSegmentGroupFiles`, built for compaction) while either
the segment is older than `maxAge` or the partition's total size still exceeds `maxBytes` even after
already-marked deletions - stopping at the first survivor, since segments are chronologically ordered by
construction (append-only), so nothing after a survivor could still need deleting either. The active segment is
never a candidate, the same protection `Compact` already gives it. Either `maxAge` or `maxBytes` can be `0` to
disable that check, matching real Kafka's own `retention.ms=-1`/`retention.bytes=-1` "unlimited" convention.

A segment's age needed an honest approximation, since this segment format tracks no "largest timestamp seen"
field: `Segment.ModTime()` reads the `.log` file's own filesystem mtime. A segment is immutable once rolled, so
its mtime already reflects exactly when its last record was written, for free - no new on-disk state, no
sparse-index approximation error. Real historical Kafka retention worked the same way.

`storage.Log`'s fifth deliberate extension: `ApplyRetention(topic, partition, maxAge, maxBytes, now)` -
`DiskLog` delegates to `Partition.ApplyRetention`; `FakeLog` gets a real equivalent (not a stub), operating on
individual entries instead of segments since it has no segment concept, each entry's simulated age tracked via
an `appendedAt` field set at `Append` time so tests can drive retention deterministically by passing a `now` far
in the future rather than needing a fake clock.

The one real correctness gap retention would otherwise open, fixed as part of this piece rather than deferred:
before this, *any* unreachable offset - "genuinely deleted by retention" and "just caught up, nothing new yet"
- resolved identically, to an empty response with no error (`Partition.ReadBatch`'s documented contract). Once
`EarliestOffset` can genuinely advance, that ambiguity stops being harmless - a consumer that fell behind past
the retention window would poll forever against data that will never arrive, with no signal telling it to
reseek. Added `ErrOffsetOutOfRange` (code 1, matching real Kafka) and a check in `HandleFetch`'s `fetchOne`: a
`fetchOffset` below the partition's current `EarliestOffset` returns the error immediately, no long-polling -
the same "don't wait for something that can't happen" shortcut already used for an unknown topic-partition.

Trigger is a background ticker (`runRetention` in `cmd/broker/main.go`), same shape as every other background
job this project runs - 5 minutes, matching real Kafka's own default `log.retention.check.interval.ms`.
`retentionMaxAge` defaults to 7 days (Kafka's real default); `retentionMaxBytes` defaults to `0` (disabled,
matching Kafka's real `-1`). The pure-function/ticker split (`applyRetention` walks the registry and is
independently unit-tested with fakes; `runRetention` is a thin wrapper) mirrors `collectMetrics`/
`runMetricsCollector` exactly.

Verified against a real running broker, with `segmentMaxBytes` and the retention interval/age both temporarily
shortened for the run (reverted before this was written up): produced 753 small records, forcing roughly 150
segment rolls. By the time of the check, retention had already swept everything but the active segment down to
3 files totaling under 150 bytes on disk - confirmed via `ListOffsets(-2)` reporting a real, non-zero
`EarliestOffset` (752, one before the log's actual end), a `Fetch` at offset 0 correctly returning
`error_code=1` (`OFFSET_OUT_OF_RANGE`), and a `Fetch` at the real earliest offset still succeeding normally.

## 2026-09-12 - Configurable advertised broker address (Phase 7 gap 5)

`cmd/broker/main.go` hardcoded `{NodeID: 1, Host: "localhost", Port: 9092}` into every `Metadata` and
`FindCoordinator` response, regardless of where the broker actually ran or what address a client would need to
reach it at. Real Kafka draws exactly this line between `listeners` (what the socket binds to) and
`advertised.listeners` (what clients are told to reconnect to) precisely because the two are not always the
same address - a container's internal bind address is rarely the address a client outside that container can
reach. `listenAddr` (`":9092"`) already binds every interface, so the only real gap was the advertised side
being a constant instead of configuration.

Added `brokerConfigFromEnv` (`cmd/broker/config.go`), a pure function reading three optional environment
variables - `KAFKA_NODE_ID`, `KAFKA_ADVERTISED_HOST`, `KAFKA_ADVERTISED_PORT` - each falling back to the exact
value that used to be hardcoded, so an unconfigured broker behaves identically to before. It takes a
`func(string) string` rather than calling `os.Getenv` directly, the same dependency-injection shape
`collectMetrics`/`applyRetention` already use for testability - tests pass a fake backed by a plain map,
`main` passes `os.Getenv`.

Chose env vars over a config file: a config file was explicitly deferred earlier this phase, and
`docs/plan.md`'s own gap description offered "env var or config file" as equally acceptable - env vars are
also what every containerized deployment target (Docker, a VPS's systemd unit, a Kubernetes manifest) already
sets without any extra file to mount.

A set-but-invalid value (`KAFKA_NODE_ID=abc`, `KAFKA_ADVERTISED_PORT=nope`) is a startup error
(`log.Fatalf`), not a silent fallback to the default - matching how `offsets.NewLogBackedStore`'s own error is
already handled in `main`. Silently defaulting on a parse failure would let a real typo in a deployment's
environment ship a broker quietly advertising `localhost` again, recreating the exact bug this change fixes
one layer further away, at the point where it's hardest to notice.

Verified against a real running broker: started with `KAFKA_ADVERTISED_HOST=203.0.113.10` and
`KAFKA_ADVERTISED_PORT=9999` set, still bound to `:9092` locally, then queried it with franz-go's
`kadm.ListBrokers` - the response reported `Host=203.0.113.10 Port=9999`, confirming the bind address and the
advertised address are now genuinely independent.
