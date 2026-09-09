# kafka-go - build plan

Reconstructed 2026-09-09 from the original plan (partially recovered), the
full contents of `docs/decisions.md` and `docs/issues.md`, and a direct audit
of the code as it actually stands. **This file is the source of truth for
what is done and what is left.** The original plan lived outside the repo and
was lost; this one lives in the repo so that cannot happen again.

Related docs: `docs/decisions.md` (why each non-obvious choice was made),
`docs/issues.md` (every real bug, with root cause), `docs/conventions.md`
(git identities, verification commands, branching rules).

---

## The project, in one paragraph

A Kafka-wire-protocol-compatible message broker written from scratch in Go.
The differentiator is wire compatibility: official Kafka clients
(`kafka-python`, `franz-go`, `kcat`) connect to it unmodified, which is what
separates this from the many "Kafka clone" repos that stop at an in-memory
queue behind a JSON API. Single-node broker first (Phases 0-5), then
replication (Phase 6) as a gated stretch goal.

Two-person project on paper, split into **Track A** (protocol/networking:
`internal/protocol`, `internal/broker`) and **Track B** (storage/state:
`internal/storage`, `internal/group`, `internal/offsets`). In practice both
tracks are built together rather than one person per track - see the
2026-08-07 entry in `docs/decisions.md`. The track labels still matter: they
mark the seam where the `Log` interface decouples protocol from storage, and
they are useful framing when describing the project.

---

## Status at a glance

| Phase | Scope | Status |
|---|---|---|
| 0 | Go ramp-up | Done |
| 1 | Protocol: framing, codecs, ApiVersions, Metadata | Done |
| 2 | Storage engine: segments, indexes, partitions, recovery | Done, **1 gap** (no fsync policy) |
| 3 | Produce, Fetch, ListOffsets, CreateTopics, DeleteTopics | Done, **1 gap** (`acks` ignored) |
| 4 | Consumer groups + `__consumer_offsets` | Done |
| 5 | Production polish | **Mostly done - 3 items left** |
| 7 | Deploy | Partially done, **blocks v1.0** |
| 6 | Replication (stretch) | Not started, gated |

26 PRs merged, one open (#28, the `Partition.ReadBatch` fetch performance
fix). All of `go build ./... && go vet ./... && go test -race ./...` passes,
and CI (`.github/workflows/ci.yml`) runs exactly that on every push and PR.

---

## What is actually built (Phases 0-4)

### Phase 1 - protocol layer

`internal/protocol` and `internal/broker`. Length-prefixed binary framing,
primitive codecs (`int8/16/32/64`, `string`, `nullable_string`, `bytes`,
arrays - all big-endian, all table-driven tested), the shared request header,
and a single `dispatch()` funnel routing by `api_key` to 14 handlers.

Deliberately conservative API versions are advertised, one version per API,
to stay entirely clear of KIP-482 flexible/compact encoding:

| API | key | version |
|---|---|---|
| Produce | 0 | 3 |
| Fetch | 1 | 0 |
| ListOffsets | 2 | 1 |
| Metadata | 3 | 1 |
| OffsetCommit | 8 | 0 |
| OffsetFetch | 9 | 2 |
| FindCoordinator | 10 | 0 |
| JoinGroup | 11 | 0 |
| Heartbeat | 12 | 0 |
| LeaveGroup | 13 | 0 |
| SyncGroup | 14 | 0 |
| ApiVersions | 18 | 0 |
| CreateTopics | 19 | 0 |
| DeleteTopics | 20 | 0 |

Three of these were bumped off v0 only when a real client proved v0 was not
enough: `Metadata` v0 to v1 (`kafka-python`'s admin client needs
`ControllerId`), `OffsetFetch` v0 to v2 (null topics array = "everything this
group committed"), `ListOffsets` v0 to v1 (v0's array-shaped response has no
scalar offset field, which franz-go's `kadm` reads - this one produced a
silent `-1` with no error, see the 2026-09-07 decisions entry).

### Phase 2 - storage engine

`internal/storage`. Append-only `Segment` files named by base offset, sparse
`Index` (relative offset to byte position) and `Timeindex` (timestamp to
relative offset), `Partition` owning an ordered segment list with rolling,
and `DiskLog` mapping `(topic, partition)` to partitions on disk. `FakeLog`
is the in-memory test double, held to the same contract (see issue 2).

Crash recovery works: `Segment.Recover()` scans forward on open and truncates
at the first torn write. On-disk blobs carry an 8-byte header (payload length
plus offset span) so a restart can rebuild `nextOffset` from the segment file
alone, with no separate bookkeeping file.

The `Log` interface was deliberately frozen and has been extended exactly
four times, each with a documented reason: `recordCount` on `Append`,
`CreatePartition`/`DeletePartition`, `Size`, and `Compact`.

Record batches are stored **verbatim**. Only the 61-byte batch header is
parsed, to rewrite `baseOffset` and recompute the CRC-32C. This is the single
most important design decision in the project and everything else bends to
preserve it.

### Phase 3 - produce and fetch

`Produce` assigns offsets and rewrites batch base offsets. `Fetch` implements
real long-polling: each requested partition is polled independently up to
`max_wait_time_ms` until `min_bytes` is available (`internal/protocol/fetch.go`),
so consumers do not hot-loop. `ListOffsets` resolves the `-1`/`-2` timestamp
sentinels. `CreateTopics`/`DeleteTopics` provision and remove storage for
real, not registry-only.

Verified end to end with `kafka-python` and `franz-go` throughout.

### Phase 4 - consumer groups

`internal/group` (coordinator + state machine, zero dependencies on anything
else in the project) and `internal/offsets` (the `__consumer_offsets`-backed
persistent offset store).

The full rebalance flow works: `FindCoordinator`, `JoinGroup` (with a
broker-side initial rebalance delay, not the client's session timeout - see
issue 11), `SyncGroup` (leader-computed assignments relayed as opaque bytes,
never parsed), `Heartbeat` with a background reaper evicting silent members,
and `LeaveGroup`. Rebalance waits use channel-close broadcasts rather than
`sync.Cond`.

Offsets went through the planned two-step: an in-memory store first, then
`LogBackedStore` persisting every commit to a real internal
`__consumer_offsets` topic that replays into an in-memory index on startup.

### Phase 5 - what shipped

- **Prometheus metrics** (`internal/metrics`): request counts, per-API latency
  histograms, bytes in/out, partition sizes, consumer group lag. Served on
  `:9101/metrics`. Own registry, no global state.
- **Grafana dashboard** (`deploy/`): fully auto-provisioned - datasource and
  dashboard both load from files, no manual import. Six panels.
- **Docker**: multi-stage build to `distroless/static-debian12`, broker joins
  `docker-compose.yml` as a third service alongside Prometheus and Grafana.
- **Benchmark** (`cmd/benchmark`): concurrent Produce/Fetch load via franz-go,
  reporting records/sec, MB/sec, and p50/p95/p99 latency.
- **Log compaction**, but internal-only: `__consumer_offsets` now gets real
  space reclamation via `Log.Compact`. This is **not** general
  `cleanup.policy=compact` for client topics - see the gap list below.

Plus one performance fix that came out of the benchmark rather than the plan
(PR #28, open at the time of writing): `DiskLog.Read` was restarting its
sparse-index lookup and forward scan for every single blob, making a full
fetch O(n * indexEvery) instead of O(n). `Partition.ReadBatch` does one
lookup and scans through, which took a real ~1MB fetch of small records from
~5.68s down to ~200ms.

---

## Confirmed gaps in "finished" phases

These were found by auditing the code on 2026-09-09, not assumed. Each is
small, real, and worth closing before calling the single-node broker done.

### 1. No fsync policy at all (Phase 2)

`Segment.Sync()` exists and is correct, but **nothing in the production code
path ever calls it** - only tests do. Every write currently sits in the OS
page cache until the OS decides to flush it, so a machine-level crash (not
just a process crash) can lose acknowledged writes.

The original plan called for exactly this: flush on a configurable policy
(every N messages or every N milliseconds), not on every append, and document
the durability/throughput tradeoff. That tradeoff is a strong interview
talking point and right now the project cannot make the claim at all.

**Work:** a flush policy on `Partition` (count-based and/or time-based),
wired through `DiskLog`. Small. Pairs naturally with the config file.

### 2. `acks` is decoded but ignored (Phase 3)

`ProduceRequest.Acks` is parsed off the wire and then never read by anything.
The broker always writes and always responds, so `acks=0` ("fire and forget,
send no response") behaves identically to `acks=1`. A client sending `acks=0`
gets a response it is not expecting on the connection.

**Work:** honor `acks=0` by suppressing the response in `dispatch`. Needs a
way for a handler to signal "no reply" back up to the connection loop.
Small, but touches the dispatch contract, so worth doing deliberately.

### 3. Retention is not implemented (Phase 5)

Nothing ever deletes old segments. `DiskLog.EarliestOffset` is hardcoded to
return `0`, with a comment saying as much. Compaction (shipped) and retention
(not shipped) are two different real Kafka policies: compaction keeps the
latest value per key, retention drops records past an age or size limit
regardless of key.

**Work:** a background goroutine deleting whole segments older than
`retention.ms` or beyond `retention.bytes`, and making `EarliestOffset`
report the real first surviving offset. The compaction PR already built the
"close handles, delete segment files, with Windows retry" machinery this
needs.

### 4. No general per-topic compaction (Phase 5, deliberate)

`Log.Compact` renumbers offsets from 0 and is documented as safe only for a
log that is never fetched by offset - true of `__consumer_offsets` and
nothing else. Real per-topic `cleanup.policy=compact` needs per-record keys
inside batches, which means decomposing and re-encoding record batches, which
breaks the store-verbatim principle, plus an on-disk format change to
represent offset gaps.

This was scoped out on purpose (2026-09-07 decisions entry). Leaving it out
is defensible; it belongs in the README's "what I'd do differently" section
rather than in the build queue.

### 5. Broker advertises a hardcoded `localhost` (Phase 7)

`cmd/broker/main.go` hardcodes `{NodeID: 1, Host: "localhost", Port: 9092}`
in its `Metadata` response. This is precisely the `advertised.listeners` trap
the original plan warned about: it works locally and breaks the moment the
broker runs anywhere a client is not also running. **This blocks any real
deployment** and therefore blocks `v1.0-singlenode`.

### 6. No config file (Phase 5)

Every tunable is a compile-time constant in `cmd/broker/main.go`: ports, data
directory, segment size, index interval, all four background intervals.
Explicitly deferred by request, but note that gaps 1, 3, and 5 all want
config knobs, so it may be cheaper to do the config file alongside them than
to hardcode three more constants first.

### 7. No integration test suite

Every real-client verification so far has been manual and one-off. There is
no `test/integration/` package, so nothing in CI catches a protocol
regression against a real client - only unit tests run. The `ListOffsets`
v0/v1 bug is exactly the class of failure an integration test would have
caught immediately.

### 8. README is a stub, and there are no release artifacts

`README.md` is nine lines. No architecture diagram, no Grafana screenshot, no
demo GIF, no benchmark numbers, no "what I'd do differently". There are also
no git tags, so there is no shippable marker anywhere in the history. For a
project whose entire purpose is to be evaluated by someone else, this is
currently the single highest-value-per-hour gap in the whole repo.

---

## Remaining work, in the order to do it

The original plan numbered Deploy as Phase 7 but gated Phase 6 behind "Phase
5 deployed and demoable". That is contradictory as written. **Resolved here:
deployment happens before replication regardless of its number.** The point
of the gate is that a working, deployed, tagged single-node broker exists
before any distributed work starts.

### Step 1 - close the single-node gaps (~10-14 hrs)

Priority order within the step:

1. **README + artifacts** (~4 hrs, Track A or either). Architecture diagram,
   Grafana screenshot, 60-second demo GIF of official Kafka tooling against
   the broker, real benchmark numbers from `cmd/benchmark`, and a "what I'd
   do differently" section covering the deliberate scope cuts (no general
   compaction, no transactions, single-version APIs). Highest value per hour
   in the repo right now.
2. **fsync policy** (~2-3 hrs, Track B). Gap 1.
3. **Retention** (~4-5 hrs, Track B). Gap 3.
4. **`acks=0`** (~2 hrs, Track A). Gap 2.
5. **Config file** (~2-3 hrs, either) - optional, but do it *before* 2/3/5 if
   doing it at all, so those land as config instead of new constants.

**Exit criteria:** a stranger can clone the repo, run one command, produce
and consume with official Kafka tooling, and see a Grafana dashboard - all
from the README alone, without asking a question.

### Step 2 - deploy, then tag `v1.0-singlenode` (~6-8 hrs)

1. Fix gap 5: make the advertised host/port configurable (env var or config
   file), bind `0.0.0.0`, advertise the real reachable address.
2. Stand it up on a VPS. Oracle Cloud Always Free ARM (2 OCPU / 12GB since
   June 2026) is the best free option; Hetzner CX22 at ~EUR 4/mo is the
   zero-hassle alternative. Fly.io's free tier is gone for new accounts, and
   Render/Vercel-style PaaS cannot work at all - this is raw TCP, not HTTP.
3. Optionally expose the Grafana dashboard read-only and link it from the
   README.
4. **Tag `v1.0-singlenode`.** This is the gate. There must always be a
   shippable artifact from here on.

**Exit criteria:** `kcat -b <public-host>:9092 -L` works from a machine that
is not the broker's host, and the tag exists.

### Step 3 - integration test suite (~4-6 hrs)

Add `test/integration/` running against a real broker process with real
clients (franz-go is already a dependency from the benchmark tool, so this is
mostly harness work). Cover at minimum: produce/consume round trip, a
consumer group rebalance across 3 consumers on a 6-partition topic, offset
commit and resume after restart, and `ListOffsets`/`OffsetFetch` admin calls.
Wire it into CI behind a build tag so unit tests stay fast.

This can be done before or after Step 2, but before Phase 6 either way -
replication work will break protocol assumptions constantly, and manual
verification will not scale to catching that.

### Step 4 - Phase 6, replication (~50 hrs, gated)

**Do not start until `v1.0-singlenode` is tagged and deployed.**

Two genuinely independent problems, which is what makes this the phase where
the Track A / Track B split actually pays off:

**Track A - metadata consensus.** Which broker leads which partition, and
cluster membership. Use `hashicorp/raft`; do not hand-roll consensus.
Hand-rolling Raft is a 4-8 week project that would consume this entire phase,
and "I evaluated writing my own and chose a proven implementation because
correctness in consensus is unforgiving" is the better interview answer.
Lands in a new `internal/cluster/`.

**Track B - the data path.** Kafka does not use Raft for message data.
Followers replicate by being ordinary `Fetch` clients of the leader, which
means most of this is reuse of Phase 3:

- Followers issue `Fetch` with `replica_id >= 0` (consumers send `-1`). The
  field is already decoded and currently ignored.
- The leader tracks each follower's log-end offset.
- **High watermark** = the minimum log-end offset across the in-sync replica
  set. Consumers may only read up to the HW. `Fetch` already returns a
  high-watermark field; today it is just the log end offset.
- **ISR**: followers caught up within `replica.lag.time.max.ms`. A lagging
  follower is removed so the HW can advance again.
- **`acks=all`**: the leader waits for the HW to reach the write's offset
  before responding. Note this depends on gap 2 being fixed first - `acks`
  has to actually mean something before `acks=all` can.
- **Leader election** on failure, from Raft-committed metadata, choosing from
  the ISR.

Also needs: a multi-broker `docker-compose.yml` (currently one broker), and
per-broker node IDs and data directories.

**Exit criteria:** 3-broker cluster, replication factor 3. Kill the leader
mid-produce with `acks=all`. A follower takes over, the producer reconnects
transparently, zero messages lost - proven by counting produced vs consumed.

---

## Full-system verification (the end state)

```bash
docker compose -f deploy/docker-compose.yml up -d
kcat -b localhost:9092 -L
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders --partitions 6 --replication-factor 3
kafka-producer-perf-test.sh --topic orders --num-records 1000000 --record-size 100 --throughput -1 \
  --producer-props bootstrap.servers=localhost:9092 acks=all
docker compose kill broker-1   # kill the leader mid-run
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --from-beginning | wc -l   # must equal 1000000
```

Using Apache's own tooling against this broker is the whole point. Everything
up to the `replication-factor 3` line should work after Step 2.

---

## Explicitly out of scope

Write these in the README's "future work" section rather than building them.
Being able to say what was deliberately left out, and why, shows more
judgment than the code does.

- Transactions and exactly-once semantics
- Tiered storage
- General per-topic `cleanup.policy=compact` (gap 4 - real work, real
  format change, no current use case)
- Arbitrary-timestamp `ListOffsets` lookups (only the two sentinels resolve;
  `Partition.LookupOffsetByTimestamp` exists but is not wired through)
- KIP-482 flexible versions / newer API versions generally
- SASL/TLS
- Multi-version support per API (one version each, on purpose)

---

## Working conventions

Full detail in `docs/conventions.md`. The load-bearing ones:

- **Two GitHub accounts.** Track A commits and PRs as `AfzalRaja001`, Track B
  as `Susan5504R` (display name Sarah). The git identity and the `gh` CLI
  active account are separate settings and must both be switched.
- **Branch from what you depend on**, not reflexively from `main`. Always
  `git fetch` first.
- **Every piece gets:** design agreed up front, TDD (RED then GREEN), live
  verification against a real running broker (never trust tests alone), a
  `docs/decisions.md` entry for anything non-obvious, then a PR with CI green.
- **Bugs go in `docs/issues.md`** with symptom, root cause, fix, and how it
  surfaced.
