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

## 2026-08-16 - `ListOffsets` v0 resolves only the two timestamp sentinels

Kafka's `ListOffsets` asks "what offset holds the first record at or after timestamp T", where `-1` and `-2`
are reserved to mean latest and earliest. Real Kafka also resolves arbitrary timestamps by searching the
time index. We implement only the two sentinels and return an error for anything else, because those two
are what `seek_to_beginning()` / `seek_to_end()` send and therefore what unblocks real consumers. Arbitrary
timestamp lookup is deferred rather than faked - `Partition.LookupOffsetByTimestamp` already exists in the
storage layer, so wiring it up later is small, and returning a wrong answer now would be worse than
returning an honest "not implemented".
