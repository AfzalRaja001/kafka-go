# Issues log

Every real bug or blocker hit while building this, with its root cause and
fix. Newest at the bottom.

Why this file exists: most of these took real debugging to find, and the
*reason* each one happened is more valuable than the diff that fixed it.
Several are also good interview material - "tell me about a bug you found"
is much easier to answer well from notes written the day it happened.

Format for each entry:

- **Symptom** - what was actually observed, not what it turned out to be
- **Root cause** - the real underlying reason
- **Fix** - what changed
- **Caught by** - how it surfaced; worth tracking, because it tells us which
  kinds of testing are actually earning their keep

---

## 1. `Segment.Truncate` fails on Windows during crash recovery

- **Symptom:** `Recover()` returned a permission error when truncating a
  segment after a simulated torn write. Only on Windows; the logic was
  correct on paper.
- **Root cause:** the segment's file handle is opened with `O_APPEND`, which
  on Windows grants append-only access. `File.Truncate` uses `SetEndOfFile`
  under the hood, which requires full write access the handle doesn't have.
- **Fix:** call `os.Truncate(s.file.Name(), pos)` instead, which opens its own
  handle with the access it needs. See the comment in `internal/storage/segment.go`.
- **Caught by:** the crash-recovery unit test, on the first run.

## 2. `FakeLog` and `DiskLog` disagreed on unknown topic-partitions

- **Symptom:** a Fetch handler test passed against `FakeLog` but the same
  scenario behaved differently against the real broker.
- **Root cause:** `DiskLog` deliberately errors when asked to read a
  topic-partition nothing was ever appended to (silently inventing empty
  storage would hide a real client bug). `FakeLog` returned empty data
  instead. Two implementations of one interface, two different contracts -
  so handler tests were validating behaviour the real broker didn't have.
- **Fix:** made all three `FakeLog` read-side methods (`Read`,
  `EarliestOffset`, `LatestOffset`) error on an unknown topic-partition,
  matching `DiskLog`. Added `TestFakeLog_ReadUnknownPartitionErrors` to pin
  it down.
- **Caught by:** writing Fetch's tests and noticing the fake made a case
  impossible to express.
- **Lesson:** a test double is only useful if it shares the real
  implementation's contract. Where they diverge, tests actively mislead.

## 3. `kafka-python` producer rejected the broker three times in a row

- **Symptom:** three different errors in sequence when first pointing a real
  client at the broker.
- **Root cause and fix, in order:**
  1. `api_version_auto_timeout_ms` isn't a recognised config in this client
     version - removed it.
  2. With no explicit `api_version`, the client negotiated down to Kafka's
     old message-set format and the broker returned `CorruptRecordError`.
     Fixed by pinning `api_version=(2, 5, 0)`, forcing magic-byte-2 record
     batches - the only format this broker parses.
  3. That alone triggered `IncompatibleBrokerVersion: InitProducerIdRequest`,
     because the idempotent producer is on by default and needs an API this
     broker doesn't implement. Fixed with `enable_idempotence=False`.
- **Caught by:** the first attempt to use a real client instead of
  hand-rolled test scripts.
- **Lesson:** hand-rolled test clients only prove the broker agrees with
  itself. Real clients exercise defaults and negotiation paths no synthetic
  test thinks to send.

## 4. Fetch encoded an empty records field as null instead of empty

- **Symptom:** consumer crashed with
  `TypeError: object of type 'NoneType' has no len()` inside kafka-python's
  `MemoryRecords.__init__`, but only once it had caught up and there was
  nothing new to read.
- **Root cause:** when a partition exists but has no records past the fetch
  offset, `HandleFetch` passed `nil` to `Encoder.Bytes`, which correctly
  encodes nil as null (wire length -1). But Fetch's `records` field must be
  present-but-empty (length 0) in that situation. A real client treats null
  there as an unexpected condition, not "caught up", and errors trying to
  build a batch reader from it.
- **Fix:** normalise `nil` to `[]byte{}` immediately before encoding, in
  `HandleFetch` only. Deliberately *not* a codec-wide change - treating nil
  as null is the correct general rule for most fields; this is a
  Fetch-specific exception. Regression test:
  `TestHandleFetch_EmptyResponseIsNotNull`.
- **Caught by:** a real kafka-python consumer, running past the end of the
  log. No unit test had covered "caught up" as distinct from "empty".

## 5. Multi-record batches advanced the log by only one offset

The most serious bug so far - a silent data-corruption path, not a crash.

- **Symptom:** produced 5 messages with a real client; all 5 stored and
  readable at offsets 0-4, but the broker reported the log ending at offset 1.
- **Root cause:** the whole storage engine assumed one appended blob consumed
  exactly one offset. True for every test written until then, because every
  test appended one record at a time. Real producers batch: kafka-python packs
  several records into a single record batch, so one append legitimately spans
  many offsets. Three places encoded the wrong assumption -
  `Partition.Append` (advanced `nextOffset` by 1), `FindRecord` (scanned
  forward one offset per blob), and `Segment.RecordCount` (counted blobs, and
  was used to restore `nextOffset` after a restart, so the error persisted
  across reboots).
- **Why it mattered more than a wrong number:** `LatestOffset` is what
  `Produce` peeks before assigning the next batch's `baseOffset`. A 5-record
  batch at offset 0 left `LatestOffset` reporting 1, so the *next* batch was
  assigned base offset 1 - colliding with offsets 1-4 the first batch already
  owned. Any producer batching more than one record per request would
  corrupt the log, which is the default behaviour of essentially every real
  client under load.
- **Fix:** taught the storage layer how many offsets a blob spans, without
  letting it parse Kafka's batch format:
  - `Log.Append` gained a `recordCount` parameter (the frozen interface's
    first deliberate change - see `docs/decisions.md`)
  - the segment's own record framing gained a 4-byte offset-span field, so
    spans survive restarts with no extra bookkeeping file
  - `FindRecord` now returns the batch *containing* a target offset plus the
    offset after it, instead of assuming one blob per offset
  - `Segment.Counts` replaced `RecordCount`, returning blob count and offset
    total separately, because `OpenPartition` genuinely needs both
- **Caught by:** noticing that `seek_to_end()` reported 1 when 5 messages had
  been produced, and not accepting the number at face value. The unit tests
  all passed both before and after, because every one of them appended single
  records.
- **Lesson:** the tests weren't wrong, they were *uniform*. Every test used
  the same shape of input, so they all shared one blind spot. Worth asking of
  any test suite: what does every test here have in common, and what does
  that hide?

## 6. Wrong GitHub account used for a Track B PR

- **Symptom:** PR #5 was opened under Afzal's account for work belonging to
  Track B.
- **Root cause:** git commit identity (`git config user.name/email`, local to
  the repo) and the `gh` CLI's active account (`gh auth switch -u`) are two
  completely independent settings. Only one had been switched.
- **Fix:** closed #5, reopened as #6 under the correct account. Both settings
  are now switched together as a pair - see `docs/conventions.md`.
- **Caught by:** review, after the fact.

## 7. Work branched from a stale base twice

- **Symptom:** twice, new work was built on a branch that didn't contain a
  dependency it needed, and once a `git checkout main && git pull` reported
  "Already up to date" while main had in fact moved.
- **Root cause:** branching from `main` when the work depended on an
  unmerged branch, and stale local refs making `pull` a no-op without
  `fetch`.
- **Fix:** the branch-basing rule in `docs/conventions.md` - always `git
  fetch` first, and branch from the branch you actually depend on. Recovered
  with a catch-up PR (#9) the first time, and a stash/fetch/ff-merge the
  second.
- **Caught by:** noticing files had reverted to older content mid-session.
