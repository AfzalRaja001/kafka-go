package storage

import (
	"fmt"
	"sync"
	"time"
)

// FakeLog is an in-memory Log used to test protocol handlers without a real
// storage engine behind them. It satisfies Log implicitly - see Lesson 7.
type FakeLog struct {
	mu      sync.RWMutex
	batches map[logKey][]fakeBatch
}

// fakeBatch mirrors what DiskLog stores per append: the opaque bytes, the
// offset they start at, and how many offsets they span. Tracking the span
// here (rather than treating each append as exactly one offset) is what
// keeps FakeLog's offset arithmetic honest against DiskLog's - a handler
// test that passes here should mean the same thing on disk.
type fakeBatch struct {
	data       []byte
	baseOffset int64
	offsetSpan int64

	// appendedAt lets ApplyRetention simulate age without a real segment
	// file's mtime to read - set once, at Append time, never touched again.
	appendedAt time.Time
}

type logKey struct {
	topic     string
	partition int32
}

func NewFakeLog() *FakeLog {
	return &FakeLog{batches: make(map[logKey][]fakeBatch)}
}

func (f *FakeLog) Append(topic string, partition int32, batch []byte, recordCount int32) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Matches Partition.Append: every append advances the log by at least
	// one offset, so blobs can never share a base offset.
	if recordCount < 1 {
		recordCount = 1
	}

	key := logKey{topic, partition}
	baseOffset := f.endOffsetLocked(key)
	f.batches[key] = append(f.batches[key], fakeBatch{
		data:       batch,
		baseOffset: baseOffset,
		offsetSpan: int64(recordCount),
		appendedAt: time.Now(),
	})
	return baseOffset, nil
}

// endOffsetLocked returns the offset one past the last one in use. Callers
// must already hold f.mu.
func (f *FakeLog) endOffsetLocked(key logKey) int64 {
	entries := f.batches[key]
	if len(entries) == 0 {
		return 0
	}
	last := entries[len(entries)-1]
	return last.baseOffset + last.offsetSpan
}

// Read matches DiskLog's contract in two ways: it errors for a
// topic-partition nothing has ever been Appended to, rather than silently
// returning empty data (found missing here by a Fetch test that assumed both
// Log implementations agreed, and they didn't until that fix); and it
// returns whole batches, including the batch that merely *contains* the
// requested offset rather than starting at it.
func (f *FakeLog) Read(topic string, partition int32, offset int64, maxBytes int32) ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	key := logKey{topic, partition}
	entries, ok := f.batches[key]
	if !ok {
		return nil, fmt.Errorf("unknown topic-partition %s-%d", topic, partition)
	}

	var out []byte
	for _, b := range entries {
		if offset >= b.baseOffset+b.offsetSpan {
			continue // entirely before the requested offset
		}
		if len(out)+len(b.data) > int(maxBytes) {
			break
		}
		out = append(out, b.data...)
	}
	return out, nil
}

// EarliestOffset returns the oldest surviving entry's base offset - 0 until
// ApplyRetention has ever deleted anything, matching DiskLog/Partition's own
// EarliestOffset contract. An existing but genuinely empty topic-partition
// (CreatePartition'd, never Appended to) also reports 0, its natural
// starting offset.
func (f *FakeLog) EarliestOffset(topic string, partition int32) (int64, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	entries, ok := f.batches[logKey{topic, partition}]
	if !ok {
		return 0, fmt.Errorf("unknown topic-partition %s-%d", topic, partition)
	}
	if len(entries) == 0 {
		return 0, nil
	}
	return entries[0].baseOffset, nil
}

func (f *FakeLog) LatestOffset(topic string, partition int32) (int64, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	key := logKey{topic, partition}
	if _, ok := f.batches[key]; !ok {
		return 0, fmt.Errorf("unknown topic-partition %s-%d", topic, partition)
	}
	return f.endOffsetLocked(key), nil
}

// CreatePartition seeds an empty (non-nil-in-the-map) batch slice for the
// key, so Read/EarliestOffset/LatestOffset's existence check - which keys
// off whether the map entry is present at all, not whether it's empty -
// treats this topic-partition as existing from this point on.
func (f *FakeLog) CreatePartition(topic string, partition int32) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	key := logKey{topic, partition}
	if _, ok := f.batches[key]; !ok {
		f.batches[key] = []fakeBatch{}
	}
	return nil
}

func (f *FakeLog) DeletePartition(topic string, partition int32) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.batches, logKey{topic, partition})
	return nil
}

// Size sums payload bytes only - unlike DiskLog, which also counts the
// per-record on-disk header (see recordHeaderSize in segment.go). FakeLog
// was never meant to be byte-exact with the real segment file format (it
// has no segments at all), so this is fine for what FakeLog exists for:
// fast handler tests that care about relative size/existence, not exact
// on-disk footprint.
// Compact replaces every batch for a topic-partition with records, each
// becoming a fresh entry starting at offset 0 - see the Log interface's own
// doc comment for why this renumbers rather than preserving original
// offsets. The topic-partition must already exist (matches Read/
// EarliestOffset/LatestOffset's contract: Compact never fabricates storage
// for something that was never Appended to or CreatePartition'd).
func (f *FakeLog) Compact(topic string, partition int32, records [][]byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	key := logKey{topic, partition}
	if _, ok := f.batches[key]; !ok {
		return fmt.Errorf("unknown topic-partition %s-%d", topic, partition)
	}

	fresh := make([]fakeBatch, len(records))
	for i, data := range records {
		fresh[i] = fakeBatch{data: data, baseOffset: int64(i), offsetSpan: 1}
	}
	f.batches[key] = fresh
	return nil
}

// ApplyRetention is FakeLog's equivalent of Partition.ApplyRetention: no
// segments here, so it deletes individual entries from the front instead of
// whole segments, but the same rules apply - either maxAge or maxBytes can
// be 0 to disable that check, the newest entry is never a candidate
// (mirroring the active segment's protection), and the scan stops at the
// first surviving entry since age and total size are both monotonic front
// to back.
func (f *FakeLog) ApplyRetention(topic string, partition int32, maxAge time.Duration, maxBytes int64, now time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	entries, ok := f.batches[logKey{topic, partition}]
	if !ok || len(entries) == 0 {
		return nil
	}

	var totalSize int64
	for _, b := range entries {
		totalSize += int64(len(b.data))
	}

	survivorsFrom := 0
	for survivorsFrom < len(entries)-1 { // never consider the newest entry
		b := entries[survivorsFrom]

		expiredByAge := maxAge > 0 && now.Sub(b.appendedAt) > maxAge
		overBudget := maxBytes > 0 && totalSize > maxBytes
		if !expiredByAge && !overBudget {
			break
		}

		totalSize -= int64(len(b.data))
		survivorsFrom++
	}

	if survivorsFrom > 0 {
		f.batches[logKey{topic, partition}] = entries[survivorsFrom:]
	}
	return nil
}

func (f *FakeLog) Size(topic string, partition int32) (int64, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	key := logKey{topic, partition}
	entries, ok := f.batches[key]
	if !ok {
		return 0, fmt.Errorf("unknown topic-partition %s-%d", topic, partition)
	}

	var total int64
	for _, b := range entries {
		total += int64(len(b.data))
	}
	return total, nil
}
