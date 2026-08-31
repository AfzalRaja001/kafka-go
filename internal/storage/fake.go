package storage

import (
	"fmt"
	"sync"
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

func (f *FakeLog) EarliestOffset(topic string, partition int32) (int64, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if _, ok := f.batches[logKey{topic, partition}]; !ok {
		return 0, fmt.Errorf("unknown topic-partition %s-%d", topic, partition)
	}
	return 0, nil
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
