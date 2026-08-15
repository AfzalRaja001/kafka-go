package storage

import (
	"fmt"
	"sync"
)

// FakeLog is an in-memory Log used to test protocol handlers without a real
// storage engine behind them. It satisfies Log implicitly - see Lesson 7.
type FakeLog struct {
	mu      sync.RWMutex
	batches map[logKey][][]byte
}

type logKey struct {
	topic     string
	partition int32
}

func NewFakeLog() *FakeLog {
	return &FakeLog{batches: make(map[logKey][][]byte)}
}

func (f *FakeLog) Append(topic string, partition int32, batch []byte) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	key := logKey{topic, partition}
	baseOffset := int64(len(f.batches[key]))
	f.batches[key] = append(f.batches[key], batch)
	return baseOffset, nil
}

// Read matches DiskLog's contract: it errors for a topic-partition nothing
// has ever been Appended to, rather than silently returning empty data -
// found missing here by a Fetch test that assumed both Log implementations
// agreed on this, and they didn't until this fix.
func (f *FakeLog) Read(topic string, partition int32, offset int64, maxBytes int32) ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	key := logKey{topic, partition}
	entries, ok := f.batches[key]
	if !ok {
		return nil, fmt.Errorf("unknown topic-partition %s-%d", topic, partition)
	}
	if offset < 0 || offset >= int64(len(entries)) {
		return nil, nil
	}

	var out []byte
	for _, b := range entries[offset:] {
		if len(out)+len(b) > int(maxBytes) {
			break
		}
		out = append(out, b...)
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
	entries, ok := f.batches[key]
	if !ok {
		return 0, fmt.Errorf("unknown topic-partition %s-%d", topic, partition)
	}
	return int64(len(entries)), nil
}
