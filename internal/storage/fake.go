package storage

import "sync"

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

func (f *FakeLog) Read(topic string, partition int32, offset int64, maxBytes int32) ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	key := logKey{topic, partition}
	entries := f.batches[key]
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
	return 0, nil
}

func (f *FakeLog) LatestOffset(topic string, partition int32) (int64, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	key := logKey{topic, partition}
	return int64(len(f.batches[key])), nil
}
