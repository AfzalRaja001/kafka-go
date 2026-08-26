package group

import "sync"

// OffsetStore is where OffsetCommit/OffsetFetch persist and retrieve
// consumption progress - a completely different shape from storage.Log:
// (group, topic, partition) -> a single committed offset, not a byte-batch
// log. InMemoryOffsetStore is the first implementation; a second, backed by
// the __consumer_offsets internal topic, is the deliberate next step once
// the group rebalance machinery exists to actually exercise commit-then-
// resume end to end.
type OffsetStore interface {
	// Commit returns an error so implementations that can genuinely fail
	// (a future disk-backed one) fit the same interface as this one, which
	// can't.
	Commit(group, topic string, partition int32, offset int64, metadata string) error

	// Fetch reports found=false rather than an error when nothing has ever
	// been committed for this key - that's the normal case for a consumer
	// group that hasn't gotten this far yet, not a failure.
	Fetch(group, topic string, partition int32) (offset int64, metadata string, found bool)

	// FetchAll returns every topic-partition this group has ever committed
	// an offset for, in no particular order. This answers OffsetFetch v2's
	// null-topics-array request ("fetch offsets for all topics") - the
	// thing kafka-python's KafkaAdminClient.list_group_offsets() actually
	// sends, per the OffsetStore design entry in docs/decisions.md.
	FetchAll(group string) []GroupOffset
}

// GroupOffset is one entry in a FetchAll result - the same fields Fetch
// returns for a single key, plus the topic and partition that key was for,
// since FetchAll doesn't get to assume the caller already knows them.
type GroupOffset struct {
	Topic     string
	Partition int32
	Offset    int64
	Metadata  string
}

type offsetKey struct {
	group     string
	topic     string
	partition int32
}

type committedOffset struct {
	offset   int64
	metadata string
}

type InMemoryOffsetStore struct {
	mu      sync.RWMutex
	offsets map[offsetKey]committedOffset
}

var _ OffsetStore = (*InMemoryOffsetStore)(nil)

func NewInMemoryOffsetStore() *InMemoryOffsetStore {
	return &InMemoryOffsetStore{offsets: make(map[offsetKey]committedOffset)}
}

func (s *InMemoryOffsetStore) Commit(group, topic string, partition int32, offset int64, metadata string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.offsets[offsetKey{group, topic, partition}] = committedOffset{offset: offset, metadata: metadata}
	return nil
}

func (s *InMemoryOffsetStore) Fetch(group, topic string, partition int32) (int64, string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.offsets[offsetKey{group, topic, partition}]
	if !ok {
		return 0, "", false
	}
	return c.offset, c.metadata, true
}

func (s *InMemoryOffsetStore) FetchAll(group string) []GroupOffset {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []GroupOffset
	for key, c := range s.offsets {
		if key.group != group {
			continue
		}
		out = append(out, GroupOffset{Topic: key.topic, Partition: key.partition, Offset: c.offset, Metadata: c.metadata})
	}
	return out
}
