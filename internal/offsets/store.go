// Package offsets provides a persistent group.OffsetStore, backed by a real
// storage.Log-managed internal topic instead of a plain in-memory map. It
// depends on both internal/group (for the OffsetStore interface it
// implements) and internal/storage (for Log) - a dependency internal/group
// itself deliberately never takes on, keeping the project's one-way
// dependency fan-out (broker -> protocol -> {storage, group}) intact by
// putting this seam in its own package instead of inside group.
package offsets

import (
	"fmt"
	"sync"

	"github.com/AfzalRaja001/kafka-go/internal/group"
	"github.com/AfzalRaja001/kafka-go/internal/storage"
)

const (
	// topicName mirrors real Kafka's name for this internal topic. It's
	// never registered with protocol.TopicRegistry, so it never appears in
	// a Metadata response or ListTopics - nothing outside this broker is
	// meant to know it exists, let alone read or write it directly.
	topicName = "__consumer_offsets"

	// partitionID is always 0: real Kafka spreads this topic across many
	// partitions purely for horizontal scalability across multiple
	// brokers, which a single-node broker has no use for.
	partitionID = int32(0)

	// replayReadBytes bounds each storage.Log.Read call made while
	// replaying at startup. It doesn't bound how much can be replayed
	// overall - the replay loop keeps calling Read, advancing by however
	// many records came back, until it catches up to the log's latest
	// offset.
	replayReadBytes = 1 << 20
)

type offsetKey struct {
	group     string
	topic     string
	partition int32
}

type committedOffset struct {
	offset   int64
	metadata string
}

// LogBackedStore is an OffsetStore whose commits survive a broker restart,
// unlike group.InMemoryOffsetStore. Every Commit is appended to
// __consumer_offsets before being reflected in the in-memory index Fetch
// reads from - so a crash between those two steps can only lose a commit
// that never made it to disk, never one that already had.
type LogBackedStore struct {
	mu     sync.RWMutex
	log    storage.Log
	latest map[offsetKey]committedOffset
}

var _ group.OffsetStore = (*LogBackedStore)(nil)

// NewLogBackedStore provisions __consumer_offsets if it doesn't already
// exist, then replays every record already in it (from a previous run, if
// any) into an in-memory index before returning - so Fetch never touches
// disk, only Commit and this one-time replay do. Returns an error if the
// existing data can't be decoded, rather than silently skipping it: this
// topic's whole purpose is to survive a restart correctly, so failing loudly
// on data it can't make sense of is safer than starting up with a partial
// or wrong picture of what every group has committed.
func NewLogBackedStore(log storage.Log) (*LogBackedStore, error) {
	if err := log.CreatePartition(topicName, partitionID); err != nil {
		return nil, fmt.Errorf("provision %s: %w", topicName, err)
	}

	s := &LogBackedStore{log: log, latest: make(map[offsetKey]committedOffset)}
	if err := s.replay(); err != nil {
		return nil, fmt.Errorf("replay %s: %w", topicName, err)
	}
	return s, nil
}

func (s *LogBackedStore) replay() error {
	latestOffset, err := s.log.LatestOffset(topicName, partitionID)
	if err != nil {
		return err
	}

	for offset := int64(0); offset < latestOffset; {
		data, err := s.log.Read(topicName, partitionID, offset, replayReadBytes)
		if err != nil {
			return err
		}
		if len(data) == 0 {
			return fmt.Errorf("stalled at offset %d before reaching latest offset %d", offset, latestOffset)
		}

		for len(data) > 0 {
			rec, consumed, err := decodeCommit(data)
			if err != nil {
				return fmt.Errorf("record at offset %d: %w", offset, err)
			}
			s.applyLocked(rec)
			data = data[consumed:]
			offset++
		}
	}
	return nil
}

// applyLocked folds one record into the in-memory index. Called both from
// replay (before any other goroutine can see s) and from Commit (under
// s.mu) - the name flags that the caller is responsible for synchronization,
// the same convention DiskLog's *Locked helpers use.
func (s *LogBackedStore) applyLocked(rec commitRecord) {
	key := offsetKey{group: rec.Group, topic: rec.Topic, partition: rec.Partition}
	s.latest[key] = committedOffset{offset: rec.Offset, metadata: rec.Metadata}
}

func (s *LogBackedStore) Commit(group, topic string, partition int32, offset int64, metadata string) error {
	rec := commitRecord{Group: group, Topic: topic, Partition: partition, Offset: offset, Metadata: metadata}

	if _, err := s.log.Append(topicName, partitionID, encodeCommit(rec), 1); err != nil {
		return err
	}

	s.mu.Lock()
	s.applyLocked(rec)
	s.mu.Unlock()
	return nil
}

func (s *LogBackedStore) Fetch(group, topic string, partition int32) (int64, string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	c, ok := s.latest[offsetKey{group: group, topic: topic, partition: partition}]
	if !ok {
		return 0, "", false
	}
	return c.offset, c.metadata, true
}

func (s *LogBackedStore) FetchAll(groupID string) []group.GroupOffset {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []group.GroupOffset
	for key, c := range s.latest {
		if key.group != groupID {
			continue
		}
		out = append(out, group.GroupOffset{Topic: key.topic, Partition: key.partition, Offset: c.offset, Metadata: c.metadata})
	}
	return out
}
