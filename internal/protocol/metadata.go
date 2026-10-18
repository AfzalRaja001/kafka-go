package protocol

import (
	"errors"
	"fmt"
	"sync"
)

type Broker struct {
	NodeID int32
	Host   string
	Port   int32
}

type PartitionMetadata struct {
	ID       int32
	Leader   int32
	Replicas []int32
	ISR      []int32
}

type Topic struct {
	Name       string
	Partitions []PartitionMetadata
}

// TopicRegistry is the broker's record of what topics exist. Guarded by a
// mutex since every Metadata request reads it, potentially from many
// connections at once, and CreateTopics/DeleteTopics write to it.
//
// A registry made with NewTopicRegistry lives only in memory. One made with
// OpenTopicRegistry also keeps a topics file (see topicstore.go) in step
// with every change, so the topic list survives a restart: the partition
// directories on disk say what data exists, but nothing on disk used to say
// which of it was a topic, so a restart left Metadata empty and every client
// unable to reach data that was still there.
type TopicRegistry struct {
	mu     sync.RWMutex
	topics map[string]*Topic

	// nodeID is the broker ID every partition reports as its leader,
	// replica and ISR member. It comes from the broker's configuration, so
	// it always matches the NodeID Metadata advertises for the broker
	// itself - a leader that is not in the broker list is a leader no
	// client can connect to.
	nodeID int32

	// path is the topics file, or "" for a registry that is memory-only.
	path string

	// maxPartitions caps the partitions across every topic together; 0 means
	// no cap. MaxPartitionsPerTopic only bounds one request, so without this
	// a client could create topics one after another until the broker ran
	// out of file handles or disk. See SetMaxPartitions.
	maxPartitions int32
}

// ErrTooManyPartitions is returned by AddTopic when registering the topic
// would take the broker past its total partition cap.
var ErrTooManyPartitions = errors.New("adding the topic would exceed the broker's total partition limit")

// NewTopicRegistry returns an empty, memory-only registry for node 1.
func NewTopicRegistry() *TopicRegistry {
	return &TopicRegistry{topics: make(map[string]*Topic), nodeID: 1}
}

// OpenTopicRegistry returns a registry backed by the topics file at path,
// pre-loaded with whatever topics that file lists. A missing file is a
// broker that has never created a topic and yields an empty registry; a
// file that exists but cannot be read or validated is an error, because
// starting empty instead would silently disown every existing topic.
func OpenTopicRegistry(path string, nodeID int32) (*TopicRegistry, error) {
	records, err := loadTopicsFile(path)
	if err != nil {
		return nil, fmt.Errorf("topics file %s: %w", path, err)
	}

	r := &TopicRegistry{topics: make(map[string]*Topic, len(records)), nodeID: nodeID, path: path}
	for _, rec := range records {
		r.topics[rec.Name] = r.NewTopic(rec.Name, rec.Partitions)
	}
	return r, nil
}

// NewTopic builds a Topic with numPartitions partitions, each led by this
// registry's node. It does not register the topic; pass the result to
// AddTopic. Callers must have validated numPartitions already.
func (r *TopicRegistry) NewTopic(name string, numPartitions int32) *Topic {
	partitions := make([]PartitionMetadata, numPartitions)
	for i := range partitions {
		partitions[i] = PartitionMetadata{
			ID:       int32(i),
			Leader:   r.nodeID,
			Replicas: []int32{r.nodeID},
			ISR:      []int32{r.nodeID},
		}
	}
	return &Topic{Name: name, Partitions: partitions}
}

// SetMaxPartitions sets the cap on partitions across all topics together.
// Zero or a negative number means no cap, which is also the default. The cap
// only limits topics added afterwards: topics already registered (loaded
// from the topics file, say) are never disowned because the cap was lowered.
func (r *TopicRegistry) SetMaxPartitions(n int32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n < 0 {
		n = 0
	}
	r.maxPartitions = n
}

// partitionCountLocked returns the partitions across all topics, not
// counting the topic called except (pass "" to count them all). The caller
// must hold r.mu.
func (r *TopicRegistry) partitionCountLocked(except string) int64 {
	var total int64
	for name, t := range r.topics {
		if name != except {
			total += int64(len(t.Partitions))
		}
	}
	return total
}

// HasRoomFor reports whether a new topic of n partitions fits under the
// total cap right now. It is a fast path so a refused create can be turned
// away before any storage is made; the answer can be stale by the time the
// caller acts on it, so AddTopic repeats the check under the lock and is the
// one that decides.
func (r *TopicRegistry) HasRoomFor(n int32) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.maxPartitions == 0 || r.partitionCountLocked("")+int64(n) <= int64(r.maxPartitions)
}

// AddTopic registers t, replacing any topic of the same name, and saves the
// topic list if this registry is file-backed. If saving fails the change is
// undone in memory and the error returned: memory never runs ahead of disk,
// so a topic that was reported as not created cannot show up in Metadata
// only to vanish at the next restart.
//
// If a total partition cap is set (SetMaxPartitions) and t would take the
// registry past it, nothing changes and ErrTooManyPartitions is returned.
// When t replaces a topic of the same name, only the difference counts.
func (r *TopicRegistry) AddTopic(t *Topic) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.maxPartitions != 0 && r.partitionCountLocked(t.Name)+int64(len(t.Partitions)) > int64(r.maxPartitions) {
		return ErrTooManyPartitions
	}

	previous, existed := r.topics[t.Name]
	r.topics[t.Name] = t
	if err := r.persistLocked(); err != nil {
		if existed {
			r.topics[t.Name] = previous
		} else {
			delete(r.topics, t.Name)
		}
		return err
	}
	return nil
}

// RemoveTopic unregisters name and saves the topic list if this registry is
// file-backed, undoing the removal in memory if the save fails. Removing a
// topic that does not exist is not an error.
func (r *TopicRegistry) RemoveTopic(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	previous, existed := r.topics[name]
	if !existed {
		return nil
	}
	delete(r.topics, name)
	if err := r.persistLocked(); err != nil {
		r.topics[name] = previous
		return err
	}
	return nil
}

// persistLocked writes the current topic list to the topics file. The
// caller must hold r.mu for writing, which is also what serialises
// concurrent writers - two interleaved file writes could otherwise leave an
// older list on disk than the one memory holds.
func (r *TopicRegistry) persistLocked() error {
	if r.path == "" {
		return nil
	}
	records := make([]topicRecord, 0, len(r.topics))
	for _, t := range r.topics {
		records = append(records, topicRecord{Name: t.Name, Partitions: int32(len(t.Partitions))})
	}
	return saveTopicsFile(r.path, records)
}

func (r *TopicRegistry) Get(name string) (*Topic, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.topics[name]
	return t, ok
}

func (r *TopicRegistry) All() []*Topic {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]*Topic, 0, len(r.topics))
	for _, t := range r.topics {
		result = append(result, t)
	}
	return result
}

// HandleMetadata builds a Metadata v1 response body. requestBody is
// whatever bytes remain after the shared request header has already been
// decoded - here, just a topics array (a null array, encoded as count -1,
// means "describe every topic this broker knows about").
//
// v1 (rather than v0) is what this broker speaks because a real
// CreateTopics/DeleteTopics client needs ControllerId to know which broker
// to send those requests to - kafka-python's AdminClient reads it straight
// out of Metadata and fails with NodeNotReadyError without it. Broker.Rack
// and Topic.IsInternal are the two other fields v1 adds; this broker has no
// rack concept and no internal topics, so both are always encoded as their
// "not applicable" value (null, false) rather than becoming new broker
// state to track.
func HandleMetadata(correlationID int32, requestBody []byte, registry *TopicRegistry, brokers []Broker) ([]byte, error) {
	dec := NewDecoder(requestBody)
	requestedTopics, err := dec.NullableStringArray()
	if err != nil {
		return nil, fmt.Errorf("metadata request: %w", err)
	}

	enc := NewEncoder()
	enc.Int32(correlationID)

	enc.Int32(int32(len(brokers)))
	for _, b := range brokers {
		enc.Int32(b.NodeID)
		enc.String(b.Host)
		enc.Int32(b.Port)
		enc.NullableString(nil) // rack: not modeled on a single-node broker
	}

	// The controller is whichever broker accepts CreateTopics/DeleteTopics.
	// On this single-node broker that's always the one broker there is; -1
	// (real Kafka's "no controller" sentinel) only if brokers is somehow
	// empty, which never happens in practice but shouldn't crash if it did.
	controllerID := int32(-1)
	if len(brokers) > 0 {
		controllerID = brokers[0].NodeID
	}
	enc.Int32(controllerID)

	names := requestedTopics
	if names == nil {
		for _, t := range registry.All() {
			names = append(names, t.Name)
		}
	}

	enc.Int32(int32(len(names)))
	for _, name := range names {
		topic, found := registry.Get(name)
		if !found {
			enc.Int16(ErrUnknownTopicOrPartition)
			enc.String(name)
			enc.Bool(false) // is_internal
			enc.Int32(0)
			continue
		}

		enc.Int16(ErrNone)
		enc.String(topic.Name)
		enc.Bool(false) // is_internal: no internal topics exist yet
		enc.Int32(int32(len(topic.Partitions)))
		for _, p := range topic.Partitions {
			enc.Int16(ErrNone)
			enc.Int32(p.ID)
			enc.Int32(p.Leader)

			enc.Int32(int32(len(p.Replicas)))
			for _, r := range p.Replicas {
				enc.Int32(r)
			}
			enc.Int32(int32(len(p.ISR)))
			for _, i := range p.ISR {
				enc.Int32(i)
			}
		}
	}
	return enc.Result(), nil
}
