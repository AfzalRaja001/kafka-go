package protocol

import (
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

// TopicRegistry is the broker's in-memory record of what topics exist.
// Guarded by a mutex since every Metadata request reads it, potentially
// from many connections at once, and a future CreateTopics handler will
// write to it.
type TopicRegistry struct {
	mu     sync.RWMutex
	topics map[string]*Topic
}

func NewTopicRegistry() *TopicRegistry {
	return &TopicRegistry{topics: make(map[string]*Topic)}
}

func (r *TopicRegistry) AddTopic(t *Topic) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.topics[t.Name] = t
}

func (r *TopicRegistry) RemoveTopic(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.topics, name)
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
	requestedTopics, err := dec.StringArray()
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
