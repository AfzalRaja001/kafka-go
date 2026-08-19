package protocol

import (
	"fmt"

	"github.com/AfzalRaja001/kafka-go/internal/storage"
)

type CreateTopicsTopicRequest struct {
	Name              string
	NumPartitions     int32
	ReplicationFactor int16
}

type CreateTopicsRequest struct {
	Topics    []CreateTopicsTopicRequest
	TimeoutMs int32
}

// DecodeCreateTopicsRequest decodes a CreateTopics v0 request body.
// Assignments and Configs are decoded and discarded, the same treatment
// max_num_offsets gets in ListOffsets: real fields that must be consumed to
// keep the rest of the message correctly framed, but that don't change
// this broker's behavior. A single-node broker has exactly one place to
// put a replica, so a manual assignment can't say anything a default
// assignment doesn't already say, and per-topic configs (retention,
// compaction, etc.) aren't wired to anything yet.
func DecodeCreateTopicsRequest(buf []byte) (CreateTopicsRequest, error) {
	dec := NewDecoder(buf)

	topicCount, err := dec.Int32()
	if err != nil {
		return CreateTopicsRequest{}, fmt.Errorf("topic count: %w", err)
	}

	var topics []CreateTopicsTopicRequest
	for i := int32(0); i < topicCount; i++ {
		name, err := dec.String()
		if err != nil {
			return CreateTopicsRequest{}, fmt.Errorf("topic %d name: %w", i, err)
		}
		numPartitions, err := dec.Int32()
		if err != nil {
			return CreateTopicsRequest{}, fmt.Errorf("topic %d num_partitions: %w", i, err)
		}
		replicationFactor, err := dec.Int16()
		if err != nil {
			return CreateTopicsRequest{}, fmt.Errorf("topic %d replication_factor: %w", i, err)
		}

		assignmentCount, err := dec.Int32()
		if err != nil {
			return CreateTopicsRequest{}, fmt.Errorf("topic %d assignment count: %w", i, err)
		}
		for j := int32(0); j < assignmentCount; j++ {
			if _, err := dec.Int32(); err != nil { // partition_index
				return CreateTopicsRequest{}, fmt.Errorf("topic %d assignment %d partition_index: %w", i, j, err)
			}
			brokerCount, err := dec.Int32()
			if err != nil {
				return CreateTopicsRequest{}, fmt.Errorf("topic %d assignment %d broker count: %w", i, j, err)
			}
			for k := int32(0); k < brokerCount; k++ {
				if _, err := dec.Int32(); err != nil { // broker id
					return CreateTopicsRequest{}, fmt.Errorf("topic %d assignment %d broker %d: %w", i, j, k, err)
				}
			}
		}

		configCount, err := dec.Int32()
		if err != nil {
			return CreateTopicsRequest{}, fmt.Errorf("topic %d config count: %w", i, err)
		}
		for j := int32(0); j < configCount; j++ {
			if _, err := dec.String(); err != nil { // config name
				return CreateTopicsRequest{}, fmt.Errorf("topic %d config %d name: %w", i, j, err)
			}
			if _, err := dec.String(); err != nil { // config value
				return CreateTopicsRequest{}, fmt.Errorf("topic %d config %d value: %w", i, j, err)
			}
		}

		topics = append(topics, CreateTopicsTopicRequest{
			Name:              name,
			NumPartitions:     numPartitions,
			ReplicationFactor: replicationFactor,
		})
	}

	timeoutMs, err := dec.Int32()
	if err != nil {
		return CreateTopicsRequest{}, fmt.Errorf("timeout_ms: %w", err)
	}

	return CreateTopicsRequest{Topics: topics, TimeoutMs: timeoutMs}, nil
}

// HandleCreateTopics builds a CreateTopics v0 response body, provisioning
// each requested topic's partitions in both the registry (so Metadata sees
// it) and storage (so it's immediately Fetch/ListOffsets-able) before
// reporting success.
func HandleCreateTopics(correlationID int32, requestBody []byte, registry *TopicRegistry, log storage.Log) ([]byte, error) {
	req, err := DecodeCreateTopicsRequest(requestBody)
	if err != nil {
		return nil, fmt.Errorf("create_topics request: %w", err)
	}

	enc := NewEncoder()
	enc.Int32(correlationID)
	enc.Int32(int32(len(req.Topics)))

	for _, topic := range req.Topics {
		errorCode := createOneTopic(registry, log, topic)
		enc.String(topic.Name)
		enc.Int16(errorCode)
	}

	return enc.Result(), nil
}

// createOneTopic provisions a single requested topic, or reports why it
// couldn't. It only registers the topic after every partition is
// successfully created - a partial failure partway through leaves orphaned
// but harmless empty partition directories on disk rather than a topic
// that's registered without all its storage, which would be worse.
func createOneTopic(registry *TopicRegistry, log storage.Log, req CreateTopicsTopicRequest) int16 {
	if _, exists := registry.Get(req.Name); exists {
		return ErrTopicAlreadyExists
	}
	if req.NumPartitions < 1 {
		return ErrInvalidPartitions
	}

	partitions := make([]PartitionMetadata, req.NumPartitions)
	for i := int32(0); i < req.NumPartitions; i++ {
		if err := log.CreatePartition(req.Name, i); err != nil {
			return ErrUnknownServerError
		}
		partitions[i] = PartitionMetadata{ID: i, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}}
	}

	registry.AddTopic(&Topic{Name: req.Name, Partitions: partitions})
	return ErrNone
}
