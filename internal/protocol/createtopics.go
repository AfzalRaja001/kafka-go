package protocol

import (
	"errors"
	"fmt"
	"log"

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

	topicCount, err := dec.NonNullArrayLen(16) // name + num_partitions + replication_factor + 2 array counts
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

		assignmentCount, err := dec.NonNullArrayLen(8) // partition_index + broker count
		if err != nil {
			return CreateTopicsRequest{}, fmt.Errorf("topic %d assignment count: %w", i, err)
		}
		for j := int32(0); j < assignmentCount; j++ {
			if _, err := dec.Int32(); err != nil { // partition_index
				return CreateTopicsRequest{}, fmt.Errorf("topic %d assignment %d partition_index: %w", i, j, err)
			}
			brokerCount, err := dec.NonNullArrayLen(4) // broker id
			if err != nil {
				return CreateTopicsRequest{}, fmt.Errorf("topic %d assignment %d broker count: %w", i, j, err)
			}
			for k := int32(0); k < brokerCount; k++ {
				if _, err := dec.Int32(); err != nil { // broker id
					return CreateTopicsRequest{}, fmt.Errorf("topic %d assignment %d broker %d: %w", i, j, k, err)
				}
			}
		}

		configCount, err := dec.NonNullArrayLen(4) // name length + value length
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
func HandleCreateTopics(correlationID int32, requestBody []byte, registry *TopicRegistry, diskLog storage.Log) ([]byte, error) {
	req, err := DecodeCreateTopicsRequest(requestBody)
	if err != nil {
		return nil, fmt.Errorf("create_topics request: %w", err)
	}

	enc := NewEncoder()
	enc.Int32(correlationID)
	enc.Int32(int32(len(req.Topics)))

	for _, topic := range req.Topics {
		errorCode := createOneTopic(registry, diskLog, topic)
		enc.String(topic.Name)
		enc.Int16(errorCode)
	}

	return enc.Result(), nil
}

// MaxPartitionsPerTopic caps num_partitions on a single CreateTopics topic.
// Real Kafka has no hard per-topic cap, but on this broker every partition
// is a directory plus three open file handles (.log, .index, .timeindex),
// so an uncapped request could exhaust the process's file-handle limit - a
// stock Linux shell allows only 1024 - or, at 2^31-1, crash the broker
// outright allocating the partition list. 1000 is generous for a single
// node: real Kafka's own guidance is a few thousand partitions per broker
// in total, across every topic.
const MaxPartitionsPerTopic = 1000

// createOneTopic provisions a single requested topic, or reports why it
// couldn't. It only registers the topic after every partition is
// successfully created - a partial failure partway through leaves orphaned
// but harmless empty partition directories on disk rather than a topic
// that's registered without all its storage, which would be worse.
//
// Registering also saves the topic list to disk, so the topic is only
// reported as created once it would survive a restart. If that save fails
// the create fails with it, leaving the same harmless orphaned directories.
func createOneTopic(registry *TopicRegistry, diskLog storage.Log, req CreateTopicsTopicRequest) int16 {
	// First, before anything touches the registry or the disk: the name
	// becomes part of a file path (see ValidateTopicName).
	if err := ValidateTopicName(req.Name); err != nil {
		return ErrInvalidTopic
	}
	if _, exists := registry.Get(req.Name); exists {
		return ErrTopicAlreadyExists
	}
	if req.NumPartitions < 1 || req.NumPartitions > MaxPartitionsPerTopic {
		return ErrInvalidPartitions
	}
	// Early turn-away for the broker-wide partition cap, before any
	// storage exists. AddTopic below repeats the check atomically.
	if !registry.HasRoomFor(req.NumPartitions) {
		return ErrInvalidPartitions
	}

	for i := int32(0); i < req.NumPartitions; i++ {
		if err := diskLog.CreatePartition(req.Name, i); err != nil {
			return ErrUnknownServerError
		}
	}

	if err := registry.AddTopic(registry.NewTopic(req.Name, req.NumPartitions)); err != nil {
		if errors.Is(err, ErrTooManyPartitions) {
			// Lost a race: other creates filled the last slots between the
			// early check and now. Take back the partitions made for this
			// one so a refused create leaves nothing behind - unless a topic
			// of this name has been registered meanwhile, whose partitions
			// these directories now are.
			if _, registered := registry.Get(req.Name); !registered {
				for i := int32(0); i < req.NumPartitions; i++ {
					diskLog.DeletePartition(req.Name, i)
				}
			}
			return ErrInvalidPartitions
		}
		log.Printf("create topic %q: saving the topic list failed: %v", req.Name, err)
		return ErrUnknownServerError
	}
	return ErrNone
}
