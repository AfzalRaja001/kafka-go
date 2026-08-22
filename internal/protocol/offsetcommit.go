package protocol

import (
	"fmt"

	"github.com/AfzalRaja001/kafka-go/internal/group"
)

type OffsetCommitPartitionRequest struct {
	Partition int32
	Offset    int64
	Metadata  string
}

type OffsetCommitTopicRequest struct {
	Topic      string
	Partitions []OffsetCommitPartitionRequest
}

type OffsetCommitRequest struct {
	GroupID string
	Topics  []OffsetCommitTopicRequest
}

// DecodeOffsetCommitRequest decodes an OffsetCommit v0 request body.
func DecodeOffsetCommitRequest(buf []byte) (OffsetCommitRequest, error) {
	dec := NewDecoder(buf)

	groupID, err := dec.String()
	if err != nil {
		return OffsetCommitRequest{}, fmt.Errorf("group_id: %w", err)
	}

	topicCount, err := dec.Int32()
	if err != nil {
		return OffsetCommitRequest{}, fmt.Errorf("topic count: %w", err)
	}

	var topics []OffsetCommitTopicRequest
	for i := int32(0); i < topicCount; i++ {
		topic, err := dec.String()
		if err != nil {
			return OffsetCommitRequest{}, fmt.Errorf("topic %d name: %w", i, err)
		}
		partCount, err := dec.Int32()
		if err != nil {
			return OffsetCommitRequest{}, fmt.Errorf("topic %d partition count: %w", i, err)
		}

		var parts []OffsetCommitPartitionRequest
		for j := int32(0); j < partCount; j++ {
			partition, err := dec.Int32()
			if err != nil {
				return OffsetCommitRequest{}, fmt.Errorf("topic %d partition %d index: %w", i, j, err)
			}
			offset, err := dec.Int64()
			if err != nil {
				return OffsetCommitRequest{}, fmt.Errorf("topic %d partition %d offset: %w", i, j, err)
			}
			metadata, err := dec.NullableString()
			if err != nil {
				return OffsetCommitRequest{}, fmt.Errorf("topic %d partition %d metadata: %w", i, j, err)
			}
			part := OffsetCommitPartitionRequest{Partition: partition, Offset: offset}
			if metadata != nil {
				part.Metadata = *metadata
			}
			parts = append(parts, part)
		}
		topics = append(topics, OffsetCommitTopicRequest{Topic: topic, Partitions: parts})
	}

	return OffsetCommitRequest{GroupID: groupID, Topics: topics}, nil
}

// HandleOffsetCommit builds an OffsetCommit v0 response body, persisting
// each requested partition's offset to store. No validation against
// TopicRegistry - matching Produce/Fetch, this trusts whatever (group,
// topic, partition) the client sends rather than cross-checking it exists.
func HandleOffsetCommit(correlationID int32, requestBody []byte, store group.OffsetStore) ([]byte, error) {
	req, err := DecodeOffsetCommitRequest(requestBody)
	if err != nil {
		return nil, fmt.Errorf("offset_commit request: %w", err)
	}

	enc := NewEncoder()
	enc.Int32(correlationID)
	enc.Int32(int32(len(req.Topics)))

	for _, topic := range req.Topics {
		enc.String(topic.Topic)
		enc.Int32(int32(len(topic.Partitions)))

		for _, part := range topic.Partitions {
			errorCode := ErrNone
			if err := store.Commit(req.GroupID, topic.Topic, part.Partition, part.Offset, part.Metadata); err != nil {
				errorCode = ErrUnknownServerError
			}
			enc.Int32(part.Partition)
			enc.Int16(errorCode)
		}
	}

	return enc.Result(), nil
}
