package protocol

import (
	"fmt"

	"github.com/AfzalRaja001/kafka-go/internal/group"
)

type OffsetFetchTopicRequest struct {
	Topic      string
	Partitions []int32
}

type OffsetFetchRequest struct {
	GroupID string
	Topics  []OffsetFetchTopicRequest
}

// DecodeOffsetFetchRequest decodes an OffsetFetch v0 request body.
func DecodeOffsetFetchRequest(buf []byte) (OffsetFetchRequest, error) {
	dec := NewDecoder(buf)

	groupID, err := dec.String()
	if err != nil {
		return OffsetFetchRequest{}, fmt.Errorf("group_id: %w", err)
	}

	topicCount, err := dec.Int32()
	if err != nil {
		return OffsetFetchRequest{}, fmt.Errorf("topic count: %w", err)
	}

	var topics []OffsetFetchTopicRequest
	for i := int32(0); i < topicCount; i++ {
		topic, err := dec.String()
		if err != nil {
			return OffsetFetchRequest{}, fmt.Errorf("topic %d name: %w", i, err)
		}
		partCount, err := dec.Int32()
		if err != nil {
			return OffsetFetchRequest{}, fmt.Errorf("topic %d partition count: %w", i, err)
		}

		var parts []int32
		for j := int32(0); j < partCount; j++ {
			partition, err := dec.Int32()
			if err != nil {
				return OffsetFetchRequest{}, fmt.Errorf("topic %d partition %d: %w", i, j, err)
			}
			parts = append(parts, partition)
		}
		topics = append(topics, OffsetFetchTopicRequest{Topic: topic, Partitions: parts})
	}

	return OffsetFetchRequest{GroupID: groupID, Topics: topics}, nil
}

// HandleOffsetFetch builds an OffsetFetch v0 response body. A partition
// with no committed offset reports offset -1 and null metadata with
// error_code NONE, matching real Kafka's sentinel for "nothing to resume
// from" - not a failure.
func HandleOffsetFetch(correlationID int32, requestBody []byte, store group.OffsetStore) ([]byte, error) {
	req, err := DecodeOffsetFetchRequest(requestBody)
	if err != nil {
		return nil, fmt.Errorf("offset_fetch request: %w", err)
	}

	enc := NewEncoder()
	enc.Int32(correlationID)
	enc.Int32(int32(len(req.Topics)))

	for _, topic := range req.Topics {
		enc.String(topic.Topic)
		enc.Int32(int32(len(topic.Partitions)))

		for _, partition := range topic.Partitions {
			offset, metadata, found := store.Fetch(req.GroupID, topic.Topic, partition)
			enc.Int32(partition)
			if !found {
				enc.Int64(-1)
				enc.NullableString(nil)
				enc.Int16(ErrNone)
				continue
			}
			enc.Int64(offset)
			enc.NullableString(&metadata)
			enc.Int16(ErrNone)
		}
	}

	return enc.Result(), nil
}
