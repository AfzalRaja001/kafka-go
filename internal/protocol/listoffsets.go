package protocol

import (
	"fmt"

	"github.com/AfzalRaja001/kafka-go/internal/storage"
)

// Timestamp sentinel values the ListOffsets request uses in place of a real
// Unix timestamp - the only two values this broker resolves at v0.
const (
	ListOffsetsLatestTimestamp   int64 = -1
	ListOffsetsEarliestTimestamp int64 = -2
)

type ListOffsetsPartitionRequest struct {
	Partition int32
	Timestamp int64
}

type ListOffsetsTopicRequest struct {
	Topic      string
	Partitions []ListOffsetsPartitionRequest
}

type ListOffsetsRequest struct {
	ReplicaID int32
	Topics    []ListOffsetsTopicRequest
}

// DecodeListOffsetsRequest decodes a ListOffsets v0 request body.
func DecodeListOffsetsRequest(buf []byte) (ListOffsetsRequest, error) {
	dec := NewDecoder(buf)

	replicaID, err := dec.Int32()
	if err != nil {
		return ListOffsetsRequest{}, fmt.Errorf("replica_id: %w", err)
	}

	topicCount, err := dec.Int32()
	if err != nil {
		return ListOffsetsRequest{}, fmt.Errorf("topic count: %w", err)
	}

	var topics []ListOffsetsTopicRequest
	for i := int32(0); i < topicCount; i++ {
		topic, err := dec.String()
		if err != nil {
			return ListOffsetsRequest{}, fmt.Errorf("topic %d name: %w", i, err)
		}
		partCount, err := dec.Int32()
		if err != nil {
			return ListOffsetsRequest{}, fmt.Errorf("topic %d partition count: %w", i, err)
		}

		var parts []ListOffsetsPartitionRequest
		for j := int32(0); j < partCount; j++ {
			partition, err := dec.Int32()
			if err != nil {
				return ListOffsetsRequest{}, fmt.Errorf("topic %d partition %d id: %w", i, j, err)
			}
			timestamp, err := dec.Int64()
			if err != nil {
				return ListOffsetsRequest{}, fmt.Errorf("topic %d partition %d timestamp: %w", i, j, err)
			}
			// max_num_offsets: a v0-only field capping how many offsets to
			// return per partition. We always resolve exactly one, so it's
			// decoded (to stay framed correctly for whatever follows) and
			// discarded, the same treatment client_id gets in dispatch.go.
			if _, err := dec.Int32(); err != nil {
				return ListOffsetsRequest{}, fmt.Errorf("topic %d partition %d max_num_offsets: %w", i, j, err)
			}
			parts = append(parts, ListOffsetsPartitionRequest{Partition: partition, Timestamp: timestamp})
		}
		topics = append(topics, ListOffsetsTopicRequest{Topic: topic, Partitions: parts})
	}

	return ListOffsetsRequest{ReplicaID: replicaID, Topics: topics}, nil
}

// HandleListOffsets builds a ListOffsets v0 response body, resolving each
// requested partition's timestamp sentinel against the log. -1 resolves to
// the log's latest (end) offset, -2 to its earliest (start) offset - the
// two cases a consumer needs for seek_to_end/seek_to_beginning.
func HandleListOffsets(correlationID int32, requestBody []byte, log storage.Log) ([]byte, error) {
	req, err := DecodeListOffsetsRequest(requestBody)
	if err != nil {
		return nil, fmt.Errorf("list_offsets request: %w", err)
	}

	enc := NewEncoder()
	enc.Int32(correlationID)
	enc.Int32(int32(len(req.Topics)))

	for _, topic := range req.Topics {
		enc.String(topic.Topic)
		enc.Int32(int32(len(topic.Partitions)))

		for _, part := range topic.Partitions {
			offset, errorCode := resolveOffset(log, topic.Topic, part.Partition, part.Timestamp)
			enc.Int32(part.Partition)
			enc.Int16(errorCode)
			if errorCode != ErrNone {
				enc.Int32(0) // offsets: empty array
				continue
			}
			enc.Int32(1) // offsets: exactly one element, this broker never returns more
			enc.Int64(offset)
		}
	}

	return enc.Result(), nil
}

// resolveOffset maps a requested timestamp to a real offset. Only the two
// sentinel values are implemented - resolving an arbitrary timestamp would
// mean searching the time index Track B built in Phase 2, which isn't wired
// through the Log interface yet. That's a deliberate v0 scope boundary, not
// an oversight: real Kafka clients almost always ask for one of the two
// sentinels (seek_to_beginning/seek_to_end), so this unblocks the common
// case without pulling time-based lookup into the protocol layer early.
func resolveOffset(log storage.Log, topic string, partition int32, timestamp int64) (int64, int16) {
	switch timestamp {
	case ListOffsetsLatestTimestamp:
		offset, err := log.LatestOffset(topic, partition)
		if err != nil {
			return 0, ErrUnknownTopicOrPartition
		}
		return offset, ErrNone
	case ListOffsetsEarliestTimestamp:
		offset, err := log.EarliestOffset(topic, partition)
		if err != nil {
			return 0, ErrUnknownTopicOrPartition
		}
		return offset, ErrNone
	default:
		return 0, ErrUnknownServerError
	}
}
