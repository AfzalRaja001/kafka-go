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

// DecodeListOffsetsRequest decodes a ListOffsets v1 request body. v1 dropped
// v0's max_num_offsets field - v0 could return several candidate offsets per
// partition (an array), but every real client only ever wants exactly one
// (the offset for a single timestamp), so v1 simplifies the response to a
// single value and the request no longer needs to say how many to return.
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
			parts = append(parts, ListOffsetsPartitionRequest{Partition: partition, Timestamp: timestamp})
		}
		topics = append(topics, ListOffsetsTopicRequest{Topic: topic, Partitions: parts})
	}

	return ListOffsetsRequest{ReplicaID: replicaID, Topics: topics}, nil
}

// HandleListOffsets builds a ListOffsets v1 response body, resolving each
// requested partition's timestamp sentinel against the log. -1 resolves to
// the log's latest (end) offset, -2 to its earliest (start) offset - the
// two cases a consumer needs for seek_to_end/seek_to_beginning. Each
// partition gets exactly one (timestamp, offset) pair, not v0's array of
// offsets - this broker never had a reason to return more than one anyway,
// so v1's simpler shape costs nothing while fixing a real compatibility gap:
// v0's array-shaped response predates a scalar `Offset` field existing on
// the wire at all, so any client whose response-parsing code reads that
// scalar field (real Kafka's own admin tooling, and every mainstream client
// library's high-level API since v1) sees an unpopulated offset - it never
// existed at v0 - against a broker that only speaks v0.
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
				enc.Int64(-1) // timestamp: unknown
				enc.Int64(-1) // offset: unknown
				continue
			}
			// This broker doesn't track per-record wall-clock timestamps
			// anywhere resolveOffset can reach, so -1 ("not applicable") is
			// reported here the same way Metadata v1 reported Rack/
			// IsInternal as their "not applicable" values - a real client
			// asking for seek_to_beginning/seek_to_end only ever reads the
			// offset field, not this timestamp.
			enc.Int64(-1)
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
