package protocol

import (
	"context"
	"fmt"
	"time"

	"github.com/AfzalRaja001/kafka-go/internal/storage"
)

type FetchPartitionRequest struct {
	Partition   int32
	FetchOffset int64
	MaxBytes    int32
}

type FetchTopicRequest struct {
	Topic      string
	Partitions []FetchPartitionRequest
}

type FetchRequest struct {
	ReplicaID     int32
	MaxWaitTimeMs int32
	MinBytes      int32
	Topics        []FetchTopicRequest
}

// DecodeFetchRequest decodes a Fetch v0 request body.
func DecodeFetchRequest(buf []byte) (FetchRequest, error) {
	dec := NewDecoder(buf)

	replicaID, err := dec.Int32()
	if err != nil {
		return FetchRequest{}, fmt.Errorf("replica_id: %w", err)
	}
	maxWaitTimeMs, err := dec.Int32()
	if err != nil {
		return FetchRequest{}, fmt.Errorf("max_wait_time: %w", err)
	}
	minBytes, err := dec.Int32()
	if err != nil {
		return FetchRequest{}, fmt.Errorf("min_bytes: %w", err)
	}

	topicCount, err := dec.Int32()
	if err != nil {
		return FetchRequest{}, fmt.Errorf("topic count: %w", err)
	}

	var topics []FetchTopicRequest
	for i := int32(0); i < topicCount; i++ {
		topic, err := dec.String()
		if err != nil {
			return FetchRequest{}, fmt.Errorf("topic %d name: %w", i, err)
		}
		partCount, err := dec.Int32()
		if err != nil {
			return FetchRequest{}, fmt.Errorf("topic %d partition count: %w", i, err)
		}

		var parts []FetchPartitionRequest
		for j := int32(0); j < partCount; j++ {
			partition, err := dec.Int32()
			if err != nil {
				return FetchRequest{}, fmt.Errorf("topic %d partition %d id: %w", i, j, err)
			}
			fetchOffset, err := dec.Int64()
			if err != nil {
				return FetchRequest{}, fmt.Errorf("topic %d partition %d fetch_offset: %w", i, j, err)
			}
			maxBytes, err := dec.Int32()
			if err != nil {
				return FetchRequest{}, fmt.Errorf("topic %d partition %d max_bytes: %w", i, j, err)
			}
			parts = append(parts, FetchPartitionRequest{Partition: partition, FetchOffset: fetchOffset, MaxBytes: maxBytes})
		}
		topics = append(topics, FetchTopicRequest{Topic: topic, Partitions: parts})
	}

	return FetchRequest{ReplicaID: replicaID, MaxWaitTimeMs: maxWaitTimeMs, MinBytes: minBytes, Topics: topics}, nil
}

// HandleFetch builds a Fetch v0 response body, long-polling each requested
// partition independently up to max_wait_time_ms if it doesn't yet have
// min_bytes worth of data.
func HandleFetch(correlationID int32, requestBody []byte, log storage.Log) ([]byte, error) {
	req, err := DecodeFetchRequest(requestBody)
	if err != nil {
		return nil, fmt.Errorf("fetch request: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(req.MaxWaitTimeMs)*time.Millisecond)
	defer cancel()

	enc := NewEncoder()
	enc.Int32(correlationID)
	enc.Int32(int32(len(req.Topics)))

	for _, topic := range req.Topics {
		enc.String(topic.Topic)
		enc.Int32(int32(len(topic.Partitions)))

		for _, part := range topic.Partitions {
			data, highWatermark, errorCode := fetchOne(ctx, log, topic.Topic, part.Partition, part.FetchOffset, req.MinBytes, part.MaxBytes)
			enc.Int32(part.Partition)
			enc.Int16(errorCode)
			enc.Int64(highWatermark)
			// Fetch's records field must be present-but-empty when there's
			// nothing new (not null): a real Kafka client's parser treats
			// null here as an unexpected condition, not "caught up," and
			// chokes trying to construct a batch reader from it. Encoder.Bytes
			// treats nil as null (the correct general rule for most fields -
			// this is a Fetch-specific exception, not a codec-wide change).
			if data == nil {
				data = []byte{}
			}
			enc.Bytes(data)
		}
	}

	return enc.Result(), nil
}

func fetchOne(ctx context.Context, log storage.Log, topic string, partition int32, fetchOffset int64, minBytes, maxBytes int32) (data []byte, highWatermark int64, errorCode int16) {
	data, err := waitForData(ctx, log, topic, partition, fetchOffset, minBytes, maxBytes)
	if err != nil {
		return nil, 0, ErrUnknownTopicOrPartition
	}

	hw, err := log.LatestOffset(topic, partition)
	if err != nil {
		hw = 0
	}
	return data, hw, ErrNone
}

// waitForData polls log.Read every pollInterval until it returns at least
// minBytes, or ctx's deadline (max_wait_time_ms) passes - in which case it
// returns whatever's currently available, even if that's less than
// minBytes or nothing at all. A first-attempt error (e.g. the
// topic-partition doesn't exist) returns immediately without polling -
// long-polling for something that will never exist just wastes the full
// timeout for no benefit.
func waitForData(ctx context.Context, log storage.Log, topic string, partition int32, offset int64, minBytes, maxBytes int32) ([]byte, error) {
	const pollInterval = 20 * time.Millisecond

	data, err := log.Read(topic, partition, offset, maxBytes)
	if err != nil {
		return nil, err
	}
	if int32(len(data)) >= minBytes {
		return data, nil
	}

	for {
		select {
		case <-ctx.Done():
			return data, nil
		case <-time.After(pollInterval):
		}

		data, err = log.Read(topic, partition, offset, maxBytes)
		if err != nil {
			return nil, err
		}
		if int32(len(data)) >= minBytes {
			return data, nil
		}
	}
}
