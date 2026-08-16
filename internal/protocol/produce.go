package protocol

import (
	"fmt"
	"sync"
	"time"

	"github.com/AfzalRaja001/kafka-go/internal/storage"
)

type ProducePartitionRequest struct {
	Partition int32
	Records   []byte
}

type ProduceTopicRequest struct {
	Topic      string
	Partitions []ProducePartitionRequest
}

type ProduceRequest struct {
	Acks      int16
	TimeoutMs int32
	Topics    []ProduceTopicRequest
}

// DecodeProduceRequest decodes a Produce v3 request body. transactional_id
// is read (v3 introduced it) but discarded - this broker doesn't implement
// transactions.
func DecodeProduceRequest(buf []byte) (ProduceRequest, error) {
	dec := NewDecoder(buf)

	if _, err := dec.NullableString(); err != nil {
		return ProduceRequest{}, fmt.Errorf("transactional_id: %w", err)
	}
	acks, err := dec.Int16()
	if err != nil {
		return ProduceRequest{}, fmt.Errorf("acks: %w", err)
	}
	timeoutMs, err := dec.Int32()
	if err != nil {
		return ProduceRequest{}, fmt.Errorf("timeout_ms: %w", err)
	}

	topicCount, err := dec.Int32()
	if err != nil {
		return ProduceRequest{}, fmt.Errorf("topic count: %w", err)
	}

	var topics []ProduceTopicRequest
	for i := int32(0); i < topicCount; i++ {
		topic, err := dec.String()
		if err != nil {
			return ProduceRequest{}, fmt.Errorf("topic %d name: %w", i, err)
		}
		partCount, err := dec.Int32()
		if err != nil {
			return ProduceRequest{}, fmt.Errorf("topic %d partition count: %w", i, err)
		}

		var parts []ProducePartitionRequest
		for j := int32(0); j < partCount; j++ {
			partition, err := dec.Int32()
			if err != nil {
				return ProduceRequest{}, fmt.Errorf("topic %d partition %d id: %w", i, j, err)
			}
			records, err := dec.Bytes()
			if err != nil {
				return ProduceRequest{}, fmt.Errorf("topic %d partition %d records: %w", i, j, err)
			}
			parts = append(parts, ProducePartitionRequest{Partition: partition, Records: records})
		}
		topics = append(topics, ProduceTopicRequest{Topic: topic, Partitions: parts})
	}

	return ProduceRequest{Acks: acks, TimeoutMs: timeoutMs, Topics: topics}, nil
}

// produceMu serializes the peek-latest-offset -> patch-batch -> append
// sequence broker-wide. The frozen Log interface has no hook for doing
// this atomically per partition, and extending it was a bigger call than
// this step needed - so for now every Produce call, across every topic and
// partition, goes through one lock. That's a real throughput ceiling (see
// docs/decisions.md) worth replacing with a per-partition lock later, but
// it keeps the interface boundary untouched and is straightforward to
// swap out in one place when it matters.
var produceMu sync.Mutex

// HandleProduce builds a Produce v3 response body, appending each
// partition's record batch to log after rewriting its baseOffset and CRC
// to match the offset actually assigned.
func HandleProduce(correlationID int32, requestBody []byte, log storage.Log) ([]byte, error) {
	req, err := DecodeProduceRequest(requestBody)
	if err != nil {
		return nil, fmt.Errorf("produce request: %w", err)
	}

	enc := NewEncoder()
	enc.Int32(correlationID)
	enc.Int32(int32(len(req.Topics)))

	for _, topic := range req.Topics {
		enc.String(topic.Topic)
		enc.Int32(int32(len(topic.Partitions)))

		for _, part := range topic.Partitions {
			baseOffset, logAppendTime, errorCode := produceOne(log, topic.Topic, part.Partition, part.Records)
			enc.Int32(part.Partition)
			enc.Int16(errorCode)
			enc.Int64(baseOffset)
			enc.Int64(logAppendTime)
		}
	}

	enc.Int32(0) // throttle_time_ms
	return enc.Result(), nil
}

func produceOne(log storage.Log, topic string, partition int32, records []byte) (baseOffset, logAppendTime int64, errorCode int16) {
	header, err := ParseRecordBatchHeader(records)
	if err != nil {
		return 0, -1, ErrCorruptMessage
	}

	produceMu.Lock()
	defer produceMu.Unlock()

	offset, err := log.LatestOffset(topic, partition)
	if err != nil {
		// Doesn't exist yet - Append (below) creates it fresh via
		// DiskLog's auto-create-on-write behavior, and a fresh partition
		// always starts at offset 0. See the produceMu doc comment.
		offset = 0
	}

	patched := RewriteRecordBatch(records, offset)

	// header.RecordCount is how many offsets this batch consumes. Passing it
	// down is what stops the log from advancing by a single offset per
	// batch: under-reporting it would make the *next* produce peek a stale
	// LatestOffset and assign a baseOffset colliding with records this batch
	// already owns.
	assignedOffset, err := log.Append(topic, partition, patched, header.RecordCount)
	if err != nil {
		return 0, -1, ErrUnknownServerError
	}

	return assignedOffset, time.Now().UnixMilli(), ErrNone
}
