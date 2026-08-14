package protocol

import (
	"testing"

	"github.com/AfzalRaja001/kafka-go/internal/storage"
)

func encodeProduceRequest(acks int16, timeoutMs int32, topic string, partition int32, records []byte) []byte {
	enc := NewEncoder()
	var txnID *string
	enc.NullableString(txnID)
	enc.Int16(acks)
	enc.Int32(timeoutMs)
	enc.Int32(1) // topic count
	enc.String(topic)
	enc.Int32(1) // partition count
	enc.Int32(partition)
	enc.Bytes(records)
	return enc.Result()
}

func TestDecodeProduceRequest(t *testing.T) {
	records := buildTestBatch(0, 2, []byte("fake-records"))
	body := encodeProduceRequest(1, 5000, "orders", 3, records)

	req, err := DecodeProduceRequest(body)
	if err != nil {
		t.Fatalf("DecodeProduceRequest: %v", err)
	}
	if req.Acks != 1 {
		t.Errorf("Acks = %d, want 1", req.Acks)
	}
	if req.TimeoutMs != 5000 {
		t.Errorf("TimeoutMs = %d, want 5000", req.TimeoutMs)
	}
	if len(req.Topics) != 1 || req.Topics[0].Topic != "orders" {
		t.Fatalf("Topics = %+v, want one topic named orders", req.Topics)
	}
	parts := req.Topics[0].Partitions
	if len(parts) != 1 || parts[0].Partition != 3 {
		t.Fatalf("Partitions = %+v, want one partition, id 3", parts)
	}
	if string(parts[0].Records) != string(records) {
		t.Errorf("Records mismatch")
	}
}

func TestHandleProduce_Success(t *testing.T) {
	log := storage.NewFakeLog()
	records := buildTestBatch(0, 2, []byte("fake-records"))
	body := encodeProduceRequest(1, 5000, "orders", 0, records)

	resp, err := HandleProduce(42, body, log)
	if err != nil {
		t.Fatalf("HandleProduce: %v", err)
	}

	dec := NewDecoder(resp)
	correlationID, _ := dec.Int32()
	if correlationID != 42 {
		t.Fatalf("correlation_id = %d, want 42", correlationID)
	}
	topicCount, _ := dec.Int32()
	if topicCount != 1 {
		t.Fatalf("topic count = %d, want 1", topicCount)
	}
	topicName, _ := dec.String()
	if topicName != "orders" {
		t.Fatalf("topic = %q, want orders", topicName)
	}
	partCount, _ := dec.Int32()
	if partCount != 1 {
		t.Fatalf("partition count = %d, want 1", partCount)
	}
	partition, _ := dec.Int32()
	errorCode, _ := dec.Int16()
	baseOffset, _ := dec.Int64()
	if partition != 0 || errorCode != ErrNone || baseOffset != 0 {
		t.Fatalf("partition response = (%d, %d, %d), want (0, %d, 0)", partition, errorCode, baseOffset, ErrNone)
	}
}

func TestHandleProduce_SecondAppendGetsNextOffset(t *testing.T) {
	log := storage.NewFakeLog()

	produceOnceExpectOffset := func(want int64) {
		t.Helper()
		records := buildTestBatch(0, 1, []byte("r"))
		body := encodeProduceRequest(1, 5000, "orders", 0, records)
		resp, err := HandleProduce(1, body, log)
		if err != nil {
			t.Fatalf("HandleProduce: %v", err)
		}
		dec := NewDecoder(resp)
		dec.Int32() // correlation_id
		dec.Int32() // topic count
		dec.String()
		dec.Int32() // partition count
		dec.Int32() // partition
		dec.Int16() // error_code
		got, _ := dec.Int64()
		if got != want {
			t.Fatalf("base_offset = %d, want %d", got, want)
		}
	}

	produceOnceExpectOffset(0)
	produceOnceExpectOffset(1)
}

func TestHandleProduce_CorruptMagicRejectedPerPartition(t *testing.T) {
	log := storage.NewFakeLog()
	records := buildTestBatch(0, 1, nil)
	records[16] = 1 // wrong magic

	body := encodeProduceRequest(1, 5000, "orders", 0, records)
	resp, err := HandleProduce(1, body, log)
	if err != nil {
		t.Fatalf("HandleProduce returned a Go error for a per-partition problem: %v", err)
	}

	dec := NewDecoder(resp)
	dec.Int32() // correlation_id
	dec.Int32() // topic count
	dec.String()
	dec.Int32() // partition count
	dec.Int32() // partition
	errorCode, _ := dec.Int16()
	if errorCode != ErrCorruptMessage {
		t.Errorf("error_code = %d, want %d (CORRUPT_MESSAGE)", errorCode, ErrCorruptMessage)
	}
}

func TestHandleProduce_RewritesBaseOffsetInStoredBytes(t *testing.T) {
	log := storage.NewFakeLog()

	first := buildTestBatch(0, 1, []byte("r0"))
	HandleProduce(1, encodeProduceRequest(1, 5000, "orders", 0, first), log)

	second := buildTestBatch(0, 1, []byte("r1")) // client always sends baseOffset 0
	HandleProduce(1, encodeProduceRequest(1, 5000, "orders", 0, second), log)

	// Read the second batch back directly from storage and confirm the
	// durable bytes really were patched to baseOffset 1, not just that the
	// response claimed offset 1.
	stored, err := log.Read("orders", 0, 1, 1024)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	h, err := ParseRecordBatchHeader(stored)
	if err != nil {
		t.Fatalf("ParseRecordBatchHeader on stored bytes: %v", err)
	}
	if h.BaseOffset != 1 {
		t.Errorf("stored baseOffset = %d, want 1", h.BaseOffset)
	}
}
