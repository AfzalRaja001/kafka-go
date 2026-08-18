package protocol

import (
	"testing"

	"github.com/AfzalRaja001/kafka-go/internal/storage"
)

func encodeListOffsetsRequest(topic string, partition int32, timestamp int64) []byte {
	enc := NewEncoder()
	enc.Int32(-1) // replica_id: -1 for a normal consumer
	enc.Int32(1)  // topic count
	enc.String(topic)
	enc.Int32(1) // partition count
	enc.Int32(partition)
	enc.Int64(timestamp)
	enc.Int32(1) // max_num_offsets: decoded, not used - we always return one
	return enc.Result()
}

func TestDecodeListOffsetsRequest(t *testing.T) {
	body := encodeListOffsetsRequest("orders", 2, ListOffsetsEarliestTimestamp)

	req, err := DecodeListOffsetsRequest(body)
	if err != nil {
		t.Fatalf("DecodeListOffsetsRequest: %v", err)
	}
	if len(req.Topics) != 1 || req.Topics[0].Topic != "orders" {
		t.Fatalf("Topics = %+v, want one topic named orders", req.Topics)
	}
	part := req.Topics[0].Partitions[0]
	if part.Partition != 2 || part.Timestamp != ListOffsetsEarliestTimestamp {
		t.Fatalf("Partition = %+v, want {2, %d}", part, ListOffsetsEarliestTimestamp)
	}
}

func decodeListOffsetsPartitionResponse(t *testing.T, resp []byte) (errorCode int16, offsets []int64) {
	t.Helper()
	dec := NewDecoder(resp)
	dec.Int32() // correlation_id
	dec.Int32() // topic count
	dec.String()
	dec.Int32() // partition count
	dec.Int32() // partition
	errorCode, _ = dec.Int16()
	count, _ := dec.Int32()
	for i := int32(0); i < count; i++ {
		offset, _ := dec.Int64()
		offsets = append(offsets, offset)
	}
	return errorCode, offsets
}

func TestHandleListOffsets_ResolvesEarliest(t *testing.T) {
	log := storage.NewFakeLog()
	log.Append("orders", 0, []byte("a"), 1)
	log.Append("orders", 0, []byte("b"), 1)
	log.Append("orders", 0, []byte("c"), 1)

	body := encodeListOffsetsRequest("orders", 0, ListOffsetsEarliestTimestamp)
	resp, err := HandleListOffsets(1, body, log)
	if err != nil {
		t.Fatalf("HandleListOffsets: %v", err)
	}

	errorCode, offsets := decodeListOffsetsPartitionResponse(t, resp)
	if errorCode != ErrNone {
		t.Fatalf("error_code = %d, want %d", errorCode, ErrNone)
	}
	if len(offsets) != 1 || offsets[0] != 0 {
		t.Errorf("offsets = %v, want [0]", offsets)
	}
}

func TestHandleListOffsets_ResolvesLatest(t *testing.T) {
	log := storage.NewFakeLog()
	log.Append("orders", 0, []byte("a"), 1)
	log.Append("orders", 0, []byte("b"), 1)
	log.Append("orders", 0, []byte("c"), 1)

	body := encodeListOffsetsRequest("orders", 0, ListOffsetsLatestTimestamp)
	resp, err := HandleListOffsets(1, body, log)
	if err != nil {
		t.Fatalf("HandleListOffsets: %v", err)
	}

	errorCode, offsets := decodeListOffsetsPartitionResponse(t, resp)
	if errorCode != ErrNone {
		t.Fatalf("error_code = %d, want %d", errorCode, ErrNone)
	}
	if len(offsets) != 1 || offsets[0] != 3 {
		t.Errorf("offsets = %v, want [3]", offsets)
	}
}

func TestHandleListOffsets_UnknownTopicPartitionReturnsError(t *testing.T) {
	log := storage.NewFakeLog()

	body := encodeListOffsetsRequest("missing", 0, ListOffsetsLatestTimestamp)
	resp, err := HandleListOffsets(1, body, log)
	if err != nil {
		t.Fatalf("HandleListOffsets: %v", err)
	}

	errorCode, offsets := decodeListOffsetsPartitionResponse(t, resp)
	if errorCode != ErrUnknownTopicOrPartition {
		t.Errorf("error_code = %d, want %d", errorCode, ErrUnknownTopicOrPartition)
	}
	if len(offsets) != 0 {
		t.Errorf("offsets = %v, want empty on error", offsets)
	}
}

// TestHandleListOffsets_UnsupportedTimestampReturnsError is a scope
// boundary test: this broker only resolves the two sentinel values
// (earliest/latest) at v0, not arbitrary timestamps via the time index -
// documented in the ListOffsets lesson write-up as a deliberate v0
// simplification, the same way Fetch documents its per-partition min_bytes
// check as a simplification rather than a bug.
func TestHandleListOffsets_UnsupportedTimestampReturnsError(t *testing.T) {
	log := storage.NewFakeLog()
	log.Append("orders", 0, []byte("a"), 1)

	body := encodeListOffsetsRequest("orders", 0, 1700000000000) // an arbitrary real timestamp
	resp, err := HandleListOffsets(1, body, log)
	if err != nil {
		t.Fatalf("HandleListOffsets: %v", err)
	}

	errorCode, offsets := decodeListOffsetsPartitionResponse(t, resp)
	if errorCode != ErrUnknownServerError {
		t.Errorf("error_code = %d, want %d", errorCode, ErrUnknownServerError)
	}
	if len(offsets) != 0 {
		t.Errorf("offsets = %v, want empty on error", offsets)
	}
}
