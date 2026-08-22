package protocol

import (
	"testing"

	"github.com/AfzalRaja001/kafka-go/internal/group"
)

func encodeOffsetFetchRequest(groupID, topic string, partitions []int32) []byte {
	enc := NewEncoder()
	enc.String(groupID)
	enc.Int32(1) // topic count
	enc.String(topic)
	enc.Int32(int32(len(partitions)))
	for _, p := range partitions {
		enc.Int32(p)
	}
	return enc.Result()
}

func TestDecodeOffsetFetchRequest(t *testing.T) {
	body := encodeOffsetFetchRequest("my-group", "orders", []int32{0, 1, 2})

	req, err := DecodeOffsetFetchRequest(body)
	if err != nil {
		t.Fatalf("DecodeOffsetFetchRequest: %v", err)
	}
	if req.GroupID != "my-group" {
		t.Fatalf("GroupID = %q, want my-group", req.GroupID)
	}
	if len(req.Topics) != 1 || req.Topics[0].Topic != "orders" {
		t.Fatalf("Topics = %+v, want one topic named orders", req.Topics)
	}
	if got := req.Topics[0].Partitions; len(got) != 3 || got[0] != 0 || got[1] != 1 || got[2] != 2 {
		t.Fatalf("Partitions = %v, want [0 1 2]", got)
	}
}

func decodeOffsetFetchResponseOnePartition(t *testing.T, resp []byte) (topic string, partition int32, offset int64, metadata *string, errorCode int16) {
	t.Helper()
	dec := NewDecoder(resp)
	dec.Int32() // correlation_id
	dec.Int32() // topic count
	topic, _ = dec.String()
	dec.Int32() // partition count
	partition, _ = dec.Int32()
	offset, _ = dec.Int64()
	metadata, _ = dec.NullableString()
	errorCode, _ = dec.Int16()
	return topic, partition, offset, metadata, errorCode
}

func TestHandleOffsetFetch_ReturnsCommittedOffset(t *testing.T) {
	store := group.NewInMemoryOffsetStore()
	store.Commit("my-group", "orders", 0, 42, "my-checkpoint")

	body := encodeOffsetFetchRequest("my-group", "orders", []int32{0})
	resp, err := HandleOffsetFetch(1, body, store)
	if err != nil {
		t.Fatalf("HandleOffsetFetch: %v", err)
	}

	topic, partition, offset, metadata, errorCode := decodeOffsetFetchResponseOnePartition(t, resp)
	if topic != "orders" || partition != 0 || offset != 42 || errorCode != ErrNone {
		t.Fatalf("response = (%q, %d, %d, %d), want (orders, 0, 42, %d)", topic, partition, offset, errorCode, ErrNone)
	}
	if metadata == nil || *metadata != "my-checkpoint" {
		t.Fatalf("metadata = %v, want my-checkpoint", metadata)
	}
}

// TestHandleOffsetFetch_NeverCommittedReturnsMinusOneNotAnError matches
// real Kafka's sentinel for "no committed offset exists" - offset -1,
// null metadata, and error_code NONE. A consumer relies on exactly this
// to detect there's nothing to resume from, so it must not be an error.
func TestHandleOffsetFetch_NeverCommittedReturnsMinusOneNotAnError(t *testing.T) {
	store := group.NewInMemoryOffsetStore()

	body := encodeOffsetFetchRequest("my-group", "orders", []int32{0})
	resp, err := HandleOffsetFetch(1, body, store)
	if err != nil {
		t.Fatalf("HandleOffsetFetch: %v", err)
	}

	topic, partition, offset, metadata, errorCode := decodeOffsetFetchResponseOnePartition(t, resp)
	if topic != "orders" || partition != 0 || offset != -1 || errorCode != ErrNone {
		t.Fatalf("response = (%q, %d, %d, %d), want (orders, 0, -1, %d)", topic, partition, offset, errorCode, ErrNone)
	}
	if metadata != nil {
		t.Fatalf("metadata = %v, want nil", metadata)
	}
}
