package protocol

import (
	"testing"

	"github.com/AfzalRaja001/kafka-go/internal/group"
)

func encodeOffsetCommitRequest(groupID, topic string, partition int32, offset int64, metadata *string) []byte {
	enc := NewEncoder()
	enc.String(groupID)
	enc.Int32(1) // topic count
	enc.String(topic)
	enc.Int32(1) // partition count
	enc.Int32(partition)
	enc.Int64(offset)
	enc.NullableString(metadata)
	return enc.Result()
}

func TestDecodeOffsetCommitRequest(t *testing.T) {
	meta := "my-checkpoint"
	body := encodeOffsetCommitRequest("my-group", "orders", 0, 42, &meta)

	req, err := DecodeOffsetCommitRequest(body)
	if err != nil {
		t.Fatalf("DecodeOffsetCommitRequest: %v", err)
	}
	if req.GroupID != "my-group" {
		t.Fatalf("GroupID = %q, want my-group", req.GroupID)
	}
	if len(req.Topics) != 1 || req.Topics[0].Topic != "orders" {
		t.Fatalf("Topics = %+v, want one topic named orders", req.Topics)
	}
	part := req.Topics[0].Partitions[0]
	if part.Partition != 0 || part.Offset != 42 || part.Metadata != "my-checkpoint" {
		t.Fatalf("partition = %+v, want {0, 42, my-checkpoint}", part)
	}
}

func decodeOffsetCommitResponseOnePartition(t *testing.T, resp []byte) (topic string, partition int32, errorCode int16) {
	t.Helper()
	dec := NewDecoder(resp)
	dec.Int32() // correlation_id
	dec.Int32() // topic count
	topic, _ = dec.String()
	dec.Int32() // partition count
	partition, _ = dec.Int32()
	errorCode, _ = dec.Int16()
	return topic, partition, errorCode
}

func TestHandleOffsetCommit_StoresTheOffset(t *testing.T) {
	store := group.NewInMemoryOffsetStore()
	meta := "my-checkpoint"

	body := encodeOffsetCommitRequest("my-group", "orders", 0, 42, &meta)
	resp, err := HandleOffsetCommit(1, body, store)
	if err != nil {
		t.Fatalf("HandleOffsetCommit: %v", err)
	}

	topic, partition, errorCode := decodeOffsetCommitResponseOnePartition(t, resp)
	if topic != "orders" || partition != 0 || errorCode != ErrNone {
		t.Fatalf("response = (%q, %d, %d), want (orders, 0, %d)", topic, partition, errorCode, ErrNone)
	}

	offset, metadata, found := store.Fetch("my-group", "orders", 0)
	if !found || offset != 42 || metadata != "my-checkpoint" {
		t.Errorf("store state after commit = %d, %q, %v, want 42, my-checkpoint, true", offset, metadata, found)
	}
}

func TestHandleOffsetCommit_NullMetadataBecomesEmptyString(t *testing.T) {
	store := group.NewInMemoryOffsetStore()

	body := encodeOffsetCommitRequest("my-group", "orders", 0, 42, nil)
	if _, err := HandleOffsetCommit(1, body, store); err != nil {
		t.Fatalf("HandleOffsetCommit: %v", err)
	}

	_, metadata, found := store.Fetch("my-group", "orders", 0)
	if !found || metadata != "" {
		t.Errorf("metadata = %q, found = %v, want empty string, true", metadata, found)
	}
}
