package protocol

import (
	"fmt"
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

// encodeOffsetFetchRequestAllTopics builds a v2 request with a null topics
// array (topic count = -1) - "fetch every offset this group has committed."
func encodeOffsetFetchRequestAllTopics(groupID string) []byte {
	enc := NewEncoder()
	enc.String(groupID)
	enc.Int32(-1) // topic count: null
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

func decodeOffsetFetchResponseOnePartition(t *testing.T, resp []byte) (topic string, partition int32, offset int64, metadata *string, errorCode int16, topLevelErrorCode int16) {
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
	topLevelErrorCode, _ = dec.Int16() // added in v2
	return topic, partition, offset, metadata, errorCode, topLevelErrorCode
}

// decodedOffsetFetchPartition is one (topic, partition, offset, metadata)
// entry from an OffsetFetch response - used by tests that need to decode
// more than the one-topic-one-partition shape
// decodeOffsetFetchResponseOnePartition assumes.
type decodedOffsetFetchPartition struct {
	Topic     string
	Partition int32
	Offset    int64
	Metadata  *string
	ErrorCode int16
}

func decodeOffsetFetchResponse(t *testing.T, resp []byte) (partitions []decodedOffsetFetchPartition, topLevelErrorCode int16) {
	t.Helper()
	dec := NewDecoder(resp)
	dec.Int32() // correlation_id

	topicCount, _ := dec.Int32()
	for i := int32(0); i < topicCount; i++ {
		topic, _ := dec.String()
		partCount, _ := dec.Int32()
		for j := int32(0); j < partCount; j++ {
			partition, _ := dec.Int32()
			offset, _ := dec.Int64()
			metadata, _ := dec.NullableString()
			errorCode, _ := dec.Int16()
			partitions = append(partitions, decodedOffsetFetchPartition{
				Topic: topic, Partition: partition, Offset: offset, Metadata: metadata, ErrorCode: errorCode,
			})
		}
	}
	topLevelErrorCode, _ = dec.Int16()
	return partitions, topLevelErrorCode
}

func TestHandleOffsetFetch_ReturnsCommittedOffset(t *testing.T) {
	store := group.NewInMemoryOffsetStore()
	store.Commit("my-group", "orders", 0, 42, "my-checkpoint")

	body := encodeOffsetFetchRequest("my-group", "orders", []int32{0})
	resp, err := HandleOffsetFetch(1, body, store)
	if err != nil {
		t.Fatalf("HandleOffsetFetch: %v", err)
	}

	topic, partition, offset, metadata, errorCode, topLevelErrorCode := decodeOffsetFetchResponseOnePartition(t, resp)
	if topic != "orders" || partition != 0 || offset != 42 || errorCode != ErrNone {
		t.Fatalf("response = (%q, %d, %d, %d), want (orders, 0, 42, %d)", topic, partition, offset, errorCode, ErrNone)
	}
	if metadata == nil || *metadata != "my-checkpoint" {
		t.Fatalf("metadata = %v, want my-checkpoint", metadata)
	}
	if topLevelErrorCode != ErrNone {
		t.Errorf("top-level error_code = %d, want %d", topLevelErrorCode, ErrNone)
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

	topic, partition, offset, metadata, errorCode, topLevelErrorCode := decodeOffsetFetchResponseOnePartition(t, resp)
	if topic != "orders" || partition != 0 || offset != -1 || errorCode != ErrNone {
		t.Fatalf("response = (%q, %d, %d, %d), want (orders, 0, -1, %d)", topic, partition, offset, errorCode, ErrNone)
	}
	if metadata != nil {
		t.Fatalf("metadata = %v, want nil", metadata)
	}
	if topLevelErrorCode != ErrNone {
		t.Errorf("top-level error_code = %d, want %d", topLevelErrorCode, ErrNone)
	}
}

// TestDecodeOffsetFetchRequest_NullTopicsMeansAllTopics pins down v2's real
// wire difference from v0: a topic count of -1 decodes to a nil Topics
// slice, not an error and not an empty-but-present slice - verified against
// Apache Kafka's own OffsetFetchRequest.json schema (branch 2.5).
func TestDecodeOffsetFetchRequest_NullTopicsMeansAllTopics(t *testing.T) {
	req, err := DecodeOffsetFetchRequest(encodeOffsetFetchRequestAllTopics("my-group"))
	if err != nil {
		t.Fatalf("DecodeOffsetFetchRequest: %v", err)
	}
	if req.GroupID != "my-group" {
		t.Fatalf("GroupID = %q, want my-group", req.GroupID)
	}
	if req.Topics != nil {
		t.Fatalf("Topics = %+v, want nil", req.Topics)
	}
}

// TestHandleOffsetFetch_NullTopicsReturnsEverythingCommitted is the actual
// point of this whole change: a null topics array answers with every
// topic-partition this group has committed, not just what an admin client
// happened to name - the thing kafka-python's
// KafkaAdminClient.list_group_offsets() actually sends (see the OffsetStore
// design entry in docs/decisions.md for why v0 alone couldn't answer this).
func TestHandleOffsetFetch_NullTopicsReturnsEverythingCommitted(t *testing.T) {
	store := group.NewInMemoryOffsetStore()
	store.Commit("my-group", "orders", 0, 42, "checkpoint-a")
	store.Commit("my-group", "orders", 1, 7, "")
	store.Commit("my-group", "payments", 0, 3, "checkpoint-p")
	store.Commit("other-group", "orders", 0, 999, "not-this-group")

	resp, err := HandleOffsetFetch(1, encodeOffsetFetchRequestAllTopics("my-group"), store)
	if err != nil {
		t.Fatalf("HandleOffsetFetch: %v", err)
	}

	partitions, topLevelErrorCode := decodeOffsetFetchResponse(t, resp)
	if topLevelErrorCode != ErrNone {
		t.Errorf("top-level error_code = %d, want %d", topLevelErrorCode, ErrNone)
	}
	if len(partitions) != 3 {
		t.Fatalf("got %d partitions, want 3: %+v", len(partitions), partitions)
	}

	want := map[string]int64{"orders-0": 42, "orders-1": 7, "payments-0": 3}
	for _, p := range partitions {
		key := p.Topic + "-" + fmt.Sprint(p.Partition)
		wantOffset, ok := want[key]
		if !ok {
			t.Errorf("unexpected partition in response: %+v", p)
			continue
		}
		if p.Offset != wantOffset || p.ErrorCode != ErrNone {
			t.Errorf("%s: offset, error_code = %d, %d, want %d, %d", key, p.Offset, p.ErrorCode, wantOffset, ErrNone)
		}
	}
}

// TestHandleOffsetFetch_NullTopicsForGroupWithNoCommitsReturnsEmpty makes
// sure "nothing committed yet" answers with zero topics, not an error and
// not a nil-vs-empty encoding mismatch.
func TestHandleOffsetFetch_NullTopicsForGroupWithNoCommitsReturnsEmpty(t *testing.T) {
	store := group.NewInMemoryOffsetStore()

	resp, err := HandleOffsetFetch(1, encodeOffsetFetchRequestAllTopics("never-heard-of-it"), store)
	if err != nil {
		t.Fatalf("HandleOffsetFetch: %v", err)
	}

	partitions, topLevelErrorCode := decodeOffsetFetchResponse(t, resp)
	if len(partitions) != 0 {
		t.Errorf("got %d partitions, want 0: %+v", len(partitions), partitions)
	}
	if topLevelErrorCode != ErrNone {
		t.Errorf("top-level error_code = %d, want %d", topLevelErrorCode, ErrNone)
	}
}
