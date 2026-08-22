package protocol

import (
	"testing"

	"github.com/AfzalRaja001/kafka-go/internal/storage"
)

func encodeDeleteTopicsRequest(topicNames []string) []byte {
	enc := NewEncoder()
	enc.StringArray(topicNames)
	enc.Int32(5000) // timeout_ms
	return enc.Result()
}

func TestDecodeDeleteTopicsRequest(t *testing.T) {
	body := encodeDeleteTopicsRequest([]string{"orders", "payments"})

	req, err := DecodeDeleteTopicsRequest(body)
	if err != nil {
		t.Fatalf("DecodeDeleteTopicsRequest: %v", err)
	}
	if len(req.TopicNames) != 2 || req.TopicNames[0] != "orders" || req.TopicNames[1] != "payments" {
		t.Fatalf("TopicNames = %v, want [orders payments]", req.TopicNames)
	}
	if req.TimeoutMs != 5000 {
		t.Fatalf("TimeoutMs = %d, want 5000", req.TimeoutMs)
	}
}

func decodeDeleteTopicsResponseOneTopic(t *testing.T, resp []byte) (name string, errorCode int16) {
	t.Helper()
	dec := NewDecoder(resp)
	dec.Int32() // correlation_id
	dec.Int32() // response count
	name, _ = dec.String()
	errorCode, _ = dec.Int16()
	return name, errorCode
}

func TestHandleDeleteTopics_DeletesExistingTopic(t *testing.T) {
	registry := NewTopicRegistry()
	log := storage.NewFakeLog()
	registry.AddTopic(&Topic{Name: "orders", Partitions: []PartitionMetadata{
		{ID: 0, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}},
		{ID: 1, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}},
	}})
	log.Append("orders", 0, []byte("data"), 1)
	log.Append("orders", 1, []byte("data"), 1)

	body := encodeDeleteTopicsRequest([]string{"orders"})
	resp, err := HandleDeleteTopics(1, body, registry, log)
	if err != nil {
		t.Fatalf("HandleDeleteTopics: %v", err)
	}

	name, errorCode := decodeDeleteTopicsResponseOneTopic(t, resp)
	if name != "orders" || errorCode != ErrNone {
		t.Fatalf("response = (%q, %d), want (orders, %d)", name, errorCode, ErrNone)
	}

	if _, ok := registry.Get("orders"); ok {
		t.Error("orders should not be registered after DeleteTopics")
	}
	if _, err := log.Read("orders", 0, 0, 1024); err == nil {
		t.Error("partition 0 should be gone from storage after DeleteTopics")
	}
	if _, err := log.Read("orders", 1, 0, 1024); err == nil {
		t.Error("partition 1 should be gone from storage after DeleteTopics")
	}
}

func TestHandleDeleteTopics_UnknownTopicReturnsError(t *testing.T) {
	registry := NewTopicRegistry()
	log := storage.NewFakeLog()

	body := encodeDeleteTopicsRequest([]string{"missing"})
	resp, err := HandleDeleteTopics(1, body, registry, log)
	if err != nil {
		t.Fatalf("HandleDeleteTopics: %v", err)
	}

	name, errorCode := decodeDeleteTopicsResponseOneTopic(t, resp)
	if name != "missing" || errorCode != ErrUnknownTopicOrPartition {
		t.Fatalf("response = (%q, %d), want (missing, %d)", name, errorCode, ErrUnknownTopicOrPartition)
	}
}
