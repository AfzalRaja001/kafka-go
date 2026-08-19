package protocol

import (
	"testing"

	"github.com/AfzalRaja001/kafka-go/internal/storage"
)

// encodeCreateTopicsRequest builds a CreateTopics v0 request with one topic
// and empty assignments/configs arrays - what a client sends for automatic
// partition assignment and no custom topic configs, the common case.
func encodeCreateTopicsRequest(name string, numPartitions int32, replicationFactor int16) []byte {
	enc := NewEncoder()
	enc.Int32(1) // topic count
	enc.String(name)
	enc.Int32(numPartitions)
	enc.Int16(replicationFactor)
	enc.Int32(0)    // assignments: empty array
	enc.Int32(0)    // configs: empty array
	enc.Int32(5000) // timeout_ms
	return enc.Result()
}

func TestDecodeCreateTopicsRequest(t *testing.T) {
	body := encodeCreateTopicsRequest("orders", 3, 1)

	req, err := DecodeCreateTopicsRequest(body)
	if err != nil {
		t.Fatalf("DecodeCreateTopicsRequest: %v", err)
	}
	if len(req.Topics) != 1 {
		t.Fatalf("Topics = %+v, want one topic", req.Topics)
	}
	topic := req.Topics[0]
	if topic.Name != "orders" || topic.NumPartitions != 3 || topic.ReplicationFactor != 1 {
		t.Fatalf("topic = %+v, want {orders, 3, 1}", topic)
	}
	if req.TimeoutMs != 5000 {
		t.Fatalf("TimeoutMs = %d, want 5000", req.TimeoutMs)
	}
}

// TestDecodeCreateTopicsRequest_DiscardsAssignmentsAndConfigs proves the
// decoder correctly consumes non-empty assignments/configs arrays rather
// than assuming every client sends them empty - getting this wrong would
// desync framing for whatever follows, exactly the class of bug the
// project's own docs/issues.md warns about for encoding mistakes.
func TestDecodeCreateTopicsRequest_DiscardsAssignmentsAndConfigs(t *testing.T) {
	enc := NewEncoder()
	enc.Int32(1) // topic count
	enc.String("orders")
	enc.Int32(-1) // num_partitions: -1, manual assignment supplies it instead
	enc.Int16(-1) // replication_factor: -1, manual assignment supplies it instead
	enc.Int32(1)  // assignments: one entry
	enc.Int32(0)  // partition_index 0
	enc.Int32(1)  // broker_ids: one entry
	enc.Int32(1)  // broker id 1
	enc.Int32(1)  // configs: one entry
	enc.String("retention.ms")
	enc.String("60000")
	enc.Int32(5000) // timeout_ms
	body := enc.Result()

	req, err := DecodeCreateTopicsRequest(body)
	if err != nil {
		t.Fatalf("DecodeCreateTopicsRequest: %v", err)
	}
	if req.TimeoutMs != 5000 {
		t.Fatalf("TimeoutMs = %d, want 5000 (framing must survive past assignments/configs)", req.TimeoutMs)
	}
}

func decodeCreateTopicsResponseOneTopic(t *testing.T, resp []byte) (name string, errorCode int16) {
	t.Helper()
	dec := NewDecoder(resp)
	dec.Int32() // correlation_id
	dec.Int32() // topic count
	name, _ = dec.String()
	errorCode, _ = dec.Int16()
	return name, errorCode
}

func TestHandleCreateTopics_CreatesNewTopic(t *testing.T) {
	registry := NewTopicRegistry()
	log := storage.NewFakeLog()

	body := encodeCreateTopicsRequest("orders", 3, 1)
	resp, err := HandleCreateTopics(1, body, registry, log)
	if err != nil {
		t.Fatalf("HandleCreateTopics: %v", err)
	}

	name, errorCode := decodeCreateTopicsResponseOneTopic(t, resp)
	if name != "orders" || errorCode != ErrNone {
		t.Fatalf("response = (%q, %d), want (orders, %d)", name, errorCode, ErrNone)
	}

	topic, ok := registry.Get("orders")
	if !ok {
		t.Fatal("orders should be registered after CreateTopics")
	}
	if len(topic.Partitions) != 3 {
		t.Fatalf("Partitions = %+v, want 3 partitions", topic.Partitions)
	}
	for i, p := range topic.Partitions {
		if p.ID != int32(i) || p.Leader != 1 || len(p.Replicas) != 1 || p.Replicas[0] != 1 {
			t.Errorf("partition %d = %+v, want {ID:%d, Leader:1, Replicas:[1]}", i, p, i)
		}
		// Eager provisioning: storage must exist too, not just the registry.
		latest, err := log.LatestOffset("orders", int32(i))
		if err != nil || latest != 0 {
			t.Errorf("LatestOffset(orders, %d) = %d, %v, want 0, nil", i, latest, err)
		}
	}
}

func TestHandleCreateTopics_AlreadyExistsReturnsError(t *testing.T) {
	registry := NewTopicRegistry()
	registry.AddTopic(&Topic{Name: "orders"})
	log := storage.NewFakeLog()

	body := encodeCreateTopicsRequest("orders", 1, 1)
	resp, err := HandleCreateTopics(1, body, registry, log)
	if err != nil {
		t.Fatalf("HandleCreateTopics: %v", err)
	}

	name, errorCode := decodeCreateTopicsResponseOneTopic(t, resp)
	if name != "orders" || errorCode != ErrTopicAlreadyExists {
		t.Fatalf("response = (%q, %d), want (orders, %d)", name, errorCode, ErrTopicAlreadyExists)
	}
}

func TestHandleCreateTopics_InvalidPartitionsReturnsError(t *testing.T) {
	registry := NewTopicRegistry()
	log := storage.NewFakeLog()

	body := encodeCreateTopicsRequest("orders", 0, 1)
	resp, err := HandleCreateTopics(1, body, registry, log)
	if err != nil {
		t.Fatalf("HandleCreateTopics: %v", err)
	}

	name, errorCode := decodeCreateTopicsResponseOneTopic(t, resp)
	if name != "orders" || errorCode != ErrInvalidPartitions {
		t.Fatalf("response = (%q, %d), want (orders, %d)", name, errorCode, ErrInvalidPartitions)
	}
	if _, ok := registry.Get("orders"); ok {
		t.Error("orders should not be registered after an invalid CreateTopics request")
	}
}
