package protocol

import "testing"

func encodeTopicsRequest(topics []string) []byte {
	enc := NewEncoder()
	enc.StringArray(topics)
	return enc.Result()
}

func TestHandleMetadata_KnownTopic(t *testing.T) {
	registry := NewTopicRegistry()
	registry.AddTopic(&Topic{
		Name: "orders",
		Partitions: []PartitionMetadata{
			{ID: 0, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}},
		},
	})
	brokers := []Broker{{NodeID: 1, Host: "localhost", Port: 9092}}

	resp, err := HandleMetadata(99, encodeTopicsRequest([]string{"orders"}), registry, brokers)
	if err != nil {
		t.Fatalf("HandleMetadata: %v", err)
	}
	dec := NewDecoder(resp)

	correlationID, _ := dec.Int32()
	if correlationID != 99 {
		t.Fatalf("correlation_id = %d, want 99", correlationID)
	}

	brokerCount, _ := dec.Int32()
	if brokerCount != 1 {
		t.Fatalf("broker count = %d, want 1", brokerCount)
	}
	nodeID, _ := dec.Int32()
	host, _ := dec.String()
	port, _ := dec.Int32()
	if nodeID != 1 || host != "localhost" || port != 9092 {
		t.Fatalf("broker = (%d, %q, %d), want (1, localhost, 9092)", nodeID, host, port)
	}

	topicCount, _ := dec.Int32()
	if topicCount != 1 {
		t.Fatalf("topic count = %d, want 1", topicCount)
	}
	topicErr, _ := dec.Int16()
	topicName, _ := dec.String()
	if topicErr != ErrNone || topicName != "orders" {
		t.Fatalf("topic = (%d, %q), want (0, orders)", topicErr, topicName)
	}
	partCount, _ := dec.Int32()
	if partCount != 1 {
		t.Fatalf("partition count = %d, want 1", partCount)
	}
	partErr, _ := dec.Int16()
	partID, _ := dec.Int32()
	leader, _ := dec.Int32()
	if partErr != ErrNone || partID != 0 || leader != 1 {
		t.Fatalf("partition = (%d, %d, %d), want (0, 0, 1)", partErr, partID, leader)
	}
	replicaCount, _ := dec.Int32()
	if replicaCount != 1 {
		t.Fatalf("replica count = %d, want 1", replicaCount)
	}
}

func TestHandleMetadata_UnknownTopic(t *testing.T) {
	registry := NewTopicRegistry()

	resp, err := HandleMetadata(1, encodeTopicsRequest([]string{"missing"}), registry, nil)
	if err != nil {
		t.Fatalf("HandleMetadata: %v", err)
	}
	dec := NewDecoder(resp)

	dec.Int32() // correlation_id
	dec.Int32() // broker count (0)
	topicCount, _ := dec.Int32()
	if topicCount != 1 {
		t.Fatalf("topic count = %d, want 1", topicCount)
	}
	topicErr, _ := dec.Int16()
	topicName, _ := dec.String()
	if topicErr != ErrUnknownTopicOrPartition || topicName != "missing" {
		t.Fatalf("topic = (%d, %q), want (%d, missing)", topicErr, topicName, ErrUnknownTopicOrPartition)
	}
}

func TestHandleMetadata_NilRequestedTopicsReturnsAll(t *testing.T) {
	registry := NewTopicRegistry()
	registry.AddTopic(&Topic{Name: "orders"})
	registry.AddTopic(&Topic{Name: "payments"})

	resp, err := HandleMetadata(1, encodeTopicsRequest(nil), registry, nil)
	if err != nil {
		t.Fatalf("HandleMetadata: %v", err)
	}
	dec := NewDecoder(resp)

	dec.Int32() // correlation_id
	dec.Int32() // broker count
	topicCount, _ := dec.Int32()
	if topicCount != 2 {
		t.Fatalf("topic count = %d, want 2", topicCount)
	}
}

func TestHandleMetadata_MalformedRequest(t *testing.T) {
	registry := NewTopicRegistry()

	_, err := HandleMetadata(1, []byte{0, 0}, registry, nil)
	if err == nil {
		t.Fatal("expected an error for a truncated request body, got nil")
	}
}
