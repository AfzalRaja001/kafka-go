package broker

import (
	"net"
	"testing"

	"github.com/AfzalRaja001/kafka-go/internal/protocol"
)

func TestHandleConn_ApiVersionsRoundTrip(t *testing.T) {
	client, server := net.Pipe()

	registry := protocol.NewTopicRegistry()
	brokers := []protocol.Broker{{NodeID: 1, Host: "localhost", Port: 9092}}

	go handleConn(server, registry, brokers)
	defer client.Close()

	req := encodeRequestHeader(protocol.ApiKeyApiVersions, 0, 55, "kcat").Result()
	if err := WriteMessage(client, req); err != nil {
		t.Fatalf("write request: %v", err)
	}

	resp, err := ReadMessage(client)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	dec := protocol.NewDecoder(resp)
	correlationID, _ := dec.Int32()
	if correlationID != 55 {
		t.Fatalf("correlation_id = %d, want 55", correlationID)
	}
	errorCode, _ := dec.Int16()
	if errorCode != protocol.ErrNone {
		t.Fatalf("error_code = %d, want %d", errorCode, protocol.ErrNone)
	}
}

func TestHandleConn_MultipleRequestsOnOneConnection(t *testing.T) {
	client, server := net.Pipe()

	registry := protocol.NewTopicRegistry()
	registry.AddTopic(&protocol.Topic{Name: "orders"})

	go handleConn(server, registry, nil)
	defer client.Close()

	// First request: ApiVersions.
	req1 := encodeRequestHeader(protocol.ApiKeyApiVersions, 0, 1, "kcat").Result()
	if err := WriteMessage(client, req1); err != nil {
		t.Fatalf("write request 1: %v", err)
	}
	if _, err := ReadMessage(client); err != nil {
		t.Fatalf("read response 1: %v", err)
	}

	// Second request on the same connection: Metadata.
	enc := encodeRequestHeader(protocol.ApiKeyMetadata, 0, 2, "kcat")
	enc.StringArray([]string{"orders"})
	if err := WriteMessage(client, enc.Result()); err != nil {
		t.Fatalf("write request 2: %v", err)
	}
	resp2, err := ReadMessage(client)
	if err != nil {
		t.Fatalf("read response 2: %v", err)
	}

	dec := protocol.NewDecoder(resp2)
	correlationID, _ := dec.Int32()
	if correlationID != 2 {
		t.Fatalf("correlation_id = %d, want 2", correlationID)
	}
}
