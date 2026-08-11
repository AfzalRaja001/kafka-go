package broker

import (
	"testing"

	"github.com/AfzalRaja001/kafka-go/internal/protocol"
)

func encodeRequestHeader(apiKey, apiVersion int16, correlationID int32, clientID string) *protocol.Encoder {
	enc := protocol.NewEncoder()
	enc.Int16(apiKey)
	enc.Int16(apiVersion)
	enc.Int32(correlationID)
	enc.NullableString(&clientID)
	return enc
}

func TestDispatch_ApiVersions(t *testing.T) {
	req := encodeRequestHeader(protocol.ApiKeyApiVersions, 0, 42, "kcat").Result()

	resp, err := dispatch(req, protocol.NewTopicRegistry(), nil)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	dec := protocol.NewDecoder(resp)
	correlationID, _ := dec.Int32()
	if correlationID != 42 {
		t.Errorf("correlation_id = %d, want 42", correlationID)
	}
}

func TestDispatch_Metadata(t *testing.T) {
	registry := protocol.NewTopicRegistry()
	registry.AddTopic(&protocol.Topic{Name: "orders"})

	enc := encodeRequestHeader(protocol.ApiKeyMetadata, 0, 7, "kcat")
	enc.StringArray([]string{"orders"})
	req := enc.Result()

	resp, err := dispatch(req, registry, nil)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	dec := protocol.NewDecoder(resp)
	correlationID, _ := dec.Int32()
	if correlationID != 7 {
		t.Errorf("correlation_id = %d, want 7", correlationID)
	}
}

func TestDispatch_UnsupportedApiKey(t *testing.T) {
	req := encodeRequestHeader(999, 0, 1, "kcat").Result()

	_, err := dispatch(req, protocol.NewTopicRegistry(), nil)
	if err == nil {
		t.Fatal("expected an error for an unsupported api_key, got nil")
	}
}

func TestDispatch_TruncatedRequest(t *testing.T) {
	_, err := dispatch([]byte{0, 18}, protocol.NewTopicRegistry(), nil) // only 2 bytes
	if err == nil {
		t.Fatal("expected an error for a truncated request, got nil")
	}
}
