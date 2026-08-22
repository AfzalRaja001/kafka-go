package broker

import (
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/AfzalRaja001/kafka-go/internal/group"
	"github.com/AfzalRaja001/kafka-go/internal/protocol"
	"github.com/AfzalRaja001/kafka-go/internal/storage"
)

// buildMinimalRecordBatch constructs a valid 61-byte record batch header
// (magic 2, correct CRC), no actual record payload - enough for dispatch
// to route to HandleProduce and have it accept the batch.
func buildMinimalRecordBatch() []byte {
	buf := make([]byte, 61)
	buf[16] = 2                               // magic
	binary.BigEndian.PutUint32(buf[57:61], 0) // recordCount
	crc := crc32.Checksum(buf[21:], crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(buf[17:21], crc)
	return buf
}

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

	resp, err := dispatch(req, protocol.NewTopicRegistry(), nil, storage.NewFakeLog(), group.NewInMemoryOffsetStore())
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

	resp, err := dispatch(req, registry, nil, storage.NewFakeLog(), group.NewInMemoryOffsetStore())
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	dec := protocol.NewDecoder(resp)
	correlationID, _ := dec.Int32()
	if correlationID != 7 {
		t.Errorf("correlation_id = %d, want 7", correlationID)
	}
}

func TestDispatch_Produce(t *testing.T) {
	enc := encodeRequestHeader(protocol.ApiKeyProduce, 3, 9, "kcat")
	var txnID *string
	enc.NullableString(txnID)
	enc.Int16(1) // acks
	enc.Int32(0) // timeout_ms
	enc.Int32(1) // topic count
	enc.String("orders")
	enc.Int32(1) // partition count
	enc.Int32(0) // partition
	enc.Bytes(buildMinimalRecordBatch())

	resp, err := dispatch(enc.Result(), protocol.NewTopicRegistry(), nil, storage.NewFakeLog(), group.NewInMemoryOffsetStore())
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	dec := protocol.NewDecoder(resp)
	correlationID, _ := dec.Int32()
	if correlationID != 9 {
		t.Errorf("correlation_id = %d, want 9", correlationID)
	}
}

func TestDispatch_FindCoordinator(t *testing.T) {
	enc := encodeRequestHeader(protocol.ApiKeyFindCoordinator, 0, 13, "kcat")
	enc.String("my-group")
	brokers := []protocol.Broker{{NodeID: 1, Host: "localhost", Port: 9092}}

	resp, err := dispatch(enc.Result(), protocol.NewTopicRegistry(), brokers, storage.NewFakeLog(), group.NewInMemoryOffsetStore())
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	dec := protocol.NewDecoder(resp)
	correlationID, _ := dec.Int32()
	if correlationID != 13 {
		t.Errorf("correlation_id = %d, want 13", correlationID)
	}
}

func TestDispatch_OffsetCommit(t *testing.T) {
	enc := encodeRequestHeader(protocol.ApiKeyOffsetCommit, 0, 14, "kcat")
	enc.String("my-group")
	enc.Int32(1) // topic count
	enc.String("orders")
	enc.Int32(1) // partition count
	enc.Int32(0) // partition
	enc.Int64(42)
	enc.NullableString(nil)

	resp, err := dispatch(enc.Result(), protocol.NewTopicRegistry(), nil, storage.NewFakeLog(), group.NewInMemoryOffsetStore())
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	dec := protocol.NewDecoder(resp)
	correlationID, _ := dec.Int32()
	if correlationID != 14 {
		t.Errorf("correlation_id = %d, want 14", correlationID)
	}
}

func TestDispatch_OffsetFetch(t *testing.T) {
	enc := encodeRequestHeader(protocol.ApiKeyOffsetFetch, 0, 15, "kcat")
	enc.String("my-group")
	enc.Int32(1) // topic count
	enc.String("orders")
	enc.Int32(1) // partition count
	enc.Int32(0) // partition

	resp, err := dispatch(enc.Result(), protocol.NewTopicRegistry(), nil, storage.NewFakeLog(), group.NewInMemoryOffsetStore())
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	dec := protocol.NewDecoder(resp)
	correlationID, _ := dec.Int32()
	if correlationID != 15 {
		t.Errorf("correlation_id = %d, want 15", correlationID)
	}
}

func TestDispatch_UnsupportedApiKey(t *testing.T) {
	req := encodeRequestHeader(999, 0, 1, "kcat").Result()

	_, err := dispatch(req, protocol.NewTopicRegistry(), nil, storage.NewFakeLog(), group.NewInMemoryOffsetStore())
	if err == nil {
		t.Fatal("expected an error for an unsupported api_key, got nil")
	}
}

func TestDispatch_TruncatedRequest(t *testing.T) {
	_, err := dispatch([]byte{0, 18}, protocol.NewTopicRegistry(), nil, storage.NewFakeLog(), group.NewInMemoryOffsetStore()) // only 2 bytes
	if err == nil {
		t.Fatal("expected an error for a truncated request, got nil")
	}
}
