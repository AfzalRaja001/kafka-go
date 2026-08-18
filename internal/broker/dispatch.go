package broker

import (
	"fmt"

	"github.com/AfzalRaja001/kafka-go/internal/protocol"
	"github.com/AfzalRaja001/kafka-go/internal/storage"
)

// dispatch decodes the request header every Kafka request shares
// (api_key, api_version, correlation_id, client_id), then routes to
// whichever handler owns that api_key, passing it whatever bytes remain.
func dispatch(msg []byte, registry *protocol.TopicRegistry, brokers []protocol.Broker, log storage.Log) ([]byte, error) {
	dec := protocol.NewDecoder(msg)

	apiKey, err := dec.Int16()
	if err != nil {
		return nil, fmt.Errorf("decode api_key: %w", err)
	}
	apiVersion, err := dec.Int16()
	if err != nil {
		return nil, fmt.Errorf("decode api_version: %w", err)
	}
	correlationID, err := dec.Int32()
	if err != nil {
		return nil, fmt.Errorf("decode correlation_id: %w", err)
	}
	if _, err := dec.NullableString(); err != nil { // client_id: decoded, not used yet
		return nil, fmt.Errorf("decode client_id: %w", err)
	}

	switch apiKey {
	case protocol.ApiKeyApiVersions:
		return protocol.HandleApiVersions(correlationID, apiVersion), nil
	case protocol.ApiKeyMetadata:
		return protocol.HandleMetadata(correlationID, dec.Remaining(), registry, brokers)
	case protocol.ApiKeyProduce:
		return protocol.HandleProduce(correlationID, dec.Remaining(), log)
	case protocol.ApiKeyFetch:
		return protocol.HandleFetch(correlationID, dec.Remaining(), log)
	case protocol.ApiKeyListOffsets:
		return protocol.HandleListOffsets(correlationID, dec.Remaining(), log)
	default:
		return nil, fmt.Errorf("unsupported api_key %d", apiKey)
	}
}
