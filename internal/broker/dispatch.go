package broker

import (
	"fmt"
	"log"
	"runtime/debug"
	"time"

	"github.com/AfzalRaja001/kafka-go/internal/group"
	"github.com/AfzalRaja001/kafka-go/internal/metrics"
	"github.com/AfzalRaja001/kafka-go/internal/protocol"
	"github.com/AfzalRaja001/kafka-go/internal/storage"
)

// dispatch decodes the request header every Kafka request shares
// (api_key, api_version, correlation_id, client_id), then routes to
// whichever handler owns that api_key, passing it whatever bytes remain.
// Named returns (resp, err) exist so the deferred recorder.RecordRequest
// call below can observe whichever switch case actually ran, without every
// one of the 14 cases needing to record anything itself.
func dispatch(msg []byte, registry *protocol.TopicRegistry, brokers []protocol.Broker, diskLog storage.Log, offsets group.OffsetStore, coord *group.Coordinator, recorder *metrics.Recorder) (resp []byte, err error) {
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

	// Only requests with a successfully decoded api_key get recorded - a
	// header too short to even contain one leaves handleConn closing the
	// connection immediately anyway, so there's no api_key to label a
	// metric with and no ongoing signal worth reporting.
	start := time.Now()
	defer func() {
		recorder.RecordRequest(protocol.ApiKeyName(apiKey), time.Since(start), len(msg), len(resp), err)
	}()

	// Deferred calls run last-registered-first, so this runs before the
	// recorder above - turning a panic into err first means the recorder
	// counts the request as an error instead of a success. Without it, a
	// panic in any handler would unwind out of handleConn's goroutine and
	// kill the whole process, disconnecting every client over one bad
	// request. handleConn already closes just this connection on any
	// dispatch error.
	//
	// This is a safety net, not error handling: a recovered panic means a
	// bug, and if it happened while a lock was held (say, inside the group
	// coordinator), that lock stays held. It also cannot catch the
	// runtime's "fatal error: out of memory", which is why every
	// client-supplied size is bounded before it's used instead.
	defer func() {
		if r := recover(); r != nil {
			log.Printf("broker: recovered panic handling %s request: %v\n%s", protocol.ApiKeyName(apiKey), r, debug.Stack())
			resp, err = nil, fmt.Errorf("panic handling %s request: %v", protocol.ApiKeyName(apiKey), r)
		}
	}()

	switch apiKey {
	case protocol.ApiKeyApiVersions:
		return protocol.HandleApiVersions(correlationID, apiVersion), nil
	case protocol.ApiKeyMetadata:
		return protocol.HandleMetadata(correlationID, dec.Remaining(), registry, brokers)
	case protocol.ApiKeyProduce:
		return protocol.HandleProduce(correlationID, dec.Remaining(), diskLog)
	case protocol.ApiKeyFetch:
		return protocol.HandleFetch(correlationID, dec.Remaining(), diskLog)
	case protocol.ApiKeyListOffsets:
		return protocol.HandleListOffsets(correlationID, dec.Remaining(), diskLog)
	case protocol.ApiKeyCreateTopics:
		return protocol.HandleCreateTopics(correlationID, dec.Remaining(), registry, diskLog)
	case protocol.ApiKeyDeleteTopics:
		return protocol.HandleDeleteTopics(correlationID, dec.Remaining(), registry, diskLog)
	case protocol.ApiKeyFindCoordinator:
		// This single-node broker is always its own coordinator - self is
		// just the first (only) entry in the broker list, the same
		// "there's only one broker" simplification Metadata's ControllerId
		// uses. An empty list only happens if the broker is misconfigured
		// with no advertised address at all; report NodeId -1 rather than
		// panicking.
		self := protocol.Broker{NodeID: -1}
		if len(brokers) > 0 {
			self = brokers[0]
		}
		return protocol.HandleFindCoordinator(correlationID, dec.Remaining(), self)
	case protocol.ApiKeyOffsetCommit:
		return protocol.HandleOffsetCommit(correlationID, dec.Remaining(), offsets)
	case protocol.ApiKeyOffsetFetch:
		return protocol.HandleOffsetFetch(correlationID, dec.Remaining(), offsets)
	case protocol.ApiKeyJoinGroup:
		return protocol.HandleJoinGroup(correlationID, dec.Remaining(), coord)
	case protocol.ApiKeySyncGroup:
		return protocol.HandleSyncGroup(correlationID, dec.Remaining(), coord)
	case protocol.ApiKeyHeartbeat:
		return protocol.HandleHeartbeat(correlationID, dec.Remaining(), coord)
	case protocol.ApiKeyLeaveGroup:
		return protocol.HandleLeaveGroup(correlationID, dec.Remaining(), coord)
	default:
		return nil, fmt.Errorf("unsupported api_key %d", apiKey)
	}
}
