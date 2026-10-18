package protocol

import (
	"fmt"
	"log"

	"github.com/AfzalRaja001/kafka-go/internal/storage"
)

type DeleteTopicsRequest struct {
	TopicNames []string
	TimeoutMs  int32
}

// DecodeDeleteTopicsRequest decodes a DeleteTopics v0 request body.
func DecodeDeleteTopicsRequest(buf []byte) (DeleteTopicsRequest, error) {
	dec := NewDecoder(buf)

	names, err := dec.StringArray()
	if err != nil {
		return DeleteTopicsRequest{}, fmt.Errorf("topic_names: %w", err)
	}
	timeoutMs, err := dec.Int32()
	if err != nil {
		return DeleteTopicsRequest{}, fmt.Errorf("timeout_ms: %w", err)
	}

	return DeleteTopicsRequest{TopicNames: names, TimeoutMs: timeoutMs}, nil
}

// HandleDeleteTopics builds a DeleteTopics v0 response body, removing each
// requested topic's partitions from storage and the registry.
func HandleDeleteTopics(correlationID int32, requestBody []byte, registry *TopicRegistry, diskLog storage.Log) ([]byte, error) {
	req, err := DecodeDeleteTopicsRequest(requestBody)
	if err != nil {
		return nil, fmt.Errorf("delete_topics request: %w", err)
	}

	enc := NewEncoder()
	enc.Int32(correlationID)
	enc.Int32(int32(len(req.TopicNames)))

	for _, name := range req.TopicNames {
		errorCode := deleteOneTopic(registry, diskLog, name)
		enc.String(name)
		enc.Int16(errorCode)
	}

	return enc.Result(), nil
}

// deleteOneTopic removes a single requested topic, or reports why it
// couldn't. Storage is deleted before the registry entry, so a crash
// mid-delete leaves a registered topic pointing at partially-removed
// storage (the safer failure mode) rather than an unregistered topic whose
// files still exist and can never be reached again. Now that the topic list
// is saved to disk, that same state survives a restart; the broker repairs
// it at startup (cmd/broker's ensureStorage re-creates the missing
// partitions empty), leaving a topic the client can simply delete again.
func deleteOneTopic(registry *TopicRegistry, diskLog storage.Log, name string) int16 {
	topic, exists := registry.Get(name)
	if !exists {
		return ErrUnknownTopicOrPartition
	}

	for _, p := range topic.Partitions {
		if err := diskLog.DeletePartition(name, p.ID); err != nil {
			return ErrUnknownServerError
		}
	}

	if err := registry.RemoveTopic(name); err != nil {
		log.Printf("delete topic %q: saving the topic list failed: %v", name, err)
		return ErrUnknownServerError
	}
	return ErrNone
}
