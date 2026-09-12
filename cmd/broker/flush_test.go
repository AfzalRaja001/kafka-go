package main

import (
	"testing"

	"github.com/AfzalRaja001/kafka-go/internal/protocol"
	"github.com/AfzalRaja001/kafka-go/internal/storage"
)

func TestFlushPartitions_SyncsEveryKnownPartition(t *testing.T) {
	registry := protocol.NewTopicRegistry()
	registry.AddTopic(&protocol.Topic{
		Name:       "orders",
		Partitions: []protocol.PartitionMetadata{{ID: 0}, {ID: 1}},
	})

	log := storage.NewFakeLog()
	flushPartitions(registry, log)

	if got := log.SyncCallCount("orders", 0); got != 1 {
		t.Errorf("SyncCallCount(orders, 0) = %d, want 1", got)
	}
	if got := log.SyncCallCount("orders", 1); got != 1 {
		t.Errorf("SyncCallCount(orders, 1) = %d, want 1", got)
	}
}

// TestFlushPartitions_SkipsPartitionRegistryKnowsAboutButLogDoesNot mirrors
// applyRetention's own test for the same defensive behavior: one partition
// the registry knows about but the log has never heard of must not crash
// the whole sweep, since this runs forever on a ticker.
func TestFlushPartitions_SkipsPartitionRegistryKnowsAboutButLogDoesNot(t *testing.T) {
	registry := protocol.NewTopicRegistry()
	registry.AddTopic(&protocol.Topic{
		Name:       "ghost-topic",
		Partitions: []protocol.PartitionMetadata{{ID: 0}},
	})

	// Must not panic - FakeLog.Sync already reports "unknown partition" as
	// a no-op, not an error, but flushPartitions itself must tolerate an
	// error here regardless, matching applyRetention's own defensiveness.
	flushPartitions(registry, storage.NewFakeLog())
}
