package main

import (
	"context"
	"time"

	"github.com/AfzalRaja001/kafka-go/internal/group"
	"github.com/AfzalRaja001/kafka-go/internal/metrics"
	"github.com/AfzalRaja001/kafka-go/internal/protocol"
	"github.com/AfzalRaja001/kafka-go/internal/storage"
)

// collectMetrics makes one full pass over every known topic-partition and
// every known consumer group, recording partition sizes and consumer group
// lag. It's a plain function - no ticker, no goroutine - so it can be unit
// tested directly with fakes, the same shape group.Coordinator.
// ReapExpiredMembers already has: the interesting logic is a pure function,
// and runMetricsCollector below is just a thin ticker wrapping it.
//
// A partition the registry knows about but the log doesn't (or any other
// per-partition error) is skipped rather than treated as fatal - this runs
// forever on a ticker, so one bad partition must never take the whole loop
// down.
func collectMetrics(registry *protocol.TopicRegistry, log storage.Log, offsetStore group.OffsetStore, recorder *metrics.Recorder) {
	for _, topic := range registry.All() {
		for _, partition := range topic.Partitions {
			size, err := log.Size(topic.Name, partition.ID)
			if err != nil {
				continue
			}
			recorder.SetPartitionBytes(topic.Name, partition.ID, size)
		}
	}

	for _, groupID := range offsetStore.Groups() {
		for _, committed := range offsetStore.FetchAll(groupID) {
			latest, err := log.LatestOffset(committed.Topic, committed.Partition)
			if err != nil {
				continue
			}
			recorder.SetConsumerGroupLag(groupID, committed.Topic, committed.Partition, latest-committed.Offset)
		}
	}
}

// runMetricsCollector calls collectMetrics on a ticker until ctx is
// canceled - the same shape runReaper already has for
// Coordinator.ReapExpiredMembers.
func runMetricsCollector(ctx context.Context, interval time.Duration, registry *protocol.TopicRegistry, log storage.Log, offsetStore group.OffsetStore, recorder *metrics.Recorder) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			collectMetrics(registry, log, offsetStore, recorder)
		}
	}
}
