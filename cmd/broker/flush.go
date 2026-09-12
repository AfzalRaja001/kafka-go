package main

import (
	"context"
	"time"

	"github.com/AfzalRaja001/kafka-go/internal/protocol"
	"github.com/AfzalRaja001/kafka-go/internal/storage"
)

// flushPartitions walks every known topic-partition, fsync-ing its active
// segment - the time-based half of the fsync policy, the same "plain
// function, no ticker" shape collectMetrics and applyRetention already
// have, for the same reason: independently unit-testable with fakes.
//
// This is the reverse case from the count-based half already wired into
// Partition.Append: a low-traffic partition that never appends enough
// records to trip its own count threshold would otherwise sit unflushed
// indefinitely. This bounds that by time instead.
//
// A per-partition error is skipped, not fatal, matching collectMetrics and
// applyRetention's own defensiveness - this runs forever on a ticker, so
// one bad partition must never take the whole sweep down.
func flushPartitions(registry *protocol.TopicRegistry, log storage.Log) {
	for _, topic := range registry.All() {
		for _, partition := range topic.Partitions {
			if err := log.Sync(topic.Name, partition.ID); err != nil {
				continue
			}
		}
	}
}

// runFlush calls flushPartitions on a ticker until ctx is canceled - the
// same shape runRetention and runMetricsCollector already have.
func runFlush(ctx context.Context, interval time.Duration, registry *protocol.TopicRegistry, log storage.Log) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			flushPartitions(registry, log)
		}
	}
}
