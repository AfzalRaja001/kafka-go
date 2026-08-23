package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/AfzalRaja001/kafka-go/internal/broker"
	"github.com/AfzalRaja001/kafka-go/internal/group"
	"github.com/AfzalRaja001/kafka-go/internal/protocol"
	"github.com/AfzalRaja001/kafka-go/internal/storage"
)

const (
	listenAddr = ":9092"
	dataDir    = "data"

	// segmentMaxBytes and indexEvery are deliberately small for now, while
	// this is being tested at hand-run scale - real Kafka defaults to
	// ~1GB segments. Worth revisiting once there's an actual reason to
	// (a benchmark, a real workload) rather than guessing at a number now.
	segmentMaxBytes = 16 * 1024 * 1024
	indexEvery      = 100

	// reapInterval is how often the broker checks every consumer group for
	// members that have gone silent past their session timeout. It doesn't
	// need to be fine-grained - a member's actual eviction can lag its true
	// expiry by up to this long, which is fine given session timeouts
	// themselves are measured in seconds.
	reapInterval = 1 * time.Second

	// initialRebalanceDelay matches real Kafka's own default for
	// group.initial.rebalance.delay.ms: how long a fresh group's join
	// window stays open for other members before finalizing with whoever
	// showed up. Deliberately not derived from any client's session
	// timeout - see the comment on group.NewCoordinator.
	initialRebalanceDelay = 3 * time.Second
)

func main() {
	diskLog := storage.NewDiskLog(dataDir, segmentMaxBytes, indexEvery)
	defer diskLog.Close()

	// No hardcoded topic anymore: CreateTopics (api_key 19) now provisions
	// topics for real, in both the registry and storage. A fresh broker
	// starts with none, matching real Kafka.
	registry := protocol.NewTopicRegistry()

	brokers := []protocol.Broker{
		{NodeID: 1, Host: "localhost", Port: 9092},
	}

	offsets := group.NewInMemoryOffsetStore()
	coord := group.NewCoordinator(initialRebalanceDelay)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	go runReaper(ctx, coord)

	log.Printf("kafka-go broker listening on %s", listenAddr)
	if err := broker.Serve(ctx, listenAddr, registry, brokers, diskLog, offsets, coord); err != nil {
		log.Fatalf("broker: %v", err)
	}
	log.Println("kafka-go broker stopped")
}

// runReaper periodically evicts consumer group members that have gone
// silent past their session timeout, until ctx is canceled.
func runReaper(ctx context.Context, coord *group.Coordinator) {
	ticker := time.NewTicker(reapInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			coord.ReapExpiredMembers(now)
		}
	}
}
