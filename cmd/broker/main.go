package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/AfzalRaja001/kafka-go/internal/broker"
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
)

func main() {
	diskLog := storage.NewDiskLog(dataDir, segmentMaxBytes, indexEvery)
	defer diskLog.Close()

	registry := protocol.NewTopicRegistry()
	// Hardcoded per the build plan's Phase 1 instructions - there's no
	// CreateTopics yet, so a fake topic is what makes `kcat -L` show
	// something meaningful.
	registry.AddTopic(&protocol.Topic{
		Name: "test-topic",
		Partitions: []protocol.PartitionMetadata{
			{ID: 0, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}},
		},
	})

	brokers := []protocol.Broker{
		{NodeID: 1, Host: "localhost", Port: 9092},
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	log.Printf("kafka-go broker listening on %s", listenAddr)
	if err := broker.Serve(ctx, listenAddr, registry, brokers, diskLog); err != nil {
		log.Fatalf("broker: %v", err)
	}
	log.Println("kafka-go broker stopped")
}
