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

	// No hardcoded topic anymore: CreateTopics (api_key 19) now provisions
	// topics for real, in both the registry and storage. A fresh broker
	// starts with none, matching real Kafka.
	registry := protocol.NewTopicRegistry()

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
