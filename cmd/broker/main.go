package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/AfzalRaja001/kafka-go/internal/broker"
	"github.com/AfzalRaja001/kafka-go/internal/protocol"
)

const listenAddr = ":9092"

func main() {
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
	if err := broker.Serve(ctx, listenAddr, registry, brokers); err != nil {
		log.Fatalf("broker: %v", err)
	}
	log.Println("kafka-go broker stopped")
}
