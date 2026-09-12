package main

import (
	"fmt"
	"strconv"

	"github.com/AfzalRaja001/kafka-go/internal/protocol"
)

// Defaults for every field brokerConfigFromEnv can override - these match
// exactly what main.go hardcoded before this change, so a broker started
// with no env vars set behaves identically to before.
const (
	defaultNodeID         = 1
	defaultAdvertisedHost = "localhost"
	defaultAdvertisedPort = 9092
)

// brokerConfigFromEnv builds the single protocol.Broker this process
// advertises in Metadata and FindCoordinator responses, reading overrides
// from environment variables via getenv (os.Getenv in main, a fake in
// tests - the same dependency-injection shape collectMetrics and
// applyRetention already use for testability).
//
// This is the fix for docs/plan.md gap 5: main.go used to hardcode
// {NodeID: 1, Host: "localhost", Port: 9092} into every response, which
// works only when the client runs on the same machine as the broker. Real
// Kafka calls this split "listeners" (what the socket binds to) vs.
// "advertised.listeners" (what clients are told to connect back to) -
// listenAddr already binds every interface (":9092" in Go binds 0.0.0.0
// implicitly), so the only missing piece was making the advertised side
// configurable.
//
// An env var that's set but fails to parse is treated as a startup error,
// not silently replaced by its default - a typo in KAFKA_ADVERTISED_PORT
// should fail loudly at boot, not quietly advertise the wrong port to
// every client that connects.
func brokerConfigFromEnv(getenv func(string) string) (protocol.Broker, error) {
	broker := protocol.Broker{
		NodeID: defaultNodeID,
		Host:   defaultAdvertisedHost,
		Port:   defaultAdvertisedPort,
	}

	if v := getenv("KAFKA_NODE_ID"); v != "" {
		nodeID, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			return protocol.Broker{}, fmt.Errorf("KAFKA_NODE_ID: %w", err)
		}
		broker.NodeID = int32(nodeID)
	}

	if v := getenv("KAFKA_ADVERTISED_HOST"); v != "" {
		broker.Host = v
	}

	if v := getenv("KAFKA_ADVERTISED_PORT"); v != "" {
		port, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			return protocol.Broker{}, fmt.Errorf("KAFKA_ADVERTISED_PORT: %w", err)
		}
		broker.Port = int32(port)
	}

	return broker, nil
}
