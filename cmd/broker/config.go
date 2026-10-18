package main

import (
	"fmt"
	"math"
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

// limits are the optional caps that keep a broker reachable by strangers
// from being filled up or exhausted. Zero means unlimited for both, which is
// what the broker always did before they existed, so leaving the variables
// unset changes nothing.
type limits struct {
	// MaxPartitions caps the partitions across all topics together
	// (KAFKA_MAX_PARTITIONS). Without it a client can create topics until
	// the broker runs out of file handles or disk.
	MaxPartitions int32

	// RetentionMaxBytes caps each partition's size on disk
	// (KAFKA_RETENTION_MAX_BYTES), oldest segments deleted first. Retention
	// runs on a timer, so a partition can overshoot the cap between runs,
	// and it never deletes the segment being written.
	RetentionMaxBytes int64
}

// limitsFromEnv reads the limits from the environment. Like
// brokerConfigFromEnv, a value that is set but does not parse is an error:
// a mistyped limit that silently fell back to "unlimited" would leave a
// public broker unprotected without anyone noticing.
func limitsFromEnv(getenv func(string) string) (limits, error) {
	var l limits

	if v := getenv("KAFKA_MAX_PARTITIONS"); v != "" {
		n, err := strconv.ParseInt(v, 10, 32)
		if err != nil || n < 0 {
			return limits{}, fmt.Errorf("KAFKA_MAX_PARTITIONS: %q is not a whole number from 0 to %d", v, math.MaxInt32)
		}
		l.MaxPartitions = int32(n)
	}

	if v := getenv("KAFKA_RETENTION_MAX_BYTES"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			return limits{}, fmt.Errorf("KAFKA_RETENTION_MAX_BYTES: %q is not a whole number of bytes, 0 or more", v)
		}
		l.RetentionMaxBytes = n
	}

	return l, nil
}

// describeLimit renders a limit for the startup log, where 0 reads as
// "unlimited" rather than as a limit of zero.
func describeLimit(n int64) string {
	if n == 0 {
		return "unlimited"
	}
	return strconv.FormatInt(n, 10)
}
