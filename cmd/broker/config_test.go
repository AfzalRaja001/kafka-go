package main

import (
	"testing"

	"github.com/AfzalRaja001/kafka-go/internal/protocol"
)

// fakeGetenv builds a getenv func backed by a plain map, so tests never
// touch real process environment variables.
func fakeGetenv(vars map[string]string) func(string) string {
	return func(key string) string {
		return vars[key]
	}
}

func TestBrokerConfigFromEnv_DefaultsMatchLocalDevValues(t *testing.T) {
	broker, err := brokerConfigFromEnv(fakeGetenv(nil))
	if err != nil {
		t.Fatalf("brokerConfigFromEnv: %v", err)
	}
	want := protocol.Broker{NodeID: 1, Host: "localhost", Port: 9092}
	if broker != want {
		t.Errorf("broker = %+v, want %+v", broker, want)
	}
}

func TestBrokerConfigFromEnv_ReadsAllThreeFromEnv(t *testing.T) {
	broker, err := brokerConfigFromEnv(fakeGetenv(map[string]string{
		"KAFKA_NODE_ID":         "3",
		"KAFKA_ADVERTISED_HOST": "broker-3.internal",
		"KAFKA_ADVERTISED_PORT": "9192",
	}))
	if err != nil {
		t.Fatalf("brokerConfigFromEnv: %v", err)
	}
	want := protocol.Broker{NodeID: 3, Host: "broker-3.internal", Port: 9192}
	if broker != want {
		t.Errorf("broker = %+v, want %+v", broker, want)
	}
}

func TestBrokerConfigFromEnv_PartialOverrideKeepsOtherDefaults(t *testing.T) {
	// This is the actual deployment case: only the advertised host changes,
	// node ID and port stay at their single-node defaults.
	broker, err := brokerConfigFromEnv(fakeGetenv(map[string]string{
		"KAFKA_ADVERTISED_HOST": "203.0.113.10",
	}))
	if err != nil {
		t.Fatalf("brokerConfigFromEnv: %v", err)
	}
	want := protocol.Broker{NodeID: 1, Host: "203.0.113.10", Port: 9092}
	if broker != want {
		t.Errorf("broker = %+v, want %+v", broker, want)
	}
}

func TestBrokerConfigFromEnv_InvalidNodeIDFailsFastInsteadOfSilentlyDefaulting(t *testing.T) {
	_, err := brokerConfigFromEnv(fakeGetenv(map[string]string{
		"KAFKA_NODE_ID": "not-a-number",
	}))
	if err == nil {
		t.Fatal("expected an error for a non-numeric KAFKA_NODE_ID, got nil")
	}
}

func TestBrokerConfigFromEnv_InvalidPortFailsFastInsteadOfSilentlyDefaulting(t *testing.T) {
	_, err := brokerConfigFromEnv(fakeGetenv(map[string]string{
		"KAFKA_ADVERTISED_PORT": "not-a-port",
	}))
	if err == nil {
		t.Fatal("expected an error for a non-numeric KAFKA_ADVERTISED_PORT, got nil")
	}
}
