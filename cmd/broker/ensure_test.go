package main

import (
	"errors"
	"testing"

	"github.com/AfzalRaja001/kafka-go/internal/protocol"
	"github.com/AfzalRaja001/kafka-go/internal/storage"
)

func registryWith(topics map[string]int32) *protocol.TopicRegistry {
	registry := protocol.NewTopicRegistry()
	for name, partitions := range topics {
		registry.AddTopic(registry.NewTopic(name, partitions))
	}
	return registry
}

// A topic that is listed in topics.json but whose partition directories are
// gone (a delete that crashed after removing storage but before saving the
// list) must come back with working, empty partitions - otherwise the first
// Produce would silently re-create one, and Fetch would report the topic as
// unknown.
func TestEnsureStorage_CreatesEveryListedPartition(t *testing.T) {
	registry := registryWith(map[string]int32{"orders": 3, "payments": 1})
	log := storage.NewFakeLog()

	if err := ensureStorage(registry, log); err != nil {
		t.Fatalf("ensureStorage: %v", err)
	}

	for topic, partitions := range map[string]int32{"orders": 3, "payments": 1} {
		for p := int32(0); p < partitions; p++ {
			if _, err := log.LatestOffset(topic, p); err != nil {
				t.Errorf("%s-%d has no storage after ensureStorage: %v", topic, p, err)
			}
		}
	}
}

func TestEnsureStorage_LeavesExistingDataAlone(t *testing.T) {
	registry := registryWith(map[string]int32{"orders": 1})
	log := storage.NewFakeLog()
	if _, err := log.Append("orders", 0, []byte("keep me"), 1); err != nil {
		t.Fatal(err)
	}

	if err := ensureStorage(registry, log); err != nil {
		t.Fatalf("ensureStorage: %v", err)
	}

	if end, err := log.LatestOffset("orders", 0); err != nil || end != 1 {
		t.Fatalf("LatestOffset after ensureStorage = (%d, %v), want (1, nil): existing records must survive", end, err)
	}
}

func TestEnsureStorage_NoTopicsIsFine(t *testing.T) {
	if err := ensureStorage(protocol.NewTopicRegistry(), storage.NewFakeLog()); err != nil {
		t.Fatalf("ensureStorage on an empty registry: %v", err)
	}
}

type failingLog struct {
	*storage.FakeLog
	err error
}

func (f failingLog) CreatePartition(string, int32) error { return f.err }

// Unlike the background sweeps, this runs once at startup: a listed topic
// whose storage cannot be provisioned is a reason to refuse to start, not
// something to skip and log.
func TestEnsureStorage_StorageFailureIsAStartupError(t *testing.T) {
	boom := errors.New("disk full")
	registry := registryWith(map[string]int32{"orders": 1})

	err := ensureStorage(registry, failingLog{storage.NewFakeLog(), boom})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want it to wrap %v", err, boom)
	}
}
