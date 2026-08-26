package offsets

import (
	"testing"

	"github.com/AfzalRaja001/kafka-go/internal/storage"
)

func TestLogBackedStore_FetchBeforeCommitReportsNotFound(t *testing.T) {
	store, err := NewLogBackedStore(storage.NewFakeLog())
	if err != nil {
		t.Fatalf("NewLogBackedStore: %v", err)
	}

	offset, metadata, found := store.Fetch("my-group", "orders", 0)
	if found {
		t.Fatalf("found = true, want false (nothing committed yet)")
	}
	if offset != 0 || metadata != "" {
		t.Errorf("offset, metadata = %d, %q, want zero values when not found", offset, metadata)
	}
}

func TestLogBackedStore_CommitThenFetchRoundTrips(t *testing.T) {
	store, err := NewLogBackedStore(storage.NewFakeLog())
	if err != nil {
		t.Fatalf("NewLogBackedStore: %v", err)
	}

	if err := store.Commit("my-group", "orders", 0, 42, "checkpoint-a"); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	offset, metadata, found := store.Fetch("my-group", "orders", 0)
	if !found {
		t.Fatal("found = false, want true after Commit")
	}
	if offset != 42 || metadata != "checkpoint-a" {
		t.Errorf("offset, metadata = %d, %q, want 42, checkpoint-a", offset, metadata)
	}
}

func TestLogBackedStore_CommitOverwritesPreviousValue(t *testing.T) {
	store, err := NewLogBackedStore(storage.NewFakeLog())
	if err != nil {
		t.Fatalf("NewLogBackedStore: %v", err)
	}

	store.Commit("my-group", "orders", 0, 10, "first")
	store.Commit("my-group", "orders", 0, 20, "second")

	offset, metadata, found := store.Fetch("my-group", "orders", 0)
	if !found || offset != 20 || metadata != "second" {
		t.Errorf("offset, metadata, found = %d, %q, %v, want 20, second, true", offset, metadata, found)
	}
}

func TestLogBackedStore_KeysAreIndependent(t *testing.T) {
	store, err := NewLogBackedStore(storage.NewFakeLog())
	if err != nil {
		t.Fatalf("NewLogBackedStore: %v", err)
	}

	store.Commit("group-a", "orders", 0, 5, "")
	store.Commit("group-b", "orders", 0, 99, "")
	store.Commit("group-a", "orders", 1, 7, "")
	store.Commit("group-a", "payments", 0, 3, "")

	tests := []struct {
		group     string
		topic     string
		partition int32
		want      int64
	}{
		{"group-a", "orders", 0, 5},
		{"group-b", "orders", 0, 99},
		{"group-a", "orders", 1, 7},
		{"group-a", "payments", 0, 3},
	}
	for _, tt := range tests {
		offset, _, found := store.Fetch(tt.group, tt.topic, tt.partition)
		if !found || offset != tt.want {
			t.Errorf("Fetch(%s, %s, %d) = %d, %v, want %d, true", tt.group, tt.topic, tt.partition, offset, found, tt.want)
		}
	}
}

// TestLogBackedStore_ReplaySeesEarlierCommits proves the replay-on-construction
// path (not just Commit-then-Fetch on the same instance): a second store
// built over a log that already has commits in it must see them without any
// of them having gone through this second instance's own Commit.
func TestLogBackedStore_ReplaySeesEarlierCommits(t *testing.T) {
	log := storage.NewFakeLog()

	first, err := NewLogBackedStore(log)
	if err != nil {
		t.Fatalf("NewLogBackedStore (first): %v", err)
	}
	first.Commit("my-group", "orders", 0, 42, "checkpoint-a")
	first.Commit("my-group", "orders", 1, 7, "")
	first.Commit("my-group", "orders", 0, 43, "checkpoint-b") // overwrite

	second, err := NewLogBackedStore(log)
	if err != nil {
		t.Fatalf("NewLogBackedStore (second): %v", err)
	}

	offset, metadata, found := second.Fetch("my-group", "orders", 0)
	if !found || offset != 43 || metadata != "checkpoint-b" {
		t.Errorf("partition 0: offset, metadata, found = %d, %q, %v, want 43, checkpoint-b, true", offset, metadata, found)
	}
	offset, _, found = second.Fetch("my-group", "orders", 1)
	if !found || offset != 7 {
		t.Errorf("partition 1: offset, found = %d, %v, want 7, true", offset, found)
	}
}

// TestLogBackedStore_SurvivesRestart is the property this whole package
// exists for: committed offsets outlive the broker process, not just the
// Go value holding them. A FakeLog can't prove this - it's an in-memory
// fake with nothing behind it but a Go map, so two instances sharing it are
// really just sharing memory. A real DiskLog, closed and reopened against
// the same directory, is what actually simulates a broker restart.
func TestLogBackedStore_SurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	log := storage.NewDiskLog(dir, 1<<20, 5)
	store, err := NewLogBackedStore(log)
	if err != nil {
		t.Fatalf("NewLogBackedStore: %v", err)
	}
	if err := store.Commit("my-group", "orders", 0, 42, "checkpoint-a"); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate a broker restart: a brand new DiskLog over the same
	// directory, and a brand new LogBackedStore over that.
	reopened := storage.NewDiskLog(dir, 1<<20, 5)
	defer reopened.Close()
	restarted, err := NewLogBackedStore(reopened)
	if err != nil {
		t.Fatalf("NewLogBackedStore after restart: %v", err)
	}

	offset, metadata, found := restarted.Fetch("my-group", "orders", 0)
	if !found || offset != 42 || metadata != "checkpoint-a" {
		t.Errorf("after restart: offset, metadata, found = %d, %q, %v, want 42, checkpoint-a, true", offset, metadata, found)
	}
}

func TestNewLogBackedStore_CorruptDataReturnsError(t *testing.T) {
	log := storage.NewFakeLog()
	if err := log.CreatePartition(topicName, partitionID); err != nil {
		t.Fatalf("CreatePartition: %v", err)
	}
	// A length prefix claiming 99 bytes of body with none actually there.
	if _, err := log.Append(topicName, partitionID, []byte{0, 0, 0, 99}, 1); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if _, err := NewLogBackedStore(log); err == nil {
		t.Fatal("expected an error constructing a store over corrupt __consumer_offsets data, got nil")
	}
}
