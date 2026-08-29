package group

import "testing"

func TestInMemoryOffsetStore_FetchBeforeCommitReportsNotFound(t *testing.T) {
	store := NewInMemoryOffsetStore()

	offset, metadata, found := store.Fetch("my-group", "orders", 0)
	if found {
		t.Fatalf("found = true, want false (nothing committed yet)")
	}
	if offset != 0 || metadata != "" {
		t.Errorf("offset, metadata = %d, %q, want zero values when not found", offset, metadata)
	}
}

func TestInMemoryOffsetStore_CommitThenFetchRoundTrips(t *testing.T) {
	store := NewInMemoryOffsetStore()

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

func TestInMemoryOffsetStore_CommitOverwritesPreviousValue(t *testing.T) {
	store := NewInMemoryOffsetStore()

	store.Commit("my-group", "orders", 0, 10, "first")
	store.Commit("my-group", "orders", 0, 20, "second")

	offset, metadata, found := store.Fetch("my-group", "orders", 0)
	if !found || offset != 20 || metadata != "second" {
		t.Errorf("offset, metadata, found = %d, %q, %v, want 20, second, true", offset, metadata, found)
	}
}

// TestInMemoryOffsetStore_KeysAreIndependent pins down that group, topic,
// and partition are all part of the key - committing for one doesn't leak
// into another, the same independence TestDiskLog_PartitionsAreIndependent
// already established for regular topic partitions.
func TestInMemoryOffsetStore_KeysAreIndependent(t *testing.T) {
	store := NewInMemoryOffsetStore()

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

func TestInMemoryOffsetStore_FetchAllReturnsEveryCommitForThatGroupOnly(t *testing.T) {
	store := NewInMemoryOffsetStore()

	store.Commit("group-a", "orders", 0, 5, "checkpoint-a0")
	store.Commit("group-a", "orders", 1, 7, "checkpoint-a1")
	store.Commit("group-a", "payments", 0, 3, "checkpoint-p0")
	store.Commit("group-b", "orders", 0, 99, "not-group-a")

	got := store.FetchAll("group-a")
	if len(got) != 3 {
		t.Fatalf("FetchAll(group-a) returned %d entries, want 3: %+v", len(got), got)
	}

	byKey := make(map[[2]any]GroupOffset)
	for _, g := range got {
		byKey[[2]any{g.Topic, g.Partition}] = g
	}

	want := []GroupOffset{
		{Topic: "orders", Partition: 0, Offset: 5, Metadata: "checkpoint-a0"},
		{Topic: "orders", Partition: 1, Offset: 7, Metadata: "checkpoint-a1"},
		{Topic: "payments", Partition: 0, Offset: 3, Metadata: "checkpoint-p0"},
	}
	for _, w := range want {
		g, ok := byKey[[2]any{w.Topic, w.Partition}]
		if !ok || g != w {
			t.Errorf("FetchAll(group-a) missing or wrong entry for (%s, %d): got %+v, want %+v", w.Topic, w.Partition, g, w)
		}
	}
}

func TestInMemoryOffsetStore_FetchAllForUnknownGroupReturnsEmpty(t *testing.T) {
	store := NewInMemoryOffsetStore()
	store.Commit("group-a", "orders", 0, 5, "")

	got := store.FetchAll("never-heard-of-it")
	if len(got) != 0 {
		t.Errorf("FetchAll(never-heard-of-it) = %+v, want empty", got)
	}
}

func TestInMemoryOffsetStore_GroupsReturnsEveryDistinctGroupThatHasEverCommitted(t *testing.T) {
	store := NewInMemoryOffsetStore()
	store.Commit("group-a", "orders", 0, 5, "")
	store.Commit("group-a", "orders", 1, 7, "") // same group, second key - must not appear twice
	store.Commit("group-b", "orders", 0, 99, "")

	got := store.Groups()
	if len(got) != 2 {
		t.Fatalf("Groups() = %v, want 2 distinct groups", got)
	}

	seen := map[string]bool{}
	for _, g := range got {
		seen[g] = true
	}
	if !seen["group-a"] || !seen["group-b"] {
		t.Errorf("Groups() = %v, want group-a and group-b", got)
	}
}

func TestInMemoryOffsetStore_GroupsOnEmptyStoreReturnsEmpty(t *testing.T) {
	store := NewInMemoryOffsetStore()

	got := store.Groups()
	if len(got) != 0 {
		t.Errorf("Groups() on empty store = %v, want empty", got)
	}
}
