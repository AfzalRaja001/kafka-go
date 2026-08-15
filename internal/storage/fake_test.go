package storage

import (
	"bytes"
	"testing"
)

// Compile-time check: if FakeLog ever stops satisfying Log, this line fails
// to compile, catching the mismatch immediately instead of wherever FakeLog
// happens to get assigned to a Log-typed variable.
var _ Log = (*FakeLog)(nil)

func TestFakeLog_AppendThenRead(t *testing.T) {
	log := NewFakeLog()

	base, err := log.Append("orders", 0, []byte("batch-one"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if base != 0 {
		t.Fatalf("first append base offset = %d, want 0", base)
	}

	base, err = log.Append("orders", 0, []byte("batch-two"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if base != 1 {
		t.Fatalf("second append base offset = %d, want 1", base)
	}

	got, err := log.Read("orders", 0, 0, 1024)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	want := []byte("batch-onebatch-two")
	if !bytes.Equal(got, want) {
		t.Fatalf("Read(offset=0) = %q, want %q", got, want)
	}

	latest, err := log.LatestOffset("orders", 0)
	if err != nil {
		t.Fatalf("LatestOffset: %v", err)
	}
	if latest != 2 {
		t.Fatalf("LatestOffset = %d, want 2", latest)
	}
}

// TestFakeLog_ReadUnknownPartitionErrors matches DiskLog's own contract:
// a partition nothing has ever been Appended to errors on read rather than
// silently returning empty data. (Appending to partition 0 doesn't make
// partition 1 "exist, but empty" - they're entirely separate partitions.)
func TestFakeLog_ReadUnknownPartitionErrors(t *testing.T) {
	log := NewFakeLog()
	log.Append("orders", 0, []byte("data"))

	if _, err := log.Read("orders", 1, 0, 1024); err == nil {
		t.Fatal("expected an error reading an unknown partition, got nil")
	}
	if _, err := log.EarliestOffset("orders", 1); err == nil {
		t.Fatal("expected an error for EarliestOffset on an unknown partition, got nil")
	}
	if _, err := log.LatestOffset("orders", 1); err == nil {
		t.Fatal("expected an error for LatestOffset on an unknown partition, got nil")
	}
}
