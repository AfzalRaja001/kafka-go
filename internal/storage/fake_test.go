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

	base, err := log.Append("orders", 0, []byte("batch-one"), 1)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if base != 0 {
		t.Fatalf("first append base offset = %d, want 0", base)
	}

	base, err = log.Append("orders", 0, []byte("batch-two"), 1)
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
	log.Append("orders", 0, []byte("data"), 1)

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

// TestFakeLog_CreatePartitionMakesEmptyTopicReadable mirrors DiskLog's
// eager-provisioning contract - handler tests against FakeLog need to
// agree with DiskLog, or a test passing here could still be wrong on disk.
func TestFakeLog_CreatePartitionMakesEmptyTopicReadable(t *testing.T) {
	log := NewFakeLog()

	if err := log.CreatePartition("fresh-topic", 0); err != nil {
		t.Fatalf("CreatePartition: %v", err)
	}

	earliest, err := log.EarliestOffset("fresh-topic", 0)
	if err != nil || earliest != 0 {
		t.Errorf("EarliestOffset = %d, %v, want 0, nil", earliest, err)
	}
	latest, err := log.LatestOffset("fresh-topic", 0)
	if err != nil || latest != 0 {
		t.Errorf("LatestOffset = %d, %v, want 0, nil", latest, err)
	}
	got, err := log.Read("fresh-topic", 0, 0, 1024)
	if err != nil || len(got) != 0 {
		t.Errorf("Read = %q, %v, want empty, nil", got, err)
	}
}

func TestFakeLog_DeletePartitionRemovesTopic(t *testing.T) {
	log := NewFakeLog()
	log.Append("orders", 0, []byte("first"), 1)

	if err := log.DeletePartition("orders", 0); err != nil {
		t.Fatalf("DeletePartition: %v", err)
	}

	if _, err := log.Read("orders", 0, 0, 1024); err == nil {
		t.Error("expected an error reading a deleted partition, got nil")
	}
}

func TestFakeLog_DeletePartitionUnknownIsNotAnError(t *testing.T) {
	log := NewFakeLog()

	if err := log.DeletePartition("never-existed", 0); err != nil {
		t.Errorf("DeletePartition on unknown partition = %v, want nil", err)
	}
}

func TestFakeLog_SizeSumsAppendedBytes(t *testing.T) {
	log := NewFakeLog()
	log.Append("orders", 0, []byte("first"), 1)  // 5 bytes
	log.Append("orders", 0, []byte("second"), 1) // 6 bytes

	got, err := log.Size("orders", 0)
	if err != nil {
		t.Fatalf("Size: %v", err)
	}
	if got != 11 {
		t.Errorf("Size = %d, want 11 (5+6)", got)
	}
}

func TestFakeLog_SizeUnknownPartitionErrors(t *testing.T) {
	log := NewFakeLog()

	if _, err := log.Size("missing", 0); err == nil {
		t.Fatal("expected an error for an unknown topic-partition, got nil")
	}
}
