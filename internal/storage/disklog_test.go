package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestDiskLog_AppendAndRead(t *testing.T) {
	log := NewDiskLog(t.TempDir(), 1<<20, 5)
	defer log.Close()

	base, err := log.Append("orders", 0, []byte("first"), 1)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if base != 0 {
		t.Fatalf("base offset = %d, want 0", base)
	}

	base, err = log.Append("orders", 0, []byte("second"), 1)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if base != 1 {
		t.Fatalf("base offset = %d, want 1", base)
	}

	got, err := log.Read("orders", 0, 0, 1024)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "firstsecond" {
		t.Errorf("got %q, want %q", got, "firstsecond")
	}
}

func TestDiskLog_ReadRespectsMaxBytes(t *testing.T) {
	log := NewDiskLog(t.TempDir(), 1<<20, 5)
	defer log.Close()

	log.Append("orders", 0, []byte("first"), 1)  // 5 bytes
	log.Append("orders", 0, []byte("second"), 1) // 6 bytes

	got, err := log.Read("orders", 0, 0, 5)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "first" {
		t.Errorf("got %q, want %q (only the first record should fit)", got, "first")
	}
}

func TestDiskLog_ReadUnknownTopicPartitionErrors(t *testing.T) {
	log := NewDiskLog(t.TempDir(), 1<<20, 5)
	defer log.Close()

	if _, err := log.Read("missing", 0, 0, 1024); err == nil {
		t.Fatal("expected an error reading an unknown topic-partition, got nil")
	}
	if _, err := log.EarliestOffset("missing", 0); err == nil {
		t.Fatal("expected an error for EarliestOffset on an unknown topic-partition, got nil")
	}
	if _, err := log.LatestOffset("missing", 0); err == nil {
		t.Fatal("expected an error for LatestOffset on an unknown topic-partition, got nil")
	}
}

func TestDiskLog_EarliestAndLatestOffset(t *testing.T) {
	log := NewDiskLog(t.TempDir(), 1<<20, 5)
	defer log.Close()

	for i := 0; i < 5; i++ {
		log.Append("orders", 0, []byte(fmt.Sprintf("record-%d", i)), 1)
	}

	earliest, err := log.EarliestOffset("orders", 0)
	if err != nil || earliest != 0 {
		t.Errorf("EarliestOffset = %d, %v, want 0", earliest, err)
	}
	latest, err := log.LatestOffset("orders", 0)
	if err != nil || latest != 5 {
		t.Errorf("LatestOffset = %d, %v, want 5", latest, err)
	}
}

func TestDiskLog_PartitionsAreIndependent(t *testing.T) {
	log := NewDiskLog(t.TempDir(), 1<<20, 5)
	defer log.Close()

	log.Append("orders", 0, []byte("orders-p0"), 1)
	log.Append("orders", 1, []byte("orders-p1-a"), 1)
	log.Append("orders", 1, []byte("orders-p1-b"), 1)

	p0Latest, _ := log.LatestOffset("orders", 0)
	p1Latest, _ := log.LatestOffset("orders", 1)
	if p0Latest != 1 {
		t.Errorf("partition 0 latest offset = %d, want 1", p0Latest)
	}
	if p1Latest != 2 {
		t.Errorf("partition 1 latest offset = %d, want 2", p1Latest)
	}
}

func TestDiskLog_SegmentRolling(t *testing.T) {
	log := NewDiskLog(t.TempDir(), 10, 1000) // tiny segmentMaxBytes forces rolling
	defer log.Close()

	for i := 0; i < 5; i++ {
		offset, err := log.Append("orders", 0, []byte(fmt.Sprintf("record-%d", i)), 1)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if offset != int64(i) {
			t.Fatalf("append %d: offset = %d, want %d", i, offset, i)
		}
	}

	for i := 0; i < 5; i++ {
		want := fmt.Sprintf("record-%d", i)
		// maxBytes = exactly one record's length, so Read stops after it
		// instead of concatenating every remaining record too.
		data, err := log.Read("orders", 0, int64(i), int32(len(want)))
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if string(data) != want {
			t.Errorf("offset %d: got %q, want %q", i, data, want)
		}
	}
}

// TestDiskLog_CreatePartitionMakesEmptyTopicReadable pins down the eager-
// provisioning contract CreateTopics needs: right after CreatePartition,
// with nothing ever Appended, Read/EarliestOffset/LatestOffset must all
// succeed as "empty", not error as "unknown topic-partition".
func TestDiskLog_CreatePartitionMakesEmptyTopicReadable(t *testing.T) {
	log := NewDiskLog(t.TempDir(), 1<<20, 5)
	defer log.Close()

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

// TestDiskLog_CreatePartitionIsIdempotent confirms calling CreatePartition
// twice - which the protocol handler never does today, since it checks the
// registry first, but which the storage layer's own contract shouldn't rely
// on callers to avoid - doesn't reopen or wipe an already-populated partition.
func TestDiskLog_CreatePartitionIsIdempotent(t *testing.T) {
	log := NewDiskLog(t.TempDir(), 1<<20, 5)
	defer log.Close()

	log.CreatePartition("orders", 0)
	log.Append("orders", 0, []byte("first"), 1)

	if err := log.CreatePartition("orders", 0); err != nil {
		t.Fatalf("second CreatePartition: %v", err)
	}

	latest, err := log.LatestOffset("orders", 0)
	if err != nil || latest != 1 {
		t.Errorf("LatestOffset after re-CreatePartition = %d, %v, want 1, nil", latest, err)
	}
}

// TestDiskLog_DeletePartitionRemovesFilesFromDisk is the real-deletion
// contract DeleteTopics needs: after DeletePartition, the partition
// directory itself must be gone, not just absent from DiskLog's in-memory
// map - otherwise a restarted broker would see the "deleted" topic reappear.
func TestDiskLog_DeletePartitionRemovesFilesFromDisk(t *testing.T) {
	dir := t.TempDir()
	log := NewDiskLog(dir, 1<<20, 5)
	defer log.Close()

	log.Append("orders", 0, []byte("first"), 1)
	partDir := filepath.Join(dir, "orders-0")
	if _, err := os.Stat(partDir); err != nil {
		t.Fatalf("partition dir should exist before delete: %v", err)
	}

	if err := log.DeletePartition("orders", 0); err != nil {
		t.Fatalf("DeletePartition: %v", err)
	}

	if _, err := os.Stat(partDir); !os.IsNotExist(err) {
		t.Errorf("partition dir should be gone after delete, stat err = %v", err)
	}
	if _, err := log.Read("orders", 0, 0, 1024); err == nil {
		t.Error("expected an error reading a deleted topic-partition, got nil")
	}
}

// TestDiskLog_DeletePartitionUnknownIsNotAnError matches the protocol
// handler's division of labor: DeleteTopics itself checks the registry and
// returns UNKNOWN_TOPIC_OR_PARTITION before ever calling into storage, so
// DeletePartition on something that was never created should not need its
// own error path for that case.
func TestDiskLog_DeletePartitionUnknownIsNotAnError(t *testing.T) {
	log := NewDiskLog(t.TempDir(), 1<<20, 5)
	defer log.Close()

	if err := log.DeletePartition("never-existed", 0); err != nil {
		t.Errorf("DeletePartition on unknown topic-partition = %v, want nil", err)
	}
}

// TestDiskLog_DeletePartitionAfterCreatePartitionAndAppend reproduces the
// exact sequence CreateTopics + Produce + DeleteTopics drives against a
// real broker: three partitions provisioned eagerly via CreatePartition,
// only one of them ever Appended to, then all three deleted. A live test
// against kafka-python's AdminClient hit UnknownError here even though the
// simpler single-partition, Append-only-no-CreatePartition case (the
// existing TestDiskLog_DeletePartitionRemovesFilesFromDisk) passed clean.
func TestDiskLog_DeletePartitionAfterCreatePartitionAndAppend(t *testing.T) {
	log := NewDiskLog(t.TempDir(), 1<<20, 5)
	defer log.Close()

	for p := int32(0); p < 3; p++ {
		if err := log.CreatePartition("verify-topic", p); err != nil {
			t.Fatalf("CreatePartition(%d): %v", p, err)
		}
	}
	if _, err := log.Append("verify-topic", 0, []byte("hello"), 1); err != nil {
		t.Fatalf("Append: %v", err)
	}

	for p := int32(0); p < 3; p++ {
		if err := log.DeletePartition("verify-topic", p); err != nil {
			t.Fatalf("DeletePartition(%d): %v", p, err)
		}
	}
}

func TestDiskLog_PersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	log1 := NewDiskLog(dir, 1<<20, 5)
	log1.Append("orders", 0, []byte("durable"), 1)
	if err := log1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	log2 := NewDiskLog(dir, 1<<20, 5)
	defer log2.Close()

	got, err := log2.Read("orders", 0, 0, 1024)
	if err != nil {
		t.Fatalf("read after reopen: %v", err)
	}
	if string(got) != "durable" {
		t.Errorf("got %q, want %q", got, "durable")
	}
}

// TestDiskLog_SizeReflectsBytesActuallyAppended expects payload bytes plus
// recordHeaderSize (8) per record, not payload bytes alone - each segment
// blob carries its own length+offset-span header on disk (see segment.go),
// and Size reports genuine on-disk footprint, the thing this metric exists
// to measure, not just logical payload size.
func TestDiskLog_SizeReflectsBytesActuallyAppended(t *testing.T) {
	log := NewDiskLog(t.TempDir(), 1<<20, 5)
	defer log.Close()

	log.Append("orders", 0, []byte("first"), 1)  // 5 bytes + 8-byte header
	log.Append("orders", 0, []byte("second"), 1) // 6 bytes + 8-byte header

	got, err := log.Size("orders", 0)
	if err != nil {
		t.Fatalf("Size: %v", err)
	}
	want := int64(5 + recordHeaderSize + 6 + recordHeaderSize)
	if got != want {
		t.Errorf("Size = %d, want %d", got, want)
	}
}

func TestDiskLog_SizeUnknownTopicPartitionErrors(t *testing.T) {
	log := NewDiskLog(t.TempDir(), 1<<20, 5)
	defer log.Close()

	if _, err := log.Size("missing", 0); err == nil {
		t.Fatal("expected an error for an unknown topic-partition, got nil")
	}
}

// TestDiskLog_SizeSurvivesReopen pins down that Size reads real on-disk
// bytes, not an in-memory counter that would reset on restart - the same
// property TestDiskLog_PersistsAcrossReopen already establishes for Read.
func TestDiskLog_SizeSurvivesReopen(t *testing.T) {
	dir := t.TempDir()

	log1 := NewDiskLog(dir, 1<<20, 5)
	log1.Append("orders", 0, []byte("durable"), 1) // 7 bytes + 8-byte header
	if err := log1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	log2 := NewDiskLog(dir, 1<<20, 5)
	defer log2.Close()

	got, err := log2.Size("orders", 0)
	if err != nil {
		t.Fatalf("Size after reopen: %v", err)
	}
	want := int64(7 + recordHeaderSize)
	if got != want {
		t.Errorf("Size after reopen = %d, want %d", got, want)
	}
}

// TestDiskLog_SizeSumsAcrossRolledSegments makes sure Size isn't just
// reporting the active segment's size - a partition's real on-disk footprint
// is every segment it's ever rolled to, not just the one still being
// written.
func TestDiskLog_SizeSumsAcrossRolledSegments(t *testing.T) {
	log := NewDiskLog(t.TempDir(), 10, 1000) // tiny segmentMaxBytes forces rolling
	defer log.Close()

	var want int64
	for i := 0; i < 5; i++ {
		record := fmt.Sprintf("record-%d", i)
		if _, err := log.Append("orders", 0, []byte(record), 1); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		want += int64(len(record) + recordHeaderSize)
	}

	got, err := log.Size("orders", 0)
	if err != nil {
		t.Fatalf("Size: %v", err)
	}
	if got != want {
		t.Errorf("Size = %d, want %d (sum of every record's payload+header, across every segment)", got, want)
	}
}
