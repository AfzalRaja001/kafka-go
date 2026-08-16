package storage

import (
	"fmt"
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
