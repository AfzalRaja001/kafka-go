package storage

import (
	"fmt"
	"testing"
)

func TestDiskLog_AppendAndRead(t *testing.T) {
	log := NewDiskLog(t.TempDir(), 5)
	defer log.Close()

	base, err := log.Append("orders", 0, []byte("first"))
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if base != 0 {
		t.Fatalf("base offset = %d, want 0", base)
	}

	base, err = log.Append("orders", 0, []byte("second"))
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
	log := NewDiskLog(t.TempDir(), 5)
	defer log.Close()

	log.Append("orders", 0, []byte("first"))  // 5 bytes
	log.Append("orders", 0, []byte("second")) // 6 bytes

	got, err := log.Read("orders", 0, 0, 5)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "first" {
		t.Errorf("got %q, want %q (only the first record should fit)", got, "first")
	}
}

func TestDiskLog_ReadUnknownTopicPartitionErrors(t *testing.T) {
	log := NewDiskLog(t.TempDir(), 5)
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
	log := NewDiskLog(t.TempDir(), 5)
	defer log.Close()

	for i := 0; i < 5; i++ {
		log.Append("orders", 0, []byte(fmt.Sprintf("record-%d", i)))
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
	log := NewDiskLog(t.TempDir(), 5)
	defer log.Close()

	log.Append("orders", 0, []byte("orders-p0"))
	log.Append("orders", 1, []byte("orders-p1-a"))
	log.Append("orders", 1, []byte("orders-p1-b"))

	p0Latest, _ := log.LatestOffset("orders", 0)
	p1Latest, _ := log.LatestOffset("orders", 1)
	if p0Latest != 1 {
		t.Errorf("partition 0 latest offset = %d, want 1", p0Latest)
	}
	if p1Latest != 2 {
		t.Errorf("partition 1 latest offset = %d, want 2", p1Latest)
	}
}

func TestDiskLog_PersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	log1 := NewDiskLog(dir, 5)
	log1.Append("orders", 0, []byte("durable"))
	if err := log1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	log2 := NewDiskLog(dir, 5)
	defer log2.Close()

	got, err := log2.Read("orders", 0, 0, 1024)
	if err != nil {
		t.Fatalf("read after reopen: %v", err)
	}
	if string(got) != "durable" {
		t.Errorf("got %q, want %q", got, "durable")
	}
}
