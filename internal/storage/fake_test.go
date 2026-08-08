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

func TestFakeLog_ReadDifferentPartitionIsEmpty(t *testing.T) {
	log := NewFakeLog()
	log.Append("orders", 0, []byte("data"))

	got, err := log.Read("orders", 1, 0, 1024)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got != nil {
		t.Fatalf("Read on untouched partition = %q, want nil", got)
	}
}
