package storage

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestAppendAndRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.log")

	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer seg.Close()

	pos1, err := seg.Append([]byte("first"), 1)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	pos2, err := seg.Append([]byte("second"), 4)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	got1, span1, err := seg.ReadAt(pos1)
	if err != nil || string(got1) != "first" {
		t.Errorf("ReadAt(pos1) = %q, %v", got1, err)
	}
	if span1 != 1 {
		t.Errorf("ReadAt(pos1) span = %d, want 1", span1)
	}
	got2, span2, err := seg.ReadAt(pos2)
	if err != nil || string(got2) != "second" {
		t.Errorf("ReadAt(pos2) = %q, %v", got2, err)
	}
	// The span is stored per blob and read back verbatim - a segment never
	// infers it, so a 4-offset batch must still report 4.
	if span2 != 4 {
		t.Errorf("ReadAt(pos2) span = %d, want 4", span2)
	}
}

// TestSegment_CountsSeparatesBlobsFromOffsets pins down the distinction the
// multi-record fix turns on: three appends spanning 1+4+2 offsets is three
// blobs but seven offsets, and OpenPartition needs both numbers.
func TestSegment_CountsSeparatesBlobsFromOffsets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.log")

	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer seg.Close()

	seg.Append([]byte("a"), 1)
	seg.Append([]byte("b"), 4)
	seg.Append([]byte("c"), 2)

	blobs, offsets, err := seg.Counts()
	if err != nil {
		t.Fatalf("Counts: %v", err)
	}
	if blobs != 3 {
		t.Errorf("blobs = %d, want 3", blobs)
	}
	if offsets != 7 {
		t.Errorf("offsets = %d, want 7", offsets)
	}
}

func TestSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.log")

	seg, _ := OpenSegment(path)
	pos, _ := seg.Append([]byte("durable data"), 3)
	seg.Sync()
	seg.Close()

	reopened, err := OpenSegment(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	got, span, err := reopened.ReadAt(pos)
	if err != nil || string(got) != "durable data" {
		t.Errorf("got %q, %v", got, err)
	}
	// The offset span has to survive the restart too - it's the only record
	// of how many offsets this blob owns, and nothing else persists it.
	if span != 3 {
		t.Errorf("span after reopen = %d, want 3", span)
	}
}

func TestRecoversFromTornWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.log")

	seg, _ := OpenSegment(path)
	seg.Append([]byte("good record"), 1)
	seg.Close()

	// Manually simulate a torn write: a blob header claiming 100 bytes
	// follow, but none of them written - exactly what a crash mid-Append
	// would leave behind. The header is now 8 bytes (length + offset span),
	// so a torn write has to be written at that width to be realistic.
	f, _ := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0644)
	header := make([]byte, recordHeaderSize)
	binary.BigEndian.PutUint32(header[0:4], 100)
	binary.BigEndian.PutUint32(header[4:8], 1)
	f.Write(header)
	f.Close()

	recovered, err := OpenSegment(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer recovered.Close()

	pos, err := recovered.Append([]byte("new record"), 1)
	got, _, err2 := recovered.ReadAt(pos)
	if err != nil || err2 != nil || string(got) != "new record" {
		t.Errorf("append after recovery failed: %q, %v, %v", got, err, err2)
	}
}
