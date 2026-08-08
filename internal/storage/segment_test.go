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

	pos1, err := seg.Append([]byte("first"))
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	pos2, err := seg.Append([]byte("second"))
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	got1, err := seg.ReadAt(pos1)
	if err != nil || string(got1) != "first" {
		t.Errorf("ReadAt(pos1) = %q, %v", got1, err)
	}
	got2, err := seg.ReadAt(pos2)
	if err != nil || string(got2) != "second" {
		t.Errorf("ReadAt(pos2) = %q, %v", got2, err)
	}
}

func TestSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.log")

	seg, _ := OpenSegment(path)
	pos, _ := seg.Append([]byte("durable data"))
	seg.Sync()
	seg.Close()

	reopened, err := OpenSegment(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	got, err := reopened.ReadAt(pos)
	if err != nil || string(got) != "durable data" {
		t.Errorf("got %q, %v", got, err)
	}
}

func TestRecoversFromTornWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.log")

	seg, _ := OpenSegment(path)
	seg.Append([]byte("good record"))
	seg.Close()

	// Manually simulate a torn write: a length prefix claiming 100 bytes
	// follow, but none of them written - exactly what a crash mid-Append
	// would leave behind.
	f, _ := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0644)
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, 100)
	f.Write(header)
	f.Close()

	recovered, err := OpenSegment(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer recovered.Close()

	pos, err := recovered.Append([]byte("new record"))
	got, err2 := recovered.ReadAt(pos)
	if err != nil || err2 != nil || string(got) != "new record" {
		t.Errorf("append after recovery failed: %q, %v, %v", got, err, err2)
	}
}
