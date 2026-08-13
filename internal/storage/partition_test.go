package storage

import (
	"fmt"
	"os"
	"sync"
	"testing"
)

func openTestPartition(t *testing.T, segmentMaxBytes int64, indexEvery int32) *Partition {
	t.Helper()
	p, err := OpenPartition(t.TempDir(), segmentMaxBytes, indexEvery)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return p
}

func TestPartition_RoundTrip(t *testing.T) {
	p := openTestPartition(t, 1<<20, 5) // large segmentMaxBytes - single segment for this test
	defer p.Close()

	for i := 0; i < 100; i++ {
		record := []byte(fmt.Sprintf("record-%d", i))
		offset, err := p.Append(record, int64(1000+i))
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if offset != int64(i) {
			t.Fatalf("expected offset %d, got %d", i, offset)
		}
	}

	for _, target := range []int64{0, 1, 47, 63, 99} {
		data, err := p.Read(target)
		if err != nil {
			t.Fatalf("read %d: %v", target, err)
		}
		want := fmt.Sprintf("record-%d", target)
		if string(data) != want {
			t.Errorf("offset %d: got %q, want %q", target, data, want)
		}
	}
}

func TestPartition_IndexIsSparse(t *testing.T) {
	p := openTestPartition(t, 1<<20, 10)
	defer p.Close()

	for i := 0; i < 100; i++ {
		p.Append([]byte(fmt.Sprintf("record-%d", i)), int64(1000+i))
	}

	active := p.segments[len(p.segments)-1]
	if got := active.idx.EntryCount(); got != 10 {
		t.Errorf("expected 10 index entries, got %d", got)
	}
	if got := active.timeindex.EntryCount(); got != 10 {
		t.Errorf("expected 10 timeindex entries, got %d", got)
	}
}

func TestPartition_LookupOffsetByTimestamp(t *testing.T) {
	p := openTestPartition(t, 1<<20, 10)
	defer p.Close()

	for i := 0; i < 100; i++ {
		p.Append([]byte(fmt.Sprintf("record-%d", i)), int64(1000+i*10))
	}

	offset, found := p.LookupOffsetByTimestamp(1250)
	if !found || offset != 20 {
		t.Errorf("LookupOffsetByTimestamp(1250) = (%d, %v), want (20, true)", offset, found)
	}

	if _, found := p.LookupOffsetByTimestamp(500); found {
		t.Errorf("LookupOffsetByTimestamp(500) found = true, want false (before first entry)")
	}
}

func TestPartition_ConcurrentAppend_Safe(t *testing.T) {
	p := openTestPartition(t, 1<<20, 5)
	defer p.Close()

	var wg sync.WaitGroup
	offsets := make(chan int64, 50)

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			offset, err := p.Append([]byte(fmt.Sprintf("record-%d", n)), int64(n))
			if err != nil {
				t.Errorf("append: %v", err)
				return
			}
			offsets <- offset
		}(i)
	}
	wg.Wait()
	close(offsets)

	seen := make(map[int64]bool)
	for offset := range offsets {
		if seen[offset] {
			t.Fatalf("offset %d assigned twice", offset)
		}
		seen[offset] = true
	}
	if len(seen) != 50 {
		t.Fatalf("expected 50 distinct offsets, got %d", len(seen))
	}
}

func TestPartition_ConcurrentReadWrite(t *testing.T) {
	p := openTestPartition(t, 1<<20, 5)
	defer p.Close()

	for i := 0; i < 20; i++ {
		p.Append([]byte(fmt.Sprintf("record-%d", i)), int64(i))
	}

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for offset := int64(0); offset < 20; offset++ {
				if _, err := p.Read(offset); err != nil {
					t.Errorf("read %d: %v", offset, err)
				}
			}
		}()
	}

	for i := 20; i < 40; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			p.Append([]byte(fmt.Sprintf("record-%d", n)), int64(n))
		}(i)
	}

	wg.Wait()
}

// TestPartition_RollsToNewSegment proves rolling actually happens: a tiny
// segmentMaxBytes forces every append into a fresh segment, and offsets
// must still be correct, sequential, and gapless across the roll.
func TestPartition_RollsToNewSegment(t *testing.T) {
	// Each record ("record-N") plus its 4-byte length prefix is at least
	// 12 bytes - segmentMaxBytes of 10 guarantees every single append
	// exceeds it, forcing a roll before every append after the first.
	p := openTestPartition(t, 10, 1000)
	defer p.Close()

	for i := 0; i < 5; i++ {
		offset, err := p.Append([]byte(fmt.Sprintf("record-%d", i)), int64(i))
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if offset != int64(i) {
			t.Fatalf("append %d: offset = %d, want %d", i, offset, i)
		}
	}

	if len(p.segments) < 2 {
		t.Fatalf("expected multiple segments after rolling, got %d", len(p.segments))
	}

	for i := 0; i < 5; i++ {
		data, err := p.Read(int64(i))
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		want := fmt.Sprintf("record-%d", i)
		if string(data) != want {
			t.Errorf("offset %d: got %q, want %q", i, data, want)
		}
	}
}

// TestPartition_ReopenAcrossMultipleSegments proves segment discovery and
// nextOffset/sinceLastIdx restoration are correct across a clean restart
// with more than one segment on disk.
func TestPartition_ReopenAcrossMultipleSegments(t *testing.T) {
	dir := t.TempDir()

	p1, err := OpenPartition(dir, 10, 1000)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 5; i++ {
		p1.Append([]byte(fmt.Sprintf("record-%d", i)), int64(i))
	}
	segmentsBefore := len(p1.segments)
	if err := p1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	p2, err := OpenPartition(dir, 10, 1000)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer p2.Close()

	if len(p2.segments) != segmentsBefore {
		t.Fatalf("segments after reopen = %d, want %d", len(p2.segments), segmentsBefore)
	}
	if got := p2.LogEndOffset(); got != 5 {
		t.Fatalf("LogEndOffset after reopen = %d, want 5", got)
	}

	// A new append after reopening must continue the offset sequence
	// correctly, not restart or collide with existing data.
	offset, err := p2.Append([]byte("record-5"), 5)
	if err != nil {
		t.Fatalf("append after reopen: %v", err)
	}
	if offset != 5 {
		t.Fatalf("append after reopen: offset = %d, want 5", offset)
	}

	for i := 0; i < 6; i++ {
		data, err := p2.Read(int64(i))
		if err != nil {
			t.Fatalf("read %d after reopen: %v", i, err)
		}
		want := fmt.Sprintf("record-%d", i)
		if string(data) != want {
			t.Errorf("offset %d: got %q, want %q", i, data, want)
		}
	}
}

// TestPartition_MultiSegmentCrashRecovery is the test phase1-kickoff-plan.md
// section 3 Track B step 4 calls for: write across several segments, kill
// the process mid-write on the active segment, restart, confirm recovery
// truncates cleanly and everything - across every segment, not just one -
// is still correct.
func TestPartition_MultiSegmentCrashRecovery(t *testing.T) {
	dir := t.TempDir()

	p1, err := OpenPartition(dir, 10, 1000)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 5; i++ {
		p1.Append([]byte(fmt.Sprintf("record-%d", i)), int64(i))
	}
	if len(p1.segments) < 2 {
		t.Fatalf("expected multiple segments before the crash, got %d", len(p1.segments))
	}
	activeBase := p1.segments[len(p1.segments)-1].baseOffset
	if err := p1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Simulate a crash mid-Append on the active segment only - older
	// segments are immutable and were never at risk.
	activeLogPath := segmentFileBase(dir, activeBase) + ".log"
	f, err := os.OpenFile(activeLogPath, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("open active segment for corruption: %v", err)
	}
	tornHeader := []byte{0, 0, 0, 100} // claims 100 bytes follow; none do
	f.Write(tornHeader)
	f.Close()

	p2, err := OpenPartition(dir, 10, 1000) // Recover() runs automatically per segment
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer p2.Close()

	// Every record written before the crash must still be readable,
	// across every segment, not just the one that was corrupted.
	for i := 0; i < 5; i++ {
		data, err := p2.Read(int64(i))
		if err != nil {
			t.Fatalf("read %d after recovery: %v", i, err)
		}
		want := fmt.Sprintf("record-%d", i)
		if string(data) != want {
			t.Errorf("offset %d after recovery: got %q, want %q", i, data, want)
		}
	}

	// The torn write must be gone: appending now should land at offset 5,
	// not after the corrupt header, and the new offset sequence must be
	// contiguous with what existed before the crash.
	offset, err := p2.Append([]byte("record-5"), 5)
	if err != nil {
		t.Fatalf("append after recovery: %v", err)
	}
	if offset != 5 {
		t.Fatalf("append after recovery: offset = %d, want 5", offset)
	}
	data, err := p2.Read(5)
	if err != nil || string(data) != "record-5" {
		t.Fatalf("read offset 5 after recovery: %q, %v, want \"record-5\"", data, err)
	}
}
