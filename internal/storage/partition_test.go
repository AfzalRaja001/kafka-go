package storage

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func openTestPartition(t *testing.T, indexEvery int32) *Partition {
	t.Helper()
	dir := t.TempDir()
	p, err := OpenPartition(
		filepath.Join(dir, "segment.log"),
		filepath.Join(dir, "segment.index"),
		filepath.Join(dir, "segment.timeindex"),
		indexEvery,
	)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return p
}

func TestPartition_RoundTrip(t *testing.T) {
	p := openTestPartition(t, 5)
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
	p := openTestPartition(t, 10)
	defer p.Close()

	for i := 0; i < 100; i++ {
		p.Append([]byte(fmt.Sprintf("record-%d", i)), int64(1000+i))
	}

	// 100 records, one index entry every 10 -> exactly 10 entries, not 100.
	if got := p.idx.EntryCount(); got != 10 {
		t.Errorf("expected 10 index entries, got %d", got)
	}
	if got := p.timeindex.EntryCount(); got != 10 {
		t.Errorf("expected 10 timeindex entries, got %d", got)
	}
}

func TestPartition_LookupOffsetByTimestamp(t *testing.T) {
	p := openTestPartition(t, 10)
	defer p.Close()

	for i := 0; i < 100; i++ {
		// Timestamps 1000, 1010, 1020, ... - one per record.
		p.Append([]byte(fmt.Sprintf("record-%d", i)), int64(1000+i*10))
	}

	// Indexed every 10 records: entries at offsets 0, 10, 20, ... with
	// timestamps 1000, 1100, 1200, ...
	offset, found := p.LookupOffsetByTimestamp(1250)
	if !found || offset != 20 {
		t.Errorf("LookupOffsetByTimestamp(1250) = (%d, %v), want (20, true)", offset, found)
	}

	if _, found := p.LookupOffsetByTimestamp(500); found {
		t.Errorf("LookupOffsetByTimestamp(500) found = true, want false (before first entry)")
	}
}

func TestPartition_ConcurrentAppend_Safe(t *testing.T) {
	p := openTestPartition(t, 5)
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
	p := openTestPartition(t, 5)
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
