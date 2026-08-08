package storage

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func TestPartition_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(
		filepath.Join(dir, "segment.log"),
		filepath.Join(dir, "segment.index"),
		5,
	)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer p.Close()

	for i := 0; i < 100; i++ {
		record := []byte(fmt.Sprintf("record-%d", i))
		offset, err := p.Append(record)
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
	dir := t.TempDir()
	p, err := OpenPartition(
		filepath.Join(dir, "segment.log"),
		filepath.Join(dir, "segment.index"),
		10,
	)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer p.Close()

	for i := 0; i < 100; i++ {
		p.Append([]byte(fmt.Sprintf("record-%d", i)))
	}

	// 100 records, one index entry every 10 -> exactly 10 entries, not 100.
	if got := p.idx.EntryCount(); got != 10 {
		t.Errorf("expected 10 index entries, got %d", got)
	}
}

func TestPartition_ConcurrentAppend_Safe(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(
		filepath.Join(dir, "segment.log"),
		filepath.Join(dir, "segment.index"),
		5,
	)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer p.Close()

	var wg sync.WaitGroup
	offsets := make(chan int64, 50)

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			offset, err := p.Append([]byte(fmt.Sprintf("record-%d", n)))
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
	dir := t.TempDir()
	p, err := OpenPartition(
		filepath.Join(dir, "segment.log"),
		filepath.Join(dir, "segment.index"),
		5,
	)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer p.Close()

	for i := 0; i < 20; i++ {
		p.Append([]byte(fmt.Sprintf("record-%d", i)))
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
			p.Append([]byte(fmt.Sprintf("record-%d", n)))
		}(i)
	}

	wg.Wait()
}
