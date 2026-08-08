package storage

import (
	"path/filepath"
	"testing"
)

func TestIndex_AppendAndLookup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.index")
	idx, err := OpenIndex(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer idx.Close()

	// Entries start at relative offset 5 - nothing indexed for 0-4.
	idx.Append(5, 100)
	idx.Append(10, 250)
	idx.Append(15, 400)

	if got := idx.EntryCount(); got != 3 {
		t.Fatalf("EntryCount = %d, want 3", got)
	}

	if _, _, found := idx.Lookup(2); found {
		t.Errorf("Lookup(2) found = true, want false (nothing indexed at or before offset 2)")
	}

	relOff, pos, found := idx.Lookup(12)
	if !found || relOff != 10 || pos != 250 {
		t.Errorf("Lookup(12) = (%d, %d, %v), want (10, 250, true)", relOff, pos, found)
	}

	relOff, pos, found = idx.Lookup(15)
	if !found || relOff != 15 || pos != 400 {
		t.Errorf("Lookup(15) = (%d, %d, %v), want (15, 400, true)", relOff, pos, found)
	}
}
