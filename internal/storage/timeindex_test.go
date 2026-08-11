package storage

import (
	"path/filepath"
	"testing"
)

func TestTimeindex_AppendAndLookup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.timeindex")
	idx, err := OpenTimeindex(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer idx.Close()

	idx.Append(1000, 0)
	idx.Append(2000, 5)
	idx.Append(3000, 10)

	if got := idx.EntryCount(); got != 3 {
		t.Fatalf("EntryCount = %d, want 3", got)
	}

	if _, _, found := idx.Lookup(500); found {
		t.Errorf("Lookup(500) found = true, want false (before the first entry)")
	}

	ts, relOff, found := idx.Lookup(2500)
	if !found || ts != 2000 || relOff != 5 {
		t.Errorf("Lookup(2500) = (%d, %d, %v), want (2000, 5, true)", ts, relOff, found)
	}

	ts, relOff, found = idx.Lookup(3000)
	if !found || ts != 3000 || relOff != 10 {
		t.Errorf("Lookup(3000) = (%d, %d, %v), want (3000, 10, true)", ts, relOff, found)
	}
}
