package storage

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

func openTestPartition(t *testing.T, segmentMaxBytes int64, indexEvery, flushEveryMessages int32) *Partition {
	t.Helper()
	p, err := OpenPartition(t.TempDir(), segmentMaxBytes, indexEvery, flushEveryMessages)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return p
}

func TestPartition_RoundTrip(t *testing.T) {
	p := openTestPartition(t, 1<<20, 5, 0) // large segmentMaxBytes - single segment for this test
	defer p.Close()

	for i := 0; i < 100; i++ {
		record := []byte(fmt.Sprintf("record-%d", i))
		offset, err := p.Append(record, 1, int64(1000+i))
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if offset != int64(i) {
			t.Fatalf("expected offset %d, got %d", i, offset)
		}
	}

	for _, target := range []int64{0, 1, 47, 63, 99} {
		data, _, err := p.Read(target)
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
	p := openTestPartition(t, 1<<20, 10, 0)
	defer p.Close()

	for i := 0; i < 100; i++ {
		p.Append([]byte(fmt.Sprintf("record-%d", i)), 1, int64(1000+i))
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
	p := openTestPartition(t, 1<<20, 10, 0)
	defer p.Close()

	for i := 0; i < 100; i++ {
		p.Append([]byte(fmt.Sprintf("record-%d", i)), 1, int64(1000+i*10))
	}

	offset, found := p.LookupOffsetByTimestamp(1250)
	if !found || offset != 20 {
		t.Errorf("LookupOffsetByTimestamp(1250) = (%d, %v), want (20, true)", offset, found)
	}

	if _, found := p.LookupOffsetByTimestamp(500); found {
		t.Errorf("LookupOffsetByTimestamp(500) found = true, want false (before first entry)")
	}
}

func TestPartition_CompactReplacesRecordsRenumberedFromZero(t *testing.T) {
	p := openTestPartition(t, 1<<20, 5, 0)
	defer p.Close()

	for i := 0; i < 10; i++ {
		p.Append([]byte(fmt.Sprintf("stale-%d", i)), 1, 1000)
	}

	if err := p.Compact([][]byte{[]byte("kept-a"), []byte("kept-b")}, 2000); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if got := p.LogEndOffset(); got != 2 {
		t.Fatalf("LogEndOffset after Compact = %d, want 2", got)
	}

	data, next, err := p.Read(0)
	if err != nil {
		t.Fatalf("Read(0): %v", err)
	}
	if string(data) != "kept-a" || next != 1 {
		t.Errorf("Read(0) = (%q, %d), want (\"kept-a\", 1)", data, next)
	}
	data, next, err = p.Read(1)
	if err != nil {
		t.Fatalf("Read(1): %v", err)
	}
	if string(data) != "kept-b" || next != 2 {
		t.Errorf("Read(1) = (%q, %d), want (\"kept-b\", 2)", data, next)
	}
}

// TestPartition_CompactActuallyReclaimsDiskSpace is the whole point of this
// feature: old segment files must be gone from disk afterward, not just
// unreferenced in memory.
func TestPartition_CompactActuallyReclaimsDiskSpace(t *testing.T) {
	// Tiny segmentMaxBytes forces several segment rolls, so this proves
	// Compact cleans up every old segment file, not just the active one.
	p := openTestPartition(t, 40, 1000, 0)
	defer p.Close()

	for i := 0; i < 20; i++ {
		p.Append([]byte(fmt.Sprintf("stale-record-%d", i)), 1, 1000)
	}
	if len(p.segments) < 2 {
		t.Fatalf("test setup: expected multiple segments before compaction, got %d", len(p.segments))
	}

	before := p.Size()

	staleBases := make([]int64, len(p.segments))
	for i, sg := range p.segments {
		staleBases[i] = sg.baseOffset
	}

	if err := p.Compact([][]byte{[]byte("k")}, 2000); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	after := p.Size()
	if after >= before {
		t.Errorf("Size after Compact = %d, want less than before (%d)", after, before)
	}

	for _, base := range staleBases {
		if base == 0 {
			continue // base 0 is legitimately reused for the fresh, compacted segment
		}
		logPath := segmentFileBase(p.dir, base) + ".log"
		if _, err := os.Stat(logPath); !os.IsNotExist(err) {
			t.Errorf("stale segment file %s still exists after Compact (err=%v)", logPath, err)
		}
	}
}

func TestPartition_CompactWithNoRecordsLeavesPartitionEmpty(t *testing.T) {
	p := openTestPartition(t, 1<<20, 5, 0)
	defer p.Close()

	p.Append([]byte("stale"), 1, 1000)

	if err := p.Compact(nil, 2000); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if got := p.LogEndOffset(); got != 0 {
		t.Fatalf("LogEndOffset after compacting to nothing = %d, want 0", got)
	}
	if _, _, err := p.Read(0); err == nil {
		t.Error("expected Read(0) to error on an empty partition, got nil")
	}
}

// TestPartition_CompactSurvivesReopen proves Compact's result is real,
// durable on-disk state, not just an in-memory illusion - the same
// distinction OpenPartition's own restart-recovery logic exists to protect.
func TestPartition_CompactSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(dir, 1<<20, 5, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	for i := 0; i < 5; i++ {
		p.Append([]byte(fmt.Sprintf("stale-%d", i)), 1, 1000)
	}
	if err := p.Compact([][]byte{[]byte("kept")}, 2000); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := OpenPartition(dir, 1<<20, 5, 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	if got := reopened.LogEndOffset(); got != 1 {
		t.Fatalf("LogEndOffset after reopen = %d, want 1", got)
	}
	data, _, err := reopened.Read(0)
	if err != nil || string(data) != "kept" {
		t.Fatalf("Read(0) after reopen = (%q, %v), want (\"kept\", nil)", data, err)
	}
}

func TestPartition_EarliestOffsetIsZeroBeforeAnyRetention(t *testing.T) {
	p := openTestPartition(t, 1<<20, 5, 0)
	defer p.Close()

	for i := 0; i < 5; i++ {
		p.Append([]byte(fmt.Sprintf("record-%d", i)), 1, 1000)
	}

	if got := p.EarliestOffset(); got != 0 {
		t.Errorf("EarliestOffset = %d, want 0 (nothing ever deleted)", got)
	}
}

// backdateSegment rewrites a segment's .log file mtime, simulating "this
// segment was last written to at oldTime" without needing a real sleep.
func backdateSegment(t *testing.T, dir string, base int64, oldTime time.Time) {
	t.Helper()
	path := segmentFileBase(dir, base) + ".log"
	if err := os.Chtimes(path, oldTime, oldTime); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}
}

func TestPartition_ApplyRetention_DeletesExpiredSegmentsByTime(t *testing.T) {
	p := openTestPartition(t, 40, 1000, 0) // tiny segmentMaxBytes forces rolling
	defer p.Close()

	for i := 0; i < 20; i++ {
		p.Append([]byte(fmt.Sprintf("record-%d", i)), 1, 1000)
	}
	if len(p.segments) < 3 {
		t.Fatalf("test setup: expected at least 3 segments, got %d", len(p.segments))
	}

	now := time.Now()
	// Backdate every segment except the active (last) one to 10 days old.
	for _, sg := range p.segments[:len(p.segments)-1] {
		backdateSegment(t, p.dir, sg.baseOffset, now.Add(-10*24*time.Hour))
	}

	if err := p.ApplyRetention(7*24*time.Hour, 0, now); err != nil {
		t.Fatalf("ApplyRetention: %v", err)
	}

	if len(p.segments) != 1 {
		t.Fatalf("segments remaining = %d, want 1 (only the active segment)", len(p.segments))
	}
	if got, want := p.EarliestOffset(), p.segments[0].baseOffset; got != want {
		t.Errorf("EarliestOffset = %d, want %d (the active segment's own base offset)", got, want)
	}
	if p.EarliestOffset() == 0 {
		t.Error("EarliestOffset = 0, want it to have advanced past the deleted segments")
	}
}

func TestPartition_ApplyRetention_NeverDeletesTheActiveSegment(t *testing.T) {
	p := openTestPartition(t, 1<<20, 5, 0) // large segmentMaxBytes - single (active) segment
	defer p.Close()

	p.Append([]byte("record"), 1, 1000)

	now := time.Now()
	backdateSegment(t, p.dir, 0, now.Add(-365*24*time.Hour))

	if err := p.ApplyRetention(time.Hour, 0, now); err != nil {
		t.Fatalf("ApplyRetention: %v", err)
	}

	if len(p.segments) != 1 {
		t.Fatalf("segments remaining = %d, want 1 (the active segment survives no matter how old)", len(p.segments))
	}
	if got := p.EarliestOffset(); got != 0 {
		t.Errorf("EarliestOffset = %d, want 0 (nothing was actually deletable)", got)
	}
}

func TestPartition_ApplyRetention_ZeroMaxAgeDisablesTimeCheck(t *testing.T) {
	p := openTestPartition(t, 40, 1000, 0)
	defer p.Close()

	for i := 0; i < 20; i++ {
		p.Append([]byte(fmt.Sprintf("record-%d", i)), 1, 1000)
	}
	now := time.Now()
	for _, sg := range p.segments[:len(p.segments)-1] {
		backdateSegment(t, p.dir, sg.baseOffset, now.Add(-365*24*time.Hour))
	}

	if err := p.ApplyRetention(0, 0, now); err != nil {
		t.Fatalf("ApplyRetention: %v", err)
	}

	if len(p.segments) < 3 {
		t.Errorf("segments remaining = %d, want unchanged (maxAge=0 disables the time check)", len(p.segments))
	}
}

func TestPartition_ApplyRetention_DeletesOldestSegmentsBySize(t *testing.T) {
	p := openTestPartition(t, 40, 1000, 0)
	defer p.Close()

	for i := 0; i < 20; i++ {
		p.Append([]byte(fmt.Sprintf("record-%d", i)), 1, 1000)
	}
	totalBefore := p.Size()
	if len(p.segments) < 3 {
		t.Fatalf("test setup: expected at least 3 segments, got %d", len(p.segments))
	}

	// Budget for roughly half the current size - enough to force deleting
	// the oldest segment(s) while keeping the active one untouched.
	budget := totalBefore / 2

	if err := p.ApplyRetention(0, budget, time.Now()); err != nil {
		t.Fatalf("ApplyRetention: %v", err)
	}

	if p.Size() > totalBefore {
		t.Errorf("Size after ApplyRetention = %d, want <= starting size %d", p.Size(), totalBefore)
	}
	if p.EarliestOffset() == 0 {
		t.Error("EarliestOffset still 0, want at least one oldest segment deleted by the size budget")
	}
	// The active segment must always survive, regardless of budget.
	if len(p.segments) < 1 {
		t.Fatal("no segments remain - the active segment must never be deleted")
	}
}

// TestPartition_ApplyRetention_ActuallyReclaimsDiskSpace mirrors the
// equivalent compaction test - the whole point is that deleted segment
// files are gone from disk, not just unreferenced in memory.
func TestPartition_ApplyRetention_ActuallyReclaimsDiskSpace(t *testing.T) {
	p := openTestPartition(t, 40, 1000, 0)
	defer p.Close()

	for i := 0; i < 20; i++ {
		p.Append([]byte(fmt.Sprintf("record-%d", i)), 1, 1000)
	}
	var deletableBases []int64
	now := time.Now()
	for _, sg := range p.segments[:len(p.segments)-1] {
		deletableBases = append(deletableBases, sg.baseOffset)
		backdateSegment(t, p.dir, sg.baseOffset, now.Add(-10*24*time.Hour))
	}

	if err := p.ApplyRetention(7*24*time.Hour, 0, now); err != nil {
		t.Fatalf("ApplyRetention: %v", err)
	}

	for _, base := range deletableBases {
		logPath := segmentFileBase(p.dir, base) + ".log"
		if _, err := os.Stat(logPath); !os.IsNotExist(err) {
			t.Errorf("deleted segment file %s still exists on disk (err=%v)", logPath, err)
		}
	}
}

func TestPartition_ApplyRetention_NoOpWhenNothingIsEligible(t *testing.T) {
	p := openTestPartition(t, 1<<20, 5, 0)
	defer p.Close()

	for i := 0; i < 5; i++ {
		p.Append([]byte(fmt.Sprintf("record-%d", i)), 1, 1000)
	}

	if err := p.ApplyRetention(7*24*time.Hour, 1<<30, time.Now()); err != nil {
		t.Fatalf("ApplyRetention: %v", err)
	}
	if got := p.EarliestOffset(); got != 0 {
		t.Errorf("EarliestOffset = %d, want 0 (nothing eligible for deletion)", got)
	}
}

func TestPartition_ReadBatchWithinOneSegment(t *testing.T) {
	p := openTestPartition(t, 1<<20, 5, 0) // large segmentMaxBytes - single segment
	defer p.Close()

	for i := 0; i < 10; i++ {
		p.Append([]byte(fmt.Sprintf("record-%d", i)), 1, 1000)
	}

	got, err := p.ReadBatch(0, 1<<20)
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	var want []byte
	for i := 0; i < 10; i++ {
		want = append(want, []byte(fmt.Sprintf("record-%d", i))...)
	}
	if string(got) != string(want) {
		t.Errorf("ReadBatch(0, big) = %q, want %q", got, want)
	}
}

func TestPartition_ReadBatchStartsMidWindowUsesSparseIndexCorrectly(t *testing.T) {
	// indexEvery=5 means offsets 0..9 span two sparse-index windows -
	// starting the read partway through the second window is what exercises
	// the "skip blobs before the target, then start accumulating" logic.
	p := openTestPartition(t, 1<<20, 5, 0)
	defer p.Close()

	for i := 0; i < 10; i++ {
		p.Append([]byte(fmt.Sprintf("record-%d", i)), 1, 1000)
	}

	got, err := p.ReadBatch(7, 1<<20)
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	want := "record-7record-8record-9"
	if string(got) != want {
		t.Errorf("ReadBatch(7, big) = %q, want %q", got, want)
	}
}

func TestPartition_ReadBatchRespectsMaxBytes(t *testing.T) {
	p := openTestPartition(t, 1<<20, 5, 0)
	defer p.Close()

	for i := 0; i < 10; i++ {
		p.Append([]byte(fmt.Sprintf("record-%d", i)), 1, 1000) // 8 bytes each
	}

	// Budget for exactly 3 records (3*8=24), not enough for a 4th.
	got, err := p.ReadBatch(0, 24)
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	want := "record-0record-1record-2"
	if string(got) != want {
		t.Errorf("ReadBatch(0, 24) = %q, want %q", got, want)
	}
}

// TestPartition_ReadBatchFirstBlobExceedsMaxBytesReturnsEmpty matches
// DiskLog.Read's existing, deliberate contract: if even the first blob
// would overflow the budget, the result is empty rather than one
// over-budget blob - this must not change just because the accumulation
// loop moved into Partition.
func TestPartition_ReadBatchFirstBlobExceedsMaxBytesReturnsEmpty(t *testing.T) {
	p := openTestPartition(t, 1<<20, 5, 0)
	defer p.Close()

	p.Append([]byte("a-fairly-long-record"), 1, 1000)

	got, err := p.ReadBatch(0, 3)
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("ReadBatch(0, 3) = %q, want empty (first blob already exceeds maxBytes)", got)
	}
}

func TestPartition_ReadBatchCrossesSegments(t *testing.T) {
	p := openTestPartition(t, 40, 1000, 0) // tiny segmentMaxBytes forces rolling
	defer p.Close()

	for i := 0; i < 20; i++ {
		p.Append([]byte(fmt.Sprintf("record-%d", i)), 1, 1000)
	}
	if len(p.segments) < 2 {
		t.Fatalf("test setup: expected multiple segments, got %d", len(p.segments))
	}

	got, err := p.ReadBatch(0, 1<<20)
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	var want []byte
	for i := 0; i < 20; i++ {
		want = append(want, []byte(fmt.Sprintf("record-%d", i))...)
	}
	if string(got) != string(want) {
		t.Errorf("ReadBatch(0, big) across segments = %q, want %q", got, want)
	}
}

func TestPartition_ReadBatchStartingMidwayThroughARolledSegment(t *testing.T) {
	p := openTestPartition(t, 40, 1000, 0)
	defer p.Close()

	for i := 0; i < 20; i++ {
		p.Append([]byte(fmt.Sprintf("record-%d", i)), 1, 1000)
	}
	if len(p.segments) < 2 {
		t.Fatalf("test setup: expected multiple segments, got %d", len(p.segments))
	}

	got, err := p.ReadBatch(15, 1<<20)
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	var want []byte
	for i := 15; i < 20; i++ {
		want = append(want, []byte(fmt.Sprintf("record-%d", i))...)
	}
	if string(got) != string(want) {
		t.Errorf("ReadBatch(15, big) = %q, want %q", got, want)
	}
}

// TestPartition_ReadBatchOffsetOutOfRangeIsEmptyNotError matches
// DiskLog.Read's overall contract (never surfaces an error - Fetch's
// long-polling logic treats "nothing here yet" as an empty response, not a
// failure). ReadBatch itself owns that contract now that DiskLog.Read just
// delegates straight to it.
func TestPartition_ReadBatchOffsetOutOfRangeIsEmptyNotError(t *testing.T) {
	p := openTestPartition(t, 1<<20, 5, 0)
	defer p.Close()

	p.Append([]byte("only-record"), 1, 1000)

	got, err := p.ReadBatch(50, 1<<20)
	if err != nil {
		t.Fatalf("ReadBatch(50, ...) returned an error, want nil: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("ReadBatch(50, ...) = %q, want empty", got)
	}
}

// TestPartition_ReadBatchManyRecordsIsFast is the actual regression test
// for the bug this method exists to fix: DiskLog.Read used to restart the
// sparse-index lookup and rescan from that window's start for every single
// blob, making a full read O(n * indexEvery) instead of O(n). With
// indexEvery=100 and 5000 small records, the old behavior would take
// multiple real seconds (this is what a live benchmark run actually
// measured); a correct single-scan implementation finishes in milliseconds.
// The bound here is deliberately generous - not a tight benchmark, just a
// tripwire against ever regressing back to quadratic behavior.
func TestPartition_ReadBatchManyRecordsIsFast(t *testing.T) {
	p := openTestPartition(t, 1<<24, 100, 0) // matches production's real indexEvery
	defer p.Close()

	const n = 5000
	for i := 0; i < n; i++ {
		p.Append([]byte(fmt.Sprintf("record-%d", i)), 1, 1000)
	}

	start := time.Now()
	got, err := p.ReadBatch(0, 1<<20)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("ReadBatch returned no data")
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("ReadBatch of %d records took %s, want well under 500ms (quadratic regression?)", n, elapsed)
	}
}

func TestPartition_ConcurrentAppend_Safe(t *testing.T) {
	p := openTestPartition(t, 1<<20, 5, 0)
	defer p.Close()

	var wg sync.WaitGroup
	offsets := make(chan int64, 50)

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			offset, err := p.Append([]byte(fmt.Sprintf("record-%d", n)), 1, int64(n))
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
	p := openTestPartition(t, 1<<20, 5, 0)
	defer p.Close()

	for i := 0; i < 20; i++ {
		p.Append([]byte(fmt.Sprintf("record-%d", i)), 1, int64(i))
	}

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for offset := int64(0); offset < 20; offset++ {
				if _, _, err := p.Read(offset); err != nil {
					t.Errorf("read %d: %v", offset, err)
				}
			}
		}()
	}

	for i := 20; i < 40; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			p.Append([]byte(fmt.Sprintf("record-%d", n)), 1, int64(n))
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
	p := openTestPartition(t, 10, 1000, 0)
	defer p.Close()

	for i := 0; i < 5; i++ {
		offset, err := p.Append([]byte(fmt.Sprintf("record-%d", i)), 1, int64(i))
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
		data, _, err := p.Read(int64(i))
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

	p1, err := OpenPartition(dir, 10, 1000, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 5; i++ {
		p1.Append([]byte(fmt.Sprintf("record-%d", i)), 1, int64(i))
	}
	segmentsBefore := len(p1.segments)
	if err := p1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	p2, err := OpenPartition(dir, 10, 1000, 0)
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
	offset, err := p2.Append([]byte("record-5"), 1, 5)
	if err != nil {
		t.Fatalf("append after reopen: %v", err)
	}
	if offset != 5 {
		t.Fatalf("append after reopen: offset = %d, want 5", offset)
	}

	for i := 0; i < 6; i++ {
		data, _, err := p2.Read(int64(i))
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

	p1, err := OpenPartition(dir, 10, 1000, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 5; i++ {
		p1.Append([]byte(fmt.Sprintf("record-%d", i)), 1, int64(i))
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

	p2, err := OpenPartition(dir, 10, 1000, 0) // Recover() runs automatically per segment
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer p2.Close()

	// Every record written before the crash must still be readable,
	// across every segment, not just the one that was corrupted.
	for i := 0; i < 5; i++ {
		data, _, err := p2.Read(int64(i))
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
	offset, err := p2.Append([]byte("record-5"), 1, 5)
	if err != nil {
		t.Fatalf("append after recovery: %v", err)
	}
	if offset != 5 {
		t.Fatalf("append after recovery: offset = %d, want 5", offset)
	}
	data, _, err := p2.Read(5)
	if err != nil || string(data) != "record-5" {
		t.Fatalf("read offset 5 after recovery: %q, %v, want \"record-5\"", data, err)
	}
}

// TestPartition_FlushEveryMessagesTriggersAtThreshold pins down the
// count-based half of the fsync policy: sinceLastFlush must reset to 0 the
// moment it reaches flushEveryMessages, and keep counting normally below
// that threshold. Segment.Sync()'s actual fsync syscall has no portable,
// fast way to observe from a unit test (it either succeeds silently or the
// append itself would have already failed) - this instead pins down the
// triggering logic that decides when Sync gets called, which is the part
// this change actually adds.
func TestPartition_FlushEveryMessagesTriggersAtThreshold(t *testing.T) {
	p := openTestPartition(t, 1<<20, 5, 3) // flush every 3 messages
	defer p.Close()

	for i := 0; i < 2; i++ {
		if _, err := p.Append([]byte("r"), 1, 1000); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if p.sinceLastFlush != 2 {
		t.Fatalf("sinceLastFlush = %d after 2 appends, want 2 (below threshold)", p.sinceLastFlush)
	}

	if _, err := p.Append([]byte("r"), 1, 1000); err != nil {
		t.Fatalf("append 3: %v", err)
	}
	if p.sinceLastFlush != 0 {
		t.Fatalf("sinceLastFlush = %d after 3rd append, want 0 (threshold reached, flushed)", p.sinceLastFlush)
	}

	for i := 0; i < 2; i++ {
		if _, err := p.Append([]byte("r"), 1, 1000); err != nil {
			t.Fatalf("append after flush %d: %v", i, err)
		}
	}
	if p.sinceLastFlush != 2 {
		t.Fatalf("sinceLastFlush = %d after 2 more appends, want 2 (counting resumed from 0)", p.sinceLastFlush)
	}
}

// TestPartition_FlushEveryMessagesZeroDisablesCountBasedFlush confirms 0
// means "never flush on count alone" - matching maxAge/maxBytes's own 0-
// disables convention on ApplyRetention, rather than "flush every 0
// messages" (which would be nonsensical - it would have to mean every
// single append, an entirely different policy from "disabled").
func TestPartition_FlushEveryMessagesZeroDisablesCountBasedFlush(t *testing.T) {
	p := openTestPartition(t, 1<<20, 5, 0)
	defer p.Close()

	for i := 0; i < 50; i++ {
		if _, err := p.Append([]byte("r"), 1, 1000); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if p.sinceLastFlush != 50 {
		t.Fatalf("sinceLastFlush = %d after 50 appends with flush disabled, want 50 (never reset)", p.sinceLastFlush)
	}
}

// TestPartition_RollingFlushesOutgoingSegmentRegardlessOfCount proves the
// roll-time flush is unconditional: flushEveryMessages is set high enough
// that count-based flushing alone would never trigger, yet a roll must
// still flush the segment being retired, since it will never be appended to
// again and therefore would otherwise sit unflushed forever.
//
// Every record is the same fixed 1-byte payload (9 bytes on disk once the
// 8-byte header is added), so with segmentMaxBytes=20 the 4th append is the
// first one where active.seg.Size() (27, after 3 appends) has crossed the
// cap - a deterministic, exactly-once roll, rather than "at least 2
// segments" the way Compact's own roll-forcing tests only need.
func TestPartition_RollingFlushesOutgoingSegmentRegardlessOfCount(t *testing.T) {
	p := openTestPartition(t, 20, 1000, 1000) // flush count never reached on its own
	defer p.Close()

	for i := 0; i < 4; i++ {
		if _, err := p.Append([]byte("r"), 1, 1000); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if len(p.segments) != 2 {
		t.Fatalf("test setup: expected exactly 2 segments after the 4th append, got %d", len(p.segments))
	}

	// sinceLastFlush is reset to 0 by the roll, then immediately
	// incremented by the append that triggered it - so 1, not 0, is what
	// proves the roll-time reset actually ran rather than count-based
	// flushing (which would need 1000 appends, far more than this test
	// makes).
	if p.sinceLastFlush != 1 {
		t.Fatalf("sinceLastFlush = %d right after a roll, want 1 (reset by the roll, then incremented once)", p.sinceLastFlush)
	}
}

// TestPartition_SyncFlushesActiveSegmentAndResetsCounter is the time-based
// half of the fsync policy: an explicit Sync call (what cmd/broker's
// runFlush ticker calls) must succeed and reset sinceLastFlush exactly like
// a count-triggered flush would, so the two triggers stay consistent with
// each other regardless of which one fires.
func TestPartition_SyncFlushesActiveSegmentAndResetsCounter(t *testing.T) {
	p := openTestPartition(t, 1<<20, 5, 1000) // count threshold never reached
	defer p.Close()

	for i := 0; i < 4; i++ {
		if _, err := p.Append([]byte("r"), 1, 1000); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if p.sinceLastFlush != 4 {
		t.Fatalf("sinceLastFlush = %d before Sync, want 4", p.sinceLastFlush)
	}

	if err := p.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if p.sinceLastFlush != 0 {
		t.Fatalf("sinceLastFlush = %d after Sync, want 0", p.sinceLastFlush)
	}
}
