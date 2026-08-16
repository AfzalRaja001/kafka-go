package storage

import (
	"fmt"
	"testing"
)

// The tests in this file all cover one bug: until now every layer of the
// storage engine assumed one appended blob consumed exactly one offset.
// That held only because every earlier test appended one record at a time.
// Real Kafka producers batch - kafka-python routinely packs several records
// into a single record batch - and a batch spanning N offsets that only
// advances the log by 1 makes LatestOffset under-report, which in turn makes
// Produce assign a baseOffset that collides with records already written.

func TestPartition_MultiRecordBatchAdvancesOffsetBySpan(t *testing.T) {
	p := openTestPartition(t, 1<<20, 5)
	defer p.Close()

	// One blob standing in for a 5-record batch.
	offset, err := p.Append([]byte("batch-of-five"), 5, 1000)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if offset != 0 {
		t.Fatalf("first batch base offset = %d, want 0", offset)
	}

	if got := p.LogEndOffset(); got != 5 {
		t.Fatalf("LogEndOffset after a 5-record batch = %d, want 5", got)
	}

	// The next batch must start after all five, not collide with offsets 1-4.
	offset, err = p.Append([]byte("batch-of-three"), 3, 1001)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if offset != 5 {
		t.Fatalf("second batch base offset = %d, want 5", offset)
	}
	if got := p.LogEndOffset(); got != 8 {
		t.Fatalf("LogEndOffset after 5+3 records = %d, want 8", got)
	}
}

// TestPartition_ReadResolvesOffsetInsideBatch checks the read side of the
// same idea: asking for any offset a batch covers returns that whole batch,
// which is exactly what real Kafka does - the client discards records below
// its fetch offset, the broker never splits a batch apart.
func TestPartition_ReadResolvesOffsetInsideBatch(t *testing.T) {
	p := openTestPartition(t, 1<<20, 5)
	defer p.Close()

	p.Append([]byte("first-batch"), 5, 1000)  // offsets 0-4
	p.Append([]byte("second-batch"), 3, 1001) // offsets 5-7

	for _, tc := range []struct {
		offset   int64
		want     string
		wantNext int64
	}{
		{0, "first-batch", 5},
		{3, "first-batch", 5},
		{4, "first-batch", 5},
		{5, "second-batch", 8},
		{7, "second-batch", 8},
	} {
		data, next, err := p.Read(tc.offset)
		if err != nil {
			t.Fatalf("read %d: %v", tc.offset, err)
		}
		if string(data) != tc.want {
			t.Errorf("read %d: got %q, want %q", tc.offset, data, tc.want)
		}
		if next != tc.wantNext {
			t.Errorf("read %d: next offset = %d, want %d", tc.offset, next, tc.wantNext)
		}
	}
}

// TestPartition_MultiRecordSpanSurvivesRestart is the durability half: the
// per-blob offset span has to be recoverable from the segment file itself,
// with nothing else persisted. If it weren't, a restart would rebuild
// nextOffset by counting blobs and silently rewind the log end.
func TestPartition_MultiRecordSpanSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	p1, err := OpenPartition(dir, 1<<20, 5)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	p1.Append([]byte("batch-a"), 4, 1000) // offsets 0-3
	p1.Append([]byte("batch-b"), 6, 1001) // offsets 4-9
	if err := p1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	p2, err := OpenPartition(dir, 1<<20, 5)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer p2.Close()

	if got := p2.LogEndOffset(); got != 10 {
		t.Fatalf("LogEndOffset after reopen = %d, want 10", got)
	}

	offset, err := p2.Append([]byte("batch-c"), 2, 1002)
	if err != nil {
		t.Fatalf("append after reopen: %v", err)
	}
	if offset != 10 {
		t.Fatalf("append after reopen: base offset = %d, want 10", offset)
	}
}

// TestPartition_MultiRecordAcrossSegments proves the span arithmetic stays
// correct across a segment roll, where offsets are tracked relative to each
// segment's own base offset rather than the partition's.
func TestPartition_MultiRecordAcrossSegments(t *testing.T) {
	p := openTestPartition(t, 10, 1000) // tiny cap - every append rolls
	defer p.Close()

	bases := []int64{}
	for i := 0; i < 4; i++ {
		offset, err := p.Append([]byte(fmt.Sprintf("batch-%d", i)), 3, int64(1000+i))
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		bases = append(bases, offset)
	}

	want := []int64{0, 3, 6, 9}
	for i, got := range bases {
		if got != want[i] {
			t.Fatalf("batch %d base offset = %d, want %d", i, got, want[i])
		}
	}
	if len(p.segments) < 2 {
		t.Fatalf("expected a segment roll, got %d segment(s)", len(p.segments))
	}

	// Every offset inside every batch must resolve to the right batch.
	for i := 0; i < 4; i++ {
		for delta := int64(0); delta < 3; delta++ {
			target := int64(i*3) + delta
			data, _, err := p.Read(target)
			if err != nil {
				t.Fatalf("read %d: %v", target, err)
			}
			if want := fmt.Sprintf("batch-%d", i); string(data) != want {
				t.Errorf("read %d: got %q, want %q", target, data, want)
			}
		}
	}
}
