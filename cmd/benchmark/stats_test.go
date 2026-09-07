package main

import (
	"testing"
	"time"
)

func TestSummarize_Empty(t *testing.T) {
	s := Summarize(nil, 0, 0, 5*time.Second)

	if s.Records != 0 {
		t.Errorf("Records = %d, want 0", s.Records)
	}
	if s.RecordsPerSec != 0 {
		t.Errorf("RecordsPerSec = %v, want 0", s.RecordsPerSec)
	}
	if s.P50 != 0 || s.P95 != 0 || s.P99 != 0 {
		t.Errorf("percentiles = %v/%v/%v, want all 0", s.P50, s.P95, s.P99)
	}
}

func TestSummarize_ZeroElapsed(t *testing.T) {
	// A phase that completes instantly (or a caller passing 0 by mistake)
	// must not divide by zero.
	s := Summarize([]time.Duration{time.Millisecond}, 1, 100, 0)

	if s.RecordsPerSec != 0 {
		t.Errorf("RecordsPerSec = %v, want 0 when elapsed is 0", s.RecordsPerSec)
	}
	if s.MBPerSec != 0 {
		t.Errorf("MBPerSec = %v, want 0 when elapsed is 0", s.MBPerSec)
	}
}

func TestSummarize_Throughput(t *testing.T) {
	latencies := make([]time.Duration, 1000)
	for i := range latencies {
		latencies[i] = time.Millisecond
	}

	s := Summarize(latencies, 1000, 1024*1024, 2*time.Second)

	if s.Records != 1000 {
		t.Errorf("Records = %d, want 1000", s.Records)
	}
	if s.RecordsPerSec != 500 {
		t.Errorf("RecordsPerSec = %v, want 500", s.RecordsPerSec)
	}
	if s.MBPerSec != 0.5 {
		t.Errorf("MBPerSec = %v, want 0.5", s.MBPerSec)
	}
}

// TestSummarize_RecordsDecoupledFromRequestCount is the Fetch-phase shape:
// one request (one latency sample) can carry many records, unlike Produce
// where each request is exactly one record. Records/throughput must reflect
// the real record count, not how many requests it took to move them.
func TestSummarize_RecordsDecoupledFromRequestCount(t *testing.T) {
	latencies := []time.Duration{10 * time.Millisecond} // one Fetch call

	s := Summarize(latencies, 5000, 0, time.Second) // ...carrying 5000 records

	if s.Records != 5000 {
		t.Errorf("Records = %d, want 5000 (the real record count, not len(latencies)=1)", s.Records)
	}
	if s.RecordsPerSec != 5000 {
		t.Errorf("RecordsPerSec = %v, want 5000", s.RecordsPerSec)
	}
	if s.P50 != 10*time.Millisecond {
		t.Errorf("P50 = %v, want 10ms (from the single request's latency)", s.P50)
	}
}

func TestSummarize_Percentiles(t *testing.T) {
	// 100 samples, 1ms through 100ms, fed in reverse order to prove
	// Summarize sorts rather than assuming sorted input.
	latencies := make([]time.Duration, 100)
	for i := range latencies {
		latencies[i] = time.Duration(100-i) * time.Millisecond
	}

	s := Summarize(latencies, 100, 0, time.Second)

	// Index-based percentile of a 100-element sorted slice [1ms..100ms]:
	// p50 -> index 50 -> value 51ms, p95 -> index 95 -> value 96ms,
	// p99 -> index 99 -> value 100ms (the max, clamped).
	if s.P50 != 51*time.Millisecond {
		t.Errorf("P50 = %v, want 51ms", s.P50)
	}
	if s.P95 != 96*time.Millisecond {
		t.Errorf("P95 = %v, want 96ms", s.P95)
	}
	if s.P99 != 100*time.Millisecond {
		t.Errorf("P99 = %v, want 100ms", s.P99)
	}
}

func TestSummarize_SingleSample(t *testing.T) {
	s := Summarize([]time.Duration{7 * time.Millisecond}, 1, 0, time.Second)

	if s.P50 != 7*time.Millisecond || s.P95 != 7*time.Millisecond || s.P99 != 7*time.Millisecond {
		t.Errorf("percentiles = %v/%v/%v, want all 7ms for a single sample", s.P50, s.P95, s.P99)
	}
}
