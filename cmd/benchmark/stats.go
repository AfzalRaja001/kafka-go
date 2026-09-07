package main

import (
	"sort"
	"time"
)

// Summary is the result of measuring one phase (a batch of Produce or Fetch
// requests) run for a fixed duration: how much work completed, how fast,
// and the shape of its per-request latency distribution.
type Summary struct {
	Records       int64
	Bytes         int64
	Elapsed       time.Duration
	RecordsPerSec float64
	MBPerSec      float64
	P50           time.Duration
	P95           time.Duration
	P99           time.Duration
}

// Summarize turns raw per-request latency samples into a Summary. latencies
// need not be pre-sorted - this sorts its own copy. records is deliberately
// a separate parameter from len(latencies): for Produce, one request moves
// exactly one record, so they happen to match, but for Fetch a single
// request can return many records at once - percentiles come from the
// per-request latency samples, while throughput must reflect the real
// record count, not how many requests it took. A phase that produced no
// samples, or one whose caller measured a zero elapsed duration, gets a
// summary with zeroed throughput rather than a divide-by-zero.
func Summarize(latencies []time.Duration, records, bytes int64, elapsed time.Duration) Summary {
	s := Summary{
		Records: records,
		Bytes:   bytes,
		Elapsed: elapsed,
	}
	if elapsed > 0 {
		s.RecordsPerSec = float64(records) / elapsed.Seconds()
		s.MBPerSec = float64(bytes) / (1024 * 1024) / elapsed.Seconds()
	}
	if len(latencies) == 0 {
		return s
	}

	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	s.P50 = percentile(sorted, 0.50)
	s.P95 = percentile(sorted, 0.95)
	s.P99 = percentile(sorted, 0.99)
	return s
}

// percentile returns the value at the p-th percentile of sorted, which must
// already be sorted ascending and non-empty. The index is clamped to the
// last element so p99 of a small sample returns the max rather than
// indexing out of bounds.
func percentile(sorted []time.Duration, p float64) time.Duration {
	idx := int(p * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
