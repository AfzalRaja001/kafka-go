package storage

import (
	"sync"
	"sync/atomic"
)

// Partition wraps one Segment and Index pair behind a sync.RWMutex: Append
// takes the exclusive lock (mutates sinceLastIdx and the underlying files),
// Read takes the shared lock (many concurrent readers, matching a real
// partition's read-heavy access pattern).
//
// This wraps exactly one segment/index pair - multi-segment rolling is
// separate work layered on top later, not handled here.
type Partition struct {
	mu           sync.RWMutex
	seg          *Segment
	idx          *Index
	timeindex    *Timeindex
	indexEvery   int32
	sinceLastIdx int32
	nextOffset   atomic.Int64
}

func OpenPartition(logPath, indexPath, timeindexPath string, indexEvery int32) (*Partition, error) {
	seg, err := OpenSegment(logPath)
	if err != nil {
		return nil, err
	}
	idx, err := OpenIndex(indexPath)
	if err != nil {
		seg.Close()
		return nil, err
	}
	timeindex, err := OpenTimeindex(timeindexPath)
	if err != nil {
		seg.Close()
		idx.Close()
		return nil, err
	}
	return &Partition{
		seg:        seg,
		idx:        idx,
		timeindex:  timeindex,
		indexEvery: indexEvery,
	}, nil
}

// Append writes data and records it at the given timestamp. timestamp comes
// from the caller rather than time.Now() internally: until record batch
// parsing exists (Phase 2/3), there's no batch-derived timestamp to use, so
// callers - currently just tests - supply one explicitly instead of this
// method silently pretending to have a real one.
func (p *Partition) Append(data []byte, timestamp int64) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	pos, err := p.seg.Append(data)
	if err != nil {
		return 0, err
	}

	offset := p.nextOffset.Load()

	if p.sinceLastIdx == 0 {
		p.idx.Append(int32(offset), int32(pos))
		p.timeindex.Append(timestamp, int32(offset))
	}

	p.sinceLastIdx = (p.sinceLastIdx + 1) % p.indexEvery
	p.nextOffset.Add(1)

	return offset, nil
}

func (p *Partition) Read(offset int64) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return FindRecord(p.seg, p.idx, int32(offset))
}

// LogEndOffset is deliberately lock-free: nextOffset doesn't need to be
// atomic for Append's own correctness (Append already holds mu for its
// whole body), but atomic.Int64 lets any goroutine - e.g. a long-polling
// Fetch checking "did new data arrive" - read it cheaply without contending
// with mu at all.
func (p *Partition) LogEndOffset() int64 {
	return p.nextOffset.Load()
}

// LookupOffsetByTimestamp returns the offset of the nearest indexed entry
// at or before targetTimestamp. This is index-granularity, not record-exact
// - pinpointing the precise record additionally needs record batch parsing,
// which doesn't exist yet, so this returns only what the sparse time index
// alone can honestly answer.
func (p *Partition) LookupOffsetByTimestamp(targetTimestamp int64) (int64, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	_, relOffset, found := p.timeindex.Lookup(targetTimestamp)
	return int64(relOffset), found
}

func (p *Partition) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.seg.Close(); err != nil {
		return err
	}
	if err := p.idx.Close(); err != nil {
		return err
	}
	return p.timeindex.Close()
}
