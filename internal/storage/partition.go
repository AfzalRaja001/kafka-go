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
	indexEvery   int32
	sinceLastIdx int32
	nextOffset   atomic.Int64
}

func OpenPartition(logPath, indexPath string, indexEvery int32) (*Partition, error) {
	seg, err := OpenSegment(logPath)
	if err != nil {
		return nil, err
	}
	idx, err := OpenIndex(indexPath)
	if err != nil {
		seg.Close()
		return nil, err
	}
	return &Partition{
		seg:        seg,
		idx:        idx,
		indexEvery: indexEvery,
	}, nil
}

func (p *Partition) Append(data []byte) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	pos, err := p.seg.Append(data)
	if err != nil {
		return 0, err
	}

	offset := p.nextOffset.Load()

	if p.sinceLastIdx == 0 {
		p.idx.Append(int32(offset), int32(pos))
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

func (p *Partition) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.seg.Close(); err != nil {
		return err
	}
	return p.idx.Close()
}
