package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// segmentGroup is one segment file plus its two sparse indexes, all sharing
// the same base offset (also their common filename, zero-padded to 20
// digits, per the real Kafka segment naming convention).
type segmentGroup struct {
	baseOffset int64
	seg        *Segment
	idx        *Index
	timeindex  *Timeindex
}

// Partition wraps an ordered list of segmentGroups behind a sync.RWMutex:
// Append takes the exclusive lock, Read takes the shared lock, matching a
// real partition's read-heavy access pattern. Exactly one segment - the
// last in the list - is active (writable); every segment before it is
// immutable, never touched again except to be read.
type Partition struct {
	mu              sync.RWMutex
	dir             string
	segmentMaxBytes int64
	indexEvery      int32
	segments        []*segmentGroup
	sinceLastIdx    int32
	nextOffset      atomic.Int64
}

// OpenPartition opens dir as a partition directory, discovering any
// existing segments by scanning for *.log files and parsing their base
// offset back out of the filename. A brand-new directory gets a single
// segment starting at offset 0.
func OpenPartition(dir string, segmentMaxBytes int64, indexEvery int32) (*Partition, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	bases, err := discoverSegmentBases(dir)
	if err != nil {
		return nil, err
	}

	p := &Partition{dir: dir, segmentMaxBytes: segmentMaxBytes, indexEvery: indexEvery}

	if len(bases) == 0 {
		sg, err := openSegmentGroup(dir, 0)
		if err != nil {
			return nil, err
		}
		p.segments = []*segmentGroup{sg}
		return p, nil
	}

	for _, base := range bases {
		sg, err := openSegmentGroup(dir, base)
		if err != nil {
			return nil, err
		}
		p.segments = append(p.segments, sg)
	}

	// Only the active (last, highest base offset) segment could have a torn
	// write - every earlier segment is immutable and was already valid the
	// moment it stopped being active. OpenSegment already ran Recover() on
	// each one above; this just restores the in-memory counters.
	active := p.segments[len(p.segments)-1]
	blobs, offsets, err := active.seg.Counts()
	if err != nil {
		return nil, err
	}
	// nextOffset advances by offsets (a batch may span several), while the
	// sparse-index counter advances by blobs (one index entry every Nth
	// append). Conflating these two is the bug this file's Counts call fixes.
	p.nextOffset.Store(active.baseOffset + offsets)
	p.sinceLastIdx = int32(blobs % int64(indexEvery))

	return p, nil
}

func discoverSegmentBases(dir string) ([]int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var bases []int64
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".log") {
			continue
		}
		base, err := strconv.ParseInt(strings.TrimSuffix(name, ".log"), 10, 64)
		if err != nil {
			continue // not a segment file we recognize - skip rather than fail startup
		}
		bases = append(bases, base)
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })
	return bases, nil
}

func segmentFileBase(dir string, baseOffset int64) string {
	return filepath.Join(dir, fmt.Sprintf("%020d", baseOffset))
}

func openSegmentGroup(dir string, baseOffset int64) (*segmentGroup, error) {
	base := segmentFileBase(dir, baseOffset)

	seg, err := OpenSegment(base + ".log")
	if err != nil {
		return nil, err
	}
	idx, err := OpenIndex(base + ".index")
	if err != nil {
		seg.Close()
		return nil, err
	}
	timeindex, err := OpenTimeindex(base + ".timeindex")
	if err != nil {
		seg.Close()
		idx.Close()
		return nil, err
	}

	return &segmentGroup{baseOffset: baseOffset, seg: seg, idx: idx, timeindex: timeindex}, nil
}

// Append writes data as one blob spanning offsetSpan offsets, recorded at
// the given timestamp, rolling to a new segment first if the active one has
// reached segmentMaxBytes. It returns the blob's base offset - the first of
// the offsetSpan offsets it now owns.
//
// offsetSpan comes from the caller (ultimately a record batch's record
// count) rather than being derived here, because deriving it would mean
// parsing Kafka's record batch format inside the storage engine - exactly
// the protocol/storage coupling the Log interface exists to prevent.
//
// timestamp likewise comes from the caller, not time.Now() internally.
func (p *Partition) Append(data []byte, offsetSpan int32, timestamp int64) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Every append must advance the log by at least one offset. A batch
	// claiming zero would leave the next append sharing its base offset,
	// making the earlier blob permanently unreachable (FindRecord would
	// always resolve that offset to the first one). Real producers never
	// send empty batches; this is purely defensive.
	if offsetSpan < 1 {
		offsetSpan = 1
	}

	active := p.segments[len(p.segments)-1]

	if active.seg.Size() >= p.segmentMaxBytes {
		newBase := p.nextOffset.Load()
		sg, err := openSegmentGroup(p.dir, newBase)
		if err != nil {
			return 0, err
		}
		p.segments = append(p.segments, sg)
		active = sg
		p.sinceLastIdx = 0 // a new segment's index restarts fresh, relative offset 0
	}

	pos, err := active.seg.Append(data, offsetSpan)
	if err != nil {
		return 0, err
	}

	offset := p.nextOffset.Load()
	relOffset := int32(offset - active.baseOffset)

	if p.sinceLastIdx == 0 {
		active.idx.Append(relOffset, int32(pos))
		active.timeindex.Append(timestamp, relOffset)
	}

	p.sinceLastIdx = (p.sinceLastIdx + 1) % p.indexEvery
	p.nextOffset.Add(int64(offsetSpan))

	return offset, nil
}

// findSegment returns the segment whose base offset is the largest one not
// exceeding offset - the same "binary search to the nearest lower entry"
// idea as Index.Lookup, applied one level up, to segments instead of
// records within one segment.
func (p *Partition) findSegment(offset int64) *segmentGroup {
	i := sort.Search(len(p.segments), func(i int) bool {
		return p.segments[i].baseOffset > offset
	})
	if i == 0 {
		return nil
	}
	return p.segments[i-1]
}

// Read returns the batch containing offset, plus the offset immediately
// after that batch. Callers walking the log forward use the returned next
// offset rather than incrementing by one, since a batch may span many
// offsets.
func (p *Partition) Read(offset int64) (data []byte, nextOffset int64, err error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	sg := p.findSegment(offset)
	if sg == nil {
		return nil, 0, fmt.Errorf("offset %d out of range", offset)
	}

	data, nextRel, err := FindRecord(sg.seg, sg.idx, int32(offset-sg.baseOffset))
	if err != nil {
		return nil, 0, err
	}
	return data, sg.baseOffset + int64(nextRel), nil
}

// LogEndOffset is deliberately lock-free: nextOffset doesn't need to be
// atomic for Append's own correctness (Append already holds mu for its
// whole body), but atomic.Int64 lets any goroutine - e.g. a long-polling
// Fetch checking "did new data arrive" - read it cheaply without contending
// with mu at all.
func (p *Partition) LogEndOffset() int64 {
	return p.nextOffset.Load()
}

// Size sums every segment's on-disk byte size, active and rolled alike -
// each Segment already tracks its own size in memory (updated on every
// Append), so this costs no disk I/O.
func (p *Partition) Size() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var total int64
	for _, sg := range p.segments {
		total += sg.seg.size
	}
	return total
}

// LookupOffsetByTimestamp returns the offset of the nearest indexed entry
// at or before targetTimestamp, checking every segment's Timeindex.
// Segments are ordered by base offset, which - since records are always
// appended in order - also means ascending timestamp order under normal
// operation, so the last segment with any match holds the best (largest,
// still not exceeding target) answer across the whole partition.
func (p *Partition) LookupOffsetByTimestamp(targetTimestamp int64) (int64, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var bestOffset int64
	found := false
	for _, sg := range p.segments {
		if _, relOffset, ok := sg.timeindex.Lookup(targetTimestamp); ok {
			bestOffset = sg.baseOffset + int64(relOffset)
			found = true
		}
	}
	return bestOffset, found
}

func (p *Partition) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, sg := range p.segments {
		if err := sg.seg.Close(); err != nil {
			return err
		}
		if err := sg.idx.Close(); err != nil {
			return err
		}
		if err := sg.timeindex.Close(); err != nil {
			return err
		}
	}
	return nil
}
