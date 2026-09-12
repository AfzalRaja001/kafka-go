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
	"time"
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
	mu                 sync.RWMutex
	dir                string
	segmentMaxBytes    int64
	indexEvery         int32
	flushEveryMessages int32
	segments           []*segmentGroup
	sinceLastIdx       int32
	sinceLastFlush     int32
	nextOffset         atomic.Int64
}

// OpenPartition opens dir as a partition directory, discovering any
// existing segments by scanning for *.log files and parsing their base
// offset back out of the filename. A brand-new directory gets a single
// segment starting at offset 0.
//
// flushEveryMessages is the count-based half of the fsync policy: once this
// many records have been appended since the active segment's last flush,
// Append fsyncs it. 0 disables this check entirely (the time-based half,
// Sync, still works regardless - see its own doc comment).
func OpenPartition(dir string, segmentMaxBytes int64, indexEvery, flushEveryMessages int32) (*Partition, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	bases, err := discoverSegmentBases(dir)
	if err != nil {
		return nil, err
	}

	p := &Partition{dir: dir, segmentMaxBytes: segmentMaxBytes, indexEvery: indexEvery, flushEveryMessages: flushEveryMessages}

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
	return p.appendLocked(data, offsetSpan, timestamp)
}

// appendLocked is Append's real body, split out so Compact can append its
// replacement records without re-entering p.mu - callers must already hold
// it (exclusively).
func (p *Partition) appendLocked(data []byte, offsetSpan int32, timestamp int64) (int64, error) {
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
		// The outgoing segment gets no more Append calls ever again, so its
		// count-based flush check above would never fire for whatever's
		// left unflushed since its own last check - fsync it unconditionally
		// here instead, regardless of flushEveryMessages or sinceLastFlush.
		if err := active.seg.Sync(); err != nil {
			return 0, err
		}

		newBase := p.nextOffset.Load()
		sg, err := openSegmentGroup(p.dir, newBase)
		if err != nil {
			return 0, err
		}
		p.segments = append(p.segments, sg)
		active = sg
		p.sinceLastIdx = 0   // a new segment's index restarts fresh, relative offset 0
		p.sinceLastFlush = 0 // the new segment starts with nothing unflushed
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

	// The time-based half of the fsync policy (Sync) covers a partition too
	// low-traffic to ever reach flushEveryMessages; this covers the reverse
	// case - bursty traffic that would otherwise sit unflushed in the OS
	// page cache for the entire time-based interval.
	p.sinceLastFlush++
	if p.flushEveryMessages > 0 && p.sinceLastFlush >= p.flushEveryMessages {
		if err := active.seg.Sync(); err != nil {
			return 0, err
		}
		p.sinceLastFlush = 0
	}

	return offset, nil
}

// Compact fully replaces this partition's on-disk log with records, each
// becoming one blob at a freshly renumbered offset starting at 0. Every
// existing segment (its .log, .index, and .timeindex files) is closed and
// deleted before the replacement is written - that deletion is the actual
// space reclamation this feature exists for, not just an in-memory swap.
//
// See the Log interface's own doc comment for why this renumbers rather
// than preserving original offsets: safe only for a partition nothing ever
// reads by a specific offset, only by full sequential replay from 0.
func (p *Partition) Compact(records [][]byte, timestamp int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	staleBases := make([]int64, len(p.segments))
	for i, sg := range p.segments {
		staleBases[i] = sg.baseOffset
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
	// Every old file must be gone before opening the replacement below -
	// OpenSegment/OpenIndex/OpenTimeindex all open with O_APPEND, not
	// O_TRUNC, so reopening base offset 0 without removing its old files
	// first would silently append the fresh records after the stale ones
	// instead of replacing them.
	for _, base := range staleBases {
		if err := removeSegmentGroupFiles(p.dir, base); err != nil {
			return err
		}
	}

	fresh, err := openSegmentGroup(p.dir, 0)
	if err != nil {
		return err
	}
	p.segments = []*segmentGroup{fresh}
	p.nextOffset.Store(0)
	p.sinceLastIdx = 0
	p.sinceLastFlush = 0

	for _, data := range records {
		if _, err := p.appendLocked(data, 1, timestamp); err != nil {
			return err
		}
	}
	return nil
}

// ApplyRetention deletes whole rolled segments from the front of this
// partition - oldest first - when either maxAge or maxBytes says they're no
// longer needed. Either can be 0 to disable that check entirely, matching
// real Kafka's own retention.ms=-1/retention.bytes=-1 "unlimited" convention.
// The active (last) segment is never a candidate - it's still being written
// to, the same protection Compact gives it.
//
// Unlike Compact, this never renumbers anything: deleting old segments just
// moves EarliestOffset forward, leaving a real gap before it. That's normal,
// expected Kafka behavior (a Fetch below EarliestOffset should error, not
// silently return nothing - see ErrOffsetOutOfRange in internal/protocol),
// not something this method needs to work around.
//
// Segments are chronologically ordered by construction (append-only), so a
// linear scan from the front that stops at the first surviving segment is
// correct and sufficient: age only decreases going forward, and the running
// total only shrinks as segments are removed, so nothing later could still
// need deleting once the current segment doesn't.
func (p *Partition) ApplyRetention(maxAge time.Duration, maxBytes int64, now time.Time) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var totalSize int64
	for _, sg := range p.segments {
		totalSize += sg.seg.size
	}

	survivorsFrom := 0
	for survivorsFrom < len(p.segments)-1 { // never consider the active segment
		sg := p.segments[survivorsFrom]

		expiredByAge := false
		if maxAge > 0 {
			mtime, err := sg.seg.ModTime()
			if err != nil {
				return err
			}
			expiredByAge = now.Sub(mtime) > maxAge
		}
		overBudget := maxBytes > 0 && totalSize > maxBytes

		if !expiredByAge && !overBudget {
			break
		}

		totalSize -= sg.seg.size
		if err := sg.seg.Close(); err != nil {
			return err
		}
		if err := sg.idx.Close(); err != nil {
			return err
		}
		if err := sg.timeindex.Close(); err != nil {
			return err
		}
		if err := removeSegmentGroupFiles(p.dir, sg.baseOffset); err != nil {
			return err
		}
		survivorsFrom++
	}

	if survivorsFrom > 0 {
		p.segments = p.segments[survivorsFrom:]
	}
	return nil
}

// removeSegmentGroupFiles deletes one segment's three files. Windows can
// briefly hold a file handle open past Close() returning, the same reason
// DiskLog.DeletePartition retries os.RemoveAll on the whole partition
// directory - the same defense applies here at the individual-file level.
func removeSegmentGroupFiles(dir string, base int64) error {
	name := segmentFileBase(dir, base)
	for _, ext := range []string{".log", ".index", ".timeindex"} {
		if err := removeAllWithRetry(os.Remove, name+ext, 5, 20*time.Millisecond); err != nil {
			return err
		}
	}
	return nil
}

// findSegmentIndex returns the index into p.segments of the segment whose
// base offset is the largest one not exceeding offset - the same "binary
// search to the nearest lower entry" idea as Index.Lookup, applied one
// level up, to segments instead of records within one segment. -1 means no
// segment starts at or before offset.
func (p *Partition) findSegmentIndex(offset int64) int {
	i := sort.Search(len(p.segments), func(i int) bool {
		return p.segments[i].baseOffset > offset
	})
	return i - 1
}

// findSegment is findSegmentIndex, dereferenced - kept as its own method
// since most callers want the segment itself, not its position.
func (p *Partition) findSegment(offset int64) *segmentGroup {
	i := p.findSegmentIndex(offset)
	if i < 0 {
		return nil
	}
	return p.segments[i]
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

// EarliestOffset returns the oldest offset still present in this partition
// - 0 until retention has ever deleted anything, and segments[0]'s own base
// offset afterward, since that's always whatever's currently oldest.
func (p *Partition) EarliestOffset() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.segments[0].baseOffset
}

// ReadBatch returns up to maxBytes of concatenated blob bytes starting at
// offset - what DiskLog.Read actually needs, and the fix for a real
// performance bug: DiskLog.Read used to get this by calling Read (above)
// once per blob, and every one of those calls repeated the same sparse-
// index lookup and re-scanned forward from that index window's start,
// making a full read O(n * indexEvery) instead of O(n). ReadBatch does the
// index lookup exactly once, then keeps scanning forward from wherever it
// left off - across segment boundaries too, if maxBytes hasn't been hit
// once the current segment runs out.
//
// Like Read, this never surfaces "offset out of range" or "nothing here
// yet" as an error - both simply mean an empty result. That distinction
// matters for Fetch's long-polling logic: nothing new to read is not a
// failure.
func (p *Partition) ReadBatch(offset int64, maxBytes int32) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	segIdx := p.findSegmentIndex(offset)
	if segIdx < 0 {
		return nil, nil
	}
	sg := p.segments[segIdx]

	// One index lookup, only for the segment actually containing offset -
	// every segment reached afterward by rolling off the end starts its own
	// scan fresh from position 0, since a fresh segment has no need for an
	// index lookup to find its own first blob.
	relTarget := int32(offset - sg.baseOffset)
	var pos int64
	var curOffset int32
	if entryOffset, entryPos, found := sg.idx.Lookup(relTarget); found {
		curOffset = entryOffset
		pos = int64(entryPos)
	}

	var out []byte
	started := false
	for {
		data, span, err := sg.seg.ReadAt(pos)
		if err != nil {
			// Ran out of blobs in this segment - normal exhaustion (real
			// I/O errors are indistinguishable here, matching Read's own
			// documented simplification). Move to the next segment, if any.
			segIdx++
			if segIdx >= len(p.segments) {
				break
			}
			sg = p.segments[segIdx]
			pos, curOffset = 0, 0
			continue
		}

		if !started {
			if relTarget >= curOffset+span {
				pos += recordHeaderSize + int64(len(data))
				curOffset += span
				continue
			}
			started = true
		}

		if len(out)+len(data) > int(maxBytes) {
			break
		}
		out = append(out, data...)
		pos += recordHeaderSize + int64(len(data))
		curOffset += span
	}

	return out, nil
}

// Sync fsyncs the active segment - the time-based half of the fsync policy,
// meant to be called on a ticker (see cmd/broker's runFlush) rather than
// from Append itself, so a low-traffic partition that never reaches
// flushEveryMessages still gets flushed within some bounded time. Takes the
// exclusive lock like Append, since it touches the same active segment and
// resets the same sinceLastFlush counter - a Sync racing with an Append that
// was about to trigger its own count-based flush must not fsync twice for
// one reason and then still leave sinceLastFlush stale for the other.
func (p *Partition) Sync() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	active := p.segments[len(p.segments)-1]
	if err := active.seg.Sync(); err != nil {
		return err
	}
	p.sinceLastFlush = 0
	return nil
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
