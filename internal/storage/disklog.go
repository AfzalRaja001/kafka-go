package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// DiskLog is the real, on-disk Log implementation: one *Partition per
// (topic, partition) pair, each in its own subdirectory under dir, matching
// the directory layout kafka-from-scratch.md Part 3 describes.
type DiskLog struct {
	mu         sync.RWMutex
	dir        string
	indexEvery int32
	parts      map[logKey]*Partition
}

var _ Log = (*DiskLog)(nil)

func NewDiskLog(dir string, indexEvery int32) *DiskLog {
	return &DiskLog{
		dir:        dir,
		indexEvery: indexEvery,
		parts:      make(map[logKey]*Partition),
	}
}

// getPartition finds a partition that already exists - either already open
// in memory, or present on disk from a previous run (lazily opened and
// cached here). It never creates new storage; ok is false only when neither
// memory nor disk has anything for this topic-partition.
func (d *DiskLog) getPartition(topic string, partition int32) (*Partition, bool) {
	d.mu.RLock()
	p, ok := d.parts[logKey{topic, partition}]
	d.mu.RUnlock()
	if ok {
		return p, true
	}

	partDir := filepath.Join(d.dir, fmt.Sprintf("%s-%d", topic, partition))
	if _, err := os.Stat(partDir); os.IsNotExist(err) {
		return nil, false
	}

	p, err := d.openPartition(topic, partition)
	if err != nil {
		return nil, false
	}
	return p, true
}

// getOrCreatePartition finds a partition the same way getPartition does,
// but creates a brand-new one (on disk and in memory) if neither memory nor
// disk has anything for this topic-partition yet. Only Append calls this -
// Read, EarliestOffset, and LatestOffset use getPartition instead, since
// silently fabricating empty storage in response to a read would hide a
// real bug (a client asking about a topic that was never created).
func (d *DiskLog) getOrCreatePartition(topic string, partition int32) (*Partition, error) {
	if p, ok := d.getPartition(topic, partition); ok {
		return p, nil
	}
	return d.openPartition(topic, partition)
}

// openPartition opens (creating on disk if necessary) the partition
// directory for (topic, partition) and caches the result in d.parts.
// OpenSegment/OpenIndex/OpenTimeindex all use O_CREATE, so this correctly
// handles both "genuinely new" and "exists on disk, not yet in memory."
func (d *DiskLog) openPartition(topic string, partition int32) (*Partition, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	key := logKey{topic, partition}
	if p, ok := d.parts[key]; ok {
		return p, nil
	}

	partDir := filepath.Join(d.dir, fmt.Sprintf("%s-%d", topic, partition))
	if err := os.MkdirAll(partDir, 0755); err != nil {
		return nil, err
	}

	// Base offset 0, zero-padded to 20 digits, per the real Kafka segment
	// naming convention - always true today since Partition only wraps one
	// segment; stays correct once segment rolling adds more.
	p, err := OpenPartition(
		filepath.Join(partDir, "00000000000000000000.log"),
		filepath.Join(partDir, "00000000000000000000.index"),
		filepath.Join(partDir, "00000000000000000000.timeindex"),
		d.indexEvery,
	)
	if err != nil {
		return nil, err
	}

	d.parts[key] = p
	return p, nil
}

func (d *DiskLog) Append(topic string, partition int32, batch []byte) (int64, error) {
	p, err := d.getOrCreatePartition(topic, partition)
	if err != nil {
		return 0, err
	}
	return p.Append(batch, time.Now().UnixMilli())
}

// Read returns up to maxBytes of records starting at offset, by repeatedly
// reading one Partition record at a time and concatenating until the byte
// budget is used or reads stop succeeding. Partition.Read doesn't currently
// distinguish "reached the end of the log" from a genuine I/O error - both
// simply stop the loop here and return whatever was collected so far. That
// distinction matters for Fetch's long-polling logic (Phase 3) and isn't
// needed yet.
func (d *DiskLog) Read(topic string, partition int32, offset int64, maxBytes int32) ([]byte, error) {
	p, ok := d.getPartition(topic, partition)
	if !ok {
		return nil, fmt.Errorf("unknown topic-partition %s-%d", topic, partition)
	}

	var out []byte
	for {
		data, err := p.Read(offset)
		if err != nil {
			break
		}
		if len(out)+len(data) > int(maxBytes) {
			break
		}
		out = append(out, data...)
		offset++
	}
	return out, nil
}

// EarliestOffset is always 0: nothing trims or rolls segments yet, so every
// record ever appended is still present. This will need to change once
// retention (Phase 5) or segment rolling can drop old data.
func (d *DiskLog) EarliestOffset(topic string, partition int32) (int64, error) {
	if _, ok := d.getPartition(topic, partition); !ok {
		return 0, fmt.Errorf("unknown topic-partition %s-%d", topic, partition)
	}
	return 0, nil
}

func (d *DiskLog) LatestOffset(topic string, partition int32) (int64, error) {
	p, ok := d.getPartition(topic, partition)
	if !ok {
		return 0, fmt.Errorf("unknown topic-partition %s-%d", topic, partition)
	}
	return p.LogEndOffset(), nil
}

func (d *DiskLog) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, p := range d.parts {
		if err := p.Close(); err != nil {
			return err
		}
	}
	return nil
}
