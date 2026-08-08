package storage

import (
	"encoding/binary"
	"os"
	"sort"
)

const indexEntrySize = 8

// Index is a sparse offset index: one fixed-width (relativeOffset, position)
// entry every few records, not one per record. Finding an exact record means
// binary-searching to the nearest indexed entry, then scanning forward a
// short, bounded distance from there via Segment.ReadAt.
type Index struct {
	file *os.File
	size int64
}

func OpenIndex(path string) (*Index, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	return &Index{file: file, size: info.Size()}, nil
}

// position is int32, not int64: segments are capped around 1GB by design
// (kafka-from-scratch.md Part 3), and int32 covers positions up to ~2.1GB.
// This would silently break if segment sizes were ever configured larger.
func (idx *Index) Append(relativeOffset int32, position int32) error {
	entry := make([]byte, indexEntrySize)
	binary.BigEndian.PutUint32(entry[0:4], uint32(relativeOffset))
	binary.BigEndian.PutUint32(entry[4:8], uint32(position))

	if _, err := idx.file.Write(entry); err != nil {
		return err
	}
	idx.size += indexEntrySize
	return nil
}

func (idx *Index) EntryCount() int64 {
	return idx.size / indexEntrySize
}

func (idx *Index) entryAt(i int64) (relativeOffset int32, position int32, err error) {
	buf := make([]byte, indexEntrySize)
	if _, err := idx.file.ReadAt(buf, i*indexEntrySize); err != nil {
		return 0, 0, err
	}
	relativeOffset = int32(binary.BigEndian.Uint32(buf[0:4]))
	position = int32(binary.BigEndian.Uint32(buf[4:8]))
	return relativeOffset, position, nil
}

// Lookup finds the index entry with the largest relativeOffset not exceeding
// targetOffset. found is false if even the first entry already exceeds it.
func (idx *Index) Lookup(targetOffset int32) (entryOffset int32, position int32, found bool) {
	n := int(idx.EntryCount())

	i := sort.Search(n, func(i int) bool {
		relOff, _, _ := idx.entryAt(int64(i))
		return relOff > targetOffset
	})

	if i == 0 {
		return 0, 0, false
	}

	relOff, pos, _ := idx.entryAt(int64(i - 1))
	return relOff, pos, true
}

func (idx *Index) Close() error {
	return idx.file.Close()
}

// FindRecord combines an index lookup with a bounded forward scan through
// seg to reach the exact target offset.
func FindRecord(seg *Segment, idx *Index, targetOffset int32) ([]byte, error) {
	var pos int64 = 0
	var offset int32 = 0

	if entryOffset, entryPos, found := idx.Lookup(targetOffset); found {
		offset = entryOffset
		pos = int64(entryPos)
	}

	for offset < targetOffset {
		data, err := seg.ReadAt(pos)
		if err != nil {
			return nil, err
		}
		pos += 4 + int64(len(data))
		offset++
	}

	return seg.ReadAt(pos)
}
