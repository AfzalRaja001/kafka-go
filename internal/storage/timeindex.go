package storage

import (
	"encoding/binary"
	"os"
	"sort"
)

const timeindexEntrySize = 12

// Timeindex is a sparse timestamp index: fixed 12-byte (timestamp,
// relativeOffset) entries, binary-searchable the same way Index is.
// Entries must be appended in non-decreasing timestamp order.
type Timeindex struct {
	file *os.File
	size int64
}

func OpenTimeindex(path string) (*Timeindex, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	return &Timeindex{file: file, size: info.Size()}, nil
}

func (t *Timeindex) Append(timestamp int64, relativeOffset int32) error {
	entry := make([]byte, timeindexEntrySize)
	binary.BigEndian.PutUint64(entry[0:8], uint64(timestamp))
	binary.BigEndian.PutUint32(entry[8:12], uint32(relativeOffset))

	if _, err := t.file.Write(entry); err != nil {
		return err
	}
	t.size += timeindexEntrySize
	return nil
}

func (t *Timeindex) EntryCount() int64 {
	return t.size / timeindexEntrySize
}

func (t *Timeindex) entryAt(i int64) (timestamp int64, relativeOffset int32, err error) {
	buf := make([]byte, timeindexEntrySize)
	if _, err := t.file.ReadAt(buf, i*timeindexEntrySize); err != nil {
		return 0, 0, err
	}
	timestamp = int64(binary.BigEndian.Uint64(buf[0:8]))
	relativeOffset = int32(binary.BigEndian.Uint32(buf[8:12]))
	return timestamp, relativeOffset, nil
}

// Lookup finds the entry with the largest timestamp not exceeding
// targetTimestamp. found is false if even the first entry is already later
// than the target.
func (t *Timeindex) Lookup(targetTimestamp int64) (timestamp int64, relativeOffset int32, found bool) {
	n := int(t.EntryCount())

	i := sort.Search(n, func(i int) bool {
		ts, _, _ := t.entryAt(int64(i))
		return ts > targetTimestamp
	})

	if i == 0 {
		return 0, 0, false
	}

	ts, relOff, _ := t.entryAt(int64(i - 1))
	return ts, relOff, true
}

func (t *Timeindex) Close() error {
	return t.file.Close()
}
