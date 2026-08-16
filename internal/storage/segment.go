package storage

import (
	"encoding/binary"
	"os"
)

// recordHeaderSize is the fixed per-blob header this segment format writes
// ahead of every blob: a 4-byte payload length, then a 4-byte offset span.
const recordHeaderSize = 8

// Segment wraps a single append-only log file: write bytes, get back the
// position they were written at; given a position, read those bytes back.
//
// Every blob carries an "offset span" alongside its length - how many log
// offsets that blob consumes. A blob is a Kafka record batch, and a batch can
// hold many records, so a single append can advance the log by more than one
// offset. Segment never parses the batch itself (that's protocol's job and
// would break the storage/protocol boundary); it only stores the span it was
// handed, which is enough to rebuild offsets after a restart without any
// separate bookkeeping file.
type Segment struct {
	file *os.File
	size int64
}

func OpenSegment(path string) (*Segment, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	s := &Segment{file: file, size: info.Size()}
	if err := s.Recover(); err != nil {
		file.Close()
		return nil, err
	}
	return s, nil
}

// Append writes data as one blob spanning offsetSpan offsets, returning the
// byte position it was written at.
func (s *Segment) Append(data []byte, offsetSpan int32) (int64, error) {
	header := make([]byte, recordHeaderSize)
	binary.BigEndian.PutUint32(header[0:4], uint32(len(data)))
	binary.BigEndian.PutUint32(header[4:8], uint32(offsetSpan))

	position := s.size
	if _, err := s.file.Write(header); err != nil {
		return 0, err
	}
	if _, err := s.file.Write(data); err != nil {
		return 0, err
	}
	s.size += int64(len(header) + len(data))
	return position, nil
}

// ReadAt reads by absolute position, never a shared cursor - this is what
// makes concurrent reads from multiple goroutines safe (Partition relies on
// this directly). It returns the blob's bytes and the offset span recorded
// with it, which callers need to know where the next blob's offsets begin.
func (s *Segment) ReadAt(position int64) ([]byte, int32, error) {
	header := make([]byte, recordHeaderSize)
	if _, err := s.file.ReadAt(header, position); err != nil {
		return nil, 0, err
	}
	length := binary.BigEndian.Uint32(header[0:4])
	offsetSpan := int32(binary.BigEndian.Uint32(header[4:8]))

	data := make([]byte, length)
	if _, err := s.file.ReadAt(data, position+recordHeaderSize); err != nil {
		return nil, 0, err
	}
	return data, offsetSpan, nil
}

func (s *Segment) Sync() error {
	return s.file.Sync()
}

// Size returns the segment's current size in bytes - used by Partition to
// decide when this segment is full and it's time to roll to a new one.
func (s *Segment) Size() int64 {
	return s.size
}

// Counts scans the whole segment once and returns both how many blobs it
// holds and how many offsets those blobs span in total. The two differ
// whenever any blob is a multi-record batch, and both are needed after a
// restart: the offset total restores Partition's nextOffset, while the blob
// total restores its sparse-index counter (which indexes every Nth append,
// not every Nth offset).
//
// Only meaningful right after OpenSegment, whose Recover call guarantees
// every blob up to s.size is complete. Returning both from a single scan
// avoids walking the file twice.
func (s *Segment) Counts() (blobs int64, offsets int64, err error) {
	var pos int64
	for pos < s.size {
		data, span, err := s.ReadAt(pos)
		if err != nil {
			return 0, 0, err
		}
		pos += recordHeaderSize + int64(len(data))
		blobs++
		offsets += int64(span)
	}
	return blobs, offsets, nil
}

func (s *Segment) Close() error {
	return s.file.Close()
}

// Recover scans forward from the start of the file and truncates at the
// first torn write - the state an unclean shutdown mid-Append leaves behind.
// Called automatically by OpenSegment on every startup.
func (s *Segment) Recover() error {
	var pos int64 = 0
	for pos < s.size {
		header := make([]byte, recordHeaderSize)
		n, err := s.file.ReadAt(header, pos)
		if err != nil || n < recordHeaderSize {
			break
		}
		length := binary.BigEndian.Uint32(header[0:4])
		recordEnd := pos + recordHeaderSize + int64(length)
		if recordEnd > s.size {
			break
		}
		pos = recordEnd
	}
	if pos < s.size {
		// os.Truncate (not s.file.Truncate) opens its own handle to do this:
		// on Windows, the handle held by s.file was opened with O_APPEND,
		// which grants append-only access, not the full write access
		// SetEndOfFile (what Truncate uses under the hood) requires.
		if err := os.Truncate(s.file.Name(), pos); err != nil {
			return err
		}
		s.size = pos
	}
	return nil
}
