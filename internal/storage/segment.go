package storage

import (
	"encoding/binary"
	"os"
)

// Segment wraps a single append-only log file: write bytes, get back the
// position they were written at; given a position, read those bytes back.
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

func (s *Segment) Append(data []byte) (int64, error) {
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(data)))

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
// this directly).
func (s *Segment) ReadAt(position int64) ([]byte, error) {
	header := make([]byte, 4)
	if _, err := s.file.ReadAt(header, position); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(header)
	data := make([]byte, length)
	if _, err := s.file.ReadAt(data, position+4); err != nil {
		return nil, err
	}
	return data, nil
}

func (s *Segment) Sync() error {
	return s.file.Sync()
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
		header := make([]byte, 4)
		n, err := s.file.ReadAt(header, pos)
		if err != nil || n < 4 {
			break
		}
		length := binary.BigEndian.Uint32(header)
		recordEnd := pos + 4 + int64(length)
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
