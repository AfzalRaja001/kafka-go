package protocol

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

const recordBatchHeaderSize = 61

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// RecordBatchHeader is the fixed-size header every Kafka record batch (v2,
// magic byte 2) starts with. Everything after RecordCount is opaque,
// variable-length record data this broker never parses.
type RecordBatchHeader struct {
	BaseOffset           int64
	BatchLength          int32
	PartitionLeaderEpoch int32
	Magic                int8
	CRC                  uint32
	Attributes           int16
	LastOffsetDelta      int32
	FirstTimestamp       int64
	MaxTimestamp         int64
	ProducerID           int64
	ProducerEpoch        int16
	BaseSequence         int32
	RecordCount          int32
}

// ParseRecordBatchHeader reads the fixed 61-byte header and validates
// magic == 2 - the only validation this broker does on a batch's contents.
func ParseRecordBatchHeader(buf []byte) (RecordBatchHeader, error) {
	if len(buf) < recordBatchHeaderSize {
		return RecordBatchHeader{}, fmt.Errorf("record batch header: %w", ErrNotEnoughData)
	}

	h := RecordBatchHeader{
		BaseOffset:           int64(binary.BigEndian.Uint64(buf[0:8])),
		BatchLength:          int32(binary.BigEndian.Uint32(buf[8:12])),
		PartitionLeaderEpoch: int32(binary.BigEndian.Uint32(buf[12:16])),
		Magic:                int8(buf[16]),
		CRC:                  binary.BigEndian.Uint32(buf[17:21]),
		Attributes:           int16(binary.BigEndian.Uint16(buf[21:23])),
		LastOffsetDelta:      int32(binary.BigEndian.Uint32(buf[23:27])),
		FirstTimestamp:       int64(binary.BigEndian.Uint64(buf[27:35])),
		MaxTimestamp:         int64(binary.BigEndian.Uint64(buf[35:43])),
		ProducerID:           int64(binary.BigEndian.Uint64(buf[43:51])),
		ProducerEpoch:        int16(binary.BigEndian.Uint16(buf[51:53])),
		BaseSequence:         int32(binary.BigEndian.Uint32(buf[53:57])),
		RecordCount:          int32(binary.BigEndian.Uint32(buf[57:61])),
	}

	if h.Magic != 2 {
		return RecordBatchHeader{}, fmt.Errorf("record batch header: unsupported magic %d, want 2", h.Magic)
	}
	return h, nil
}

// RewriteRecordBatch returns a copy of buf with baseOffset rewritten to
// newBaseOffset and the CRC recomputed to match - never mutates buf itself,
// since it may be a subslice of a larger decoder buffer (Lesson 5's
// aliasing rule, same reasoning as Decoder.Bytes). The CRC covers
// everything from byte 21 (right after the CRC field itself) onward -
// not the whole batch, and not from baseOffset - getting this range wrong
// is the single most common record batch bug.
func RewriteRecordBatch(buf []byte, newBaseOffset int64) []byte {
	out := make([]byte, len(buf))
	copy(out, buf)

	binary.BigEndian.PutUint64(out[0:8], uint64(newBaseOffset))

	crc := crc32.Checksum(out[21:], castagnoliTable)
	binary.BigEndian.PutUint32(out[17:21], crc)

	return out
}
