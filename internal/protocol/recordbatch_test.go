package protocol

import (
	"encoding/binary"
	"hash/crc32"
	"testing"
)

// buildTestBatch constructs a minimal, well-formed record batch: a valid
// 61-byte header (magic=2, the given baseOffset, a correct CRC) followed by
// arbitrary trailing bytes standing in for records this broker never parses.
func buildTestBatch(baseOffset int64, recordCount int32, trailing []byte) []byte {
	buf := make([]byte, recordBatchHeaderSize+len(trailing))

	binary.BigEndian.PutUint64(buf[0:8], uint64(baseOffset))
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(buf)-12)) // batchLength: rough, not load-bearing for these tests
	binary.BigEndian.PutUint32(buf[12:16], 0)                  // partitionLeaderEpoch
	buf[16] = 2                                                // magic
	// buf[17:21] (crc) filled in below
	binary.BigEndian.PutUint16(buf[21:23], 0) // attributes
	binary.BigEndian.PutUint32(buf[23:27], uint32(recordCount-1))
	binary.BigEndian.PutUint64(buf[27:35], 1000) // firstTimestamp
	binary.BigEndian.PutUint64(buf[35:43], 1000) // maxTimestamp
	var noProducerID int64 = -1
	binary.BigEndian.PutUint64(buf[43:51], uint64(noProducerID))
	binary.BigEndian.PutUint16(buf[51:53], 0) // producerEpoch
	binary.BigEndian.PutUint32(buf[53:57], 0) // baseSequence
	binary.BigEndian.PutUint32(buf[57:61], uint32(recordCount))
	copy(buf[61:], trailing)

	crc := crc32.Checksum(buf[21:], castagnoliTable)
	binary.BigEndian.PutUint32(buf[17:21], crc)

	return buf
}

func TestParseRecordBatchHeader(t *testing.T) {
	batch := buildTestBatch(0, 3, []byte("fake-records"))

	h, err := ParseRecordBatchHeader(batch)
	if err != nil {
		t.Fatalf("ParseRecordBatchHeader: %v", err)
	}
	if h.Magic != 2 {
		t.Errorf("Magic = %d, want 2", h.Magic)
	}
	if h.RecordCount != 3 {
		t.Errorf("RecordCount = %d, want 3", h.RecordCount)
	}
	if h.FirstTimestamp != 1000 {
		t.Errorf("FirstTimestamp = %d, want 1000", h.FirstTimestamp)
	}
}

func TestParseRecordBatchHeader_RejectsWrongMagic(t *testing.T) {
	batch := buildTestBatch(0, 1, nil)
	batch[16] = 1 // magic 1 - the old message-set format, not record batch v2

	if _, err := ParseRecordBatchHeader(batch); err == nil {
		t.Fatal("expected an error for magic != 2, got nil")
	}
}

func TestParseRecordBatchHeader_TooShort(t *testing.T) {
	if _, err := ParseRecordBatchHeader([]byte{0, 0, 0}); err == nil {
		t.Fatal("expected an error for a too-short buffer, got nil")
	}
}

func TestRewriteRecordBatch(t *testing.T) {
	original := buildTestBatch(0, 2, []byte("fake-records"))

	rewritten := RewriteRecordBatch(original, 12345)

	h, err := ParseRecordBatchHeader(rewritten)
	if err != nil {
		t.Fatalf("ParseRecordBatchHeader on rewritten batch: %v", err)
	}
	if h.BaseOffset != 12345 {
		t.Errorf("BaseOffset = %d, want 12345", h.BaseOffset)
	}

	// The CRC must actually verify against the real byte range - not just
	// "some value got written."
	wantCRC := crc32.Checksum(rewritten[21:], castagnoliTable)
	if h.CRC != wantCRC {
		t.Errorf("CRC = %d, want %d (recomputed over the rewritten bytes)", h.CRC, wantCRC)
	}

	// Trailing "record" bytes must survive completely untouched.
	if string(rewritten[recordBatchHeaderSize:]) != "fake-records" {
		t.Errorf("trailing bytes = %q, want %q", rewritten[recordBatchHeaderSize:], "fake-records")
	}
}

func TestRewriteRecordBatch_DoesNotMutateInput(t *testing.T) {
	original := buildTestBatch(0, 1, nil)
	originalCopy := make([]byte, len(original))
	copy(originalCopy, original)

	RewriteRecordBatch(original, 999)

	for i := range original {
		if original[i] != originalCopy[i] {
			t.Fatalf("RewriteRecordBatch mutated its input at byte %d", i)
		}
	}
}
