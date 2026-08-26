package offsets

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// commitRecord is everything one OffsetCommit call needs to persist - the
// same fields group.OffsetStore's Commit/Fetch key and carry, just bundled
// into one value so encodeCommit/decodeCommit have one thing to round-trip.
type commitRecord struct {
	Group     string
	Topic     string
	Partition int32
	Offset    int64
	Metadata  string
}

// encodeCommit serializes one commit as a single length-prefixed record.
// This is deliberately not a real Kafka record batch: nothing outside this
// broker ever Fetches __consumer_offsets over the wire (see
// docs/decisions.md), so there's no reason to pay for a CRC and a batch
// header nobody parses. What the length prefix buys instead is the thing
// replay actually needs: storage.Log.Read concatenates raw batch bytes with
// no boundaries of its own, so each record has to say its own length to let
// a concatenated blob of many records be walked back into individual ones.
func encodeCommit(r commitRecord) []byte {
	var body bytes.Buffer
	writeString(&body, r.Group)
	writeString(&body, r.Topic)
	writeInt32(&body, r.Partition)
	writeInt64(&body, r.Offset)
	writeString(&body, r.Metadata)

	out := make([]byte, 4+body.Len())
	binary.BigEndian.PutUint32(out, uint32(body.Len()))
	copy(out[4:], body.Bytes())
	return out
}

// decodeCommit reads one length-prefixed record from the start of buf,
// returning how many bytes it consumed so the caller can advance to the
// next record in a concatenated blob (see TestEncodeDecodeCommit_
// ConcatenatedRecordsDecodeIndependently).
func decodeCommit(buf []byte) (commitRecord, int, error) {
	if len(buf) < 4 {
		return commitRecord{}, 0, fmt.Errorf("record length prefix: need 4 bytes, got %d", len(buf))
	}
	bodyLen := int(binary.BigEndian.Uint32(buf))
	if bodyLen < 0 || len(buf) < 4+bodyLen {
		return commitRecord{}, 0, fmt.Errorf("record body: need %d bytes, got %d", bodyLen, len(buf)-4)
	}

	body := buf[4 : 4+bodyLen]
	r := commitRecord{}
	var err error

	if r.Group, body, err = readString(body); err != nil {
		return commitRecord{}, 0, fmt.Errorf("group: %w", err)
	}
	if r.Topic, body, err = readString(body); err != nil {
		return commitRecord{}, 0, fmt.Errorf("topic: %w", err)
	}
	if r.Partition, body, err = readInt32(body); err != nil {
		return commitRecord{}, 0, fmt.Errorf("partition: %w", err)
	}
	if r.Offset, body, err = readInt64(body); err != nil {
		return commitRecord{}, 0, fmt.Errorf("offset: %w", err)
	}
	if r.Metadata, _, err = readString(body); err != nil {
		return commitRecord{}, 0, fmt.Errorf("metadata: %w", err)
	}

	return r, 4 + bodyLen, nil
}

func writeString(buf *bytes.Buffer, v string) {
	writeInt32(buf, int32(len(v)))
	buf.WriteString(v)
}

func writeInt32(buf *bytes.Buffer, v int32) {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], uint32(v))
	buf.Write(tmp[:])
}

func writeInt64(buf *bytes.Buffer, v int64) {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], uint64(v))
	buf.Write(tmp[:])
}

func readInt32(buf []byte) (int32, []byte, error) {
	if len(buf) < 4 {
		return 0, nil, fmt.Errorf("int32: need 4 bytes, got %d", len(buf))
	}
	return int32(binary.BigEndian.Uint32(buf[:4])), buf[4:], nil
}

func readInt64(buf []byte) (int64, []byte, error) {
	if len(buf) < 8 {
		return 0, nil, fmt.Errorf("int64: need 8 bytes, got %d", len(buf))
	}
	return int64(binary.BigEndian.Uint64(buf[:8])), buf[8:], nil
}

func readString(buf []byte) (string, []byte, error) {
	length, rest, err := readInt32(buf)
	if err != nil {
		return "", nil, fmt.Errorf("string length: %w", err)
	}
	if length < 0 || len(rest) < int(length) {
		return "", nil, fmt.Errorf("string: need %d bytes, got %d", length, len(rest))
	}
	return string(rest[:length]), rest[length:], nil
}
