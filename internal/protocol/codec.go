package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

var ErrNotEnoughData = errors.New("not enough data to decode")

// Decoder reads Kafka primitive types from a byte slice, tracking its own
// read position so callers don't have to thread an offset through every
// field read by hand.
type Decoder struct {
	buf []byte
	pos int
}

func NewDecoder(buf []byte) *Decoder {
	return &Decoder{buf: buf}
}

// Remaining returns every byte not yet consumed by a Decoder method call -
// used to decode a shared header, then hand the rest to a specific handler
// without that handler needing to know where in the original buffer it
// starts.
func (d *Decoder) Remaining() []byte {
	return d.buf[d.pos:]
}

func (d *Decoder) Int8() (int8, error) {
	if d.pos+1 > len(d.buf) {
		return 0, fmt.Errorf("int8: %w", ErrNotEnoughData)
	}
	v := int8(d.buf[d.pos])
	d.pos++
	return v, nil
}

func (d *Decoder) Bool() (bool, error) {
	v, err := d.Int8()
	if err != nil {
		return false, fmt.Errorf("bool: %w", err)
	}
	return v != 0, nil
}

func (d *Decoder) Int16() (int16, error) {
	if d.pos+2 > len(d.buf) {
		return 0, fmt.Errorf("int16: %w", ErrNotEnoughData)
	}
	v := int16(binary.BigEndian.Uint16(d.buf[d.pos : d.pos+2]))
	d.pos += 2
	return v, nil
}

func (d *Decoder) Int32() (int32, error) {
	if d.pos+4 > len(d.buf) {
		return 0, fmt.Errorf("int32: %w", ErrNotEnoughData)
	}
	v := int32(binary.BigEndian.Uint32(d.buf[d.pos : d.pos+4]))
	d.pos += 4
	return v, nil
}

func (d *Decoder) Int64() (int64, error) {
	if d.pos+8 > len(d.buf) {
		return 0, fmt.Errorf("int64: %w", ErrNotEnoughData)
	}
	v := int64(binary.BigEndian.Uint64(d.buf[d.pos : d.pos+8]))
	d.pos += 8
	return v, nil
}

// String decodes a 2-byte length followed by that many UTF-8 bytes.
func (d *Decoder) String() (string, error) {
	length, err := d.Int16()
	if err != nil {
		return "", fmt.Errorf("string length: %w", err)
	}
	if length < 0 {
		return "", fmt.Errorf("string: negative length %d", length)
	}
	if d.pos+int(length) > len(d.buf) {
		return "", fmt.Errorf("string: %w", ErrNotEnoughData)
	}

	v := string(d.buf[d.pos : d.pos+int(length)])
	d.pos += int(length)
	return v, nil
}

// NullableString distinguishes "absent" (nil) from "present but empty"
// (a valid pointer to ""), which a plain string return type cannot: the
// wire encoding uses length -1 for absent, any length >= 0 for present.
func (d *Decoder) NullableString() (*string, error) {
	length, err := d.Int16()
	if err != nil {
		return nil, fmt.Errorf("nullable string length: %w", err)
	}
	if length == -1 {
		return nil, nil
	}
	if length < 0 {
		return nil, fmt.Errorf("nullable string: invalid length %d", length)
	}
	if d.pos+int(length) > len(d.buf) {
		return nil, fmt.Errorf("nullable string: %w", ErrNotEnoughData)
	}

	v := string(d.buf[d.pos : d.pos+int(length)])
	d.pos += int(length)
	return &v, nil
}

// Bytes decodes a 4-byte length followed by that many raw bytes, always
// returning an independent copy - never a subslice of the shared input
// buffer, which could otherwise be silently corrupted if that buffer is
// reused for a later message.
func (d *Decoder) Bytes() ([]byte, error) {
	length, err := d.Int32()
	if err != nil {
		return nil, fmt.Errorf("bytes length: %w", err)
	}
	if length == -1 {
		return nil, nil
	}
	if length < 0 {
		return nil, fmt.Errorf("bytes: invalid length %d", length)
	}
	if d.pos+int(length) > len(d.buf) {
		return nil, fmt.Errorf("bytes: %w", ErrNotEnoughData)
	}

	v := make([]byte, length)
	copy(v, d.buf[d.pos:d.pos+int(length)])
	d.pos += int(length)
	return v, nil
}

// StringArray decodes a 4-byte signed count followed by that many strings.
func (d *Decoder) StringArray() ([]string, error) {
	count, err := d.Int32()
	if err != nil {
		return nil, fmt.Errorf("array count: %w", err)
	}
	if count < 0 {
		return nil, nil
	}

	result := make([]string, 0, count)
	for i := int32(0); i < count; i++ {
		s, err := d.String()
		if err != nil {
			return nil, fmt.Errorf("array element %d: %w", i, err)
		}
		result = append(result, s)
	}
	return result, nil
}

// Encoder builds a byte slice from Kafka primitive types, ready to hand to
// broker.WriteMessage.
type Encoder struct {
	buf bytes.Buffer
}

func NewEncoder() *Encoder {
	return &Encoder{}
}

func (e *Encoder) Int8(v int8) {
	e.buf.WriteByte(byte(v))
}

func (e *Encoder) Bool(v bool) {
	if v {
		e.Int8(1)
		return
	}
	e.Int8(0)
}

func (e *Encoder) Int16(v int16) {
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], uint16(v))
	e.buf.Write(tmp[:])
}

func (e *Encoder) Int32(v int32) {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], uint32(v))
	e.buf.Write(tmp[:])
}

func (e *Encoder) Int64(v int64) {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], uint64(v))
	e.buf.Write(tmp[:])
}

func (e *Encoder) String(v string) {
	e.Int16(int16(len(v)))
	e.buf.WriteString(v)
}

func (e *Encoder) NullableString(v *string) {
	if v == nil {
		e.Int16(-1)
		return
	}
	e.String(*v)
}

func (e *Encoder) Bytes(v []byte) {
	if v == nil {
		e.Int32(-1)
		return
	}
	e.Int32(int32(len(v)))
	e.buf.Write(v)
}

func (e *Encoder) StringArray(v []string) {
	if v == nil {
		e.Int32(-1)
		return
	}
	e.Int32(int32(len(v)))
	for _, s := range v {
		e.String(s)
	}
}

// Result returns the fully encoded bytes built so far.
func (e *Encoder) Result() []byte {
	return e.buf.Bytes()
}
