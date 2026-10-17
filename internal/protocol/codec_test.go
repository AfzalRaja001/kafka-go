package protocol

import (
	"encoding/binary"
	"math"
	"testing"
)

func TestInt32(t *testing.T) {
	tests := []struct {
		name string
		want int32
	}{
		{"zero", 0},
		{"positive", 42},
		{"negative", -42},
		{"max", 2147483647},
		{"min", -2147483648},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc := NewEncoder()
			enc.Int32(tt.want)

			dec := NewDecoder(enc.Result())
			got, err := dec.Int32()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
		})
	}
}

func TestInt8Int16Int64(t *testing.T) {
	enc := NewEncoder()
	enc.Int8(-5)
	enc.Int16(-1000)
	enc.Int64(9223372036854775807)

	dec := NewDecoder(enc.Result())

	i8, err := dec.Int8()
	if err != nil || i8 != -5 {
		t.Errorf("Int8 = %d, %v, want -5", i8, err)
	}
	i16, err := dec.Int16()
	if err != nil || i16 != -1000 {
		t.Errorf("Int16 = %d, %v, want -1000", i16, err)
	}
	i64, err := dec.Int64()
	if err != nil || i64 != 9223372036854775807 {
		t.Errorf("Int64 = %d, %v, want max int64", i64, err)
	}
}

func TestString(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{"empty", ""},
		{"ascii", "hello kafka"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc := NewEncoder()
			enc.String(tt.want)

			dec := NewDecoder(enc.Result())
			got, err := dec.String()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNullableString(t *testing.T) {
	present := "hello"
	empty := ""

	tests := []struct {
		name string
		want *string
	}{
		{"present", &present},
		{"present-but-empty", &empty},
		{"absent", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc := NewEncoder()
			enc.NullableString(tt.want)

			dec := NewDecoder(enc.Result())
			got, err := dec.NullableString()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.want == nil {
				if got != nil {
					t.Errorf("got %v, want nil", *got)
				}
				return
			}
			if got == nil || *got != *tt.want {
				t.Errorf("got %v, want %v", got, *tt.want)
			}
		})
	}
}

func TestBool(t *testing.T) {
	for _, want := range []bool{true, false} {
		enc := NewEncoder()
		enc.Bool(want)

		dec := NewDecoder(enc.Result())
		got, err := dec.Bool()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func TestBytes(t *testing.T) {
	tests := []struct {
		name string
		want []byte
	}{
		{"present", []byte{1, 2, 3, 4}},
		{"empty-but-present", []byte{}},
		{"nil", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc := NewEncoder()
			enc.Bytes(tt.want)

			dec := NewDecoder(enc.Result())
			got, err := dec.Bytes()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.want == nil {
				if got != nil {
					t.Errorf("got %v, want nil", got)
				}
				return
			}
			if len(got) != len(tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

// TestBytes_ReturnsIndependentCopy proves Decoder.Bytes never hands back a
// slice aliasing the shared input buffer - mutating the original buffer
// after decoding must not affect the already-decoded value.
func TestBytes_ReturnsIndependentCopy(t *testing.T) {
	enc := NewEncoder()
	enc.Bytes([]byte{1, 2, 3, 4})
	buf := enc.Result()

	dec := NewDecoder(buf)
	got, err := dec.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Corrupt the original buffer after decoding.
	for i := range buf {
		buf[i] = 0xFF
	}

	want := []byte{1, 2, 3, 4}
	for i, b := range got {
		if b != want[i] {
			t.Fatalf("decoded value changed after mutating input buffer: got %v, want %v", got, want)
		}
	}
}

func TestStringArray(t *testing.T) {
	tests := []struct {
		name string
		want []string
	}{
		{"empty", []string{}},
		{"single", []string{"orders"}},
		{"several", []string{"orders", "payments", "shipments"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc := NewEncoder()
			enc.StringArray(tt.want)

			dec := NewDecoder(enc.Result())
			got, err := dec.StringArray()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("element %d: got %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestDecoder_ArrayLen(t *testing.T) {
	tests := []struct {
		name        string
		count       int32
		remaining   int
		minElemSize int
		want        int32
		wantErr     bool
	}{
		{"null passes through as -1", -1, 0, 2, -1, false},
		{"empty", 0, 0, 2, 0, false},
		{"fits exactly", 3, 6, 2, 3, false},
		{"fits with room to spare", 2, 100, 4, 2, false},
		{"one element more than could fit", 4, 6, 2, 0, true},
		{"negative other than -1", -2, 10, 2, 0, true},
		{"maxint32 with almost nothing behind it", math.MaxInt32, 10, 1, 0, true},
		// count * minElemSize must not overflow into a small number and
		// sneak past the check.
		{"multiplication would overflow int32", 1 << 30, 64, 16, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := binary.BigEndian.AppendUint32(nil, uint32(tt.count))
			buf = append(buf, make([]byte, tt.remaining)...)
			dec := NewDecoder(buf)

			got, err := dec.ArrayLen(tt.minElemSize)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("count = %d, want %d", got, tt.want)
			}
			if !tt.wantErr && len(dec.Remaining()) != tt.remaining {
				t.Errorf("consumed %d bytes beyond the count itself", tt.remaining-len(dec.Remaining()))
			}
		})
	}
}

func TestDecoder_ArrayLen_TruncatedCount(t *testing.T) {
	if _, err := NewDecoder([]byte{0, 0}).ArrayLen(1); err == nil {
		t.Fatal("expected an error for a 2-byte count, got nil")
	}
}

func TestStringArray_RejectsNull(t *testing.T) {
	enc := NewEncoder()
	enc.Int32(-1)
	if _, err := NewDecoder(enc.Result()).StringArray(); err == nil {
		t.Fatal("non-nullable StringArray accepted a null (-1) count")
	}
}

func TestNullableStringArray(t *testing.T) {
	enc := NewEncoder()
	enc.Int32(-1)
	got, err := NewDecoder(enc.Result()).NullableStringArray()
	if err != nil || got != nil {
		t.Fatalf("null = (%v, %v), want (nil, nil)", got, err)
	}

	enc = NewEncoder()
	enc.StringArray([]string{})
	got, err = NewDecoder(enc.Result()).NullableStringArray()
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("empty = (%#v, %v), want a non-nil empty slice - empty must stay distinguishable from null", got, err)
	}

	enc = NewEncoder()
	enc.StringArray([]string{"a", "b"})
	got, err = NewDecoder(enc.Result()).NullableStringArray()
	if err != nil || len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("two elements = (%v, %v)", got, err)
	}
}

func TestNotEnoughData(t *testing.T) {
	dec := NewDecoder([]byte{0, 0})
	_, err := dec.Int32()
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
}

// TestDecodeRequestHeader decodes the actual Kafka request header shape
// (kafka-from-scratch.md Part 5): api_key, api_version, correlation_id,
// client_id - the first real piece of Phase 1.
func TestDecodeRequestHeader(t *testing.T) {
	enc := NewEncoder()
	enc.Int16(18)             // api_key: ApiVersions
	enc.Int16(0)               // api_version
	enc.Int32(1234)            // correlation_id
	clientID := "kcat"
	enc.NullableString(&clientID)

	dec := NewDecoder(enc.Result())

	apiKey, err := dec.Int16()
	if err != nil || apiKey != 18 {
		t.Fatalf("api_key = %d, %v", apiKey, err)
	}
	apiVersion, err := dec.Int16()
	if err != nil || apiVersion != 0 {
		t.Fatalf("api_version = %d, %v", apiVersion, err)
	}
	correlationID, err := dec.Int32()
	if err != nil || correlationID != 1234 {
		t.Fatalf("correlation_id = %d, %v", correlationID, err)
	}
	clientIDGot, err := dec.NullableString()
	if err != nil || clientIDGot == nil || *clientIDGot != "kcat" {
		t.Fatalf("client_id = %v, %v", clientIDGot, err)
	}
}
