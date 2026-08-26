package offsets

import "testing"

func TestEncodeDecodeCommit_RoundTrips(t *testing.T) {
	want := commitRecord{Group: "my-group", Topic: "orders", Partition: 3, Offset: 42, Metadata: "checkpoint-a"}

	encoded := encodeCommit(want)
	got, consumed, err := decodeCommit(encoded)
	if err != nil {
		t.Fatalf("decodeCommit: %v", err)
	}
	if consumed != len(encoded) {
		t.Errorf("consumed = %d, want %d (the whole record)", consumed, len(encoded))
	}
	if got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestEncodeDecodeCommit_EmptyMetadataRoundTrips(t *testing.T) {
	want := commitRecord{Group: "g", Topic: "t", Partition: 0, Offset: 0, Metadata: ""}

	got, _, err := decodeCommit(encodeCommit(want))
	if err != nil {
		t.Fatalf("decodeCommit: %v", err)
	}
	if got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

// TestEncodeDecodeCommit_ConcatenatedRecordsDecodeIndependently pins down the
// property replay actually depends on: storage.Log.Read concatenates raw
// batch bytes with no boundary markers of its own (see DiskLog.Read's doc
// comment), so each record's own length prefix has to be enough to walk a
// concatenated blob of many records back into individual ones.
func TestEncodeDecodeCommit_ConcatenatedRecordsDecodeIndependently(t *testing.T) {
	first := commitRecord{Group: "g", Topic: "orders", Partition: 0, Offset: 10, Metadata: "first"}
	second := commitRecord{Group: "g", Topic: "orders", Partition: 1, Offset: 20, Metadata: "second"}

	blob := append(encodeCommit(first), encodeCommit(second)...)

	gotFirst, consumed, err := decodeCommit(blob)
	if err != nil {
		t.Fatalf("decode first: %v", err)
	}
	if gotFirst != first {
		t.Errorf("first = %+v, want %+v", gotFirst, first)
	}

	gotSecond, _, err := decodeCommit(blob[consumed:])
	if err != nil {
		t.Fatalf("decode second: %v", err)
	}
	if gotSecond != second {
		t.Errorf("second = %+v, want %+v", gotSecond, second)
	}
}

func TestDecodeCommit_TruncatedLengthPrefixErrors(t *testing.T) {
	if _, _, err := decodeCommit([]byte{0, 0}); err == nil {
		t.Fatal("expected an error decoding a 2-byte buffer (length prefix needs 4), got nil")
	}
}

func TestDecodeCommit_TruncatedBodyErrors(t *testing.T) {
	full := encodeCommit(commitRecord{Group: "g", Topic: "t", Partition: 0, Offset: 1, Metadata: "meta"})

	if _, _, err := decodeCommit(full[:len(full)-2]); err == nil {
		t.Fatal("expected an error decoding a record body cut short, got nil")
	}
}
