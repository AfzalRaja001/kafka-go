package broker

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"runtime"
	"testing"
)

func frame(claimedSize uint32, body []byte) io.Reader {
	return bytes.NewReader(append(binary.BigEndian.AppendUint32(nil, claimedSize), body...))
}

// TestReadMessage_RejectsFrameOverLimit: a length prefix over the cap is
// refused from the 4 header bytes alone, before any body is read or any
// memory is set aside for it.
func TestReadMessage_RejectsFrameOverLimit(t *testing.T) {
	for _, size := range []uint32{MaxRequestBytes + 1, 0x7FFFFFFF, 0xFFFFFFFF} {
		_, err := ReadMessage(frame(size, nil))
		if !errors.Is(err, ErrRequestTooLarge) {
			t.Errorf("claimed size %d: err = %v, want ErrRequestTooLarge", size, err)
		}
	}
}

func TestReadMessage_FrameAtLimitAccepted(t *testing.T) {
	body := bytes.Repeat([]byte("x"), 64)
	msg, err := readMessage(frame(64, body), 64)
	if err != nil || !bytes.Equal(msg, body) {
		t.Fatalf("64-byte frame with a 64-byte limit = (%d bytes, %v), want the body back", len(msg), err)
	}
	if _, err := readMessage(frame(65, append(body, 'y')), 64); !errors.Is(err, ErrRequestTooLarge) {
		t.Fatalf("65-byte frame with a 64-byte limit: err = %v, want ErrRequestTooLarge", err)
	}
}

// TestReadMessage_AllocatesOnlyWhatActuallyArrives is the evaluation's
// 6.2GB scenario from the inside: a frame claiming the full 100MB under the
// cap, followed by only 10 real bytes, must cost roughly those 10 bytes of
// memory - not the 100MB the client claimed.
func TestReadMessage_AllocatesOnlyWhatActuallyArrives(t *testing.T) {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := ReadMessage(frame(MaxRequestBytes, make([]byte, 10)))
	runtime.ReadMemStats(&after)

	if err != io.ErrUnexpectedEOF {
		t.Fatalf("err = %v, want io.ErrUnexpectedEOF (10 of %d bytes arrived)", err, MaxRequestBytes)
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 1<<20 {
		t.Fatalf("allocated %d bytes for a frame where only 10 body bytes arrived, want < 1MB", allocated)
	}
}

func TestReadWriteMessage_RoundTrip(t *testing.T) {
	r, w := io.Pipe()

	go func() {
		WriteMessage(w, []byte("hello kafka"))
		w.Close()
	}()

	msg, err := ReadMessage(r)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if string(msg) != "hello kafka" {
		t.Errorf("got %q, want %q", msg, "hello kafka")
	}
}

func TestReadMessage_SplitAcrossMultipleWrites(t *testing.T) {
	r, w := io.Pipe()

	go func() {
		// Simulate a message arriving in fragments smaller than the whole
		// frame, the way a real TCP stream can split writes arbitrarily.
		full := []byte("hello kafka")
		header := make([]byte, 4)
		header[3] = byte(len(full))
		w.Write(header[:2])
		w.Write(header[2:])
		w.Write(full[:3])
		w.Write(full[3:])
		w.Close()
	}()

	msg, err := ReadMessage(r)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if string(msg) != "hello kafka" {
		t.Errorf("got %q, want %q", msg, "hello kafka")
	}
}

func TestReadMessage_CleanEOFBetweenMessages(t *testing.T) {
	r, w := io.Pipe()
	go w.Close()

	_, err := ReadMessage(r)
	if err != io.EOF {
		t.Errorf("got %v, want io.EOF", err)
	}
}

func TestReadMessage_UnexpectedEOFMidMessage(t *testing.T) {
	r, w := io.Pipe()

	go func() {
		header := make([]byte, 4)
		header[3] = 10 // claims 10 bytes follow
		w.Write(header)
		w.Write([]byte("abc")) // only 3 actually arrive
		w.Close()
	}()

	_, err := ReadMessage(r)
	if err != io.ErrUnexpectedEOF {
		t.Errorf("got %v, want io.ErrUnexpectedEOF", err)
	}
}
