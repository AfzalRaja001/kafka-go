package broker

import (
	"io"
	"testing"
)

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
