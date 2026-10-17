package broker

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// MaxRequestBytes caps the size a single request frame may claim - real
// Kafka's own socket.request.max.bytes default. Without it, the 4-byte
// length prefix let any client make the broker set aside up to 4GiB before
// a single body byte arrived.
const MaxRequestBytes = 100 * 1024 * 1024

// ErrRequestTooLarge is returned for a frame whose length prefix exceeds
// the limit. The connection can't be recovered after this - the body that
// was announced is still on its way, and there is no way to skip it and
// find the next frame boundary - so the caller closes it.
var ErrRequestTooLarge = errors.New("request exceeds the maximum request size")

// initialReadBuffer is how much memory a frame body gets up front, however
// large it claims to be. Anything bigger grows only as bytes actually
// arrive.
const initialReadBuffer = 64 * 1024

// ReadMessage reads one length-prefixed message: a 4-byte big-endian length,
// then exactly that many bytes. A length over MaxRequestBytes is rejected
// from the header alone.
func ReadMessage(r io.Reader) ([]byte, error) {
	return readMessage(r, MaxRequestBytes)
}

// readMessage is ReadMessage with the size limit as a parameter, so tests
// can exercise the boundary without 100MB frames.
//
// The body is read into a buffer that starts at no more than 64KB and grows
// as data arrives, instead of being allocated at the claimed size up front.
// A client that announces a large frame and then sends nothing costs the
// broker almost nothing - memory is only ever spent on bytes it has really
// received.
func readMessage(r io.Reader, limit uint32) ([]byte, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	size := binary.BigEndian.Uint32(header)
	if size > limit {
		return nil, fmt.Errorf("%w: frame claims %d bytes, limit is %d", ErrRequestTooLarge, size, limit)
	}

	body := bytes.NewBuffer(make([]byte, 0, min(size, initialReadBuffer)))
	n, err := body.ReadFrom(io.LimitReader(r, int64(size)))
	if err != nil {
		return nil, err
	}
	if n < int64(size) {
		return nil, io.ErrUnexpectedEOF
	}
	return body.Bytes(), nil
}

// WriteMessage writes body prefixed with its 4-byte big-endian length.
func WriteMessage(w io.Writer, body []byte) error {
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(body)))

	if _, err := w.Write(header); err != nil {
		return err
	}
	_, err := w.Write(body)
	return err
}
