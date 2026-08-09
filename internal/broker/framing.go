package broker

import (
	"encoding/binary"
	"io"
)

// ReadMessage reads one length-prefixed message: a 4-byte big-endian length,
// then exactly that many bytes. Safe against a Read returning fewer bytes
// than requested, since io.ReadFull retries until buf is completely filled.
func ReadMessage(r io.Reader) ([]byte, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	size := binary.BigEndian.Uint32(header)

	body := make([]byte, size)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}

	return body, nil
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
