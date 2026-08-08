package storage

// Log is the boundary between protocol handlers and the storage engine.
// Handlers depend only on this interface, never on a concrete implementation.
type Log interface {
	Append(topic string, partition int32, batch []byte) (baseOffset int64, err error)
	Read(topic string, partition int32, offset int64, maxBytes int32) ([]byte, error)
	EarliestOffset(topic string, partition int32) (int64, error)
	LatestOffset(topic string, partition int32) (int64, error)
}
