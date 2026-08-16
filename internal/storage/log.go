package storage

// Log is the boundary between protocol handlers and the storage engine.
// Handlers depend only on this interface, never on a concrete implementation.
//
// Append takes recordCount because a Kafka record batch can hold many
// records and therefore consumes many offsets, and only the protocol layer
// can read that count out of the batch header. Passing it across this
// boundary is what lets the storage engine assign offsets correctly while
// still treating a batch's contents as opaque bytes it never parses.
type Log interface {
	Append(topic string, partition int32, batch []byte, recordCount int32) (baseOffset int64, err error)
	Read(topic string, partition int32, offset int64, maxBytes int32) ([]byte, error)
	EarliestOffset(topic string, partition int32) (int64, error)
	LatestOffset(topic string, partition int32) (int64, error)
}
