package protocol

// Kafka protocol error codes this broker currently produces. See the
// protocol guide's error code table for the full registry.
const (
	ErrUnknownServerError      int16 = -1
	ErrNone                    int16 = 0
	ErrUnknownTopicOrPartition int16 = 3
	ErrCorruptMessage          int16 = 2
	ErrUnsupportedVersion      int16 = 35
	ErrTopicAlreadyExists      int16 = 36
	ErrInvalidPartitions       int16 = 37
)
