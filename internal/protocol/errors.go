package protocol

// Kafka protocol error codes this broker currently produces. See the
// protocol guide's error code table for the full registry.
const (
	ErrNone                    int16 = 0
	ErrUnknownTopicOrPartition int16 = 3
	ErrUnsupportedVersion      int16 = 35
)
