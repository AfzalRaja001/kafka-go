package storage

import "time"

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

	// Size reports the total on-disk log bytes for a topic-partition - not
	// index/timeindex files, matching real Kafka's own meaning of partition
	// size. Used for the kafkago_partition_bytes metric, not by any request
	// handler.
	Size(topic string, partition int32) (int64, error)

	// CreatePartition provisions a topic-partition's storage eagerly, so it
	// is immediately readable at offset 0 even before anything is Appended.
	// DeletePartition removes it, including its on-disk data. Both exist
	// for CreateTopics/DeleteTopics - every other method treats a
	// topic-partition's existence as something Append alone establishes.
	CreatePartition(topic string, partition int32) error
	DeletePartition(topic string, partition int32) error

	// Compact fully replaces a topic-partition's log with records, each
	// becoming one blob at a freshly renumbered offset starting at 0.
	//
	// This is NOT real Kafka compaction, which preserves original offsets
	// and leaves gaps where removed records used to be - this segment
	// format has no way to represent a gap at all, since a blob's offset is
	// only ever derived by summing spans forward from a known starting
	// point, never stored explicitly. Renumbering from 0 sidesteps that
	// entirely, at the cost of a real constraint: Compact is only safe for
	// a topic-partition that is never read by a specific offset, only ever
	// replayed sequentially from 0. That's true today of __consumer_offsets
	// (see internal/offsets) and nothing else - do not call this on a
	// topic-partition a real client might Fetch.
	Compact(topic string, partition int32, records [][]byte) error

	// ApplyRetention deletes whole old segments - oldest first, never the
	// active one - once they're older than maxAge or the partition exceeds
	// maxBytes. Either can be 0 to disable that check. Unlike Compact, this
	// never renumbers anything: EarliestOffset simply advances past whatever
	// got deleted, leaving a real gap - the normal, expected shape of a
	// Kafka log under retention, not something to work around.
	ApplyRetention(topic string, partition int32, maxAge time.Duration, maxBytes int64, now time.Time) error

	// Sync forces the active segment to disk. Append already does this on
	// its own once a configurable per-partition message count is reached
	// (and unconditionally whenever a segment rolls), which bounds worst-case
	// data loss by volume; Sync exists so a background ticker can also bound
	// it by time, for a partition too low-traffic to ever cross the message
	// count on its own. A topic-partition the caller doesn't know about is
	// not an error - like ApplyRetention, this is meant to be called by a
	// sweep across every known partition on a timer, and one racing with
	// DeleteTopics shouldn't be treated as a failure worth logging.
	Sync(topic string, partition int32) error
}
