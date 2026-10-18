package main

import (
	"fmt"

	"github.com/AfzalRaja001/kafka-go/internal/protocol"
	"github.com/AfzalRaja001/kafka-go/internal/storage"
)

// ensureStorage makes sure every partition the topics file lists has storage
// behind it, creating any that are missing as empty partitions. CreatePartition
// is idempotent, so partitions that already exist (and their data) are left
// exactly as they are.
//
// The only way a listed partition can be missing is a DeleteTopics that
// crashed after removing a topic's storage but before saving the shorter
// topic list. Re-creating the partitions empty leaves a topic the client can
// simply delete again, instead of one whose first Produce would silently
// re-create a partition, or whose Fetch would report it as unknown.
//
// Unlike the background sweeps, which skip a bad partition and carry on, this
// runs once at startup and any failure is returned: a listed topic that
// cannot be given storage is a reason to refuse to start, not to serve a
// broker that advertises topics it cannot read.
func ensureStorage(registry *protocol.TopicRegistry, diskLog storage.Log) error {
	for _, topic := range registry.All() {
		for _, partition := range topic.Partitions {
			if err := diskLog.CreatePartition(topic.Name, partition.ID); err != nil {
				return fmt.Errorf("providing storage for %s-%d: %w", topic.Name, partition.ID, err)
			}
		}
	}
	return nil
}
