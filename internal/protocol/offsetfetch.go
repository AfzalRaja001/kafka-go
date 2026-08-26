package protocol

import (
	"fmt"
	"sort"

	"github.com/AfzalRaja001/kafka-go/internal/group"
)

type OffsetFetchTopicRequest struct {
	Topic      string
	Partitions []int32
}

// OffsetFetchRequest's Topics is nil in exactly one case: the client sent a
// null topics array (v2+), meaning "fetch every offset this group has ever
// committed" rather than naming topic-partitions explicitly. An empty,
// non-nil slice (topic_count = 0) is a different, valid request that simply
// asks about nothing - decodeTopics keeps that distinction rather than
// collapsing both to nil.
type OffsetFetchRequest struct {
	GroupID string
	Topics  []OffsetFetchTopicRequest
}

// DecodeOffsetFetchRequest decodes an OffsetFetch v2 request body. v2's only
// wire difference from v0 is that the top-level topics array count can be
// -1 (null) instead of always being >= 0 - verified against Apache Kafka's
// own OffsetFetchRequest.json schema (branch 2.5): "Starting in version 2,
// the request can contain a null topics array to indicate that offsets for
// all topics should be fetched."
func DecodeOffsetFetchRequest(buf []byte) (OffsetFetchRequest, error) {
	dec := NewDecoder(buf)

	groupID, err := dec.String()
	if err != nil {
		return OffsetFetchRequest{}, fmt.Errorf("group_id: %w", err)
	}

	topicCount, err := dec.Int32()
	if err != nil {
		return OffsetFetchRequest{}, fmt.Errorf("topic count: %w", err)
	}
	if topicCount == -1 {
		return OffsetFetchRequest{GroupID: groupID, Topics: nil}, nil
	}

	topics := make([]OffsetFetchTopicRequest, 0, topicCount)
	for i := int32(0); i < topicCount; i++ {
		topic, err := dec.String()
		if err != nil {
			return OffsetFetchRequest{}, fmt.Errorf("topic %d name: %w", i, err)
		}
		partCount, err := dec.Int32()
		if err != nil {
			return OffsetFetchRequest{}, fmt.Errorf("topic %d partition count: %w", i, err)
		}

		var parts []int32
		for j := int32(0); j < partCount; j++ {
			partition, err := dec.Int32()
			if err != nil {
				return OffsetFetchRequest{}, fmt.Errorf("topic %d partition %d: %w", i, j, err)
			}
			parts = append(parts, partition)
		}
		topics = append(topics, OffsetFetchTopicRequest{Topic: topic, Partitions: parts})
	}

	return OffsetFetchRequest{GroupID: groupID, Topics: topics}, nil
}

// HandleOffsetFetch builds an OffsetFetch v2 response body. A partition
// with no committed offset reports offset -1 and null metadata with
// error_code NONE, matching real Kafka's sentinel for "nothing to resume
// from" - not a failure. v2 adds a top-level error_code (always ErrNone
// here - this broker has no group/coordinator-level failure this API can
// report) and lets req.Topics be nil, meaning "every topic this group has
// ever committed" rather than an explicit list.
func HandleOffsetFetch(correlationID int32, requestBody []byte, store group.OffsetStore) ([]byte, error) {
	req, err := DecodeOffsetFetchRequest(requestBody)
	if err != nil {
		return nil, fmt.Errorf("offset_fetch request: %w", err)
	}

	topics := req.Topics
	if topics == nil {
		topics = allCommittedTopics(store.FetchAll(req.GroupID))
	}

	enc := NewEncoder()
	enc.Int32(correlationID)
	enc.Int32(int32(len(topics)))

	for _, topic := range topics {
		enc.String(topic.Topic)
		enc.Int32(int32(len(topic.Partitions)))

		for _, partition := range topic.Partitions {
			offset, metadata, found := store.Fetch(req.GroupID, topic.Topic, partition)
			enc.Int32(partition)
			if !found {
				enc.Int64(-1)
				enc.NullableString(nil)
				enc.Int16(ErrNone)
				continue
			}
			enc.Int64(offset)
			enc.NullableString(&metadata)
			enc.Int16(ErrNone)
		}
	}

	enc.Int16(ErrNone) // top-level error_code, added in v2
	return enc.Result(), nil
}

// allCommittedTopics groups a flat FetchAll result back into the same
// per-topic shape an explicit request already has, so the response-writing
// loop above doesn't need to know which of the two cases it's in. Sorted by
// topic then partition for a deterministic response - FetchAll itself makes
// no ordering promise, since OffsetStore doesn't know or care about Kafka's
// wire shape (see the FetchAll design entry in docs/decisions.md).
func allCommittedTopics(committed []group.GroupOffset) []OffsetFetchTopicRequest {
	byTopic := make(map[string][]int32)
	for _, c := range committed {
		byTopic[c.Topic] = append(byTopic[c.Topic], c.Partition)
	}

	names := make([]string, 0, len(byTopic))
	for name := range byTopic {
		names = append(names, name)
	}
	sort.Strings(names)

	topics := make([]OffsetFetchTopicRequest, 0, len(names))
	for _, name := range names {
		partitions := byTopic[name]
		sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
		topics = append(topics, OffsetFetchTopicRequest{Topic: name, Partitions: partitions})
	}
	return topics
}
