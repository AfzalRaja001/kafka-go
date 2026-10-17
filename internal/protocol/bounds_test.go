package protocol

import (
	"math"
	"testing"
)

// hostileCount is one malformed request body plus the decode call that must
// reject it. Every body here is built so that one array count is the only
// thing wrong with it - nothing else would make decoding fail.
type hostileCount struct {
	name   string
	decode func([]byte) error
	body   []byte
	// control is the same body with the count set to 0 - it must decode
	// cleanly, proving the count is the only thing wrong with body.
	control []byte
}

// fieldsThen builds a body from a prefix of valid fields, a single array
// count, then whatever valid fields the request has after that array -
// without the suffix, a decoder could fail on the missing trailing field
// instead of the count, and the test would pass for the wrong reason.
func fieldsThen(prefix func(e *Encoder), count int32, suffix ...func(e *Encoder)) []byte {
	e := NewEncoder()
	prefix(e)
	e.Int32(count)
	for _, s := range suffix {
		s(e)
	}
	return e.Result()
}

func noPrefix(*Encoder) {}

func decodeErr[T any](fn func([]byte) (T, error)) func([]byte) error {
	return func(b []byte) error {
		_, err := fn(b)
		return err
	}
}

func hostileCountCases() []hostileCount {
	metadata := func(b []byte) error {
		_, err := HandleMetadata(1, b, NewTopicRegistry(), nil)
		return err
	}
	fetchPrefix := func(e *Encoder) { e.Int32(-1); e.Int32(100); e.Int32(1) }
	producePrefix := func(e *Encoder) { e.NullableString(nil); e.Int16(1); e.Int32(1000) }
	listOffsetsPrefix := func(e *Encoder) { e.Int32(-1) }
	groupPrefix := func(e *Encoder) { e.String("g") }
	joinPrefix := func(e *Encoder) { e.String("g"); e.Int32(6000); e.String(""); e.String("consumer") }
	syncPrefix := func(e *Encoder) { e.String("g"); e.Int32(1); e.String("m") }
	oneTopicThenPartitions := func(prefix func(*Encoder)) func(*Encoder) {
		return func(e *Encoder) { prefix(e); e.Int32(1); e.String("t") }
	}
	createTopicThen := func(e *Encoder) { e.Int32(1); e.String("t"); e.Int32(1); e.Int16(1) }

	timeout := func(e *Encoder) { e.Int32(5000) }
	emptyConfigsThenTimeout := func(e *Encoder) { e.Int32(0); e.Int32(5000) }

	var cases []hostileCount
	addWithSuffix := func(name string, decode func([]byte) error, prefix, suffix func(*Encoder), counts ...int32) {
		for _, c := range counts {
			label := map[int32]string{-1: "null", -2: "negative", -5: "negative5", math.MaxInt32: "maxint32", 1 << 20: "exceeds_remaining"}[c]
			cases = append(cases, hostileCount{name + "/" + label, decode, fieldsThen(prefix, c, suffix), fieldsThen(prefix, 0, suffix)})
		}
	}
	add := func(name string, decode func([]byte) error, prefix func(*Encoder), counts ...int32) {
		addWithSuffix(name, decode, prefix, func(*Encoder) {}, counts...)
	}

	big := []int32{math.MaxInt32, 1 << 20}
	// Arrays the protocol says may be null only get the "too big" and
	// "negative" cases; non-nullable ones also must reject -1.
	add("Metadata.topics", metadata, noPrefix, append(big, -2)...)
	addWithSuffix("DeleteTopics.topics", decodeErr(DecodeDeleteTopicsRequest), noPrefix, timeout, append(big, -2, -1)...)
	add("OffsetFetch.topics", decodeErr(DecodeOffsetFetchRequest), groupPrefix, append(big, -2, -5)...)
	add("OffsetFetch.partitions", decodeErr(DecodeOffsetFetchRequest), oneTopicThenPartitions(groupPrefix), append(big, -2, -1)...)
	add("Fetch.topics", decodeErr(DecodeFetchRequest), fetchPrefix, append(big, -2, -1)...)
	add("Fetch.partitions", decodeErr(DecodeFetchRequest), oneTopicThenPartitions(fetchPrefix), append(big, -2, -1)...)
	add("Produce.topics", decodeErr(DecodeProduceRequest), producePrefix, append(big, -2, -1)...)
	add("Produce.partitions", decodeErr(DecodeProduceRequest), oneTopicThenPartitions(producePrefix), append(big, -2, -1)...)
	add("ListOffsets.topics", decodeErr(DecodeListOffsetsRequest), listOffsetsPrefix, append(big, -2, -1)...)
	add("ListOffsets.partitions", decodeErr(DecodeListOffsetsRequest), oneTopicThenPartitions(listOffsetsPrefix), append(big, -2, -1)...)
	add("OffsetCommit.topics", decodeErr(DecodeOffsetCommitRequest), groupPrefix, append(big, -2, -1)...)
	add("OffsetCommit.partitions", decodeErr(DecodeOffsetCommitRequest), oneTopicThenPartitions(groupPrefix), append(big, -2, -1)...)
	add("JoinGroup.protocols", decodeErr(DecodeJoinGroupRequest), joinPrefix, append(big, -2, -1)...)
	add("SyncGroup.assignments", decodeErr(DecodeSyncGroupRequest), syncPrefix, append(big, -2, -1)...)
	addWithSuffix("CreateTopics.topics", decodeErr(DecodeCreateTopicsRequest), noPrefix, timeout, append(big, -2, -1)...)
	addWithSuffix("CreateTopics.assignments", decodeErr(DecodeCreateTopicsRequest), createTopicThen, emptyConfigsThenTimeout, append(big, -2, -1)...)
	addWithSuffix("CreateTopics.configs", decodeErr(DecodeCreateTopicsRequest),
		func(e *Encoder) { createTopicThen(e); e.Int32(0) }, timeout, append(big, -2, -1)...)
	addWithSuffix("CreateTopics.assignment_brokers", decodeErr(DecodeCreateTopicsRequest),
		func(e *Encoder) { createTopicThen(e); e.Int32(1); e.Int32(0) }, emptyConfigsThenTimeout, append(big, -2, -1)...)
	return cases
}

// TestDecoders_RejectHostileArrayCounts replays, against every decoder,
// the class of input the 2026-10-13 evaluation found crashing the broker: a
// 4-7 byte body whose array count is negative or far larger than the bytes
// that actually follow. Each must come back as an ordinary decode error -
// never a panic, and never an attempt to allocate memory for elements that
// were never sent.
func TestDecoders_RejectHostileArrayCounts(t *testing.T) {
	for _, tc := range hostileCountCases() {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panicked: %v", r)
				}
			}()
			if err := tc.decode(tc.body); err == nil {
				t.Fatalf("accepted a %d-byte body with a hostile array count; want a decode error", len(tc.body))
			}
		})
	}
}

func TestDecoders_HostileCaseControlsDecodeCleanly(t *testing.T) {
	for _, tc := range hostileCountCases() {
		if err := tc.decode(tc.control); err != nil {
			t.Errorf("%s: control body (count 0) failed to decode, so the hostile case would pass for the wrong reason: %v", tc.name, err)
		}
	}
}

// TestDecoders_NullableArraysStillAcceptNull guards the other direction:
// the two arrays the protocol defines as nullable must keep meaning "all
// topics" when sent as -1, exactly as they did before bounds checking.
func TestDecoders_NullableArraysStillAcceptNull(t *testing.T) {
	if _, err := HandleMetadata(1, fieldsThen(noPrefix, -1), NewTopicRegistry(), nil); err != nil {
		t.Errorf("Metadata with null topics: %v", err)
	}
	req, err := DecodeOffsetFetchRequest(fieldsThen(func(e *Encoder) { e.String("g") }, -1))
	if err != nil || req.Topics != nil {
		t.Errorf("OffsetFetch with null topics = (%v, %v), want (nil topics, nil error)", req.Topics, err)
	}
}
