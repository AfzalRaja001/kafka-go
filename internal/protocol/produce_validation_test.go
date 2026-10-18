package protocol

import (
	"testing"

	"github.com/AfzalRaja001/kafka-go/internal/storage"
)

// registryWithTopic returns an in-memory registry that already holds one
// topic with the given number of partitions - the state Produce now needs,
// because a topic only exists once CreateTopics has made it.
func registryWithTopic(name string, partitions int32) *TopicRegistry {
	r := NewTopicRegistry()
	if err := r.AddTopic(r.NewTopic(name, partitions)); err != nil {
		panic(err)
	}
	return r
}

// encodeProduceRequestMulti builds a Produce v3 request with several
// (topic, partition) targets, each carrying the same record batch.
func encodeProduceRequestMulti(targets []produceTarget, records []byte) []byte {
	byTopic := map[string][]int32{}
	var order []string
	for _, tg := range targets {
		if _, seen := byTopic[tg.topic]; !seen {
			order = append(order, tg.topic)
		}
		byTopic[tg.topic] = append(byTopic[tg.topic], tg.partition)
	}

	enc := NewEncoder()
	var txnID *string
	enc.NullableString(txnID)
	enc.Int16(1)    // acks
	enc.Int32(5000) // timeout_ms
	enc.Int32(int32(len(order)))
	for _, topic := range order {
		enc.String(topic)
		enc.Int32(int32(len(byTopic[topic])))
		for _, p := range byTopic[topic] {
			enc.Int32(p)
			enc.Bytes(records)
		}
	}
	return enc.Result()
}

type produceTarget struct {
	topic     string
	partition int32
}

type produceResult struct {
	topic     string
	partition int32
	errorCode int16
	offset    int64
}

func decodeProduceResponse(t *testing.T, resp []byte) []produceResult {
	t.Helper()
	dec := NewDecoder(resp)
	dec.Int32() // correlation_id
	topicCount, _ := dec.Int32()
	var out []produceResult
	for i := int32(0); i < topicCount; i++ {
		topic, _ := dec.String()
		partCount, _ := dec.Int32()
		for j := int32(0); j < partCount; j++ {
			partition, _ := dec.Int32()
			code, _ := dec.Int16()
			offset, _ := dec.Int64()
			dec.Int64() // log_append_time
			out = append(out, produceResult{topic, partition, code, offset})
		}
	}
	return out
}

// TestHandleProduce_UnknownTopicOrPartitionRejected covers every way a
// client can name a place that CreateTopics never made. Each must come back
// as UNKNOWN_TOPIC_OR_PARTITION and must write nothing - before the fix the
// first append silently created a hidden, unregistered partition directory.
func TestHandleProduce_UnknownTopicOrPartitionRejected(t *testing.T) {
	cases := []struct {
		label     string
		topic     string
		partition int32
	}{
		{"unknown topic", "ghost", 0},
		{"partition equal to the partition count", "orders", 1},
		{"partition far above the count", "orders", 7},
		{"negative partition", "orders", -1},
		{"most negative partition", "orders", -2147483648},
		{"largest partition", "orders", 2147483647},
		{"unknown topic, other partition", "ghost", 3},
		{"empty topic name", "", 0},
		{"path-like topic name", "../escape", 0},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			fake := storage.NewFakeLog()
			registry := registryWithTopic("orders", 1)
			records := buildTestBatch(0, 1, []byte("r"))
			body := encodeProduceRequest(1, 5000, tc.topic, tc.partition, records)

			resp, err := HandleProduce(1, body, registry, fake)
			if err != nil {
				t.Fatalf("HandleProduce returned a Go error for a per-partition problem: %v", err)
			}
			got := decodeProduceResponse(t, resp)
			if len(got) != 1 || got[0].errorCode != ErrUnknownTopicOrPartition {
				t.Fatalf("response = %+v, want one result with error_code %d", got, ErrUnknownTopicOrPartition)
			}
			if got[0].partition != tc.partition || got[0].topic != tc.topic {
				t.Errorf("response names (%q, %d), want it to echo (%q, %d)", got[0].topic, got[0].partition, tc.topic, tc.partition)
			}
			if got[0].offset != -1 {
				t.Errorf("base_offset = %d, want -1 for a rejected partition", got[0].offset)
			}
			if _, err := fake.LatestOffset(tc.topic, tc.partition); err == nil {
				t.Errorf("storage now has (%q, %d): the rejected produce created a partition", tc.topic, tc.partition)
			}
		})
	}
}

// One request can target several partitions; the error is per partition, so
// a bad one must not stop a good one from being written, nor the reverse.
func TestHandleProduce_MixedRequestGivesEachPartitionItsOwnResult(t *testing.T) {
	fake := storage.NewFakeLog()
	registry := registryWithTopic("orders", 2)
	for p := int32(0); p < 2; p++ {
		if err := fake.CreatePartition("orders", p); err != nil {
			t.Fatal(err)
		}
	}
	records := buildTestBatch(0, 1, []byte("r"))
	body := encodeProduceRequestMulti([]produceTarget{
		{"orders", 0},
		{"orders", 5},
		{"ghost", 0},
		{"orders", 1},
	}, records)

	resp, err := HandleProduce(1, body, registry, fake)
	if err != nil {
		t.Fatalf("HandleProduce: %v", err)
	}
	got := map[produceTarget]produceResult{}
	for _, r := range decodeProduceResponse(t, resp) {
		got[produceTarget{r.topic, r.partition}] = r
	}

	want := map[produceTarget]int16{
		{"orders", 0}: ErrNone,
		{"orders", 5}: ErrUnknownTopicOrPartition,
		{"ghost", 0}:  ErrUnknownTopicOrPartition,
		{"orders", 1}: ErrNone,
	}
	if len(got) != len(want) {
		t.Fatalf("got %d results, want %d: %+v", len(got), len(want), got)
	}
	for target, code := range want {
		if got[target].errorCode != code {
			t.Errorf("%+v: error_code = %d, want %d", target, got[target].errorCode, code)
		}
	}
	for p := int32(0); p < 2; p++ {
		if latest, _ := fake.LatestOffset("orders", p); latest != 1 {
			t.Errorf("orders-%d latest offset = %d, want 1 (one record written)", p, latest)
		}
	}
	if _, err := fake.LatestOffset("orders", 5); err == nil {
		t.Error("orders-5 exists in storage: the rejected partition was created")
	}
}

// Every valid partition id of a multi-partition topic still works, including
// the first and the last, so the bounds check is not off by one either way.
func TestHandleProduce_EveryPartitionOfATopicAccepted(t *testing.T) {
	const partitions = 4
	fake := storage.NewFakeLog()
	registry := registryWithTopic("orders", partitions)
	for p := int32(0); p < partitions; p++ {
		fake.CreatePartition("orders", p)
		resp, err := HandleProduce(1, encodeProduceRequest(1, 5000, "orders", p, buildTestBatch(0, 1, []byte("r"))), registry, fake)
		if err != nil {
			t.Fatalf("partition %d: %v", p, err)
		}
		got := decodeProduceResponse(t, resp)
		if len(got) != 1 || got[0].errorCode != ErrNone {
			t.Errorf("partition %d: response = %+v, want error_code 0", p, got)
		}
	}
}

// A topic that is deleted stops accepting writes immediately, even if some
// storage for it were still around.
func TestHandleProduce_DeletedTopicRejected(t *testing.T) {
	fake := storage.NewFakeLog()
	registry := registryWithTopic("orders", 1)
	fake.CreatePartition("orders", 0)
	if err := registry.RemoveTopic("orders"); err != nil {
		t.Fatal(err)
	}

	resp, err := HandleProduce(1, encodeProduceRequest(1, 5000, "orders", 0, buildTestBatch(0, 1, []byte("r"))), registry, fake)
	if err != nil {
		t.Fatalf("HandleProduce: %v", err)
	}
	got := decodeProduceResponse(t, resp)
	if len(got) != 1 || got[0].errorCode != ErrUnknownTopicOrPartition {
		t.Fatalf("response = %+v, want error_code %d", got, ErrUnknownTopicOrPartition)
	}
	if latest, _ := fake.LatestOffset("orders", 0); latest != 0 {
		t.Errorf("latest offset = %d, want 0: a record was written to a deleted topic", latest)
	}
}

// Registered in the registry but missing in storage is a broker-side fault
// (the state ensureStorage repairs at startup), not the client's mistake, so
// it reports a server error rather than UNKNOWN_TOPIC_OR_PARTITION and does
// not quietly create the partition.
func TestHandleProduce_RegisteredButMissingInStorageIsAServerError(t *testing.T) {
	fake := storage.NewFakeLog() // no CreatePartition: storage lacks the partition
	registry := registryWithTopic("orders", 1)

	resp, err := HandleProduce(1, encodeProduceRequest(1, 5000, "orders", 0, buildTestBatch(0, 1, []byte("r"))), registry, fake)
	if err != nil {
		t.Fatalf("HandleProduce: %v", err)
	}
	got := decodeProduceResponse(t, resp)
	if len(got) != 1 || got[0].errorCode != ErrUnknownServerError {
		t.Fatalf("response = %+v, want error_code %d", got, ErrUnknownServerError)
	}
}
