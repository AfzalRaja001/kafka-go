package protocol

import (
	"testing"
	"time"

	"github.com/AfzalRaja001/kafka-go/internal/storage"
)

func encodeFetchRequest(maxWaitTimeMs, minBytes int32, topic string, partition int32, fetchOffset int64, maxBytes int32) []byte {
	enc := NewEncoder()
	enc.Int32(-1) // replica_id: -1 for a normal consumer
	enc.Int32(maxWaitTimeMs)
	enc.Int32(minBytes)
	enc.Int32(1) // topic count
	enc.String(topic)
	enc.Int32(1) // partition count
	enc.Int32(partition)
	enc.Int64(fetchOffset)
	enc.Int32(maxBytes)
	return enc.Result()
}

func TestDecodeFetchRequest(t *testing.T) {
	body := encodeFetchRequest(1000, 1, "orders", 2, 42, 4096)

	req, err := DecodeFetchRequest(body)
	if err != nil {
		t.Fatalf("DecodeFetchRequest: %v", err)
	}
	if req.MaxWaitTimeMs != 1000 || req.MinBytes != 1 {
		t.Fatalf("MaxWaitTimeMs/MinBytes = %d/%d, want 1000/1", req.MaxWaitTimeMs, req.MinBytes)
	}
	if len(req.Topics) != 1 || req.Topics[0].Topic != "orders" {
		t.Fatalf("Topics = %+v, want one topic named orders", req.Topics)
	}
	part := req.Topics[0].Partitions[0]
	if part.Partition != 2 || part.FetchOffset != 42 || part.MaxBytes != 4096 {
		t.Fatalf("Partition = %+v, want {2, 42, 4096}", part)
	}
}

func decodeFetchPartitionResponse(t *testing.T, resp []byte) (data []byte, errorCode int16, highWatermark int64) {
	t.Helper()
	dec := NewDecoder(resp)
	dec.Int32() // correlation_id
	dec.Int32() // topic count
	dec.String()
	dec.Int32() // partition count
	dec.Int32() // partition
	errorCode, _ = dec.Int16()
	highWatermark, _ = dec.Int64()
	data, _ = dec.Bytes()
	return data, errorCode, highWatermark
}

func TestHandleFetch_ReturnsImmediatelyWhenDataAvailable(t *testing.T) {
	log := storage.NewFakeLog()
	log.Append("orders", 0, []byte("hello"), 1)

	body := encodeFetchRequest(2000, 0, "orders", 0, 0, 1024)

	start := time.Now()
	resp, err := HandleFetch(1, body, log)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("HandleFetch: %v", err)
	}
	if elapsed > 200*time.Millisecond {
		t.Errorf("took %v to return already-available data, want near-instant (max_wait_time_ms was 2000)", elapsed)
	}

	data, errorCode, highWatermark := decodeFetchPartitionResponse(t, resp)
	if errorCode != ErrNone {
		t.Fatalf("error_code = %d, want %d", errorCode, ErrNone)
	}
	if string(data) != "hello" {
		t.Errorf("data = %q, want %q", data, "hello")
	}
	if highWatermark != 1 {
		t.Errorf("high_watermark = %d, want 1", highWatermark)
	}
}

// TestHandleFetch_LongPollsUntilDataArrives is the test that actually
// proves long-polling works: the partition already exists (seeded with one
// record) and the fetch offset is caught up to its current end - the
// realistic "consumer waiting for the next record" case, not "partition
// was never created" (that's the separate fail-fast case tested below). A
// goroutine appends the next record after a short delay, and Fetch must
// return the moment that happens - not instantly (which would mean it
// isn't really waiting) and not after the full timeout (which would mean
// it's just sleeping, not actually noticing new data).
func TestHandleFetch_LongPollsUntilDataArrives(t *testing.T) {
	log := storage.NewFakeLog()
	log.Append("orders", 0, []byte("seed"), 1) // offset 0 - partition now exists

	const appendDelay = 100 * time.Millisecond
	go func() {
		time.Sleep(appendDelay)
		log.Append("orders", 0, []byte("late-arrival"), 1) // offset 1
	}()

	body := encodeFetchRequest(2000, 1, "orders", 0, 1, 1024) // fetch from offset 1 - caught up

	start := time.Now()
	resp, err := HandleFetch(1, body, log)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("HandleFetch: %v", err)
	}

	if elapsed < appendDelay {
		t.Errorf("returned after %v, before the append at %v - didn't actually wait for the data", elapsed, appendDelay)
	}
	if elapsed > 1*time.Second {
		t.Errorf("returned after %v - waited far longer than the append delay, long-polling isn't reacting promptly", elapsed)
	}

	data, errorCode, _ := decodeFetchPartitionResponse(t, resp)
	if errorCode != ErrNone {
		t.Fatalf("error_code = %d, want %d", errorCode, ErrNone)
	}
	if string(data) != "late-arrival" {
		t.Errorf("data = %q, want %q", data, "late-arrival")
	}
}

func TestHandleFetch_TimesOutWhenNoDataArrives(t *testing.T) {
	log := storage.NewFakeLog()
	log.Append("orders", 0, []byte("x"), 1) // 1 byte - less than minBytes below

	const maxWait = 100 * time.Millisecond
	body := encodeFetchRequest(int32(maxWait.Milliseconds()), 1000, "orders", 0, 0, 1024)

	start := time.Now()
	resp, err := HandleFetch(1, body, log)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("HandleFetch: %v", err)
	}

	if elapsed < maxWait {
		t.Errorf("returned after %v, before max_wait_time (%v) elapsed", elapsed, maxWait)
	}
	if elapsed > maxWait+300*time.Millisecond {
		t.Errorf("returned after %v - well past max_wait_time (%v)", elapsed, maxWait)
	}

	data, errorCode, _ := decodeFetchPartitionResponse(t, resp)
	if errorCode != ErrNone {
		t.Fatalf("error_code = %d, want %d (timing out with insufficient data is not an error)", errorCode, ErrNone)
	}
	if string(data) != "x" {
		t.Errorf("data = %q, want %q (whatever was available, even though it's less than min_bytes)", data, "x")
	}
}

// TestHandleFetch_EmptyResponseIsNotNull is a regression test for a real
// bug found testing against kafka-python: when a partition exists and is
// simply caught up (no new records past the fetch offset), the records
// field must be encoded as present-but-empty (wire length 0), not null
// (wire length -1). A real client's batch parser treats null here as
// unexpected rather than "nothing new yet," and errors trying to read one.
func TestHandleFetch_EmptyResponseIsNotNull(t *testing.T) {
	log := storage.NewFakeLog()
	log.Append("orders", 0, []byte("only-record"), 1) // offset 0 - partition exists

	const maxWait = 100 * time.Millisecond
	// Fetch from offset 1: caught up, nothing there, but the partition
	// itself is real - this is the exact path that previously encoded a
	// null records field instead of an empty one.
	body := encodeFetchRequest(int32(maxWait.Milliseconds()), 1, "orders", 0, 1, 1024)

	resp, err := HandleFetch(1, body, log)
	if err != nil {
		t.Fatalf("HandleFetch: %v", err)
	}

	data, errorCode, _ := decodeFetchPartitionResponse(t, resp)
	if errorCode != ErrNone {
		t.Fatalf("error_code = %d, want %d", errorCode, ErrNone)
	}
	if data == nil {
		t.Fatal("records field decoded as null - real clients reject this; it must be present-but-empty")
	}
	if len(data) != 0 {
		t.Errorf("data = %q, want empty", data)
	}
}

func TestHandleFetch_UnknownTopicPartitionReturnsQuickly(t *testing.T) {
	log := storage.NewFakeLog()
	body := encodeFetchRequest(2000, 1, "missing", 0, 0, 1024)

	start := time.Now()
	resp, err := HandleFetch(1, body, log)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("HandleFetch: %v", err)
	}
	if elapsed > 200*time.Millisecond {
		t.Errorf("took %v to fail on an unknown topic-partition, want near-instant (max_wait_time_ms was 2000)", elapsed)
	}

	_, errorCode, _ := decodeFetchPartitionResponse(t, resp)
	if errorCode != ErrUnknownTopicOrPartition {
		t.Errorf("error_code = %d, want %d", errorCode, ErrUnknownTopicOrPartition)
	}
}

func TestHandleFetch_RespectsMaxBytes(t *testing.T) {
	log := storage.NewFakeLog()
	log.Append("orders", 0, []byte("first"), 1)
	log.Append("orders", 0, []byte("second"), 1)

	body := encodeFetchRequest(100, 0, "orders", 0, 0, 5) // only room for "first"

	resp, err := HandleFetch(1, body, log)
	if err != nil {
		t.Fatalf("HandleFetch: %v", err)
	}
	data, _, _ := decodeFetchPartitionResponse(t, resp)
	if string(data) != "first" {
		t.Errorf("data = %q, want %q", data, "first")
	}
}
