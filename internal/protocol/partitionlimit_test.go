package protocol

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/AfzalRaja001/kafka-go/internal/storage"
)

// createCode sends one CreateTopics request through the real handler and
// returns the per-topic error code.
func createCode(t *testing.T, registry *TopicRegistry, fake storage.Log, name string, partitions int32) int16 {
	t.Helper()
	resp, err := HandleCreateTopics(1, encodeCreateTopicsRequest(name, partitions, 1), registry, fake)
	if err != nil {
		t.Fatalf("HandleCreateTopics(%q): %v", name, err)
	}
	_, code := decodeCreateTopicsResponseOneTopic(t, resp)
	return code
}

func TestHandleCreateTopics_TotalPartitionCapRejectsTheTopicThatWouldExceedIt(t *testing.T) {
	registry := NewTopicRegistry()
	registry.SetMaxPartitions(5)
	fake := storage.NewFakeLog()

	if code := createCode(t, registry, fake, "a", 3); code != ErrNone {
		t.Fatalf("a(3) = %d, want %d", code, ErrNone)
	}
	// 3 + 3 = 6 > 5: must be refused whole, not created partially.
	if code := createCode(t, registry, fake, "b", 3); code != ErrInvalidPartitions {
		t.Fatalf("b(3) = %d, want %d (INVALID_PARTITIONS)", code, ErrInvalidPartitions)
	}
	if _, ok := registry.Get("b"); ok {
		t.Error("b is registered despite being refused")
	}
	for p := int32(0); p < 3; p++ {
		if _, err := fake.LatestOffset("b", p); err == nil {
			t.Errorf("b-%d exists in storage: a refused create left storage behind", p)
		}
	}
	// Exactly reaching the cap is allowed: 3 + 2 = 5.
	if code := createCode(t, registry, fake, "c", 2); code != ErrNone {
		t.Fatalf("c(2) = %d, want %d (exactly at the cap)", code, ErrNone)
	}
	if code := createCode(t, registry, fake, "d", 1); code != ErrInvalidPartitions {
		t.Fatalf("d(1) = %d, want %d (one over the cap)", code, ErrInvalidPartitions)
	}
}

func TestTopicRegistry_DeletingATopicFreesItsPartitionsUnderTheCap(t *testing.T) {
	registry := NewTopicRegistry()
	registry.SetMaxPartitions(4)
	fake := storage.NewFakeLog()

	if code := createCode(t, registry, fake, "a", 4); code != ErrNone {
		t.Fatalf("a(4) = %d", code)
	}
	if code := createCode(t, registry, fake, "b", 1); code != ErrInvalidPartitions {
		t.Fatalf("b(1) = %d, want refused while a holds all 4", code)
	}
	if err := registry.RemoveTopic("a"); err != nil {
		t.Fatal(err)
	}
	if code := createCode(t, registry, fake, "b", 4); code != ErrNone {
		t.Fatalf("b(4) after deleting a = %d, want %d", code, ErrNone)
	}
}

func TestTopicRegistry_NoCapByDefaultOrWhenZeroOrNegative(t *testing.T) {
	for _, limit := range []int32{0, -1, -2147483648} {
		registry := NewTopicRegistry()
		registry.SetMaxPartitions(limit)
		fake := storage.NewFakeLog()
		for i := 0; i < 5; i++ {
			if code := createCode(t, registry, fake, fmt.Sprintf("t%d", i), 400); code != ErrNone {
				t.Fatalf("limit %d: topic %d = %d, want %d (no cap)", limit, i, code, ErrNone)
			}
		}
	}
	registry := NewTopicRegistry() // never configured
	if code := createCode(t, registry, storage.NewFakeLog(), "x", 1000); code != ErrNone {
		t.Fatalf("default registry refused 1000 partitions: %d", code)
	}
}

// AddTopic is the authority on the cap (the handler's early check is only a
// fast path), so it must enforce it on its own and leave no trace when it
// refuses.
func TestTopicRegistry_AddTopicEnforcesTheCapAndChangesNothingWhenRefusing(t *testing.T) {
	path := topicsPath(t)
	registry := openRegistry(t, path, 1)
	registry.SetMaxPartitions(3)
	mustAdd(t, registry, "a", 3)

	err := registry.AddTopic(registry.NewTopic("b", 1))
	if !errors.Is(err, ErrTooManyPartitions) {
		t.Fatalf("AddTopic over the cap = %v, want ErrTooManyPartitions", err)
	}
	if _, ok := registry.Get("b"); ok {
		t.Error("b is in memory after a refused AddTopic")
	}
	reopened := openRegistry(t, path, 1)
	if _, ok := reopened.Get("b"); ok {
		t.Error("b was saved to the topics file by a refused AddTopic")
	}
	if len(reopened.All()) != 1 {
		t.Errorf("reopened registry has %d topics, want 1", len(reopened.All()))
	}
}

// Replacing a topic of the same name swaps its partitions for the new
// ones; only the difference counts against the cap.
func TestTopicRegistry_ReplacingATopicCountsOnlyTheDifference(t *testing.T) {
	registry := NewTopicRegistry()
	registry.SetMaxPartitions(4)
	mustAdd(t, registry, "a", 4)
	if err := registry.AddTopic(registry.NewTopic("a", 4)); err != nil {
		t.Fatalf("replacing a(4) with a(4) under a cap of 4: %v", err)
	}
	if err := registry.AddTopic(registry.NewTopic("a", 5)); !errors.Is(err, ErrTooManyPartitions) {
		t.Fatalf("replacing a(4) with a(5) under a cap of 4 = %v, want ErrTooManyPartitions", err)
	}
}

// A topics file written under a higher (or no) cap must still load: the cap
// only limits new topics, never disowns existing ones.
func TestOpenTopicRegistry_CapDoesNotDisownExistingTopics(t *testing.T) {
	path := topicsPath(t)
	first := openRegistry(t, path, 1)
	mustAdd(t, first, "a", 10)

	second := openRegistry(t, path, 1)
	second.SetMaxPartitions(5)
	if _, ok := second.Get("a"); !ok {
		t.Fatal("existing topic a vanished after lowering the cap")
	}
	if code := createCode(t, second, storage.NewFakeLog(), "b", 1); code != ErrInvalidPartitions {
		t.Fatalf("b(1) = %d, want refused: 10 existing partitions already exceed the cap of 5", code)
	}
}

// Many clients creating topics at once must not be able to slip past the
// cap between a check and the write, and the refused ones must not leave
// partitions behind in storage.
func TestHandleCreateTopics_ConcurrentCreatesRespectTheCapExactly(t *testing.T) {
	const limit, clients = 5, 40
	registry := NewTopicRegistry()
	registry.SetMaxPartitions(limit)
	fake := storage.NewFakeLog()

	var wg sync.WaitGroup
	results := make(chan int16, clients)
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := HandleCreateTopics(1, encodeCreateTopicsRequest(fmt.Sprintf("t%d", i), 1, 1), registry, fake)
			if err != nil {
				t.Errorf("HandleCreateTopics: %v", err)
				return
			}
			_, code := decodeCreateTopicsResponseOneTopic(t, resp)
			results <- code
		}(i)
	}
	wg.Wait()
	close(results)

	created := 0
	for code := range results {
		switch code {
		case ErrNone:
			created++
		case ErrInvalidPartitions:
		default:
			t.Errorf("unexpected code %d", code)
		}
	}
	if created != limit {
		t.Errorf("%d creates succeeded, want exactly %d", created, limit)
	}
	if got := len(registry.All()); got != limit {
		t.Errorf("registry holds %d topics, want %d", got, limit)
	}
	inStorage := 0
	for i := 0; i < clients; i++ {
		if _, err := fake.LatestOffset(fmt.Sprintf("t%d", i), 0); err == nil {
			inStorage++
		}
	}
	if inStorage != limit {
		t.Errorf("%d partitions exist in storage, want %d: refused creates left orphans", inStorage, limit)
	}
}

// raceyLog registers other topics in the middle of createOneTopic, after its
// early check has passed and before its AddTopic runs - the window a real
// concurrent client can hit - and records which partitions get deleted.
type raceyLog struct {
	*storage.FakeLog
	onCreate func()
	deleted  []string
}

func (l *raceyLog) CreatePartition(topic string, partition int32) error {
	if l.onCreate != nil {
		hook := l.onCreate
		l.onCreate = nil
		hook()
	}
	return l.FakeLog.CreatePartition(topic, partition)
}

func (l *raceyLog) DeletePartition(topic string, partition int32) error {
	l.deleted = append(l.deleted, fmt.Sprintf("%s-%d", topic, partition))
	return l.FakeLog.DeletePartition(topic, partition)
}

// If a refused create lost a race to other creates, it cleans up the
// partitions it made. But when a topic of the same name was registered in
// the meantime, those directories now belong to that topic, and deleting
// them would destroy its data.
func TestHandleCreateTopics_RefusedRaceDoesNotDeleteAConcurrentlyRegisteredTopicsPartitions(t *testing.T) {
	registry := NewTopicRegistry()
	registry.SetMaxPartitions(4)
	log := &raceyLog{FakeLog: storage.NewFakeLog()}
	log.onCreate = func() {
		// Another client creates "y" (3 partitions) and "x" (1 partition)
		// while this request is still creating x's storage.
		for _, tp := range []struct {
			name string
			n    int32
		}{{"y", 3}, {"x", 1}} {
			for p := int32(0); p < tp.n; p++ {
				log.FakeLog.CreatePartition(tp.name, p)
			}
			if err := registry.AddTopic(registry.NewTopic(tp.name, tp.n)); err != nil {
				t.Errorf("setup AddTopic(%s): %v", tp.name, err)
			}
		}
	}

	// x(2): fits an empty registry under a cap of 4, but not once y(3) and
	// x(1) are in (3 + 2 = 5).
	if code := createCode(t, registry, log, "x", 2); code != ErrInvalidPartitions {
		t.Fatalf("x(2) = %d, want %d", code, ErrInvalidPartitions)
	}
	if len(log.deleted) != 0 {
		t.Errorf("DeletePartition called for %v: the refused create destroyed storage that now belongs to the registered topic x", log.deleted)
	}
	if _, err := log.FakeLog.LatestOffset("x", 0); err != nil {
		t.Errorf("x-0 is gone from storage: %v", err)
	}
}

// And the ordinary losing side of the race, with no same-named topic, still
// cleans up after itself.
func TestHandleCreateTopics_RefusedRaceCleansUpItsOwnPartitions(t *testing.T) {
	registry := NewTopicRegistry()
	registry.SetMaxPartitions(4)
	log := &raceyLog{FakeLog: storage.NewFakeLog()}
	log.onCreate = func() {
		log.FakeLog.CreatePartition("y", 0)
		if err := registry.AddTopic(registry.NewTopic("y", 3)); err != nil {
			t.Errorf("setup AddTopic: %v", err)
		}
	}

	if code := createCode(t, registry, log, "x", 2); code != ErrInvalidPartitions {
		t.Fatalf("x(2) = %d, want %d", code, ErrInvalidPartitions)
	}
	for p := int32(0); p < 2; p++ {
		if _, err := log.FakeLog.LatestOffset("x", p); err == nil {
			t.Errorf("x-%d still exists in storage after the refused create", p)
		}
	}
}
