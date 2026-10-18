package protocol

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/AfzalRaja001/kafka-go/internal/storage"
)

func topicsPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "data", "topics.json")
}

func openRegistry(t *testing.T, path string, nodeID int32) *TopicRegistry {
	t.Helper()
	r, err := OpenTopicRegistry(path, nodeID)
	if err != nil {
		t.Fatalf("OpenTopicRegistry: %v", err)
	}
	return r
}

func mustAdd(t *testing.T, r *TopicRegistry, name string, partitions int32) {
	t.Helper()
	if err := r.AddTopic(r.NewTopic(name, partitions)); err != nil {
		t.Fatalf("AddTopic(%q): %v", name, err)
	}
}

func partitionIDs(topic *Topic) []int32 {
	ids := make([]int32, len(topic.Partitions))
	for i, p := range topic.Partitions {
		ids[i] = p.ID
	}
	return ids
}

// TestTopicRegistry_TopicsSurviveReopen is bug 5 itself: before this, the
// registry was only a map in memory, so a restart emptied it while the
// partition directories stayed on disk.
func TestTopicRegistry_TopicsSurviveReopen(t *testing.T) {
	path := topicsPath(t)
	r := openRegistry(t, path, 1)
	mustAdd(t, r, "orders", 3)
	mustAdd(t, r, "payments", 1)

	reopened := openRegistry(t, path, 1)

	orders, ok := reopened.Get("orders")
	if !ok {
		t.Fatal("orders missing after reopen")
	}
	if ids := partitionIDs(orders); len(ids) != 3 || ids[0] != 0 || ids[1] != 1 || ids[2] != 2 {
		t.Errorf("orders partitions = %v, want [0 1 2]", ids)
	}
	payments, ok := reopened.Get("payments")
	if !ok || len(payments.Partitions) != 1 {
		t.Errorf("payments after reopen = %v (ok=%v), want 1 partition", payments, ok)
	}
	if got := len(reopened.All()); got != 2 {
		t.Errorf("All() has %d topics after reopen, want 2", got)
	}
}

func TestTopicRegistry_RemovalSurvivesReopen(t *testing.T) {
	path := topicsPath(t)
	r := openRegistry(t, path, 1)
	mustAdd(t, r, "orders", 3)
	mustAdd(t, r, "payments", 1)

	if err := r.RemoveTopic("orders"); err != nil {
		t.Fatalf("RemoveTopic: %v", err)
	}

	reopened := openRegistry(t, path, 1)
	if _, ok := reopened.Get("orders"); ok {
		t.Error("orders still present after being removed and reopened")
	}
	if _, ok := reopened.Get("payments"); !ok {
		t.Error("payments lost after removing a different topic")
	}
}

func TestTopicRegistry_RemovingUnknownTopicIsNotAnError(t *testing.T) {
	r := openRegistry(t, topicsPath(t), 1)
	if err := r.RemoveTopic("never-existed"); err != nil {
		t.Fatalf("RemoveTopic of an unknown topic: %v", err)
	}
}

// The file stores only what cannot be derived: a name and a partition
// count. Leader, replicas and ISR come from the node ID in the broker's
// configuration, so they can never go stale inside the file.
func TestTopicRegistry_FileHoldsOnlyNameAndPartitionCountSortedByName(t *testing.T) {
	path := topicsPath(t)
	r := openRegistry(t, path, 7)
	mustAdd(t, r, "zeta", 2)
	mustAdd(t, r, "alpha", 5)

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read topics file: %v", err)
	}
	var got struct {
		Version int `json:"version"`
		Topics  []struct {
			Name       string `json:"name"`
			Partitions int    `json:"partitions"`
		} `json:"topics"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("topics file is not valid JSON: %v\n%s", err, raw)
	}
	if got.Version != 1 || len(got.Topics) != 2 ||
		got.Topics[0].Name != "alpha" || got.Topics[0].Partitions != 5 ||
		got.Topics[1].Name != "zeta" || got.Topics[1].Partitions != 2 {
		t.Fatalf("file = %+v, want version 1 with alpha(5) then zeta(2)\n%s", got, raw)
	}
	for _, derived := range []string{"leader", "replicas", "isr"} {
		if strings.Contains(string(raw), "\""+derived+"\"") {
			t.Errorf("file contains a %q key, which should be derived from configuration, not stored:\n%s", derived, raw)
		}
	}
}

func TestOpenTopicRegistry_MissingFileStartsEmptyAndCreatesNothing(t *testing.T) {
	path := topicsPath(t)
	r := openRegistry(t, path, 1)

	if got := len(r.All()); got != 0 {
		t.Fatalf("a fresh registry has %d topics, want 0", got)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("opening an empty registry created %s (err=%v); the file should only appear on the first write", path, err)
	}
}

// A corrupt or unrecognised file stops startup instead of silently starting
// empty - starting empty would look like total data loss to every client,
// the very failure this file exists to prevent.
func TestOpenTopicRegistry_RejectsBadFiles(t *testing.T) {
	cases := []struct {
		name    string
		content string
	}{
		{"empty file", ""},
		{"not json", "this is not json"},
		{"truncated json", `{"version":1,"topics":[{"name":"orders","parti`},
		{"unknown version", `{"version":2,"topics":[]}`},
		{"missing version", `{"topics":[]}`},
		{"empty topic name", `{"version":1,"topics":[{"name":"","partitions":1}]}`},
		{"zero partitions", `{"version":1,"topics":[{"name":"orders","partitions":0}]}`},
		{"negative partitions", `{"version":1,"topics":[{"name":"orders","partitions":-3}]}`},
		{"partitions over the cap", fmt.Sprintf(`{"version":1,"topics":[{"name":"orders","partitions":%d}]}`, MaxPartitionsPerTopic+1)},
		{"huge partition count", `{"version":1,"topics":[{"name":"orders","partitions":2147483647}]}`},
		{"duplicate topic", `{"version":1,"topics":[{"name":"a","partitions":1},{"name":"a","partitions":2}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := topicsPath(t)
			if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte(tc.content), 0644); err != nil {
				t.Fatal(err)
			}
			if _, err := OpenTopicRegistry(path, 1); err == nil {
				t.Fatalf("OpenTopicRegistry accepted %q, want an error", tc.content)
			}
		})
	}
}

func TestOpenTopicRegistry_AcceptsPartitionCountAtTheCap(t *testing.T) {
	path := topicsPath(t)
	os.MkdirAll(filepath.Dir(path), 0755)
	content := fmt.Sprintf(`{"version":1,"topics":[{"name":"big","partitions":%d}]}`, MaxPartitionsPerTopic)
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	r := openRegistry(t, path, 1)
	if topic, ok := r.Get("big"); !ok || len(topic.Partitions) != MaxPartitionsPerTopic {
		t.Fatalf("big = %v (ok=%v), want %d partitions", topic, ok, MaxPartitionsPerTopic)
	}
}

// TestTopicRegistry_NodeIDComesFromConfigurationNotTheFile covers the
// KAFKA_NODE_ID bug: partitions used to hardcode Leader: 1, so a broker
// configured as node 3 told clients the leader was a node that does not
// exist, and producing timed out.
func TestTopicRegistry_NodeIDComesFromConfigurationNotTheFile(t *testing.T) {
	path := topicsPath(t)
	r := openRegistry(t, path, 3)
	mustAdd(t, r, "orders", 2)

	check := func(label string, reg *TopicRegistry, want int32) {
		t.Helper()
		topic, ok := reg.Get("orders")
		if !ok {
			t.Fatalf("%s: orders missing", label)
		}
		for _, p := range topic.Partitions {
			if p.Leader != want || len(p.Replicas) != 1 || p.Replicas[0] != want || len(p.ISR) != 1 || p.ISR[0] != want {
				t.Errorf("%s: partition %d = leader %d replicas %v isr %v, want everything %d",
					label, p.ID, p.Leader, p.Replicas, p.ISR, want)
			}
		}
	}
	check("as created with node 3", r, 3)
	check("reloaded with node 3", openRegistry(t, path, 3), 3)
	// Reconfiguring the node ID takes effect on the next start, because the
	// file never recorded it.
	check("reloaded after reconfiguring to node 5", openRegistry(t, path, 5), 5)
}

func TestNewTopicRegistry_InMemoryDefaultsToNodeOneAndWritesNothing(t *testing.T) {
	r := NewTopicRegistry()
	topic := r.NewTopic("orders", 1)
	if topic.Partitions[0].Leader != 1 {
		t.Errorf("default node ID leader = %d, want 1", topic.Partitions[0].Leader)
	}
	if err := r.AddTopic(topic); err != nil {
		t.Fatalf("AddTopic on an in-memory registry: %v", err)
	}
	if err := r.RemoveTopic("orders"); err != nil {
		t.Fatalf("RemoveTopic on an in-memory registry: %v", err)
	}
}

func TestTopicRegistry_WriteLeavesNoTempFileBehind(t *testing.T) {
	path := topicsPath(t)
	r := openRegistry(t, path, 1)
	for i := 0; i < 5; i++ {
		mustAdd(t, r, fmt.Sprintf("t%d", i), 1)
	}
	r.RemoveTopic("t0")

	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if len(names) != 1 || names[0] != "topics.json" {
		t.Fatalf("data dir contains %v, want only topics.json", names)
	}
}

// If the write fails, memory must not run ahead of disk: the caller was
// told the create failed, so a topic that is visible in Metadata but would
// vanish on the next restart is the worst outcome.
func TestTopicRegistry_FailedWriteRollsMemoryBack(t *testing.T) {
	path := topicsPath(t)
	r := openRegistry(t, path, 1)
	mustAdd(t, r, "kept", 1)

	// Replace the data directory with a regular file, so every later write fails.
	dir := filepath.Dir(path)
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir, []byte("not a directory"), 0644); err != nil {
		t.Fatal(err)
	}

	if err := r.AddTopic(r.NewTopic("doomed", 1)); err == nil {
		t.Fatal("AddTopic succeeded although the topics file cannot be written")
	}
	if _, ok := r.Get("doomed"); ok {
		t.Error("a topic whose write failed is still visible in memory")
	}
	if err := r.RemoveTopic("kept"); err == nil {
		t.Fatal("RemoveTopic succeeded although the topics file cannot be written")
	}
	if _, ok := r.Get("kept"); !ok {
		t.Error("a topic whose removal failed to persist was removed from memory anyway")
	}
}

func TestTopicRegistry_ConcurrentAddsAllSurvive(t *testing.T) {
	path := topicsPath(t)
	r := openRegistry(t, path, 1)

	const n = 40
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := r.AddTopic(r.NewTopic(fmt.Sprintf("topic-%02d", i), int32(1+i%3))); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent AddTopic: %v", err)
	}

	reopened := openRegistry(t, path, 1)
	var names []string
	for _, topic := range reopened.All() {
		names = append(names, topic.Name)
	}
	sort.Strings(names)
	if len(names) != n {
		t.Fatalf("%d topics survived %d concurrent adds: %v", len(names), n, names)
	}
}

func TestHandleCreateTopics_PersistsAcrossRegistryReopen(t *testing.T) {
	path := topicsPath(t)
	registry := openRegistry(t, path, 1)

	resp, err := HandleCreateTopics(1, encodeCreateTopicsRequest("orders", 3, 1), registry, storage.NewFakeLog())
	if err != nil {
		t.Fatalf("HandleCreateTopics: %v", err)
	}
	if _, code := decodeCreateTopicsResponseOneTopic(t, resp); code != ErrNone {
		t.Fatalf("error_code = %d, want %d", code, ErrNone)
	}

	topic, ok := openRegistry(t, path, 1).Get("orders")
	if !ok || len(topic.Partitions) != 3 {
		t.Fatalf("after reopen orders = %v (ok=%v), want 3 partitions", topic, ok)
	}
}

func TestHandleCreateTopics_UsesTheConfiguredNodeIDAsLeader(t *testing.T) {
	registry := openRegistry(t, topicsPath(t), 3)
	if _, err := HandleCreateTopics(1, encodeCreateTopicsRequest("orders", 2, 1), registry, storage.NewFakeLog()); err != nil {
		t.Fatalf("HandleCreateTopics: %v", err)
	}

	brokers := []Broker{{NodeID: 3, Host: "localhost", Port: 9092}}
	resp, err := HandleMetadata(1, encodeTopicsRequest([]string{"orders"}), registry, brokers)
	if err != nil {
		t.Fatalf("HandleMetadata: %v", err)
	}
	dec := NewDecoder(resp)
	dec.Int32() // correlation_id
	dec.Int32() // broker count
	advertised, _ := dec.Int32()
	dec.String()
	dec.Int32()
	dec.NullableString()
	dec.Int32() // controller_id
	dec.Int32() // topic count
	dec.Int16()
	dec.String()
	dec.Bool()
	partCount, _ := dec.Int32()
	for i := int32(0); i < partCount; i++ {
		dec.Int16()
		dec.Int32() // partition id
		leader, _ := dec.Int32()
		if leader != advertised {
			t.Errorf("partition %d leader = %d, but the broker advertises node %d: clients cannot find the leader", i, leader, advertised)
		}
		replicas, _ := dec.Int32()
		for j := int32(0); j < replicas; j++ {
			dec.Int32()
		}
		isr, _ := dec.Int32()
		for j := int32(0); j < isr; j++ {
			dec.Int32()
		}
	}
}

func TestHandleCreateTopics_ReportsAServerErrorWhenThePersistFails(t *testing.T) {
	path := topicsPath(t)
	registry := openRegistry(t, path, 1)
	dir := filepath.Dir(path)
	os.RemoveAll(dir)
	os.WriteFile(dir, []byte("not a directory"), 0644)

	resp, err := HandleCreateTopics(1, encodeCreateTopicsRequest("orders", 1, 1), registry, storage.NewFakeLog())
	if err != nil {
		t.Fatalf("HandleCreateTopics: %v", err)
	}
	if _, code := decodeCreateTopicsResponseOneTopic(t, resp); code != ErrUnknownServerError {
		t.Errorf("error_code = %d, want %d: a create that could not be saved must not be reported as a success", code, ErrUnknownServerError)
	}
	if _, ok := registry.Get("orders"); ok {
		t.Error("orders is registered although saving it failed")
	}
}

func TestHandleDeleteTopics_PersistsAcrossRegistryReopen(t *testing.T) {
	path := topicsPath(t)
	registry := openRegistry(t, path, 1)
	log := storage.NewFakeLog()
	if _, err := HandleCreateTopics(1, encodeCreateTopicsRequest("orders", 2, 1), registry, log); err != nil {
		t.Fatal(err)
	}

	resp, err := HandleDeleteTopics(1, encodeDeleteTopicsRequest([]string{"orders"}), registry, log)
	if err != nil {
		t.Fatalf("HandleDeleteTopics: %v", err)
	}
	if _, code := decodeDeleteTopicsResponseOneTopic(t, resp); code != ErrNone {
		t.Fatalf("error_code = %d, want %d", code, ErrNone)
	}

	if _, ok := openRegistry(t, path, 1).Get("orders"); ok {
		t.Error("orders came back after a restart although it was deleted")
	}
}
