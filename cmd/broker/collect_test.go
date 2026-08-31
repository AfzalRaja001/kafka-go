package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/AfzalRaja001/kafka-go/internal/group"
	"github.com/AfzalRaja001/kafka-go/internal/metrics"
	"github.com/AfzalRaja001/kafka-go/internal/protocol"
	"github.com/AfzalRaja001/kafka-go/internal/storage"
)

// scrapeMetrics serves recorder's own /metrics handler into a string, the
// same way a real Prometheus scrape would see it.
func scrapeMetrics(t *testing.T, recorder *metrics.Recorder) string {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	recorder.Handler().ServeHTTP(rec, req)
	return rec.Body.String()
}

func TestCollectMetrics_RecordsPartitionSizeForEveryKnownPartition(t *testing.T) {
	registry := protocol.NewTopicRegistry()
	registry.AddTopic(&protocol.Topic{
		Name:       "orders",
		Partitions: []protocol.PartitionMetadata{{ID: 0}, {ID: 1}},
	})

	log := storage.NewFakeLog()
	log.Append("orders", 0, []byte("hello"), 1)       // 5 bytes
	log.Append("orders", 1, []byte("hello world"), 1) // 11 bytes

	recorder := metrics.NewRecorder()
	collectMetrics(registry, log, group.NewInMemoryOffsetStore(), recorder)

	body := scrapeMetrics(t, recorder)
	if !strings.Contains(body, `kafkago_partition_bytes{partition="0",topic="orders"} 5`) {
		t.Errorf("missing orders partition 0 size:\n%s", body)
	}
	if !strings.Contains(body, `kafkago_partition_bytes{partition="1",topic="orders"} 11`) {
		t.Errorf("missing orders partition 1 size:\n%s", body)
	}
}

// TestCollectMetrics_SkipsPartitionRegistryKnowsAboutButLogDoesNot makes
// sure a registry/log mismatch (a partition CreateTopics registered but
// nothing has ever been Appended to) doesn't crash the collector - it's
// meant to run forever on a ticker, so one bad partition must not take the
// whole loop down.
func TestCollectMetrics_SkipsPartitionRegistryKnowsAboutButLogDoesNot(t *testing.T) {
	registry := protocol.NewTopicRegistry()
	registry.AddTopic(&protocol.Topic{
		Name:       "ghost-topic",
		Partitions: []protocol.PartitionMetadata{{ID: 0}},
	})

	recorder := metrics.NewRecorder()
	collectMetrics(registry, storage.NewFakeLog(), group.NewInMemoryOffsetStore(), recorder)

	body := scrapeMetrics(t, recorder)
	if strings.Contains(body, "ghost-topic") {
		t.Errorf("expected no metric for a partition the log has never heard of:\n%s", body)
	}
}

func TestCollectMetrics_RecordsConsumerGroupLagBehindLatestOffset(t *testing.T) {
	log := storage.NewFakeLog()
	log.Append("orders", 0, []byte("r0"), 1)
	log.Append("orders", 0, []byte("r1"), 1)
	log.Append("orders", 0, []byte("r2"), 1) // latest offset is now 3

	offsetStore := group.NewInMemoryOffsetStore()
	offsetStore.Commit("my-group", "orders", 0, 1, "") // committed only through offset 1

	recorder := metrics.NewRecorder()
	collectMetrics(protocol.NewTopicRegistry(), log, offsetStore, recorder)

	body := scrapeMetrics(t, recorder)
	if !strings.Contains(body, `kafkago_consumer_group_lag{group="my-group",partition="0",topic="orders"} 2`) {
		t.Errorf("expected lag of 2 (latest offset 3 - committed offset 1):\n%s", body)
	}
}

func TestCollectMetrics_ReportsZeroLagWhenCaughtUp(t *testing.T) {
	log := storage.NewFakeLog()
	log.Append("orders", 0, []byte("r0"), 1) // latest offset is now 1

	offsetStore := group.NewInMemoryOffsetStore()
	offsetStore.Commit("my-group", "orders", 0, 1, "") // fully caught up

	recorder := metrics.NewRecorder()
	collectMetrics(protocol.NewTopicRegistry(), log, offsetStore, recorder)

	body := scrapeMetrics(t, recorder)
	if !strings.Contains(body, `kafkago_consumer_group_lag{group="my-group",partition="0",topic="orders"} 0`) {
		t.Errorf("expected lag of 0:\n%s", body)
	}
}
