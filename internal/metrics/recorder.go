// Package metrics exposes request-path Prometheus metrics for the broker:
// how many requests came in per API, how long each took, and how many
// bytes moved in and out. It knows nothing about the Kafka wire protocol
// itself - RecordRequest takes an already-resolved label string, not an
// api_key, so this package stays a generic recorder rather than depending
// on internal/protocol.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Recorder holds every metric this broker currently exports, backed by its
// own prometheus.Registry rather than the package-level global one -
// nothing here is a package-level var, matching how every other stateful
// piece of this broker (storage.Log, group.Coordinator, group.OffsetStore)
// is constructed once and threaded through explicitly instead of living as
// global state. It also means multiple Recorders can coexist in the same
// test binary without a "duplicate metrics collector registration
// attempted" panic.
type Recorder struct {
	registry        *prometheus.Registry
	requestsTotal   *prometheus.CounterVec
	requestDuration *prometheus.HistogramVec
	bytesTotal      *prometheus.CounterVec
	partitionBytes  *prometheus.GaugeVec
	consumerLag     *prometheus.GaugeVec
}

func NewRecorder() *Recorder {
	requestsTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kafkago_requests_total",
		Help: "Total requests handled, labeled by API and outcome.",
	}, []string{"api_key", "result"})

	requestDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kafkago_request_duration_seconds",
		Help:    "Request handling latency, labeled by API.",
		Buckets: prometheus.DefBuckets,
	}, []string{"api_key"})

	bytesTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kafkago_request_bytes_total",
		Help: "Total bytes moved across all requests, labeled by direction (in/out).",
	}, []string{"direction"})

	partitionBytes := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafkago_partition_bytes",
		Help: "On-disk log bytes per topic-partition.",
	}, []string{"topic", "partition"})

	consumerLag := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafkago_consumer_group_lag",
		Help: "How many offsets behind the partition's latest a group's committed offset is.",
	}, []string{"group", "topic", "partition"})

	registry := prometheus.NewRegistry()
	registry.MustRegister(requestsTotal, requestDuration, bytesTotal, partitionBytes, consumerLag)

	return &Recorder{
		registry:        registry,
		requestsTotal:   requestsTotal,
		requestDuration: requestDuration,
		bytesTotal:      bytesTotal,
		partitionBytes:  partitionBytes,
		consumerLag:     consumerLag,
	}
}

// RecordRequest records the outcome of one request. apiKey is a readable
// label (e.g. "Produce"), not the raw wire api_key - the caller resolves
// that (see protocol.ApiKeyName) before calling in, since this package has
// no notion of the Kafka protocol's api_key scheme.
//
// requestBytes is always the raw request size; responseBytes is 0 on a
// failed request, since dispatch never wrote a real response in that case -
// that's the honest number, not something to substitute around.
func (r *Recorder) RecordRequest(apiKey string, duration time.Duration, requestBytes, responseBytes int, err error) {
	result := "success"
	if err != nil {
		result = "error"
	}

	r.requestsTotal.WithLabelValues(apiKey, result).Inc()
	r.requestDuration.WithLabelValues(apiKey).Observe(duration.Seconds())
	r.bytesTotal.WithLabelValues("in").Add(float64(requestBytes))
	r.bytesTotal.WithLabelValues("out").Add(float64(responseBytes))
}

// SetPartitionBytes and SetConsumerGroupLag are Gauges, not Counters -
// unlike a request count, both values can legitimately go down (a
// compacted or retained partition shrinks; a group catching up reduces its
// own lag), so each call replaces the previous value rather than adding to
// it. Both take plain labels and a number, the same as RecordRequest - this
// package still has no notion of what a "partition" or a "consumer group"
// actually is; the periodic walk that computes these numbers lives outside
// internal/metrics entirely (see collectMetrics in cmd/broker).
func (r *Recorder) SetPartitionBytes(topic string, partition int32, bytes int64) {
	r.partitionBytes.WithLabelValues(topic, strconv.Itoa(int(partition))).Set(float64(bytes))
}

func (r *Recorder) SetConsumerGroupLag(group, topic string, partition int32, lag int64) {
	r.consumerLag.WithLabelValues(group, topic, strconv.Itoa(int(partition))).Set(float64(lag))
}

// Handler serves this Recorder's metrics in Prometheus text-exposition
// format - what a Prometheus server's scrape config points at.
func (r *Recorder) Handler() http.Handler {
	return promhttp.HandlerFor(r.registry, promhttp.HandlerOpts{})
}
