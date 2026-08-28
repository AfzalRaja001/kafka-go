// Package metrics exposes request-path Prometheus metrics for the broker:
// how many requests came in per API, how long each took, and how many
// bytes moved in and out. It knows nothing about the Kafka wire protocol
// itself - RecordRequest takes an already-resolved label string, not an
// api_key, so this package stays a generic recorder rather than depending
// on internal/protocol.
package metrics

import (
	"net/http"
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

	registry := prometheus.NewRegistry()
	registry.MustRegister(requestsTotal, requestDuration, bytesTotal)

	return &Recorder{
		registry:        registry,
		requestsTotal:   requestsTotal,
		requestDuration: requestDuration,
		bytesTotal:      bytesTotal,
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

// Handler serves this Recorder's metrics in Prometheus text-exposition
// format - what a Prometheus server's scrape config points at.
func (r *Recorder) Handler() http.Handler {
	return promhttp.HandlerFor(r.registry, promhttp.HandlerOpts{})
}
