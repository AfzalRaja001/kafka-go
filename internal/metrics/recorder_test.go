package metrics

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

// gather scrapes r's own registry and returns every metric family, keyed by
// name - the same mechanism promhttp.Handler uses to serve /metrics, just
// called directly instead of over HTTP, so these tests exercise the real
// prometheus internals rather than a hand-rolled substitute.
func gather(t *testing.T, r *Recorder) map[string]*dto.MetricFamily {
	t.Helper()
	families, err := r.registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	out := make(map[string]*dto.MetricFamily)
	for _, f := range families {
		out[f.GetName()] = f
	}
	return out
}

func counterValue(t *testing.T, family *dto.MetricFamily, labels map[string]string) float64 {
	t.Helper()
	for _, m := range family.GetMetric() {
		if labelsMatch(m.GetLabel(), labels) {
			return m.GetCounter().GetValue()
		}
	}
	t.Fatalf("no metric in family %s matching labels %v", family.GetName(), labels)
	return 0
}

func labelsMatch(pairs []*dto.LabelPair, want map[string]string) bool {
	if len(pairs) != len(want) {
		return false
	}
	for _, p := range pairs {
		if want[p.GetName()] != p.GetValue() {
			return false
		}
	}
	return true
}

func TestRecordRequest_IncrementsRequestsTotalLabeledByApiKeyAndResult(t *testing.T) {
	r := NewRecorder()

	r.RecordRequest("Produce", 5*time.Millisecond, 100, 50, nil)
	r.RecordRequest("Produce", 5*time.Millisecond, 100, 50, nil)
	r.RecordRequest("Produce", 5*time.Millisecond, 100, 0, errors.New("boom"))
	r.RecordRequest("Fetch", 5*time.Millisecond, 20, 200, nil)

	families := gather(t, r)
	requestsTotal, ok := families["kafkago_requests_total"]
	if !ok {
		t.Fatal("kafkago_requests_total not found")
	}

	if got := counterValue(t, requestsTotal, map[string]string{"api_key": "Produce", "result": "success"}); got != 2 {
		t.Errorf("Produce/success = %v, want 2", got)
	}
	if got := counterValue(t, requestsTotal, map[string]string{"api_key": "Produce", "result": "error"}); got != 1 {
		t.Errorf("Produce/error = %v, want 1", got)
	}
	if got := counterValue(t, requestsTotal, map[string]string{"api_key": "Fetch", "result": "success"}); got != 1 {
		t.Errorf("Fetch/success = %v, want 1", got)
	}
}

func TestRecordRequest_IncrementsBytesTotalByDirection(t *testing.T) {
	r := NewRecorder()

	r.RecordRequest("Produce", time.Millisecond, 100, 50, nil)
	r.RecordRequest("Fetch", time.Millisecond, 20, 200, nil)

	families := gather(t, r)
	bytesTotal, ok := families["kafkago_request_bytes_total"]
	if !ok {
		t.Fatal("kafkago_request_bytes_total not found")
	}

	if got := counterValue(t, bytesTotal, map[string]string{"direction": "in"}); got != 120 {
		t.Errorf("bytes in = %v, want 120 (100+20)", got)
	}
	if got := counterValue(t, bytesTotal, map[string]string{"direction": "out"}); got != 250 {
		t.Errorf("bytes out = %v, want 250 (50+200)", got)
	}
}

// TestRecordRequest_ErrorResponseStillCountsRequestBytesNotResponseBytes
// pins down what "bytes out" means on a failed request: the caller passes
// responseBytes=0 for an error (there's no real response body to count),
// and RecordRequest must not silently substitute something else - a
// dispatch() error path never writes a Kafka response at all, so 0 is the
// honest number, not a bug to work around.
func TestRecordRequest_ErrorResponseStillCountsRequestBytesNotResponseBytes(t *testing.T) {
	r := NewRecorder()
	r.RecordRequest("Produce", time.Millisecond, 42, 0, errors.New("boom"))

	families := gather(t, r)
	bytesTotal := families["kafkago_request_bytes_total"]

	if got := counterValue(t, bytesTotal, map[string]string{"direction": "in"}); got != 42 {
		t.Errorf("bytes in = %v, want 42", got)
	}
	if got := counterValue(t, bytesTotal, map[string]string{"direction": "out"}); got != 0 {
		t.Errorf("bytes out = %v, want 0", got)
	}
}

func TestRecordRequest_ObservesDurationInHistogram(t *testing.T) {
	r := NewRecorder()
	r.RecordRequest("Produce", 25*time.Millisecond, 10, 10, nil)

	families := gather(t, r)
	duration, ok := families["kafkago_request_duration_seconds"]
	if !ok {
		t.Fatal("kafkago_request_duration_seconds not found")
	}

	var found bool
	for _, m := range duration.GetMetric() {
		if !labelsMatch(m.GetLabel(), map[string]string{"api_key": "Produce"}) {
			continue
		}
		found = true
		h := m.GetHistogram()
		if h.GetSampleCount() != 1 {
			t.Errorf("sample count = %d, want 1", h.GetSampleCount())
		}
		// 25ms observed - loose bounds, this isn't testing prometheus's own
		// bucketing math, just that a real (not zero, not wildly wrong)
		// duration was recorded.
		if h.GetSampleSum() < 0.02 || h.GetSampleSum() > 0.03 {
			t.Errorf("sample sum = %v seconds, want ~0.025", h.GetSampleSum())
		}
	}
	if !found {
		t.Fatal("no histogram sample found for api_key=Produce")
	}
}

func TestNewRecorder_HandlerServesTextExpositionFormat(t *testing.T) {
	r := NewRecorder()
	r.RecordRequest("Produce", time.Millisecond, 10, 10, nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	r.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if !strings.Contains(rec.Body.String(), "kafkago_requests_total") {
		t.Fatalf("response body missing kafkago_requests_total:\n%s", rec.Body.String())
	}
}
