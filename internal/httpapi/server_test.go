package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Ohimoiza1205/pulse/internal/ingest"
	"github.com/Ohimoiza1205/pulse/internal/stream"
	"github.com/Ohimoiza1205/pulse/internal/telemetry"
)

type harness struct {
	srv   *Server
	store *telemetry.Store
	pipe  *ingest.Pipeline
	hub   *stream.Hub
	mux   *http.ServeMux
}

func newHarness(t *testing.T, queue, workers int) *harness {
	t.Helper()
	store := telemetry.NewStore(100)
	hub := stream.NewHub()
	pipe := ingest.New(store, hub, queue, workers)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if workers > 0 {
		pipe.Start(ctx)
	}

	s := New(store, pipe, hub, nil)
	return &harness{srv: s, store: store, pipe: pipe, hub: hub, mux: s.Routes()}
}

func (h *harness) do(t *testing.T, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.mux.ServeHTTP(rec, req)
	return rec
}

// drain waits for the pipeline to catch up so query assertions aren't racy.
func (h *harness) drain(t *testing.T, want uint64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, processed := h.pipe.Stats(); processed >= want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("pipeline did not process %d readings in time", want)
}

func TestIngestAcceptsBatchArray(t *testing.T) {
	h := newHarness(t, 64, 2)

	body := `[{"device_id":"dev-1","metric":"engine_temp_c","value":91.4},
	          {"device_id":"dev-2","metric":"engine_temp_c","value":78.1}]`
	rec := h.do(t, http.MethodPost, "/v1/ingest", body)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rec.Code)
	}
	var res ingestResult
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if res.Accepted != 2 || res.Invalid != 0 {
		t.Errorf("accepted=%d invalid=%d, want 2 and 0", res.Accepted, res.Invalid)
	}

	h.drain(t, 2)
	if got := h.store.DeviceCount(); got != 2 {
		t.Errorf("DeviceCount() = %d, want 2", got)
	}
}

func TestIngestAcceptsWrappedAndSingleShapes(t *testing.T) {
	h := newHarness(t, 64, 2)

	wrapped := `{"readings":[{"device_id":"dev-1","metric":"m","value":1}]}`
	if rec := h.do(t, http.MethodPost, "/v1/ingest", wrapped); rec.Code != http.StatusAccepted {
		t.Errorf("wrapped shape status = %d, want 202", rec.Code)
	}

	single := `{"device_id":"dev-2","metric":"m","value":2}`
	if rec := h.do(t, http.MethodPost, "/v1/ingest", single); rec.Code != http.StatusAccepted {
		t.Errorf("single shape status = %d, want 202", rec.Code)
	}
}

func TestIngestCountsInvalidReadingsWithoutFailingBatch(t *testing.T) {
	h := newHarness(t, 64, 2)

	body := `[{"device_id":"dev-1","metric":"m","value":1},
	          {"metric":"m","value":2},
	          {"device_id":"dev-3","value":3}]`
	rec := h.do(t, http.MethodPost, "/v1/ingest", body)

	var res ingestResult
	_ = json.Unmarshal(rec.Body.Bytes(), &res)
	if res.Accepted != 1 || res.Invalid != 2 {
		t.Errorf("accepted=%d invalid=%d, want 1 and 2", res.Accepted, res.Invalid)
	}
}

func TestIngestRejectsMalformedJSON(t *testing.T) {
	h := newHarness(t, 64, 2)
	rec := h.do(t, http.MethodPost, "/v1/ingest", `{"device_id":`)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

// TestIngestReturns429WhenSaturated pins the contract the load generator and
// any real client depend on.
func TestIngestReturns429WhenSaturated(t *testing.T) {
	h := newHarness(t, 1, 0) // capacity 1, no workers draining it

	first := h.do(t, http.MethodPost, "/v1/ingest", `{"device_id":"d","metric":"m","value":1}`)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first status = %d, want 202", first.Code)
	}

	second := h.do(t, http.MethodPost, "/v1/ingest", `{"device_id":"d","metric":"m","value":1}`)
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("saturated status = %d, want 429", second.Code)
	}
	if second.Header().Get("Retry-After") == "" {
		t.Error("429 response is missing Retry-After")
	}
}

func TestLatestAndSeries(t *testing.T) {
	h := newHarness(t, 64, 2)

	body := `[{"device_id":"dev-1","metric":"m","value":10},
	          {"device_id":"dev-1","metric":"m","value":20}]`
	h.do(t, http.MethodPost, "/v1/ingest", body)
	h.drain(t, 2)

	rec := h.do(t, http.MethodGet, "/v1/devices/dev-1/latest", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("latest status = %d, want 200", rec.Code)
	}
	var latest telemetry.Reading
	_ = json.Unmarshal(rec.Body.Bytes(), &latest)
	if latest.Value != 20 {
		t.Errorf("latest value = %v, want 20", latest.Value)
	}

	rec = h.do(t, http.MethodGet, "/v1/devices/dev-1/series?window=5m", "")
	var series struct {
		Count int `json:"count"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &series)
	if series.Count != 2 {
		t.Errorf("series count = %d, want 2", series.Count)
	}
}

func TestUnknownDeviceReturns404(t *testing.T) {
	h := newHarness(t, 64, 2)
	if rec := h.do(t, http.MethodGet, "/v1/devices/ghost/latest", ""); rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}

func TestSeriesRejectsBadParams(t *testing.T) {
	h := newHarness(t, 64, 2)
	for _, q := range []string{"?window=banana", "?window=-5m", "?limit=0", "?limit=abc"} {
		if rec := h.do(t, http.MethodGet, "/v1/devices/dev-1/series"+q, ""); rec.Code != http.StatusBadRequest {
			t.Errorf("%s status = %d, want 400", q, rec.Code)
		}
	}
}

func TestMethodRoutingIsEnforced(t *testing.T) {
	h := newHarness(t, 64, 2)
	if rec := h.do(t, http.MethodGet, "/v1/ingest", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET /v1/ingest = %d, want 405", rec.Code)
	}
}

func TestStatsReportsQueueAndStream(t *testing.T) {
	h := newHarness(t, 64, 2)
	h.do(t, http.MethodPost, "/v1/ingest", `{"device_id":"dev-1","metric":"m","value":1}`)
	h.drain(t, 1)

	rec := h.do(t, http.MethodGet, "/v1/stats", "")
	var stats struct {
		Devices int `json:"devices"`
		Ingest  struct {
			Accepted      uint64 `json:"accepted"`
			QueueCapacity int    `json:"queue_capacity"`
		} `json:"ingest"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &stats); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	if stats.Devices != 1 || stats.Ingest.Accepted != 1 || stats.Ingest.QueueCapacity != 64 {
		t.Errorf("stats = %+v, want 1 device, 1 accepted, capacity 64", stats)
	}
}

// TestStreamDeliversLiveReading exercises the real upgrade path over a real
// socket rather than mocking the hub.
func TestStreamDeliversLiveReading(t *testing.T) {
	h := newHarness(t, 64, 2)
	ts := httptest.NewServer(h.mux)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/v1/stream?device=dev-1"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Wait for the subscriber to register before publishing.
	deadline := time.Now().Add(time.Second)
	for h.hub.SubscriberCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}

	body := `[{"device_id":"dev-2","metric":"m","value":1},
	          {"device_id":"dev-1","metric":"engine_temp_c","value":93.5}]`
	resp, err := ts.Client().Post(ts.URL+"/v1/ingest", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	resp.Body.Close()

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	var got telemetry.Reading
	if err := conn.ReadJSON(&got); err != nil {
		t.Fatalf("read stream: %v", err)
	}
	// The dev-2 reading must not reach a dev-1 filtered subscriber.
	if got.DeviceID != "dev-1" || got.Value != 93.5 {
		t.Errorf("streamed %+v, want dev-1 at 93.5", got)
	}
}

func TestHealthz(t *testing.T) {
	h := newHarness(t, 64, 1)
	if rec := h.do(t, http.MethodGet, "/healthz", ""); rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
}
