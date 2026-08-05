// Package httpapi exposes the ingest and query surface over HTTP.
package httpapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/Ohimoiza1205/pulse/internal/ingest"
	"github.com/Ohimoiza1205/pulse/internal/stream"
	"github.com/Ohimoiza1205/pulse/internal/telemetry"
)

// maxBodyBytes caps an ingest request. Devices batch, they don't upload files.
const maxBodyBytes = 1 << 20

// Server wires the HTTP handlers to the pipeline, store, and stream hub.
type Server struct {
	store *telemetry.Store
	pipe  *ingest.Pipeline
	hub   *stream.Hub
	log   *slog.Logger
	now   func() time.Time
}

// New returns a Server. Pass nil for log to discard output.
func New(store *telemetry.Store, pipe *ingest.Pipeline, hub *stream.Hub, log *slog.Logger) *Server {
	if log == nil {
		log = slog.New(slog.NewTextHandler(discard{}, nil))
	}
	return &Server{store: store, pipe: pipe, hub: hub, log: log, now: time.Now}
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

// Routes builds the mux. Method and wildcard patterns come from the standard
// library router in Go 1.22, so there is no third party router to audit.
func (s *Server) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/ingest", s.handleIngest)
	mux.HandleFunc("GET /v1/devices", s.handleDevices)
	mux.HandleFunc("GET /v1/devices/{id}/latest", s.handleLatest)
	mux.HandleFunc("GET /v1/devices/{id}/series", s.handleSeries)
	mux.HandleFunc("GET /v1/stream", s.handleStream)
	mux.HandleFunc("GET /v1/stats", s.handleStats)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return mux
}

type ingestResult struct {
	Accepted int    `json:"accepted"`
	Invalid  int    `json:"invalid"`
	Shed     int    `json:"shed"`
	Detail   string `json:"detail,omitempty"`
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)

	readings, err := decodeReadings(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, ingestResult{Detail: err.Error()})
		return
	}

	now := s.now()
	var res ingestResult
	for i := range readings {
		if err := readings[i].Normalize(now); err != nil {
			res.Invalid++
			continue
		}
		if s.pipe.Submit(readings[i]) {
			res.Accepted++
		} else {
			res.Shed++
		}
	}

	// Full shed means the queue is saturated. Say so with 429 and a Retry-After
	// so well behaved clients back off instead of hammering.
	if res.Accepted == 0 && res.Shed > 0 {
		res.Detail = "ingest queue saturated, retry shortly"
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusTooManyRequests, res)
		return
	}
	writeJSON(w, http.StatusAccepted, res)
}

// decodeReadings accepts either a bare array of readings or an object with a
// readings field, because both shapes show up in the wild.
func decodeReadings(r *http.Request) ([]telemetry.Reading, error) {
	var raw json.RawMessage
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&raw); err != nil {
		return nil, errors.New("body must be valid JSON")
	}

	var batch []telemetry.Reading
	if err := json.Unmarshal(raw, &batch); err == nil {
		return batch, nil
	}

	var wrapper struct {
		Readings []telemetry.Reading `json:"readings"`
	}
	if err := json.Unmarshal(raw, &wrapper); err == nil && wrapper.Readings != nil {
		return wrapper.Readings, nil
	}

	var single telemetry.Reading
	if err := json.Unmarshal(raw, &single); err == nil {
		return []telemetry.Reading{single}, nil
	}
	return nil, errors.New("expected a reading, an array of readings, or {\"readings\":[...]}")
}

func (s *Server) handleDevices(w http.ResponseWriter, r *http.Request) {
	devices := s.store.Devices()
	writeJSON(w, http.StatusOK, map[string]any{
		"count":   len(devices),
		"devices": devices,
	})
}

func (s *Server) handleLatest(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	reading, ok := s.store.Latest(id)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown device"})
		return
	}
	writeJSON(w, http.StatusOK, reading)
}

func (s *Server) handleSeries(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	var since time.Time
	if win := r.URL.Query().Get("window"); win != "" {
		d, err := time.ParseDuration(win)
		if err != nil || d <= 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "window must be a positive duration, for example 5m"})
			return
		}
		since = s.now().Add(-d)
	}

	limit := 0
	if l := r.URL.Query().Get("limit"); l != "" {
		n, err := strconv.Atoi(l)
		if err != nil || n < 1 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "limit must be a positive integer"})
			return
		}
		limit = n
	}

	series := s.store.Series(id, since, limit)
	writeJSON(w, http.StatusOK, map[string]any{
		"device_id": id,
		"count":     len(series),
		"readings":  series,
	})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	accepted, shed, processed := s.pipe.Stats()
	delivered, dropped := s.hub.Stats()

	writeJSON(w, http.StatusOK, map[string]any{
		"devices": s.store.DeviceCount(),
		"ingest": map[string]any{
			"accepted":       accepted,
			"shed":           shed,
			"processed":      processed,
			"queue_depth":    s.pipe.Depth(),
			"queue_capacity": s.pipe.Capacity(),
		},
		"stream": map[string]any{
			"subscribers": s.hub.SubscriberCount(),
			"delivered":   delivered,
			"dropped":     dropped,
		},
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
