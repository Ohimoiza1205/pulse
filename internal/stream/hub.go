// Package stream fans live readings out to connected WebSocket subscribers.
package stream

import (
	"sync"
	"sync/atomic"

	"github.com/Ohimoiza1205/pulse/internal/telemetry"
)

// Hub tracks live subscribers and pushes readings to them.
//
// Broadcast never blocks. A subscriber on a bad network cannot be allowed to
// back up the ingest workers behind it, so a full subscriber buffer drops the
// frame and increments a counter instead. Losing a sample for one dashboard
// beats stalling ingestion for the whole fleet.
type Hub struct {
	mu   sync.RWMutex
	subs map[*Subscriber]struct{}

	delivered atomic.Uint64
	dropped   atomic.Uint64
}

// Subscriber is one connected client, optionally scoped to a single device.
type Subscriber struct {
	C      chan telemetry.Reading
	device string
}

// Device returns the device filter, empty when the subscriber wants the whole
// fleet.
func (s *Subscriber) Device() string { return s.device }

// NewHub returns an empty hub.
func NewHub() *Hub {
	return &Hub{subs: make(map[*Subscriber]struct{})}
}

// Subscribe registers a client. A device filter of "" means all devices.
// buffer sets how many readings may queue for this client before frames drop.
func (h *Hub) Subscribe(device string, buffer int) *Subscriber {
	if buffer < 1 {
		buffer = 1
	}
	s := &Subscriber{C: make(chan telemetry.Reading, buffer), device: device}
	h.mu.Lock()
	h.subs[s] = struct{}{}
	h.mu.Unlock()
	return s
}

// Unsubscribe removes a client and closes its channel. Safe to call twice.
func (h *Hub) Unsubscribe(s *Subscriber) {
	h.mu.Lock()
	if _, ok := h.subs[s]; ok {
		delete(h.subs, s)
		close(s.C)
	}
	h.mu.Unlock()
}

// Broadcast delivers a reading to every matching subscriber without blocking.
func (h *Hub) Broadcast(r telemetry.Reading) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for s := range h.subs {
		if s.device != "" && s.device != r.DeviceID {
			continue
		}
		select {
		case s.C <- r:
			h.delivered.Add(1)
		default:
			h.dropped.Add(1)
		}
	}
}

// SubscriberCount reports live connections.
func (h *Hub) SubscriberCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subs)
}

// Stats snapshots delivery counters.
func (h *Hub) Stats() (delivered, dropped uint64) {
	return h.delivered.Load(), h.dropped.Load()
}

// CloseAll disconnects every subscriber, used during graceful shutdown.
func (h *Hub) CloseAll() {
	h.mu.Lock()
	for s := range h.subs {
		delete(h.subs, s)
		close(s.C)
	}
	h.mu.Unlock()
}
