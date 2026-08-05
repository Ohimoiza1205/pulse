package stream

import (
	"sync"
	"testing"
	"time"

	"github.com/Ohimoiza1205/pulse/internal/telemetry"
)

func reading(id string) telemetry.Reading {
	return telemetry.Reading{DeviceID: id, Metric: "m", Value: 1, TS: time.Now()}
}

func TestBroadcastReachesAllSubscribers(t *testing.T) {
	h := NewHub()
	a := h.Subscribe("", 4)
	b := h.Subscribe("", 4)

	h.Broadcast(reading("dev-1"))

	for i, sub := range []*Subscriber{a, b} {
		select {
		case got := <-sub.C:
			if got.DeviceID != "dev-1" {
				t.Errorf("subscriber %d got %q, want dev-1", i, got.DeviceID)
			}
		default:
			t.Errorf("subscriber %d received nothing", i)
		}
	}
	if h.SubscriberCount() != 2 {
		t.Errorf("SubscriberCount() = %d, want 2", h.SubscriberCount())
	}
}

func TestDeviceFilter(t *testing.T) {
	h := NewHub()
	scoped := h.Subscribe("dev-1", 4)
	all := h.Subscribe("", 4)

	h.Broadcast(reading("dev-2"))

	select {
	case got := <-scoped.C:
		t.Errorf("filtered subscriber received %q, want nothing", got.DeviceID)
	default:
	}
	select {
	case <-all.C:
	default:
		t.Error("unfiltered subscriber received nothing")
	}
}

// TestSlowConsumerDropsInsteadOfBlocking is the property that keeps one bad
// network connection from stalling ingest for the whole fleet.
func TestSlowConsumerDropsInsteadOfBlocking(t *testing.T) {
	h := NewHub()
	h.Subscribe("", 2) // never drained

	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			h.Broadcast(reading("dev-1"))
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Broadcast() blocked on a slow subscriber")
	}

	delivered, dropped := h.Stats()
	if delivered != 2 {
		t.Errorf("delivered = %d, want 2 (the buffer)", delivered)
	}
	if dropped != 98 {
		t.Errorf("dropped = %d, want 98", dropped)
	}
}

func TestUnsubscribeIsIdempotent(t *testing.T) {
	h := NewHub()
	s := h.Subscribe("", 1)

	h.Unsubscribe(s)
	h.Unsubscribe(s) // a second close of the channel would panic

	if h.SubscriberCount() != 0 {
		t.Errorf("SubscriberCount() = %d, want 0", h.SubscriberCount())
	}
	if _, open := <-s.C; open {
		t.Error("subscriber channel still open after Unsubscribe")
	}
}

func TestConcurrentSubscribeAndBroadcast(t *testing.T) {
	h := NewHub()
	var wg sync.WaitGroup

	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := h.Subscribe("", 8)
			for j := 0; j < 50; j++ {
				select {
				case <-s.C:
				default:
				}
			}
			h.Unsubscribe(s)
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				h.Broadcast(reading("dev-1"))
			}
		}()
	}
	wg.Wait()

	h.CloseAll()
	if h.SubscriberCount() != 0 {
		t.Errorf("SubscriberCount() = %d after CloseAll, want 0", h.SubscriberCount())
	}
}
