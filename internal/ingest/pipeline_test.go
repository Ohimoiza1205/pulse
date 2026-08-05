package ingest

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ohimoiza1205/pulse/internal/telemetry"
)

type countingBroadcaster struct{ n atomic.Uint64 }

func (c *countingBroadcaster) Broadcast(telemetry.Reading) { c.n.Add(1) }

func reading(id string) telemetry.Reading {
	return telemetry.Reading{DeviceID: id, Metric: "m", Value: 1, TS: time.Now()}
}

// TestSubmitShedsWhenQueueFull is the core backpressure guarantee: Submit
// never blocks, it reports failure so the caller can return 429.
func TestSubmitShedsWhenQueueFull(t *testing.T) {
	p := New(telemetry.NewStore(10), nil, 2, 1) // workers not started

	if !p.Submit(reading("a")) || !p.Submit(reading("b")) {
		t.Fatal("Submit() failed while queue had room")
	}
	if p.Submit(reading("c")) {
		t.Error("Submit() succeeded on a full queue, want shed")
	}

	accepted, shed, _ := p.Stats()
	if accepted != 2 || shed != 1 {
		t.Errorf("stats accepted=%d shed=%d, want 2 and 1", accepted, shed)
	}
	if p.Depth() != 2 || p.Capacity() != 2 {
		t.Errorf("depth=%d capacity=%d, want 2 and 2", p.Depth(), p.Capacity())
	}
}

func TestPipelineStoresAndBroadcasts(t *testing.T) {
	store := telemetry.NewStore(10)
	bcast := &countingBroadcaster{}
	p := New(store, bcast, 64, 4)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	const n = 50
	for i := 0; i < n; i++ {
		if !p.Submit(reading("dev-1")) {
			t.Fatalf("Submit() shed at %d with a queue of 64", i)
		}
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelShutdown()
	if err := p.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown() = %v, want nil", err)
	}

	if _, _, processed := p.Stats(); processed != n {
		t.Errorf("processed = %d, want %d", processed, n)
	}
	if got := bcast.n.Load(); got != n {
		t.Errorf("broadcast = %d, want %d", got, n)
	}
	if _, ok := store.Latest("dev-1"); !ok {
		t.Error("store has no reading for dev-1 after drain")
	}
}

// TestShutdownDrainsQueuedWork guards against dropping accepted readings on
// deploy. Anything Submit accepted must reach the store before we exit.
func TestShutdownDrainsQueuedWork(t *testing.T) {
	store := telemetry.NewStore(1000)
	p := New(store, nil, 500, 2)

	const n = 400
	for i := 0; i < n; i++ {
		p.Submit(reading("dev-1"))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx) // workers start only after the queue is already loaded

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelShutdown()
	if err := p.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown() = %v, want nil", err)
	}

	if _, _, processed := p.Stats(); processed != n {
		t.Errorf("processed = %d after drain, want %d", processed, n)
	}
}

func TestShutdownIsIdempotent(t *testing.T) {
	p := New(telemetry.NewStore(10), nil, 4, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = p.Shutdown(c)
		}()
	}
	wg.Wait() // a second close of the queue would panic here
}
