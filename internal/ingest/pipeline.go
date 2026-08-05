// Package ingest moves validated readings from the edge of the service into
// storage and out to live subscribers.
package ingest

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/Ohimoiza1205/pulse/internal/telemetry"
)

// Broadcaster receives every reading that survives the pipeline. The stream
// hub implements it. Keeping it an interface means ingest can be tested
// without opening a socket.
type Broadcaster interface {
	Broadcast(telemetry.Reading)
}

// ErrShutdownTimeout means workers did not drain the queue before the caller
// gave up waiting.
var ErrShutdownTimeout = errors.New("ingest: shutdown timed out with work still queued")

// Pipeline fans readings out to a fixed pool of workers over a bounded queue.
//
// The queue is bounded on purpose. An unbounded channel turns a traffic spike
// into an out of memory kill; a bounded one turns it into a 429 the caller can
// retry. Shedding load at the edge is the cheapest place to shed it.
type Pipeline struct {
	queue   chan telemetry.Reading
	store   *telemetry.Store
	bcast   Broadcaster
	workers int

	wg   sync.WaitGroup
	once sync.Once

	accepted  atomic.Uint64
	shed      atomic.Uint64
	processed atomic.Uint64
}

// New builds a pipeline. queueSize caps in flight readings; workers caps
// concurrent processing.
func New(store *telemetry.Store, bcast Broadcaster, queueSize, workers int) *Pipeline {
	if queueSize < 1 {
		queueSize = 1
	}
	if workers < 1 {
		workers = 1
	}
	return &Pipeline{
		queue:   make(chan telemetry.Reading, queueSize),
		store:   store,
		bcast:   bcast,
		workers: workers,
	}
}

// Start launches the worker pool. Workers exit when the queue is closed by
// Shutdown, or immediately when ctx is cancelled.
func (p *Pipeline) Start(ctx context.Context) {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for {
				select {
				case r, ok := <-p.queue:
					if !ok {
						return
					}
					p.handle(r)
				case <-ctx.Done():
					return
				}
			}
		}()
	}
}

func (p *Pipeline) handle(r telemetry.Reading) {
	p.store.Append(r)
	if p.bcast != nil {
		p.bcast.Broadcast(r)
	}
	p.processed.Add(1)
}

// Submit enqueues a reading without blocking. It reports false when the queue
// is full, which the HTTP layer surfaces as 429 rather than stalling the
// connection and letting the caller's timeout decide for us.
func (p *Pipeline) Submit(r telemetry.Reading) bool {
	select {
	case p.queue <- r:
		p.accepted.Add(1)
		return true
	default:
		p.shed.Add(1)
		return false
	}
}

// Depth reports current queue occupancy, the signal worth alerting on.
func (p *Pipeline) Depth() int { return len(p.queue) }

// Capacity reports the queue bound.
func (p *Pipeline) Capacity() int { return cap(p.queue) }

// Stats snapshots the pipeline counters.
func (p *Pipeline) Stats() (accepted, shed, processed uint64) {
	return p.accepted.Load(), p.shed.Load(), p.processed.Load()
}

// Shutdown stops accepting work and waits for the queue to drain, or for ctx
// to expire, whichever lands first.
func (p *Pipeline) Shutdown(ctx context.Context) error {
	p.once.Do(func() { close(p.queue) })

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ErrShutdownTimeout
	}
}
