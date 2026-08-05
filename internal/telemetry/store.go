package telemetry

import (
	"hash/maphash"
	"sort"
	"sync"
	"time"
)

// shardCount fixes lock granularity. Every device maps to exactly one shard,
// so writes from unrelated devices don't contend on the same mutex.
const shardCount = 32

// Store keeps the most recent N readings per device in fixed memory.
//
// The retention bound is per device rather than global on purpose: one chatty
// device cannot evict the history of every other device in the fleet.
type Store struct {
	shards   [shardCount]*shard
	perDev   int
	hashSeed maphash.Seed
}

type shard struct {
	mu      sync.RWMutex
	devices map[string]*ring
}

// NewStore returns a store retaining perDevice readings for each device.
func NewStore(perDevice int) *Store {
	if perDevice < 1 {
		perDevice = 1
	}
	s := &Store{perDev: perDevice, hashSeed: maphash.MakeSeed()}
	for i := range s.shards {
		s.shards[i] = &shard{devices: make(map[string]*ring)}
	}
	return s
}

func (s *Store) shardFor(deviceID string) *shard {
	h := maphash.String(s.hashSeed, deviceID)
	return s.shards[h%shardCount]
}

// Append records a reading in constant time and constant additional memory.
func (s *Store) Append(r Reading) {
	sh := s.shardFor(r.DeviceID)
	sh.mu.Lock()
	rg, ok := sh.devices[r.DeviceID]
	if !ok {
		rg = newRing(s.perDev)
		sh.devices[r.DeviceID] = rg
	}
	rg.append(r)
	sh.mu.Unlock()
}

// Latest returns the most recent reading for a device.
func (s *Store) Latest(deviceID string) (Reading, bool) {
	sh := s.shardFor(deviceID)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	rg, ok := sh.devices[deviceID]
	if !ok {
		return Reading{}, false
	}
	return rg.latest()
}

// Series returns retained readings for a device, oldest first, limited to
// those at or after since. A zero since means everything still retained.
func (s *Store) Series(deviceID string, since time.Time, limit int) []Reading {
	sh := s.shardFor(deviceID)
	sh.mu.RLock()
	rg, ok := sh.devices[deviceID]
	if !ok {
		sh.mu.RUnlock()
		return nil
	}
	out := rg.snapshot(since)
	sh.mu.RUnlock()

	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out
}

// Devices lists every device that has reported at least once.
func (s *Store) Devices() []string {
	out := make([]string, 0, s.DeviceCount())
	for _, sh := range s.shards {
		sh.mu.RLock()
		for id := range sh.devices {
			out = append(out, id)
		}
		sh.mu.RUnlock()
	}
	sort.Strings(out)
	return out
}

// DeviceCount reports fleet size without allocating the full device list.
func (s *Store) DeviceCount() int {
	n := 0
	for _, sh := range s.shards {
		sh.mu.RLock()
		n += len(sh.devices)
		sh.mu.RUnlock()
	}
	return n
}

// ring is a fixed capacity circular buffer of readings.
type ring struct {
	buf  []Reading
	next int
	n    int
}

func newRing(capacity int) *ring {
	return &ring{buf: make([]Reading, capacity)}
}

func (r *ring) append(v Reading) {
	r.buf[r.next] = v
	r.next = (r.next + 1) % len(r.buf)
	if r.n < len(r.buf) {
		r.n++
	}
}

func (r *ring) latest() (Reading, bool) {
	if r.n == 0 {
		return Reading{}, false
	}
	idx := (r.next - 1 + len(r.buf)) % len(r.buf)
	return r.buf[idx], true
}

func (r *ring) snapshot(since time.Time) []Reading {
	if r.n == 0 {
		return nil
	}
	out := make([]Reading, 0, r.n)
	start := (r.next - r.n + len(r.buf)) % len(r.buf)
	for i := 0; i < r.n; i++ {
		v := r.buf[(start+i)%len(r.buf)]
		if !since.IsZero() && v.TS.Before(since) {
			continue
		}
		out = append(out, v)
	}
	return out
}
