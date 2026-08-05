package telemetry

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"time"
)

func TestNormalizeFillsMissingTimestamp(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	r := Reading{DeviceID: "dev-1", Metric: "engine_temp_c", Value: 91.2}

	if err := r.Normalize(now); err != nil {
		t.Fatalf("Normalize() = %v, want nil", err)
	}
	if !r.TS.Equal(now) {
		t.Errorf("TS = %v, want server time %v", r.TS, now)
	}
}

func TestNormalizeRejectsBadInput(t *testing.T) {
	now := time.Now()
	cases := map[string]Reading{
		"no device": {Metric: "m", Value: 1},
		"no metric": {DeviceID: "d", Value: 1},
		"NaN value": {DeviceID: "d", Metric: "m", Value: math.NaN()},
		"inf value": {DeviceID: "d", Metric: "m", Value: math.Inf(1)},
		"far future": {DeviceID: "d", Metric: "m", Value: 1,
			TS: now.Add(MaxClockSkew + time.Minute)},
	}

	for name, r := range cases {
		t.Run(name, func(t *testing.T) {
			if err := r.Normalize(now); err == nil {
				t.Error("Normalize() = nil, want error")
			}
		})
	}
}

func TestStoreRetentionIsBounded(t *testing.T) {
	const retain = 5
	s := NewStore(retain)
	base := time.Now()

	for i := 0; i < 100; i++ {
		s.Append(Reading{DeviceID: "dev-1", Metric: "m",
			Value: float64(i), TS: base.Add(time.Duration(i) * time.Second)})
	}

	got := s.Series("dev-1", time.Time{}, 0)
	if len(got) != retain {
		t.Fatalf("retained %d readings, want %d", len(got), retain)
	}
	// Oldest first, and only the newest window survives.
	if got[0].Value != 95 || got[retain-1].Value != 99 {
		t.Errorf("window = [%v..%v], want [95..99]", got[0].Value, got[retain-1].Value)
	}

	latest, ok := s.Latest("dev-1")
	if !ok || latest.Value != 99 {
		t.Errorf("Latest() = %v, %v, want 99, true", latest.Value, ok)
	}
}

func TestStoreSeriesWindowAndLimit(t *testing.T) {
	s := NewStore(100)
	base := time.Now()
	for i := 0; i < 10; i++ {
		s.Append(Reading{DeviceID: "dev-1", Metric: "m",
			Value: float64(i), TS: base.Add(time.Duration(i) * time.Minute)})
	}

	windowed := s.Series("dev-1", base.Add(7*time.Minute), 0)
	if len(windowed) != 3 {
		t.Errorf("window returned %d readings, want 3", len(windowed))
	}

	limited := s.Series("dev-1", time.Time{}, 4)
	if len(limited) != 4 {
		t.Fatalf("limit returned %d readings, want 4", len(limited))
	}
	if limited[len(limited)-1].Value != 9 {
		t.Errorf("limit kept oldest, want newest 4 ending at 9, got %v", limited[len(limited)-1].Value)
	}
}

func TestStoreUnknownDevice(t *testing.T) {
	s := NewStore(10)
	if _, ok := s.Latest("nope"); ok {
		t.Error("Latest() on unknown device returned ok")
	}
	if got := s.Series("nope", time.Time{}, 0); got != nil {
		t.Errorf("Series() on unknown device = %v, want nil", got)
	}
}

// TestStoreConcurrentAppend is the reason the store is sharded. Run with -race.
func TestStoreConcurrentAppend(t *testing.T) {
	s := NewStore(50)
	const devices, perDevice = 64, 200

	var wg sync.WaitGroup
	for d := 0; d < devices; d++ {
		wg.Add(1)
		go func(d int) {
			defer wg.Done()
			id := fmt.Sprintf("dev-%d", d)
			for i := 0; i < perDevice; i++ {
				s.Append(Reading{DeviceID: id, Metric: "m", Value: float64(i), TS: time.Now()})
			}
		}(d)
	}
	// Readers race the writers on purpose.
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				s.DeviceCount()
				s.Latest("dev-1")
			}
		}()
	}
	wg.Wait()

	if got := s.DeviceCount(); got != devices {
		t.Errorf("DeviceCount() = %d, want %d", got, devices)
	}
}
