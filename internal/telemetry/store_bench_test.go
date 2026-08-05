package telemetry

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkStoreAppend(b *testing.B) {
	s := NewStore(720)
	r := Reading{DeviceID: "dev-00001", Metric: "engine_temp_c", Value: 91.4, TS: time.Now()}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Append(r)
	}
}

// BenchmarkStoreAppendParallel is the number that justifies sharding: run it
// against a single mutex store and throughput collapses under contention.
func BenchmarkStoreAppendParallel(b *testing.B) {
	s := NewStore(720)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Append(Reading{
				DeviceID: fmt.Sprintf("dev-%05d", i%2000),
				Metric:   "engine_temp_c",
				Value:    91.4,
				TS:       time.Now(),
			})
			i++
		}
	})
}

func BenchmarkStoreLatest(b *testing.B) {
	s := NewStore(720)
	for i := 0; i < 720; i++ {
		s.Append(Reading{DeviceID: "dev-00001", Metric: "m", Value: float64(i), TS: time.Now()})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Latest("dev-00001")
	}
}
