// Command loadgen simulates a fleet of devices posting telemetry, so the
// backpressure and retention paths can be exercised without hardware.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type reading struct {
	DeviceID string    `json:"device_id"`
	Metric   string    `json:"metric"`
	Value    float64   `json:"value"`
	TS       time.Time `json:"ts"`
}

func main() {
	var (
		target  = flag.String("target", "http://localhost:8080/v1/ingest", "ingest endpoint")
		devices = flag.Int("devices", 500, "simulated devices")
		hz      = flag.Float64("hz", 2, "readings per device per second")
		batch   = flag.Int("batch", 10, "readings per request")
		dur     = flag.Duration("duration", 30*time.Second, "how long to run")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *dur)
	defer cancel()

	client := &http.Client{Timeout: 5 * time.Second}
	var sent, shed, failed atomic.Uint64

	interval := time.Duration(float64(time.Second) * float64(*batch) / *hz)
	var wg sync.WaitGroup

	for i := 0; i < *devices; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			deviceID := fmt.Sprintf("dev-%05d", id)
			// Stagger starts so the fleet doesn't arrive in lockstep.
			select {
			case <-time.After(time.Duration(rand.Int63n(int64(interval)))):
			case <-ctx.Done():
				return
			}

			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			phase := rand.Float64() * math.Pi * 2

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					body := make([]reading, 0, *batch)
					now := time.Now()
					for b := 0; b < *batch; b++ {
						phase += 0.05
						body = append(body, reading{
							DeviceID: deviceID,
							Metric:   "engine_temp_c",
							Value:    85 + 12*math.Sin(phase) + rand.NormFloat64(),
							TS:       now.Add(time.Duration(b) * time.Millisecond),
						})
					}
					status, err := post(client, *target, body)
					switch {
					case err != nil:
						failed.Add(1)
					case status == http.StatusTooManyRequests:
						shed.Add(1)
					default:
						sent.Add(1)
					}
				}
			}
		}(i)
	}

	wg.Wait()
	fmt.Printf("requests ok=%d shed=%d failed=%d over %s across %d devices\n",
		sent.Load(), shed.Load(), failed.Load(), *dur, *devices)
}

func post(c *http.Client, url string, body []reading) (int, error) {
	buf, err := json.Marshal(body)
	if err != nil {
		return 0, err
	}
	resp, err := c.Post(url, "application/json", bytes.NewReader(buf))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return resp.StatusCode, nil
}
