// Package telemetry defines the device reading model and the in-memory store
// that backs it.
package telemetry

import (
	"errors"
	"math"
	"time"
)

// MaxClockSkew bounds how far into the future a device timestamp may sit
// before we reject it. Fleet devices drift; they don't time travel.
const MaxClockSkew = 5 * time.Minute

var (
	ErrNoDeviceID  = errors.New("device_id is required")
	ErrNoMetric    = errors.New("metric is required")
	ErrBadValue    = errors.New("value must be a finite number")
	ErrFutureStamp = errors.New("ts is too far in the future")
)

// Reading is a single metric sample emitted by one device.
type Reading struct {
	DeviceID string    `json:"device_id"`
	Metric   string    `json:"metric"`
	Value    float64   `json:"value"`
	TS       time.Time `json:"ts"`
}

// Normalize validates the reading and fills in a server timestamp when the
// device did not supply one. Devices in the field frequently don't.
func (r *Reading) Normalize(now time.Time) error {
	if r.DeviceID == "" || len(r.DeviceID) > 128 {
		return ErrNoDeviceID
	}
	if r.Metric == "" || len(r.Metric) > 128 {
		return ErrNoMetric
	}
	if math.IsNaN(r.Value) || math.IsInf(r.Value, 0) {
		return ErrBadValue
	}
	if r.TS.IsZero() {
		r.TS = now
		return nil
	}
	if r.TS.After(now.Add(MaxClockSkew)) {
		return ErrFutureStamp
	}
	return nil
}
