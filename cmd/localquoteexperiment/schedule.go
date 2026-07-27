package localquoteexperiment

import (
	"fmt"
	"time"
)

// MeasurementSchedule returns fixed start targets after initialization. The
// first measurement occurs after one full interval, so 12 five-minute targets
// cover the requested hour and end at minute 60.
func MeasurementSchedule(initializedAt time.Time, interval time.Duration, samples int) ([]time.Time, error) {
	if initializedAt.IsZero() || interval <= 0 || samples < 1 {
		return nil, fmt.Errorf("measurement schedule requires a start, positive interval, and samples")
	}
	result := make([]time.Time, samples)
	for index := range result {
		result[index] = initializedAt.Add(time.Duration(index+1) * interval)
	}
	return result, nil
}
