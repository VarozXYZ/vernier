package livecompare

import (
	"sync"
	"time"
)

// simulationCadence allows an immediate first attempt and then at most one
// attempt per interval. New snapshots replace the pending one while a round
// is running; local tracking remains independent from this coalescing.
type SimulationCadence struct {
	mu       sync.Mutex
	interval time.Duration
	last     time.Time
	active   bool
	pending  bool
}

func NewSimulationCadence(interval time.Duration) *SimulationCadence {
	if interval <= 0 {
		interval = time.Second
	}
	return &SimulationCadence{interval: interval}
}

func (c *SimulationCadence) Request(now time.Time) (start bool, wait time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pending = true
	if c.active {
		return false, 0
	}
	if c.last.IsZero() || !now.Before(c.last.Add(c.interval)) {
		c.active = true
		c.pending = false
		// Rate-limit by start time. A slow RPC simulation must not extend the
		// one-per-second window after it has already started.
		c.last = now
		return true, 0
	}
	return false, c.last.Add(c.interval).Sub(now)
}

func (c *SimulationCadence) Finished(now time.Time) (start bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.active = false
	return false
}

func (c *SimulationCadence) PendingAt(now time.Time) (start bool, wait time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active || !c.pending {
		return false, 0
	}
	if c.last.IsZero() || !now.Before(c.last.Add(c.interval)) {
		c.active = true
		c.pending = false
		c.last = now
		return true, 0
	}
	return false, c.last.Add(c.interval).Sub(now)
}
