package livecompare

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	notificationport "github.com/VarozXYZ/vernier/ports/notification"
	persistenceport "github.com/VarozXYZ/vernier/ports/persistence"
	simulationport "github.com/VarozXYZ/vernier/ports/simulation"
)

// simulationCoordinator keeps local tracking independent from RPC simulation.
// It retains only the newest pending point and starts no more than one round
// per configured interval. A provider/RPC failure is persisted and projected
// into Telegram, but never closes the economic window.
type simulationCoordinator struct {
	ctx      context.Context
	pair     simulationport.PairSimulator
	store    persistenceport.SimulationStore
	logger   *slog.Logger
	cadence  *SimulationCadence
	onUpdate func(notificationport.TrackingWindowUpdate)

	mu             sync.Mutex
	pendingRequest *arbitrage.SimulationRequest
	pendingUpdate  notificationport.TrackingWindowUpdate
	running        bool
	timer          *time.Timer
}

func newSimulationCoordinator(ctx context.Context, pair simulationport.PairSimulator, store persistenceport.SimulationStore, interval time.Duration, logger *slog.Logger, onUpdate func(notificationport.TrackingWindowUpdate)) *simulationCoordinator {
	if pair == nil || store == nil {
		return nil
	}
	return &simulationCoordinator{ctx: ctx, pair: pair, store: store, cadence: NewSimulationCadence(interval), logger: logger, onUpdate: onUpdate}
}

func (c *simulationCoordinator) submit(request arbitrage.SimulationRequest, update notificationport.TrackingWindowUpdate) {
	if c == nil {
		return
	}
	if err := request.Validate(); err != nil {
		c.logger.Warn("simulation request ignored", "error", err)
		return
	}
	now := time.Now().UTC()
	c.mu.Lock()
	c.pendingRequest = &request
	c.pendingUpdate = update
	if c.running {
		_, _ = c.cadence.Request(now)
		c.mu.Unlock()
		return
	}
	start, wait := c.cadence.Request(now)
	if !start {
		c.scheduleLocked(wait)
		c.mu.Unlock()
		return
	}
	req, projection := c.takePendingLocked()
	c.running = true
	c.mu.Unlock()
	go c.execute(req, projection)
}

func (c *simulationCoordinator) scheduleLocked(wait time.Duration) {
	if wait <= 0 || c.timer != nil {
		return
	}
	c.timer = time.AfterFunc(wait, func() {
		c.mu.Lock()
		c.timer = nil
		if c.running || c.pendingRequest == nil {
			c.mu.Unlock()
			return
		}
		start, nextWait := c.cadence.PendingAt(time.Now().UTC())
		if !start {
			c.scheduleLocked(nextWait)
			c.mu.Unlock()
			return
		}
		req, projection := c.takePendingLocked()
		c.running = true
		c.mu.Unlock()
		go c.execute(req, projection)
	})
}

func (c *simulationCoordinator) takePendingLocked() (arbitrage.SimulationRequest, notificationport.TrackingWindowUpdate) {
	request := *c.pendingRequest
	update := c.pendingUpdate
	c.pendingRequest = nil
	c.pendingUpdate = notificationport.TrackingWindowUpdate{}
	return request, update
}

func (c *simulationCoordinator) execute(request arbitrage.SimulationRequest, update notificationport.TrackingWindowUpdate) {
	round, err := c.pair.SimulatePair(c.ctx, request)
	if err != nil {
		c.logger.Error("simulation round failed", "window", request.WindowID, "point", request.PointSequence, "error", err)
		round = arbitrage.SimulationRound{
			ID: fmt.Sprintf("simulation-%s-%d", request.WindowID, request.PointSequence), WindowID: request.WindowID, PointSequence: request.PointSequence,
			RequestedAt: request.RequestedAt, StartedAt: time.Now().UTC(), FinishedAt: time.Now().UTC(), Status: arbitrage.SimulationUnavailable,
			FailureClass: arbitrage.SimulationFailureInfrastructure, Error: err.Error(), Buy: request.Buy, Sell: request.Sell,
			LocalQualified: request.LocalQualified, LocalNetPnL: request.LocalNetPnL, LocalThreshold: request.LocalThreshold,
		}
	}
	if err := c.store.RecordSimulationRound(c.ctx, &round); err != nil {
		c.logger.Error("persist simulation round failed", "round", round.ID, "error", err)
	}
	update.SimulationStatus = string(round.Status)
	update.SimulationFailure = string(round.FailureClass)
	update.SimulationError = round.Error
	update.SimulationBuyStatus = string(round.Buy.Status)
	update.SimulationSellStatus = string(round.Sell.Status)
	if c.onUpdate != nil {
		c.onUpdate(update)
	}

	c.mu.Lock()
	c.cadence.Finished(time.Now().UTC())
	c.running = false
	if c.pendingRequest == nil {
		c.mu.Unlock()
		return
	}
	start, wait := c.cadence.PendingAt(time.Now().UTC())
	if !start {
		c.scheduleLocked(wait)
		c.mu.Unlock()
		return
	}
	nextRequest, nextUpdate := c.takePendingLocked()
	c.running = true
	c.mu.Unlock()
	go c.execute(nextRequest, nextUpdate)
}
