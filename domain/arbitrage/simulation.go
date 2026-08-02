package arbitrage

import (
	"fmt"
	"time"

	"github.com/VarozXYZ/vernier/domain/market"
)

// SimulationStatus is deliberately independent from the economic window
// classification. A qualified local opportunity may be temporarily unable to
// reach an RPC or may fail a real VM simulation without being unprofitable.
type SimulationStatus string

const (
	SimulationPending     SimulationStatus = "pending"
	SimulationConfirmed   SimulationStatus = "confirmed"
	SimulationRejected    SimulationStatus = "rejected"
	SimulationUnavailable SimulationStatus = "unavailable"
	SimulationStale       SimulationStatus = "stale"
)

type SimulationFailureClass string

const (
	SimulationFailureNone           SimulationFailureClass = ""
	SimulationFailureInfrastructure SimulationFailureClass = "infrastructure"
	SimulationFailureExecution      SimulationFailureClass = "execution"
	SimulationFailureStateAdvanced  SimulationFailureClass = "state_advanced"
	SimulationFailureModelMismatch  SimulationFailureClass = "model_mismatch"
	SimulationFailureFixture        SimulationFailureClass = "fixture"
)

type SimulationLeg struct {
	Chain             string
	Market            market.MarketID
	Input             market.TokenAmount
	LocalOutput       market.TokenAmount
	SimulatedOutput   market.TokenAmount
	Status            SimulationStatus
	FailureClass      SimulationFailureClass
	Error             string
	SnapshotVersion   uint64
	SnapshotHash      [32]byte
	Context           string
	ContextPosition   uint64
	GasOrComputeUnits uint64
	StartedAt         time.Time
	FinishedAt        time.Time
}

func (l SimulationLeg) Duration() time.Duration {
	if l.StartedAt.IsZero() || l.FinishedAt.IsZero() || l.FinishedAt.Before(l.StartedAt) {
		return 0
	}
	return l.FinishedAt.Sub(l.StartedAt)
}

type SimulationRound struct {
	ID              string
	WindowID        WindowID
	PointSequence   uint64
	RequestedAt     time.Time
	StartedAt       time.Time
	FinishedAt      time.Time
	Status          SimulationStatus
	FailureClass    SimulationFailureClass
	Error           string
	Buy             SimulationLeg
	Sell            SimulationLeg
	LocalQualified  bool
	LocalNetPnL     market.AssetQuantity
	LocalThreshold  market.AssetQuantity
	SimulatedNetPnL market.AssetQuantity
}

func (r SimulationRound) Validate() error {
	if r.ID == "" || r.WindowID == "" || r.PointSequence == 0 {
		return fmt.Errorf("simulation round identity is required")
	}
	if r.RequestedAt.IsZero() || r.StartedAt.IsZero() {
		return fmt.Errorf("simulation round timestamps are required")
	}
	if r.Status == "" {
		return fmt.Errorf("simulation round status is required")
	}
	if r.Buy.Market == "" || r.Sell.Market == "" {
		return fmt.Errorf("simulation round requires buy and sell legs")
	}
	if r.Buy.Input.Token() == "" || r.Sell.Input.Token() == "" {
		return fmt.Errorf("simulation round leg inputs are required")
	}
	return nil
}

type SimulationRequest struct {
	WindowID       WindowID
	PointSequence  uint64
	RequestedAt    time.Time
	Buy            SimulationLeg
	Sell           SimulationLeg
	LocalQualified bool
	LocalNetPnL    market.AssetQuantity
	LocalThreshold market.AssetQuantity
}

func (r SimulationRequest) Validate() error {
	if r.WindowID == "" || r.PointSequence == 0 || r.RequestedAt.IsZero() {
		return fmt.Errorf("simulation request identity is required")
	}
	if r.Buy.Market == "" || r.Sell.Market == "" {
		return fmt.Errorf("simulation request requires both legs")
	}
	return nil
}
