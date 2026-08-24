package arbitrage

import (
	"fmt"
	"time"

	"github.com/VarozXYZ/vernier/domain/market"
)

type ExecutableValidationStatus string

const (
	ValidationConfirmed   ExecutableValidationStatus = "confirmed_profitable"
	ValidationDisappeared ExecutableValidationStatus = "disappeared_during_build"
	ValidationRejected    ExecutableValidationStatus = "build_or_simulation_rejected"
	ValidationUnavailable ExecutableValidationStatus = "provider_or_data_unavailable"
)

// ExecutableValidationRound is normalized evidence only. Provider payloads
// and calldata are deliberately absent from this durable type.
type ExecutableValidationRound struct {
	ID                   string
	WindowID             WindowID
	PointSequence        uint64
	Direction            Direction
	Status               ExecutableValidationStatus
	FailureStage         string
	FailureClass         string
	Error                string
	RequestedAt          time.Time
	RouteFinishedAt      time.Time
	BuildFinishedAt      time.Time
	SimulationFinishedAt time.Time
	LocalRecapturedAt    time.Time
	RecalculatedAt       time.Time
	PersistedAt          time.Time
	DiscoveryOutput      market.AssetQuantity
	BuildOutput          market.AssetQuantity
	DiscoveryNet         market.AssetQuantity
	FinalNet             market.AssetQuantity
	Threshold            market.AssetQuantity
	RemoteMarket         market.MarketID
	LocalMarket          market.MarketID
	InitialLocalSnapshot market.SnapshotMetadata
	FinalLocalSnapshot   market.SnapshotMetadata
	RouteHash            string
	BuildHash            string
	RouteHTTPStatus      int
	BuildHTTPStatus      int
	RouteDuration        time.Duration
	BuildDuration        time.Duration
	BuildAttempts        int
}

func (r ExecutableValidationRound) Validate() error {
	if r.ID == "" || r.WindowID == "" || r.PointSequence == 0 || r.Direction.BuyMarket == "" || r.Direction.SellMarket == "" || r.RequestedAt.IsZero() || r.Status == "" {
		return fmt.Errorf("executable validation round identity is incomplete")
	}
	if r.DiscoveryNet.Asset() == "" || r.Threshold.Asset() == "" {
		return fmt.Errorf("executable validation economics are incomplete")
	}
	return nil
}
