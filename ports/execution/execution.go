// Package execution defines external build and validation effects required by
// the Live core.
package execution

import (
	"context"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
)

type ValidationRequest struct {
	Operation   execution.OperationID
	Leg         execution.Leg
	Discovery   market.Quote
	Snapshot    market.MarketSnapshot
	RequestedAt time.Time
}

// Artifact is an in-memory executable leg. Payload may contain calldata or a
// provider response and must never be written to the OperationalStore.
type Artifact struct {
	Leg                  execution.Leg
	ValidatedQuote       market.Quote
	Allocation           *execution.RouteAllocation
	Payload              []byte
	Metadata             map[string]string
	BuiltAt              time.Time
	Blockhash            string
	LastValidBlockHeight uint64
}

type Validator interface {
	Validate(context.Context, ValidationRequest) (Artifact, error)
}

type CostRequest struct {
	Opportunity arbitrage.LiveOpportunity
	Artifacts   map[execution.StepID]Artifact
	RequestedAt time.Time
}

// CostEstimator consumes only preloaded/cached fee state. It must not make a
// network call from the executable validation path.
type CostEstimator interface {
	Estimate(context.Context, CostRequest) (market.AssetQuantity, error)
}

// CostSnapshotSource exposes the current cached cost before discovery so a
// stale fee snapshot cannot trigger an unnecessary executable build.
type CostSnapshotSource interface {
	Current(context.Context) (market.AssetQuantity, error)
}
