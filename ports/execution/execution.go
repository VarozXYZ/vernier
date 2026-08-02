// Package execution defines external build and validation effects required by
// the Live core.
package execution

import (
	"context"
	"fmt"
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
	Slippage    *SlippageConstraint
	RequestedAt time.Time
}

// SlippageConstraint carries a stage-specific tolerance. A non-nil constraint
// distinguishes an explicit zero BPS policy from the validator default.
type SlippageConstraint struct {
	BPS           uint16
	MinimumOutput market.TokenAmount
	Reason        string
	Evidence      map[string]string
}

// SlippageThresholdError means a fresh provider artifact cannot preserve the
// economic floor selected by the runtime. Retrying the same evaluation would
// add hot-path I/O without new economic information.
type SlippageThresholdError struct {
	Provider string
	Actual   market.TokenAmount
	Required market.TokenAmount
}

// SimulationInvariantError means the chain accepted the exact payload for
// simulation but the adapter could not prove its economic output. Continuing
// would silently disable Live's joint profitability guard.
type SimulationInvariantError struct {
	Chain    market.ChainID
	Market   market.MarketID
	Provider string
	Identity string
	Err      error
}

func (e *SimulationInvariantError) Error() string {
	if e == nil || e.Err == nil {
		return "successful simulation has no attributable economic output"
	}
	return fmt.Sprintf(
		"successful simulation has no attributable economic output on %s/%s: %v",
		e.Chain, e.Market, e.Err,
	)
}

func (e *SimulationInvariantError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func (e *SlippageThresholdError) Error() string {
	if e == nil {
		return "swap threshold is below the required output"
	}
	return fmt.Sprintf(
		"%s swap threshold %s is below required output %s",
		e.Provider,
		e.Actual,
		e.Required,
	)
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

// CompactValidator can rebuild a provider artifact under a tighter account or
// payload budget after the chain adapter proves that the first artifact cannot
// fit on wire. The previous artifact is in-memory only.
type CompactValidator interface {
	ValidateCompact(context.Context, ValidationRequest, Artifact) (Artifact, error)
}

// AllowanceRequiredError preserves the spender proven by a failed executable
// artifact. Recovery must inspect allowance and balance instead of inferring
// either from provider text such as TRANSFER_FROM_FAILED.
type AllowanceRequiredError struct {
	Spender string
	Err     error
}

func (e *AllowanceRequiredError) Error() string {
	if e == nil || e.Err == nil {
		return "token allowance or balance prevented execution"
	}
	return e.Err.Error()
}

func (e *AllowanceRequiredError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

type ArtifactTooLargeError struct {
	ActualBytes  int
	MaximumBytes int
}

func (e *ArtifactTooLargeError) Error() string {
	return fmt.Sprintf(
		"serialized transaction is too large: %d bytes exceeds %d",
		e.ActualBytes,
		e.MaximumBytes,
	)
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
