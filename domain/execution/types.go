// Package execution defines protocol-neutral durable execution state.
package execution

import (
	"fmt"
	"math/big"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
)

type (
	PlanID      string
	OperationID string
	StepID      string
	AccountID   string
)

type LegSide string

const (
	LegBuy  LegSide = "buy"
	LegSell LegSide = "sell"
)

type Leg struct {
	ID             StepID
	Side           LegSide
	Chain          market.ChainID
	Account        AccountID
	Market         market.MarketID
	Input          market.TokenAmount
	ExpectedOutput market.TokenAmount
}

func (l Leg) Validate() error {
	if l.ID == "" || l.Chain == "" || l.Account == "" || l.Market == "" {
		return fmt.Errorf("execution leg identity is incomplete")
	}
	if l.Side != LegBuy && l.Side != LegSell {
		return fmt.Errorf("execution leg %q has invalid side %q", l.ID, l.Side)
	}
	if l.Input.Token() == "" || l.Input.IsZero() || l.ExpectedOutput.Token() == "" || l.ExpectedOutput.IsZero() {
		return fmt.Errorf("execution leg %q requires positive input and output", l.ID)
	}
	return nil
}

// SagaPlan is immutable by convention. Legs returns a defensive copy.
type SagaPlan struct {
	id          PlanID
	opportunity arbitrage.LiveOpportunity
	legs        []Leg
	createdAt   time.Time
}

func NewSagaPlan(id PlanID, opportunity arbitrage.LiveOpportunity, legs []Leg, createdAt time.Time) (SagaPlan, error) {
	if id == "" || createdAt.IsZero() || len(legs) != 2 {
		return SagaPlan{}, fmt.Errorf("parallel prefunded saga requires identity, timestamp, and two legs")
	}
	if err := opportunity.Validate(); err != nil {
		return SagaPlan{}, err
	}
	seen := map[LegSide]bool{}
	for _, leg := range legs {
		if err := leg.Validate(); err != nil {
			return SagaPlan{}, err
		}
		if seen[leg.Side] {
			return SagaPlan{}, fmt.Errorf("parallel saga repeats %s leg", leg.Side)
		}
		seen[leg.Side] = true
	}
	if !seen[LegBuy] || !seen[LegSell] {
		return SagaPlan{}, fmt.Errorf("parallel saga requires buy and sell legs")
	}
	return SagaPlan{
		id: id, opportunity: opportunity, legs: append([]Leg(nil), legs...), createdAt: createdAt.UTC(),
	}, nil
}

func (p SagaPlan) ID() PlanID                             { return p.id }
func (p SagaPlan) Opportunity() arbitrage.LiveOpportunity { return p.opportunity }
func (p SagaPlan) Legs() []Leg                            { return append([]Leg(nil), p.legs...) }
func (p SagaPlan) CreatedAt() time.Time                   { return p.createdAt }

type TechnicalState string

const (
	StatePrepared           TechnicalState = "prepared"
	StateCommitted          TechnicalState = "committed"
	StateBroadcastRejected  TechnicalState = "broadcast_rejected"
	StateBroadcastPossible  TechnicalState = "broadcast_possible"
	StateConfirmedSuccess   TechnicalState = "confirmed_success"
	StateConfirmedRevert    TechnicalState = "confirmed_revert"
	StateOutcomeUnknown     TechnicalState = "outcome_unknown"
	StateManualIntervention TechnicalState = "manual_intervention_required"
)

type EconomicState string

const (
	EconomicReserved       EconomicState = "reserved"
	EconomicEffectVerified EconomicState = "effect_verified"
	EconomicEffectMismatch EconomicState = "effect_mismatch"
	EconomicExposureOpen   EconomicState = "exposure_open"
)

// TransactionIdentity contains durable transaction identity but never a
// signed payload.
type TransactionIdentity struct {
	Chain                market.ChainID
	Account              AccountID
	Hash                 string
	Nonce                *uint64
	Blockhash            string
	LastValidBlockHeight uint64
}

func (i TransactionIdentity) Validate() error {
	if i.Chain == "" || i.Account == "" || i.Hash == "" {
		return fmt.Errorf("transaction chain, account, and hash/signature are required")
	}
	return nil
}

type OperationStep struct {
	Operation  OperationID
	Leg        Leg
	Identity   TransactionIdentity
	Allocation *RouteAllocation
	Technical  TechnicalState
	Economic   EconomicState
}

// OperationEconomics is the durable economic evidence fixed before
// broadcast. It deliberately excludes provider payloads and signed
// transactions.
type OperationEconomics struct {
	Valuation    arbitrage.ValuationSnapshot
	QuoteDelta   market.AssetQuantity
	BaseDelta    market.AssetQuantity
	MarkedBase   market.AssetQuantity
	GrossPnL     market.AssetQuantity
	Cost         market.AssetQuantity
	NetPnL       market.AssetQuantity
	Threshold    market.AssetQuantity
	DiscoveredAt time.Time
	ValidatedAt  time.Time
}

func EconomicsFromOpportunity(opportunity arbitrage.LiveOpportunity) OperationEconomics {
	return OperationEconomics{
		Valuation: opportunity.Valuation, QuoteDelta: opportunity.QuoteDelta,
		BaseDelta: opportunity.BaseDelta, MarkedBase: opportunity.MarkedBase,
		GrossPnL: opportunity.GrossPnL, Cost: opportunity.Cost,
		NetPnL: opportunity.NetPnL, Threshold: opportunity.Threshold,
		DiscoveredAt: opportunity.DiscoveredAt, ValidatedAt: opportunity.ValidatedAt,
	}
}

func (e OperationEconomics) Validate() error {
	if e.Valuation.Version() == 0 || e.Valuation.Base() == "" || e.Valuation.Quote() == "" ||
		e.DiscoveredAt.IsZero() || e.ValidatedAt.IsZero() {
		return fmt.Errorf("operation economics requires valuation and discovery/validation timestamps")
	}
	if e.BaseDelta.Asset() != e.Valuation.Base() ||
		e.QuoteDelta.Asset() != e.Valuation.Quote() ||
		e.MarkedBase.Asset() != e.Valuation.Quote() ||
		e.GrossPnL.Asset() != e.Valuation.Quote() ||
		e.Cost.Asset() != e.Valuation.Quote() ||
		e.NetPnL.Asset() != e.Valuation.Quote() ||
		e.Threshold.Asset() != e.Valuation.Quote() {
		return fmt.Errorf("operation economics assets are inconsistent")
	}
	if e.Cost.Sign() < 0 || e.Threshold.Sign() < 0 {
		return fmt.Errorf("operation costs and threshold cannot be negative")
	}
	marked, err := market.NewAssetQuantity(
		e.Valuation.Quote(),
		new(big.Rat).Mul(e.BaseDelta.Rat(), e.Valuation.Price()),
	)
	if err != nil || marked.Rat().Cmp(e.MarkedBase.Rat()) != 0 {
		return fmt.Errorf("operation marked base value is inconsistent")
	}
	gross, err := e.QuoteDelta.Add(e.MarkedBase)
	if err != nil || gross.Rat().Cmp(e.GrossPnL.Rat()) != 0 {
		return fmt.Errorf("operation gross PnL is inconsistent")
	}
	net, err := e.GrossPnL.Sub(e.Cost)
	if err != nil || net.Rat().Cmp(e.NetPnL.Rat()) != 0 {
		return fmt.Errorf("operation net PnL is inconsistent")
	}
	return nil
}

type Operation struct {
	ID            OperationID
	Plan          PlanID
	OpportunityID string
	ConfigHash    string
	Steps         []OperationStep
	Economics     OperationEconomics
	Technical     TechnicalState
	Economic      EconomicState
	CreatedAt     time.Time
	CommittedAt   time.Time
}

func (o Operation) ValidatePrepared() error {
	if o.ID == "" || o.Plan == "" || o.OpportunityID == "" || o.ConfigHash == "" ||
		len(o.Steps) != 2 || o.CreatedAt.IsZero() {
		return fmt.Errorf("prepared operation is incomplete")
	}
	if err := o.Economics.Validate(); err != nil {
		return err
	}
	for _, step := range o.Steps {
		if step.Operation != "" && step.Operation != o.ID {
			return fmt.Errorf("operation step %q belongs to another operation", step.Leg.ID)
		}
		if err := step.Leg.Validate(); err != nil {
			return err
		}
		if err := step.Identity.Validate(); err != nil {
			return err
		}
		if step.Allocation != nil {
			if err := step.Allocation.Validate(); err != nil {
				return fmt.Errorf("operation step %q allocation: %w", step.Leg.ID, err)
			}
			if step.Allocation.Input.Token() != step.Leg.Input.Token() ||
				step.Allocation.Input.Units().Cmp(step.Leg.Input.Units()) != 0 ||
				step.Allocation.ExpectedOutput.Token() != step.Leg.ExpectedOutput.Token() ||
				step.Allocation.ExpectedOutput.Units().Cmp(step.Leg.ExpectedOutput.Units()) != 0 {
				return fmt.Errorf("operation step %q allocation does not match leg amounts", step.Leg.ID)
			}
		}
	}
	return nil
}

type Settlement struct {
	Operation  OperationID
	Step       StepID
	Identity   TransactionIdentity
	Technical  TechnicalState
	Economic   EconomicState
	ActualIn   market.TokenAmount
	ActualOut  market.TokenAmount
	Costs      []CostComponent
	ObservedAt time.Time
	Evidence   string
}
