package execution

import (
	"fmt"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
)

// SequentialStage identifies the four economically dependent stages used by
// an inventory-carrying cross-chain arbitrage. The output confirmed by one
// stage is the only valid input for the next stage.
type SequentialStage string

const (
	StageBuy               SequentialStage = "buy"
	StageBridgeBase        SequentialStage = "bridge_base"
	StageSell              SequentialStage = "sell"
	StageBridgeQuoteReturn SequentialStage = "bridge_quote_return"
)

// SequentialExitRoute records the irreversible liquidation decision made
// after the base token has reached the first bridge destination.
type SequentialExitRoute string

const (
	ExitSellAtDestination SequentialExitRoute = "sell_at_destination"
	ExitReturnToOrigin    SequentialExitRoute = "return_to_origin"
)

type SequentialStagePlan struct {
	Ordinal          int
	Stage            SequentialStage
	SourceChain      market.ChainID
	DestinationChain market.ChainID
	InputToken       market.TokenID
	OutputToken      market.TokenID
	Market           market.MarketID
}

func (s SequentialStagePlan) Validate() error {
	if s.Ordinal < 1 || s.Ordinal > 4 || s.Stage == "" ||
		s.SourceChain == "" || s.InputToken == "" || s.OutputToken == "" {
		return fmt.Errorf("sequential stage %d is incomplete", s.Ordinal)
	}
	switch s.Stage {
	case StageBuy, StageSell:
		if s.Market == "" {
			return fmt.Errorf("swap stage %d requires a market", s.Ordinal)
		}
		if s.DestinationChain != "" {
			return fmt.Errorf("swap stage %d cannot have a destination chain", s.Ordinal)
		}
	case StageBridgeBase, StageBridgeQuoteReturn:
		if s.DestinationChain == "" || s.DestinationChain == s.SourceChain {
			return fmt.Errorf("bridge stage %d requires different source and destination chains", s.Ordinal)
		}
		if s.Market != "" {
			return fmt.Errorf("bridge stage %d cannot have a market", s.Ordinal)
		}
	default:
		return fmt.Errorf("unsupported sequential stage %q", s.Stage)
	}
	return nil
}

// SequentialPlan contains only economic intent and token identities. It never
// contains calldata, signed transactions, private configuration, or provider
// payloads.
type SequentialPlan struct {
	ID              PlanID
	Opportunity     arbitrage.Opportunity
	InitialInput    market.TokenAmount
	Stages          []SequentialStagePlan
	DiscoveryAmount market.TokenAmount
	CreatedAt       time.Time
}

// ReturnExitStages replaces the normal destination sale and quote-token
// return with one base-token bridge back to the purchase chain followed by a
// sale there. The returned route is terminal: it cannot bridge the base token
// again.
func (p SequentialPlan) ReturnExitStages() ([]SequentialStagePlan, error) {
	if len(p.Stages) != 4 ||
		p.Stages[0].Stage != StageBuy ||
		p.Stages[1].Stage != StageBridgeBase ||
		p.Stages[2].Stage != StageSell ||
		p.Stages[3].Stage != StageBridgeQuoteReturn {
		return nil, fmt.Errorf("sequential plan cannot derive a return exit")
	}
	buy := p.Stages[0]
	firstBridge := p.Stages[1]
	result := []SequentialStagePlan{
		{
			Ordinal: 3, Stage: StageBridgeBase,
			SourceChain:      firstBridge.DestinationChain,
			DestinationChain: firstBridge.SourceChain,
			InputToken:       firstBridge.OutputToken,
			OutputToken:      firstBridge.InputToken,
		},
		{
			Ordinal: 4, Stage: StageSell,
			SourceChain: buy.SourceChain,
			InputToken:  buy.OutputToken,
			OutputToken: buy.InputToken,
			Market:      buy.Market,
		},
	}
	for _, stage := range result {
		if err := stage.Validate(); err != nil {
			return nil, err
		}
	}
	return result, nil
}

type SequentialExitDecision struct {
	Operation             OperationID
	Route                 SequentialExitRoute
	DestinationOutput     market.TokenAmount
	ReturnOutput          market.TokenAmount
	DestinationRecovery   market.AssetQuantity
	ReturnRecovery        market.AssetQuantity
	SafetyMargin          market.AssetQuantity
	DestinationQualified  bool
	CostEvidenceAvailable bool
	DecidedAt             time.Time
	Evidence              string
}

func (d SequentialExitDecision) Validate() error {
	if d.Operation == "" || d.DecidedAt.IsZero() || d.Evidence == "" ||
		d.DestinationRecovery.Asset() == "" ||
		d.SafetyMargin.Asset() != d.DestinationRecovery.Asset() ||
		d.SafetyMargin.Sign() < 0 {
		return fmt.Errorf("sequential exit decision is incomplete")
	}
	switch d.Route {
	case ExitSellAtDestination:
		if d.DestinationOutput.IsZero() {
			return fmt.Errorf("destination exit decision has no executable output")
		}
	case ExitReturnToOrigin:
		if d.ReturnOutput.IsZero() ||
			d.ReturnRecovery.Asset() != d.DestinationRecovery.Asset() {
			return fmt.Errorf("return exit decision has no comparable recovery")
		}
	default:
		return fmt.Errorf("unsupported sequential exit route %q", d.Route)
	}
	return nil
}

func NewSequentialPlan(
	id PlanID,
	opportunity arbitrage.Opportunity,
	initialInput market.TokenAmount,
	buyChain, sellChain market.ChainID,
	createdAt time.Time,
) (SequentialPlan, error) {
	if id == "" || createdAt.IsZero() {
		return SequentialPlan{}, fmt.Errorf("sequential plan identity and timestamp are required")
	}
	if opportunity.Classification != arbitrage.ClassificationPolicyQualified ||
		opportunity.SelectedIndex < 0 ||
		opportunity.SelectedIndex >= len(opportunity.Candidates) {
		return SequentialPlan{}, fmt.Errorf("sequential plan requires a policy-qualified opportunity")
	}
	if buyChain == "" || sellChain == "" || buyChain == sellChain {
		return SequentialPlan{}, fmt.Errorf("sequential plan requires two different chains")
	}
	candidate := opportunity.Candidates[opportunity.SelectedIndex]
	if initialInput.IsZero() ||
		initialInput.Token() != candidate.BuyQuote.AmountIn.Token() {
		return SequentialPlan{}, fmt.Errorf("execution input must use the buy quote input token")
	}
	stages := []SequentialStagePlan{
		{
			Ordinal: 1, Stage: StageBuy, SourceChain: buyChain,
			InputToken:  candidate.BuyQuote.AmountIn.Token(),
			OutputToken: candidate.BuyQuote.AmountOut.Token(),
			Market:      opportunity.Direction.BuyMarket,
		},
		{
			Ordinal: 2, Stage: StageBridgeBase, SourceChain: buyChain,
			DestinationChain: sellChain,
			InputToken:       candidate.BuyQuote.AmountOut.Token(),
			OutputToken:      candidate.SellQuote.AmountIn.Token(),
		},
		{
			Ordinal: 3, Stage: StageSell, SourceChain: sellChain,
			InputToken:  candidate.SellQuote.AmountIn.Token(),
			OutputToken: candidate.SellQuote.AmountOut.Token(),
			Market:      opportunity.Direction.SellMarket,
		},
		{
			Ordinal: 4, Stage: StageBridgeQuoteReturn, SourceChain: sellChain,
			DestinationChain: buyChain,
			InputToken:       candidate.SellQuote.AmountOut.Token(),
			OutputToken:      candidate.BuyQuote.AmountIn.Token(),
		},
	}
	for _, stage := range stages {
		if err := stage.Validate(); err != nil {
			return SequentialPlan{}, err
		}
	}
	return SequentialPlan{
		ID: id, Opportunity: opportunity, InitialInput: initialInput,
		DiscoveryAmount: candidate.BuyQuote.AmountIn,
		Stages:          append([]SequentialStagePlan(nil), stages...),
		CreatedAt:       createdAt.UTC(),
	}, nil
}

type SequentialOperationState string

const (
	SequentialRunning            SequentialOperationState = "running"
	SequentialCompleted          SequentialOperationState = "completed"
	SequentialAborted            SequentialOperationState = "aborted"
	SequentialManualIntervention SequentialOperationState = "manual_intervention_required"
	SequentialReconciledManually SequentialOperationState = "reconciled_manually"
)

type SequentialStageRequest struct {
	Operation OperationID
	Plan      PlanID
	Stage     SequentialStagePlan
	Input     market.TokenAmount
}

func (r SequentialStageRequest) Validate() error {
	if r.Operation == "" || r.Plan == "" {
		return fmt.Errorf("sequential stage request identity is incomplete")
	}
	if err := r.Stage.Validate(); err != nil {
		return err
	}
	if r.Input.IsZero() || r.Input.Token() != r.Stage.InputToken {
		return fmt.Errorf("stage %d input does not match the confirmed predecessor output", r.Stage.Ordinal)
	}
	return nil
}

type SequentialStageSettlement struct {
	Request             SequentialStageRequest
	ActualInput         market.TokenAmount
	ActualOutput        market.TokenAmount
	Costs               []CostComponent
	SourceIdentity      TransactionIdentity
	DestinationIdentity *TransactionIdentity
	ObservedAt          time.Time
	Evidence            string
}

func (s SequentialStageSettlement) Validate() error {
	if err := s.Request.Validate(); err != nil {
		return err
	}
	if s.ActualInput.IsZero() || s.ActualInput.Token() != s.Request.Stage.InputToken {
		return fmt.Errorf("stage %d settlement has invalid actual input", s.Request.Stage.Ordinal)
	}
	if s.ActualOutput.IsZero() || s.ActualOutput.Token() != s.Request.Stage.OutputToken {
		return fmt.Errorf("stage %d settlement has invalid actual output", s.Request.Stage.Ordinal)
	}
	if err := s.SourceIdentity.Validate(); err != nil {
		return fmt.Errorf("stage %d source identity: %w", s.Request.Stage.Ordinal, err)
	}
	if s.Request.Stage.DestinationChain != "" {
		if s.DestinationIdentity == nil {
			return fmt.Errorf("bridge stage %d requires destination identity", s.Request.Stage.Ordinal)
		}
		if err := s.DestinationIdentity.Validate(); err != nil {
			return fmt.Errorf("stage %d destination identity: %w", s.Request.Stage.Ordinal, err)
		}
	}
	if s.ObservedAt.IsZero() || s.Evidence == "" {
		return fmt.Errorf("stage %d settlement requires observation evidence", s.Request.Stage.Ordinal)
	}
	for index, cost := range s.Costs {
		if err := cost.Validate(); err != nil {
			return fmt.Errorf("stage %d cost %d: %w", s.Request.Stage.Ordinal, index, err)
		}
	}
	return nil
}

type SequentialOperation struct {
	ID            OperationID
	Plan          PlanID
	OpportunityID string
	ConfigHash    string
	State         SequentialOperationState
	CurrentStage  int
	CurrentAmount market.TokenAmount
	StartedAt     time.Time
	UpdatedAt     time.Time
	LastError     string
}
