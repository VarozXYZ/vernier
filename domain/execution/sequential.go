package execution

import (
	"fmt"
	"math/big"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
)

// ExecutionPolicyKind selects one compiled economic lifecycle. It is durable
// evidence, not a provider or a user-programmable workflow name.
type ExecutionPolicyKind string

const (
	PolicyTransportedSequential ExecutionPolicyKind = "transported_sequential"
	PolicyPrefundedSequential   ExecutionPolicyKind = "prefunded_sequential"
	PolicyPrefundedParallel     ExecutionPolicyKind = "prefunded_parallel"
)

// SequentialStage identifies the typed capabilities used by dependent plans.
type SequentialStage string

const (
	StageBuy               SequentialStage = "buy"
	StageBridgeBase        SequentialStage = "bridge_base"
	StageSell              SequentialStage = "sell"
	StageBridgeQuoteReturn SequentialStage = "bridge_quote_return"
)

type SequentialBranch string

const (
	BranchMain           SequentialBranch = "main"
	BranchCircuitBreaker SequentialBranch = "circuit_breaker"
)

// SequentialExitRoute records the irreversible liquidation decision.
type SequentialExitRoute string

const (
	ExitSellAtDestination SequentialExitRoute = "sell_at_destination"
	ExitReturnToOrigin    SequentialExitRoute = "return_to_origin"
	ExitSellAtOrigin      SequentialExitRoute = "sell_at_origin"
)

type SequentialStagePlan struct {
	Ordinal          int
	Stage            SequentialStage
	Branch           SequentialBranch
	DependsOn        []int
	InputFromOrdinal int
	SourceChain      market.ChainID
	DestinationChain market.ChainID
	InputToken       market.TokenID
	OutputToken      market.TokenID
	Market           market.MarketID
}

func (s SequentialStagePlan) Validate() error {
	if s.Ordinal < 1 || s.Stage == "" ||
		s.SourceChain == "" || s.InputToken == "" || s.OutputToken == "" {
		return fmt.Errorf("sequential stage %d is incomplete", s.Ordinal)
	}
	if s.Branch == "" {
		s.Branch = BranchMain
	}
	if s.Branch != BranchMain && s.Branch != BranchCircuitBreaker {
		return fmt.Errorf("sequential stage %d has unsupported branch %q", s.Ordinal, s.Branch)
	}
	if s.InputFromOrdinal < 0 || s.InputFromOrdinal >= s.Ordinal {
		return fmt.Errorf("sequential stage %d has an invalid input reference", s.Ordinal)
	}
	seen := make(map[int]struct{}, len(s.DependsOn))
	for _, dependency := range s.DependsOn {
		if dependency < 1 || dependency >= s.Ordinal {
			return fmt.Errorf("sequential stage %d has an invalid dependency", s.Ordinal)
		}
		if _, duplicate := seen[dependency]; duplicate {
			return fmt.Errorf("sequential stage %d repeats dependency %d", s.Ordinal, dependency)
		}
		seen[dependency] = struct{}{}
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
	Policy          ExecutionPolicyKind
	Opportunity     arbitrage.Opportunity
	InitialInput    market.TokenAmount
	Stages          []SequentialStagePlan
	CircuitBreaker  []SequentialStagePlan
	DiscoveryAmount market.TokenAmount
	BaseAsset       market.AssetID
	QuoteAsset      market.AssetID
	TokenDecimals   map[market.TokenID]uint8
	CreatedAt       time.Time
}

func (p SequentialPlan) EffectivePolicy() ExecutionPolicyKind {
	if p.Policy == "" {
		return PolicyTransportedSequential
	}
	return p.Policy
}

func (p SequentialPlan) Validate() error {
	if p.ID == "" || p.InitialInput.IsZero() || p.CreatedAt.IsZero() ||
		len(p.Stages) == 0 {
		return fmt.Errorf("dependent execution plan is incomplete")
	}
	switch p.EffectivePolicy() {
	case PolicyTransportedSequential:
		if len(p.Stages) != 4 {
			return fmt.Errorf("transported sequential plan requires four main stages")
		}
	case PolicyPrefundedSequential, PolicyPrefundedParallel:
		if len(p.Stages) != 4 || len(p.CircuitBreaker) != 1 {
			return fmt.Errorf("prefunded plan requires four main stages and one circuit-breaker stage")
		}
		if p.EffectivePolicy() == PolicyPrefundedParallel &&
			(p.BaseAsset == "" || p.QuoteAsset == "" || len(p.TokenDecimals) == 0) {
			return fmt.Errorf("prefunded parallel plan requires durable valuation metadata")
		}
	default:
		return fmt.Errorf("unsupported dependent execution policy %q", p.Policy)
	}
	ordinals := make(map[int]struct{}, len(p.Stages)+len(p.CircuitBreaker))
	for _, stages := range [][]SequentialStagePlan{p.Stages, p.CircuitBreaker} {
		for _, stage := range stages {
			if err := stage.Validate(); err != nil {
				return err
			}
			if _, duplicate := ordinals[stage.Ordinal]; duplicate {
				return fmt.Errorf("dependent execution plan repeats ordinal %d", stage.Ordinal)
			}
			ordinals[stage.Ordinal] = struct{}{}
		}
	}
	if p.EffectivePolicy() == PolicyPrefundedParallel {
		if err := p.validateParallelDependencies(); err != nil {
			return err
		}
	}
	return nil
}

func (p SequentialPlan) validateParallelDependencies() error {
	expectedStages := []SequentialStage{StageBuy, StageSell, StageBridgeBase, StageBridgeQuoteReturn}
	expectedInputs := []int{0, 0, 1, 2}
	expectedDependencies := [][]int{nil, nil, {1}, {2}}
	for index, stage := range p.Stages {
		if stage.Ordinal != index+1 || stage.Stage != expectedStages[index] ||
			stage.InputFromOrdinal != expectedInputs[index] ||
			!sameOrdinals(stage.DependsOn, expectedDependencies[index]) {
			return fmt.Errorf(
				"prefunded parallel stage %d has sequential or invalid dependencies",
				stage.Ordinal,
			)
		}
	}
	return nil
}

func sameOrdinals(left, right []int) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

// InputFor resolves a typed stage input only from durable economic
// settlements. The zero reference denotes the configured initial input.
func (p SequentialPlan) InputFor(
	stage SequentialStagePlan,
	outputs map[int]market.TokenAmount,
) (market.TokenAmount, error) {
	if p.EffectivePolicy() == PolicyPrefundedParallel &&
		stage.Ordinal == 2 && stage.Stage == StageSell {
		return p.ParallelSellInput()
	}
	if stage.InputFromOrdinal == 0 {
		if stage.Ordinal != 1 {
			return market.TokenAmount{}, fmt.Errorf(
				"stage %d has no settlement input reference", stage.Ordinal,
			)
		}
		if p.InitialInput.Token() != stage.InputToken {
			return market.TokenAmount{}, fmt.Errorf("initial input token does not match stage 1")
		}
		return p.InitialInput, nil
	}
	source, ok := outputs[stage.InputFromOrdinal]
	if !ok || source.IsZero() {
		return market.TokenAmount{}, fmt.Errorf(
			"stage %d awaits settlement %d", stage.Ordinal, stage.InputFromOrdinal,
		)
	}
	if source.Token() == stage.InputToken {
		return source, nil
	}
	// The executor delegates chain-local identity/decimal conversion to the
	// selected stage driver. Returning the immutable source settlement here
	// keeps that conversion explicit and deterministic.
	return source, nil
}

// ParallelSellInput fixes the sale input from discovery and the configured
// execution notional. It never depends on the realized purchase output.
func (p SequentialPlan) ParallelSellInput() (market.TokenAmount, error) {
	if p.EffectivePolicy() != PolicyPrefundedParallel ||
		p.Opportunity.SelectedIndex < 0 ||
		p.Opportunity.SelectedIndex >= len(p.Opportunity.Candidates) {
		return market.TokenAmount{}, fmt.Errorf("parallel sell intent is unavailable")
	}
	candidate := p.Opportunity.Candidates[p.Opportunity.SelectedIndex]
	if candidate.BuyQuote.AmountIn.IsZero() ||
		candidate.SellQuote.AmountIn.IsZero() || p.InitialInput.IsZero() {
		return market.TokenAmount{}, fmt.Errorf("parallel discovery amounts are incomplete")
	}
	units := new(big.Int).Quo(
		new(big.Int).Mul(
			candidate.SellQuote.AmountIn.Units(),
			p.InitialInput.Units(),
		),
		candidate.BuyQuote.AmountIn.Units(),
	)
	if units.Sign() <= 0 {
		return market.TokenAmount{}, fmt.Errorf("parallel sell input rounds to zero")
	}
	return market.NewTokenAmount(candidate.SellQuote.AmountIn.Token(), units)
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
	case ExitReturnToOrigin, ExitSellAtOrigin:
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
	plan := SequentialPlan{
		ID: id, Opportunity: opportunity, InitialInput: initialInput,
		Policy:          PolicyTransportedSequential,
		DiscoveryAmount: candidate.BuyQuote.AmountIn,
		Stages:          append([]SequentialStagePlan(nil), stages...),
		CreatedAt:       createdAt.UTC(),
	}
	return plan, plan.Validate()
}

// NewPrefundedSequentialPlan builds the compiled destination-first lifecycle:
// buy -> sell destination -> bridge base -> bridge quote. The terminal origin
// sale is a separate durable branch and consumes the buy settlement directly.
func NewPrefundedSequentialPlan(
	id PlanID,
	opportunity arbitrage.Opportunity,
	initialInput market.TokenAmount,
	buyChain, sellChain market.ChainID,
	createdAt time.Time,
) (SequentialPlan, error) {
	transported, err := NewSequentialPlan(
		id, opportunity, initialInput, buyChain, sellChain, createdAt,
	)
	if err != nil {
		return SequentialPlan{}, err
	}
	candidate := opportunity.Candidates[opportunity.SelectedIndex]
	main := []SequentialStagePlan{
		{
			Ordinal: 1, Stage: StageBuy, Branch: BranchMain,
			SourceChain: buyChain, InputToken: candidate.BuyQuote.AmountIn.Token(),
			OutputToken: candidate.BuyQuote.AmountOut.Token(),
			Market:      opportunity.Direction.BuyMarket,
		},
		{
			Ordinal: 2, Stage: StageSell, Branch: BranchMain,
			DependsOn: []int{1}, InputFromOrdinal: 1,
			SourceChain: sellChain, InputToken: candidate.SellQuote.AmountIn.Token(),
			OutputToken: candidate.SellQuote.AmountOut.Token(),
			Market:      opportunity.Direction.SellMarket,
		},
		{
			Ordinal: 3, Stage: StageBridgeBase, Branch: BranchMain,
			DependsOn: []int{1, 2}, InputFromOrdinal: 1,
			SourceChain: buyChain, DestinationChain: sellChain,
			InputToken:  candidate.BuyQuote.AmountOut.Token(),
			OutputToken: candidate.SellQuote.AmountIn.Token(),
		},
		{
			Ordinal: 4, Stage: StageBridgeQuoteReturn, Branch: BranchMain,
			DependsOn: []int{2, 3}, InputFromOrdinal: 2,
			SourceChain: sellChain, DestinationChain: buyChain,
			InputToken:  candidate.SellQuote.AmountOut.Token(),
			OutputToken: candidate.BuyQuote.AmountIn.Token(),
		},
	}
	circuit := []SequentialStagePlan{{
		Ordinal: 5, Stage: StageSell, Branch: BranchCircuitBreaker,
		DependsOn: []int{1}, InputFromOrdinal: 1,
		SourceChain: buyChain, InputToken: candidate.BuyQuote.AmountOut.Token(),
		OutputToken: candidate.BuyQuote.AmountIn.Token(),
		Market:      opportunity.Direction.BuyMarket,
	}}
	transported.Policy = PolicyPrefundedSequential
	transported.Stages = main
	transported.CircuitBreaker = circuit
	if err := transported.Validate(); err != nil {
		return SequentialPlan{}, err
	}
	return transported, nil
}

func NewPrefundedParallelPlan(
	id PlanID,
	opportunity arbitrage.Opportunity,
	initialInput market.TokenAmount,
	buyChain, sellChain market.ChainID,
	createdAt time.Time,
) (SequentialPlan, error) {
	plan, err := NewPrefundedSequentialPlan(
		id, opportunity, initialInput, buyChain, sellChain, createdAt,
	)
	if err != nil {
		return SequentialPlan{}, err
	}
	plan.Policy = PolicyPrefundedParallel
	plan.Stages[1].DependsOn = nil
	plan.Stages[1].InputFromOrdinal = 0
	plan.Stages[2].DependsOn = []int{1}
	plan.Stages[3].DependsOn = []int{2}
	// Valuation assets and token decimals are supplied by the setup-neutral
	// runtime planner before the final validation.
	return plan, nil
}

type SequentialOperationState string

const (
	SequentialRunning            SequentialOperationState = "running"
	SequentialRecovering         SequentialOperationState = "recovering"
	SequentialRecoveryBlocked    SequentialOperationState = "recovery_blocked"
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
	Request                  SequentialStageRequest
	ActualInput              market.TokenAmount
	ActualOutput             market.TokenAmount
	Costs                    []CostComponent
	SourceIdentity           TransactionIdentity
	DestinationIdentity      *TransactionIdentity
	DestinationBalanceBefore *big.Int
	DestinationBalanceAfter  *big.Int
	ObservedAt               time.Time
	Evidence                 string
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
		if (s.DestinationBalanceBefore == nil) !=
			(s.DestinationBalanceAfter == nil) {
			return fmt.Errorf(
				"bridge stage %d has incomplete destination balance evidence",
				s.Request.Stage.Ordinal,
			)
		}
		if s.DestinationBalanceBefore != nil &&
			(s.DestinationBalanceBefore.Sign() < 0 ||
				s.DestinationBalanceAfter.Cmp(
					s.DestinationBalanceBefore,
				) < 0) {
			return fmt.Errorf(
				"bridge stage %d has invalid destination balance evidence",
				s.Request.Stage.Ordinal,
			)
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
