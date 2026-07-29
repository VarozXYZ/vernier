// Package arbitrage defines protocol-neutral Research evaluation results.
package arbitrage

import (
	"fmt"
	"time"

	"github.com/VarozXYZ/vernier/domain/market"
)

type (
	SetupID       string
	ResearchRunID string
	StrategyID    string
	EvaluationID  string
)

type Direction struct {
	BuyMarket  market.MarketID
	SellMarket market.MarketID
}

type ArbitrageSetup struct {
	id         SetupID
	pair       market.PairID
	markets    []market.MarketID
	directions []Direction
}

func NewArbitrageSetup(id SetupID, pair market.PairID, markets []market.MarketID, registry *market.Registry) (ArbitrageSetup, error) {
	if id == "" || pair == "" || registry == nil {
		return ArbitrageSetup{}, fmt.Errorf("setup ID, pair, and registry are required")
	}
	if len(markets) < 2 {
		return ArbitrageSetup{}, fmt.Errorf("setup requires at least two markets")
	}
	seen := make(map[market.MarketID]struct{}, len(markets))
	for _, marketID := range markets {
		candidate, ok := registry.Market(marketID)
		if !ok {
			return ArbitrageSetup{}, fmt.Errorf("setup references unknown market %q", marketID)
		}
		if candidate.Pair != pair {
			return ArbitrageSetup{}, fmt.Errorf("market %q does not use pair %q", marketID, pair)
		}
		if _, duplicate := seen[marketID]; duplicate {
			return ArbitrageSetup{}, fmt.Errorf("setup repeats market %q", marketID)
		}
		seen[marketID] = struct{}{}
	}

	directions := make([]Direction, 0, len(markets)*(len(markets)-1))
	for _, buy := range markets {
		for _, sell := range markets {
			if buy != sell {
				directions = append(directions, Direction{BuyMarket: buy, SellMarket: sell})
			}
		}
	}
	return ArbitrageSetup{id: id, pair: pair, markets: append([]market.MarketID(nil), markets...), directions: directions}, nil
}

func (s ArbitrageSetup) ID() SetupID         { return s.id }
func (s ArbitrageSetup) Pair() market.PairID { return s.pair }
func (s ArbitrageSetup) Markets() []market.MarketID {
	return append([]market.MarketID(nil), s.markets...)
}
func (s ArbitrageSetup) Directions() []Direction { return append([]Direction(nil), s.directions...) }

type CostSnapshot struct {
	ID         string
	Amount     market.AssetQuantity
	CapturedAt time.Time
}

type Evaluation struct {
	id          EvaluationID
	run         ResearchRunID
	strategy    StrategyID
	configHash  string
	snapshots   []market.MarketSnapshot
	cost        CostSnapshot
	costs       map[Direction]CostSnapshot
	trigger     TriggerMetadata
	hasTrigger  bool
	triggeredAt time.Time
	startedAt   time.Time
}

func NewEvaluation(id EvaluationID, run ResearchRunID, strategy StrategyID, configHash string, snapshots []market.MarketSnapshot, cost CostSnapshot, triggeredAt, startedAt time.Time) (Evaluation, error) {
	if id == "" || run == "" || strategy == "" || configHash == "" {
		return Evaluation{}, fmt.Errorf("evaluation identity and config hash are required")
	}
	if len(snapshots) < 2 {
		return Evaluation{}, fmt.Errorf("evaluation requires at least two snapshots")
	}
	if cost.ID == "" || cost.Amount.Asset() == "" || cost.CapturedAt.IsZero() {
		return Evaluation{}, fmt.Errorf("valid cost snapshot is required")
	}
	if triggeredAt.IsZero() || startedAt.IsZero() {
		return Evaluation{}, fmt.Errorf("evaluation timestamps are required")
	}
	seen := make(map[market.MarketID]struct{}, len(snapshots))
	for _, snapshot := range snapshots {
		marketID := snapshot.Metadata().Market
		if _, duplicate := seen[marketID]; duplicate {
			return Evaluation{}, fmt.Errorf("duplicate snapshot for market %q", marketID)
		}
		seen[marketID] = struct{}{}
	}
	return Evaluation{
		id: id, run: run, strategy: strategy, configHash: configHash,
		snapshots: append([]market.MarketSnapshot(nil), snapshots...), cost: cost,
		triggeredAt: triggeredAt.UTC(), startedAt: startedAt.UTC(),
	}, nil
}

func (e Evaluation) ID() EvaluationID     { return e.id }
func (e Evaluation) Run() ResearchRunID   { return e.run }
func (e Evaluation) Strategy() StrategyID { return e.strategy }
func (e Evaluation) ConfigHash() string   { return e.configHash }
func (e Evaluation) Cost() CostSnapshot   { return e.cost }

// CostFor returns the cost snapshot for one complete arbitrage direction.
// Configurations without a directional cost oracle retain the common snapshot
// supplied to NewEvaluation.
func (e Evaluation) CostFor(direction Direction) CostSnapshot {
	if cost, ok := e.costs[direction]; ok {
		return cost
	}
	return e.cost
}
func (e Evaluation) TriggeredAt() time.Time { return e.triggeredAt }
func (e Evaluation) StartedAt() time.Time   { return e.startedAt }
func (e Evaluation) Trigger() (TriggerMetadata, bool) {
	return e.trigger, e.hasTrigger
}

// WithTrigger returns an evaluation carrying the feed event that caused it.
// Point-in-time evaluations intentionally have no trigger. The copy keeps the
// evaluation immutable to strategies and persistence consumers.
func (e Evaluation) WithTrigger(trigger TriggerMetadata) Evaluation {
	trigger.At = trigger.At.UTC()
	return Evaluation{id: e.id, run: e.run, strategy: e.strategy, configHash: e.configHash,
		snapshots: append([]market.MarketSnapshot(nil), e.snapshots...), cost: e.cost, costs: cloneCosts(e.costs),
		trigger: trigger, hasTrigger: true, triggeredAt: e.triggeredAt, startedAt: e.startedAt}
}

// WithDirectionalCosts returns an immutable evaluation copy with one cost for
// every supplied direction. All costs must use the same quote asset as the
// common fallback snapshot.
func (e Evaluation) WithDirectionalCosts(costs map[Direction]CostSnapshot) (Evaluation, error) {
	for direction, cost := range costs {
		if direction.BuyMarket == "" || direction.SellMarket == "" ||
			direction.BuyMarket == direction.SellMarket {
			return Evaluation{}, fmt.Errorf("directional cost has an invalid direction")
		}
		if cost.ID == "" || cost.Amount.Asset() == "" || cost.CapturedAt.IsZero() {
			return Evaluation{}, fmt.Errorf("directional cost for %s -> %s is invalid", direction.BuyMarket, direction.SellMarket)
		}
		if cost.Amount.Asset() != e.cost.Amount.Asset() {
			return Evaluation{}, fmt.Errorf("directional cost asset does not match common cost asset")
		}
	}
	e.snapshots = append([]market.MarketSnapshot(nil), e.snapshots...)
	e.costs = cloneCosts(costs)
	return e, nil
}

func cloneCosts(source map[Direction]CostSnapshot) map[Direction]CostSnapshot {
	if len(source) == 0 {
		return nil
	}
	result := make(map[Direction]CostSnapshot, len(source))
	for direction, cost := range source {
		result[direction] = cost
	}
	return result
}
func (e Evaluation) Snapshots() []market.MarketSnapshot {
	return append([]market.MarketSnapshot(nil), e.snapshots...)
}

func (e Evaluation) Snapshot(id market.MarketID) (market.MarketSnapshot, bool) {
	for _, snapshot := range e.snapshots {
		if snapshot.Metadata().Market == id {
			return snapshot, true
		}
	}
	return market.MarketSnapshot{}, false
}

type Classification string

const (
	ClassificationNoSpread         Classification = "no_spread"
	ClassificationObservedSpread   Classification = "observed_spread"
	ClassificationEconomic         Classification = "economic"
	ClassificationPolicyQualified  Classification = "policy_qualified"
	ClassificationUnclassifiable   Classification = "unclassifiable"
	ClassificationExecutable       Classification = "executable"
	ClassificationModeledCandidate Classification = "modeled_execution_candidate"
)

type Candidate struct {
	Size      market.AssetQuantity
	Input     market.AssetQuantity
	Output    market.AssetQuantity
	GrossPnL  market.AssetQuantity
	Cost      CostSnapshot
	NetPnL    market.AssetQuantity
	BuyQuote  market.Quote
	SellQuote market.Quote
}

type Opportunity struct {
	Evaluation     EvaluationID
	Run            ResearchRunID
	ConfigHash     string
	Strategy       StrategyID
	Direction      Direction
	Classification Classification
	Snapshots      []market.SnapshotMetadata
	Candidates     []Candidate
	SelectedIndex  int
	Threshold      market.AssetQuantity
	Reasons        []string
	Trigger        TriggerMetadata
	HasTrigger     bool
	TriggeredAt    time.Time
	StartedAt      time.Time
	FinishedAt     time.Time
}
