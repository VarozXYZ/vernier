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
	id             EvaluationID
	run            ResearchRunID
	strategy       StrategyID
	configHash     string
	snapshots      []market.MarketSnapshot
	cost           CostSnapshot
	triggeredAt    time.Time
	startedAt      time.Time
	maxSnapshotAge time.Duration
}

func NewEvaluation(id EvaluationID, run ResearchRunID, strategy StrategyID, configHash string, snapshots []market.MarketSnapshot, cost CostSnapshot, triggeredAt, startedAt time.Time, maxAge time.Duration) (Evaluation, error) {
	if id == "" || run == "" || strategy == "" || configHash == "" {
		return Evaluation{}, fmt.Errorf("evaluation identity and config hash are required")
	}
	if len(snapshots) < 2 {
		return Evaluation{}, fmt.Errorf("evaluation requires at least two snapshots")
	}
	if cost.ID == "" || cost.Amount.Asset() == "" || cost.CapturedAt.IsZero() {
		return Evaluation{}, fmt.Errorf("valid cost snapshot is required")
	}
	if triggeredAt.IsZero() || startedAt.IsZero() || maxAge < 0 {
		return Evaluation{}, fmt.Errorf("evaluation timestamps and non-negative max age are required")
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
		triggeredAt: triggeredAt.UTC(), startedAt: startedAt.UTC(), maxSnapshotAge: maxAge,
	}, nil
}

func (e Evaluation) ID() EvaluationID              { return e.id }
func (e Evaluation) Run() ResearchRunID            { return e.run }
func (e Evaluation) Strategy() StrategyID          { return e.strategy }
func (e Evaluation) ConfigHash() string            { return e.configHash }
func (e Evaluation) Cost() CostSnapshot            { return e.cost }
func (e Evaluation) TriggeredAt() time.Time        { return e.triggeredAt }
func (e Evaluation) StartedAt() time.Time          { return e.startedAt }
func (e Evaluation) MaxSnapshotAge() time.Duration { return e.maxSnapshotAge }
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
	Input     market.AssetQuantity
	Output    market.AssetQuantity
	GrossPnL  market.AssetQuantity
	Cost      market.AssetQuantity
	NetPnL    market.AssetQuantity
	BuyQuote  market.Quote
	SellQuote market.Quote
}

type Opportunity struct {
	Evaluation     EvaluationID
	Strategy       StrategyID
	Direction      Direction
	Classification Classification
	Candidates     []Candidate
	SelectedIndex  int
	Threshold      market.AssetQuantity
	Reasons        []string
	FinishedAt     time.Time
}
