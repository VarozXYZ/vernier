package research_test

import (
	"context"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/core/research"
	"github.com/VarozXYZ/vernier/core/strategy"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
)

type recordingStrategy struct {
	id       arbitrage.StrategyID
	versions []uint64
}

func (s *recordingStrategy) ID() arbitrage.StrategyID { return s.id }

func (s *recordingStrategy) Evaluate(_ context.Context, evaluation arbitrage.Evaluation) ([]arbitrage.Opportunity, error) {
	for _, snapshot := range evaluation.Snapshots() {
		s.versions = append(s.versions, snapshot.Metadata().Version)
	}
	return []arbitrage.Opportunity{{Evaluation: evaluation.ID(), Strategy: s.id}}, nil
}

func TestEvaluatorSharesTheSameSnapshotVersionsAcrossStrategies(t *testing.T) {
	first := &recordingStrategy{id: "first"}
	second := &recordingStrategy{id: "second"}
	evaluator, err := research.NewEvaluator([]strategy.Strategy{first, second})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	snapshots := []market.MarketSnapshot{researchSnapshot(t, "a", 3, now), researchSnapshot(t, "b", 7, now)}
	costAmount, _ := market.ParseAssetQuantity("quote", "0")
	results, err := evaluator.Evaluate(context.Background(), research.EvaluationRequest{
		IDPrefix: "evaluation", Run: "run", ConfigHash: "hash", Snapshots: snapshots,
		Cost:        arbitrage.CostSnapshot{ID: "cost", Amount: costAmount, CapturedAt: now},
		TriggeredAt: now, StartedAt: now, MaxSnapshotAge: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || results[0].Evaluation == results[1].Evaluation {
		t.Fatalf("unexpected evaluation identities: %+v", results)
	}
	if len(first.versions) != 2 || len(second.versions) != 2 || first.versions[0] != second.versions[0] || first.versions[1] != second.versions[1] {
		t.Fatalf("strategies saw different snapshots: %v versus %v", first.versions, second.versions)
	}
}

type researchSnapshotData struct{}

func (researchSnapshotData) SnapshotKind() string { return "test" }

func researchSnapshot(t *testing.T, id market.MarketID, version uint64, now time.Time) market.MarketSnapshot {
	t.Helper()
	snapshot, err := market.NewMarketSnapshot(market.SnapshotMetadata{
		Market: id, Source: "source", Version: version,
		EventPosition: market.SourcePosition{Kind: market.SourcePositionBlock, Value: version},
		Finality:      market.FinalityConfirmed, ReceivedAt: now, AppliedAt: now,
		Health: market.HealthHealthy, HealthChangedAt: now,
	}, researchSnapshotData{})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
