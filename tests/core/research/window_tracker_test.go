package research_test

import (
	"context"
	"testing"
	"time"

	coreresearch "github.com/VarozXYZ/vernier/core/research"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
)

type windowStore struct {
	opened       []arbitrage.OpportunityWindow
	observations []arbitrage.WindowObservation
	closed       []arbitrage.WindowClosing
	failed       []arbitrage.WindowFailure
	finalized    int
}

func (s *windowStore) OpenWindow(_ context.Context, opening arbitrage.WindowOpening) error {
	s.opened = append(s.opened, opening.Window)
	return nil
}
func (s *windowStore) RecordImprovement(_ context.Context, observation arbitrage.WindowObservation) error {
	s.observations = append(s.observations, observation)
	return nil
}
func (s *windowStore) CloseWindow(_ context.Context, closing arbitrage.WindowClosing) error {
	s.closed = append(s.closed, closing)
	return nil
}
func (s *windowStore) FailWindow(_ context.Context, failure arbitrage.WindowFailure) error {
	s.failed = append(s.failed, failure)
	return nil
}
func (s *windowStore) FinalizeDangling(context.Context, time.Time) error { s.finalized++; return nil }
func (s *windowStore) ListWindows(context.Context, arbitrage.WindowQuery) ([]arbitrage.WindowRecord, error) {
	return nil, nil
}
func (s *windowStore) Close() error { return nil }

func TestWindowTrackerOpensImprovesAndClosesWithoutPersistingNonProfitableEvaluations(t *testing.T) {
	clock := func() time.Time { return time.Date(2026, 7, 22, 10, 0, 10, 0, time.UTC) }
	store := &windowStore{}
	tracker, err := coreresearch.NewWindowTrackerWithSession(store, clock, "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := tracker.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.finalized != 1 {
		t.Fatalf("finalize calls: got %d", store.finalized)
	}
	base := opportunity(t, arbitrage.ClassificationEconomic, 10, time.Second)
	if err := tracker.Observe(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	if len(store.opened) != 1 || len(tracker.ActiveWindows()) != 1 {
		t.Fatalf("window was not opened: opened=%d active=%d", len(store.opened), len(tracker.ActiveWindows()))
	}
	qualified := opportunity(t, arbitrage.ClassificationPolicyQualified, 20, 2*time.Second)
	if err := tracker.Observe(context.Background(), qualified); err != nil {
		t.Fatal(err)
	}
	if len(store.observations) != 1 || !store.observations[0].Best {
		t.Fatalf("best observation was not recorded: %+v", store.observations)
	}
	if store.opened[0].Best.NetPnL.String() != "1/10" {
		t.Fatalf("opening best changed unexpectedly: %s", store.opened[0].Best.NetPnL.String())
	}
	if tracker.ActiveWindows()[0].Best.NetPnL.String() != "1/5" {
		t.Fatalf("improved best not retained: %s", tracker.ActiveWindows()[0].Best.NetPnL.String())
	}
	nonProfitable := opportunity(t, arbitrage.ClassificationObservedSpread, 0, 3*time.Second)
	if err := tracker.Observe(context.Background(), nonProfitable); err != nil {
		t.Fatal(err)
	}
	if len(store.closed) != 1 || store.closed[0].Reason != "profitability_lost" || len(tracker.ActiveWindows()) != 0 {
		t.Fatalf("window was not closed: closed=%+v active=%d", store.closed, len(tracker.ActiveWindows()))
	}
}

func TestWindowTrackerMarksWebSocketDegradationAsFailure(t *testing.T) {
	clock := func() time.Time { return time.Date(2026, 7, 22, 10, 0, 10, 0, time.UTC) }
	store := &windowStore{}
	tracker, err := coreresearch.NewWindowTrackerWithSession(store, clock, "session-2")
	if err != nil {
		t.Fatal(err)
	}
	if err := tracker.Observe(context.Background(), opportunity(t, arbitrage.ClassificationEconomic, 10, time.Second)); err != nil {
		t.Fatal(err)
	}
	degraded := opportunity(t, arbitrage.ClassificationUnclassifiable, 0, 2*time.Second)
	degraded.Reasons = []string{"degraded_market_snapshot"}
	if err := tracker.Observe(context.Background(), degraded); err != nil {
		t.Fatal(err)
	}
	if len(store.failed) != 1 || store.failed[0].Reason != "websocket_disconnected" || len(store.closed) != 0 {
		t.Fatalf("degraded window was not failed: failed=%+v closed=%+v", store.failed, store.closed)
	}
}

func opportunity(t *testing.T, classification arbitrage.Classification, net int, offset time.Duration) arbitrage.Opportunity {
	t.Helper()
	base := time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC).Add(offset)
	if net == 0 {
		return arbitrage.Opportunity{
			Evaluation: arbitrage.EvaluationID("evaluation-" + classificationSuffix(classification)), Run: "run-1", Strategy: "strategy-1", ConfigHash: "hash-1",
			Direction: arbitrage.Direction{BuyMarket: "robinhood", SellMarket: "base"}, Classification: classification, SelectedIndex: -1,
			TriggeredAt: base, StartedAt: base, FinishedAt: base, Reasons: []string{"costs_exceed_gross_profit"},
		}
	}
	amount := marketQuantity(t, "WETH", "1")
	gross := marketQuantity(t, "WETH", "0.2")
	netQuantity := marketQuantity(t, "WETH", "0."+string(rune('0'+net/10)))
	return arbitrage.Opportunity{
		Evaluation: arbitrage.EvaluationID("evaluation-" + classificationSuffix(classification)), Run: "run-1", Strategy: "strategy-1", ConfigHash: "hash-1",
		Direction: arbitrage.Direction{BuyMarket: "robinhood", SellMarket: "base"}, Classification: classification, SelectedIndex: 0,
		Candidates:  []arbitrage.Candidate{{Size: amount, GrossPnL: gross, NetPnL: netQuantity, Cost: arbitrage.CostSnapshot{ID: "cost", Amount: marketQuantity(t, "WETH", "0.1"), CapturedAt: base}}},
		TriggeredAt: base, StartedAt: base, FinishedAt: base,
	}
}

func marketQuantity(t *testing.T, asset, value string) market.AssetQuantity {
	t.Helper()
	quantity, err := market.ParseAssetQuantity(market.AssetID(asset), value)
	if err != nil {
		t.Fatal(err)
	}
	return quantity
}

func classificationSuffix(classification arbitrage.Classification) string {
	return string(classification)
}
