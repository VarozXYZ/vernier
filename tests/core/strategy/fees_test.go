package strategy_test

import (
	"context"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/adapters/market/constantproduct"
	"github.com/VarozXYZ/vernier/core/sizing"
	"github.com/VarozXYZ/vernier/core/strategy"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

type unmodeledFeeSource struct {
	delegate quoteport.Source
}

func (s unmodeledFeeSource) ID() market.SourceID { return s.delegate.ID() }

func (s unmodeledFeeSource) Quote(ctx context.Context, input quoteport.Input) (market.Quote, error) {
	quote, err := s.delegate.Quote(ctx, input)
	if err != nil {
		return market.Quote{}, err
	}
	amount, _ := market.ParseTokenAmount("third-token", "1")
	fee, _ := market.NewQuoteFee("external", market.QuoteFeeCost, amount, false)
	return market.NewQuote(quote, append(quote.Fees(), fee)...)
}

func TestUnmodeledQuoteFeeClosesClassification(t *testing.T) {
	fixture := newStrategyFixture(t, "1", "0.5")
	setup, err := arbitrage.NewArbitrageSetup("setup", "pair", []market.MarketID{"market-a", "market-b"}, fixture.registry)
	if err != nil {
		t.Fatal(err)
	}
	grid, err := sizing.NewGrid([]market.AssetQuantity{quantity(t, "10")})
	if err != nil {
		t.Fatal(err)
	}
	marketA, _ := fixture.registry.Market("market-a")
	marketB, _ := fixture.registry.Market("market-b")
	sources := map[market.MarketID]quoteport.Source{
		"market-a": unmodeledFeeSource{delegate: constantProductQuoter(t, "local-a", marketA)},
		"market-b": constantProductQuoter(t, "local-b", marketB),
	}
	candidate, err := strategy.NewTwoMarket(strategy.TwoMarketConfig{
		ID: "strategy-fees", Setup: setup, Registry: fixture.registry, Sources: sources,
		Grid: grid, Threshold: quantity(t, "1"), Clock: func() time.Time { return fixture.now.Add(time.Millisecond) },
	})
	if err != nil {
		t.Fatal(err)
	}
	evaluation, err := arbitrage.NewEvaluation(
		"evaluation-fees", "run", "strategy-fees", "config-hash", fixture.snapshots, fixture.cost,
		fixture.now, fixture.now,
	)
	if err != nil {
		t.Fatal(err)
	}
	opportunities, err := candidate.Evaluate(context.Background(), evaluation)
	if err != nil {
		t.Fatal(err)
	}
	if len(opportunities) != 2 {
		t.Fatalf("opportunities = %d, want 2", len(opportunities))
	}
	for _, opportunity := range opportunities {
		if opportunity.Classification != arbitrage.ClassificationUnclassifiable {
			t.Fatalf("unmodeled fee classified as %q", opportunity.Classification)
		}
	}
}

func constantProductQuoter(t *testing.T, id market.SourceID, candidate market.Market) quoteport.Source {
	t.Helper()
	quoter, err := constantproduct.NewQuoter(id, candidate)
	if err != nil {
		t.Fatal(err)
	}
	return quoter
}
