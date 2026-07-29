package strategy_test

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/core/strategy"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

type remoteQuoteSource struct {
	id          market.SourceID
	market      market.MarketID
	buyOut      *big.Int
	sellOut     *big.Int
	arrived     chan<- struct{}
	release     <-chan struct{}
	sellArrived chan<- struct{}
	mu          sync.Mutex
	calls       int
	buyCalls    int
	sellCalls   int
	sellIn      *big.Int
	failOnce    bool
	status      int
}

func (s *remoteQuoteSource) ID() market.SourceID { return s.id }
func (*remoteQuoteSource) CacheQuotes() bool     { return false }
func (s *remoteQuoteSource) Quote(ctx context.Context, input quoteport.Input) (market.Quote, error) {
	isBuy := input.TokenOut == "base-a" || input.TokenOut == "base-b"
	s.mu.Lock()
	s.calls++
	if isBuy {
		s.buyCalls++
	} else {
		s.sellCalls++
	}
	fail := s.failOnce && isBuy
	if fail {
		s.failOnce = false
	}
	s.mu.Unlock()
	if fail {
		return market.Quote{}, statusError{s.status}
	}
	outputUnits := s.buyOut
	if isBuy {
		if s.arrived != nil {
			s.arrived <- struct{}{}
			select {
			case <-ctx.Done():
				return market.Quote{}, ctx.Err()
			case <-s.release:
			}
		}
	} else {
		outputUnits = s.sellOut
		s.mu.Lock()
		s.sellIn = input.AmountIn.Units()
		s.mu.Unlock()
		if s.sellArrived != nil {
			s.sellArrived <- struct{}{}
		}
	}
	output, err := market.NewTokenAmount(input.TokenOut, outputUnits)
	if err != nil {
		return market.Quote{}, err
	}
	metadata := input.Snapshot.Metadata()
	return market.NewQuote(market.Quote{
		Source: s.id, Market: s.market, SnapshotVersion: metadata.Version,
		SnapshotHash: metadata.StateHash, SourcePosition: metadata.EventPosition,
		Purpose: input.Purpose, Mode: market.QuoteModeExactInput,
		AmountIn: input.AmountIn, AmountOut: output, QuotedAt: input.QuotedAt,
	})
}

type statusError struct{ code int }

func (e statusError) Error() string       { return fmt.Sprintf("http %d", e.code) }
func (e statusError) HTTPStatusCode() int { return e.code }

func TestBestBuyOppositeSellEvaluatesBothRoundTripsAndSelectsBestFinalOutput(t *testing.T) {
	now := time.Date(2026, 7, 27, 10, 0, 0, 0, time.UTC)
	arrived := make(chan struct{}, 2)
	release := make(chan struct{})
	// Market A has the better buy output, but buying on B and selling on A
	// produces more final quote asset and must therefore win.
	left := &remoteQuoteSource{
		id: "provider-a", market: "market-a",
		buyOut: big.NewInt(14_550_000_000), sellOut: big.NewInt(753_000_000),
		arrived: arrived, release: release,
	}
	right := &remoteQuoteSource{
		id: "provider-b", market: "market-b",
		buyOut: big.NewInt(1_450_000_000_000), sellOut: big.NewInt(751_000_000),
		arrived: arrived, release: release,
	}
	candidate, evaluation := bestBuyFixture(t, now, left, right)

	type evaluationResult struct {
		opportunities []arbitrage.Opportunity
		timing        strategy.EvaluationTiming
	}
	done := make(chan evaluationResult, 1)
	go func() {
		opportunities, timing, err := candidate.EvaluateWithTiming(context.Background(), evaluation)
		if err != nil {
			t.Error(err)
		}
		done <- evaluationResult{opportunities: opportunities, timing: timing}
	}()
	for count := 0; count < 2; count++ {
		select {
		case <-arrived:
		case <-time.After(time.Second):
			t.Fatal("buy quotes did not start concurrently")
		}
	}
	close(release)
	result := <-done
	opportunities := result.opportunities
	if left.calls != 2 || right.calls != 2 || left.buyCalls != 1 || right.buyCalls != 1 ||
		left.sellCalls != 1 || right.sellCalls != 1 {
		t.Fatalf("calls: left=%d (buy=%d sell=%d) right=%d (buy=%d sell=%d), want one complete path per direction",
			left.calls, left.buyCalls, left.sellCalls, right.calls, right.buyCalls, right.sellCalls)
	}
	if right.sellIn == nil || right.sellIn.String() != "1455000000000" {
		t.Fatalf("A->B cross-decimal conversion did not floor correctly: %v", right.sellIn)
	}
	if left.sellIn == nil || left.sellIn.String() != "14500000000" {
		t.Fatalf("B->A cross-decimal conversion did not floor correctly: %v", left.sellIn)
	}
	if opportunities[0].Classification != arbitrage.ClassificationNoSpread ||
		opportunities[1].Classification != arbitrage.ClassificationPolicyQualified {
		t.Fatalf("unexpected directional classifications: %s %s", opportunities[0].Classification, opportunities[1].Classification)
	}
	if opportunities[0].SelectedIndex != -1 {
		t.Fatalf("lower-output route remained selected: %+v", opportunities[0])
	}
	selected := opportunities[1].Candidates[opportunities[1].SelectedIndex]
	if selected.GrossPnL.String() != "3" || selected.NetPnL.String() != "2" {
		t.Fatalf("unexpected PnL at threshold: gross=%s net=%s", selected.GrossPnL.String(), selected.NetPnL.String())
	}
	var quoteTimings []strategy.QuoteTiming
	for _, direction := range result.timing.Directions {
		quoteTimings = append(quoteTimings, direction.Quotes...)
	}
	if len(quoteTimings) != 4 {
		t.Fatalf("quote timings=%d, want two complete buy-then-sell paths", len(quoteTimings))
	}
	for _, quote := range quoteTimings {
		if quote.Source == "" || quote.AmountIn.Token() == "" || quote.AmountOut.Token() == "" ||
			quote.Input.Asset() == "" || quote.Output.Asset() == "" {
			t.Fatalf("quote timing omitted economic amounts: %+v", quote)
		}
	}
}

func TestBestBuyOppositeSellDoesNotWaitForBothBuysBeforeStartingReadySell(t *testing.T) {
	now := time.Date(2026, 7, 27, 10, 0, 0, 0, time.UTC)
	slowBuyStarted := make(chan struct{}, 1)
	releaseSlowBuy := make(chan struct{})
	dependentSellStarted := make(chan struct{}, 1)
	left := &remoteQuoteSource{
		id: "provider-a", market: "market-a",
		buyOut: big.NewInt(14_550_000_000), sellOut: big.NewInt(753_000_000),
	}
	right := &remoteQuoteSource{
		id: "provider-b", market: "market-b",
		buyOut: big.NewInt(1_450_000_000_000), sellOut: big.NewInt(751_000_000),
		arrived: slowBuyStarted, release: releaseSlowBuy, sellArrived: dependentSellStarted,
	}
	candidate, evaluation := bestBuyFixture(t, now, left, right)
	done := make(chan error, 1)
	go func() {
		_, _, err := candidate.EvaluateWithTiming(context.Background(), evaluation)
		done <- err
	}()

	select {
	case <-slowBuyStarted:
	case <-time.After(time.Second):
		t.Fatal("slow opposite buy did not start")
	}
	select {
	case <-dependentSellStarted:
		// The A->B route advanced to its sell while the B->A buy remained blocked.
	case <-time.After(time.Second):
		t.Fatal("ready route waited for the opposite buy before starting its sell")
	}
	close(releaseSlowBuy)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestBestBuyOppositeSellThresholdAndSingleTransientRetry(t *testing.T) {
	for name, sellOut := range map[string]struct {
		units int64
		class arbitrage.Classification
	}{
		"below": {751_999_999, arbitrage.ClassificationEconomic},
		"equal": {752_000_000, arbitrage.ClassificationPolicyQualified},
	} {
		t.Run(name, func(t *testing.T) {
			now := time.Date(2026, 7, 27, 10, 0, 0, 0, time.UTC)
			left := &remoteQuoteSource{
				id: "provider-a", market: "market-a", buyOut: big.NewInt(14_550_000_000),
				sellOut: big.NewInt(700_000_000), failOnce: true, status: 429,
			}
			right := &remoteQuoteSource{
				id: "provider-b", market: "market-b", buyOut: big.NewInt(1_450_000_000_000),
				sellOut: big.NewInt(sellOut.units),
			}
			candidate, evaluation := bestBuyFixture(t, now, left, right)
			opportunities, _, err := candidate.EvaluateWithTiming(context.Background(), evaluation)
			if err != nil {
				t.Fatal(err)
			}
			if left.buyCalls != 2 || left.sellCalls != 1 {
				t.Fatalf("temporary buy attempts=%d sell calls=%d, want one buy retry and one opposite-path sell", left.buyCalls, left.sellCalls)
			}
			if opportunities[0].Classification != sellOut.class {
				t.Fatalf("classification=%s, want %s", opportunities[0].Classification, sellOut.class)
			}
		})
	}
}

func TestBestBuyOppositeSellDoesNotSelectEqualRoundTripOutputs(t *testing.T) {
	now := time.Date(2026, 7, 27, 10, 0, 0, 0, time.UTC)
	left := &remoteQuoteSource{
		id: "provider-a", market: "market-a",
		buyOut: big.NewInt(14_550_000_000), sellOut: big.NewInt(752_000_000),
	}
	right := &remoteQuoteSource{
		id: "provider-b", market: "market-b",
		buyOut: big.NewInt(1_450_000_000_000), sellOut: big.NewInt(752_000_000),
	}
	candidate, evaluation := bestBuyFixture(t, now, left, right)
	opportunities, _, err := candidate.EvaluateWithTiming(context.Background(), evaluation)
	if err != nil {
		t.Fatal(err)
	}
	for _, opportunity := range opportunities {
		if opportunity.SelectedIndex != -1 ||
			opportunity.Classification != arbitrage.ClassificationNoSpread ||
			len(opportunity.Reasons) != 1 ||
			opportunity.Reasons[0] != "equal_round_trip_output" {
			t.Fatalf("equal routes should not select a direction: %+v", opportunities)
		}
	}
}

func TestBestBuyOppositeSellUsesTheCostOfEachCompleteDirection(t *testing.T) {
	now := time.Date(2026, 7, 27, 10, 0, 0, 0, time.UTC)
	left := &remoteQuoteSource{
		id: "provider-a", market: "market-a",
		buyOut: big.NewInt(14_550_000_000), sellOut: big.NewInt(753_000_000),
	}
	right := &remoteQuoteSource{
		id: "provider-b", market: "market-b",
		buyOut: big.NewInt(1_450_000_000_000), sellOut: big.NewInt(752_500_000),
	}
	candidate, evaluation := bestBuyFixture(t, now, left, right)
	leftDirection := arbitrage.Direction{BuyMarket: "market-a", SellMarket: "market-b"}
	rightDirection := arbitrage.Direction{BuyMarket: "market-b", SellMarket: "market-a"}
	var err error
	evaluation, err = evaluation.WithDirectionalCosts(map[arbitrage.Direction]arbitrage.CostSnapshot{
		leftDirection: {
			ID: "flow/a-b", Amount: quantity(t, "0.25"), CapturedAt: now,
		},
		rightDirection: {
			ID: "flow/b-a", Amount: quantity(t, "2"), CapturedAt: now,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	opportunities, _, err := candidate.EvaluateWithTiming(context.Background(), evaluation)
	if err != nil {
		t.Fatal(err)
	}
	for _, opportunity := range opportunities {
		if len(opportunity.Candidates) != 1 {
			t.Fatalf("missing candidate for %v", opportunity.Direction)
		}
		selected := opportunity.Candidates[0]
		switch opportunity.Direction {
		case leftDirection:
			if selected.Cost.Amount.String() != "1/4" ||
				selected.NetPnL.String() != "9/4" {
				t.Fatalf("left cost/net = %s/%s", selected.Cost.Amount, selected.NetPnL)
			}
		case rightDirection:
			if selected.Cost.Amount.String() != "2" ||
				selected.NetPnL.String() != "1" {
				t.Fatalf("right cost/net = %s/%s", selected.Cost.Amount, selected.NetPnL)
			}
		}
	}
}

func TestBestBuyOppositeSellDoesNotRetryPermanentClientError(t *testing.T) {
	now := time.Date(2026, 7, 27, 10, 0, 0, 0, time.UTC)
	left := &remoteQuoteSource{
		id: "provider-a", market: "market-a", buyOut: big.NewInt(14_550_000_000),
		sellOut: big.NewInt(700_000_000), failOnce: true, status: 400,
	}
	right := &remoteQuoteSource{
		id: "provider-b", market: "market-b", buyOut: big.NewInt(1_450_000_000_000),
		sellOut: big.NewInt(752_000_000),
	}
	candidate, evaluation := bestBuyFixture(t, now, left, right)
	opportunities, _, err := candidate.EvaluateWithTiming(context.Background(), evaluation)
	if err != nil {
		t.Fatal(err)
	}
	if left.buyCalls != 1 {
		t.Fatalf("permanent buy error attempts=%d, want no retry", left.buyCalls)
	}
	for _, opportunity := range opportunities {
		if opportunity.Classification != arbitrage.ClassificationUnclassifiable {
			t.Fatalf("missing buy should make both directions unclassifiable: %+v", opportunities)
		}
	}
}

func bestBuyFixture(t *testing.T, now time.Time, left, right quoteport.Source) (*strategy.BestBuyOppositeSell, arbitrage.Evaluation) {
	t.Helper()
	registry := strategyRegistry(t)
	setup, err := arbitrage.NewArbitrageSetup("setup", "pair", []market.MarketID{"market-a", "market-b"}, registry)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := strategy.NewBestBuyOppositeSell(strategy.BestBuyOppositeSellConfig{
		ID: "strategy", Setup: setup, Registry: registry,
		Sources:  map[market.MarketID]quoteport.Source{"market-a": left, "market-b": right},
		Notional: quantity(t, "750"), Threshold: quantity(t, "1"), Clock: time.Now,
		Retries: 1, RetryDelay: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	evaluation, err := arbitrage.NewEvaluation(
		"evaluation", "run", "strategy", "config-hash",
		[]market.MarketSnapshot{
			strategySnapshot(t, "market-a", "1000000000", "1800000000", now),
			strategySnapshot(t, "market-b", "100000000000", "2200000000", now),
		},
		arbitrage.CostSnapshot{ID: "fixed", Amount: quantity(t, "1"), CapturedAt: now},
		now, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	return candidate, evaluation
}
