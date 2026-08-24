package strategy_test

import (
	"context"
	"crypto/sha256"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/core/strategy"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

type snapshotData struct{}

func (snapshotData) SnapshotKind() string { return "live-test/v1" }

type blockingSource struct {
	id      market.SourceID
	started chan quoteport.Input
	release <-chan struct{}
	output  *big.Int
}

type recordingSource struct {
	id     market.SourceID
	mu     sync.Mutex
	inputs []quoteport.Input
}

func (s *recordingSource) ID() market.SourceID { return s.id }
func (s *recordingSource) Quote(_ context.Context, input quoteport.Input) (market.Quote, error) {
	s.mu.Lock()
	s.inputs = append(s.inputs, input)
	s.mu.Unlock()
	units := big.NewInt(20)
	if strings.HasPrefix(string(input.TokenIn), "base-") {
		units = big.NewInt(105_000_000)
	}
	output, _ := market.NewTokenAmount(input.TokenOut, units)
	return market.NewQuote(market.Quote{Source: s.id, Market: input.Snapshot.Metadata().Market, SnapshotVersion: input.Snapshot.Metadata().Version,
		Purpose: input.Purpose, Mode: market.QuoteModeExactInput, Quality: market.QuoteQualityExact, AmountIn: input.AmountIn, AmountOut: output, QuotedAt: input.QuotedAt})
}

func TestPrefundedParallelUsesTwoRemoteRoutesAndIndependentFixedInputs(t *testing.T) {
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	registry, setup, snapshots := liveRegistry(t, now)
	local := &recordingSource{id: "local"}
	remote := &recordingSource{id: "remote"}
	notional, _ := market.NewAssetQuantity("quote", big.NewRat(100, 1))
	threshold, _ := market.NewAssetQuantity("quote", big.NewRat(1, 10))
	zero, _ := market.NewAssetQuantity("quote", new(big.Rat))
	evaluator, err := strategy.NewPrefundedParallel(strategy.PrefundedParallelConfig{ID: "prefunded", Setup: setup, Registry: registry,
		Sources: map[market.MarketID]quoteport.Source{"market-a": local, "market-b": remote}, Notional: notional, Threshold: threshold,
		ThresholdFixed: zero, ValuationMarket: "market-a", Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	evaluation, err := arbitrage.NewEvaluation("evaluation", "run", "prefunded", "hash", []market.MarketSnapshot{snapshots["market-a"], snapshots["market-b"]},
		arbitrage.CostSnapshot{ID: "fixed", Amount: zero, CapturedAt: now}, now, now)
	if err != nil {
		t.Fatal(err)
	}
	opportunities, _, err := evaluator.EvaluateWithTiming(context.Background(), evaluation)
	if err != nil {
		t.Fatal(err)
	}
	remote.mu.Lock()
	remoteInputs := append([]quoteport.Input(nil), remote.inputs...)
	remote.mu.Unlock()
	if len(remoteInputs) != 2 {
		t.Fatalf("remote routes = %d; want exactly one per direction", len(remoteInputs))
	}
	seenBuy, seenSell := false, false
	for _, input := range remoteInputs {
		if input.AmountIn.String() == "100000000" {
			seenBuy = true
		}
		if input.AmountIn.String() == "20" {
			seenSell = true
		}
	}
	if !seenBuy || !seenSell {
		t.Fatalf("remote inputs = %+v; want independently fixed quote/base inputs", remoteInputs)
	}
	selected := 0
	for _, opportunity := range opportunities {
		if opportunity.SelectedIndex >= 0 {
			selected++
			candidate := opportunity.Candidates[opportunity.SelectedIndex]
			if candidate.Valuation == nil || candidate.SellQuote.AmountIn.String() != "20" {
				t.Fatalf("candidate did not retain valuation/fixed sell input: %+v", candidate)
			}
		}
	}
	if selected != 1 {
		t.Fatalf("selected directions = %d; want one winner", selected)
	}
}

func TestPrefundedParallelSelectsBestEligibleDiscreteSize(t *testing.T) {
	now := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	registry, setup, snapshots := liveRegistry(t, now)
	local := &recordingSource{id: "local"}
	remote := &recordingSource{id: "remote"}
	notionals := make([]market.AssetQuantity, 0, 4)
	for _, value := range []int64{250, 500, 750, 1000} {
		quantity, _ := market.NewAssetQuantity("quote", big.NewRat(value, 1))
		notionals = append(notionals, quantity)
	}
	zero, _ := market.NewAssetQuantity("quote", new(big.Rat))
	evaluator, err := strategy.NewPrefundedParallel(strategy.PrefundedParallelConfig{
		ID: "prefunded", Setup: setup, Registry: registry,
		Sources:   map[market.MarketID]quoteport.Source{"market-a": local, "market-b": remote},
		Notionals: notionals, Threshold: zero, ThresholdFixed: zero,
		ValuationMarket: "market-a", Clock: func() time.Time { return now },
		CandidateEligible: func(_ arbitrage.Direction, candidate arbitrage.Candidate) bool {
			return candidate.Input.Rat().Cmp(big.NewRat(750, 1)) == 0
		},
		CandidateCost: func(_ arbitrage.Direction, input market.AssetQuantity, at time.Time) (arbitrage.CostSnapshot, bool) {
			amount, _ := market.NewAssetQuantity("quote", new(big.Rat).Quo(input.Rat(), big.NewRat(1000, 1)))
			return arbitrage.CostSnapshot{ID: "sized", Amount: amount, CapturedAt: at}, true
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	evaluation, err := arbitrage.NewEvaluation("evaluation", "run", "prefunded", "hash",
		[]market.MarketSnapshot{snapshots["market-a"], snapshots["market-b"]},
		arbitrage.CostSnapshot{ID: "fixed", Amount: zero, CapturedAt: now}, now, now)
	if err != nil {
		t.Fatal(err)
	}
	opportunities, _, err := evaluator.EvaluateWithTiming(context.Background(), evaluation)
	if err != nil {
		t.Fatal(err)
	}
	selectedDirections := 0
	for _, opportunity := range opportunities {
		if len(opportunity.Candidates) != 4 {
			t.Fatalf("candidate count=%d want=4", len(opportunity.Candidates))
		}
		for index, candidate := range opportunity.Candidates {
			want := new(big.Rat).Quo(notionals[index].Rat(), big.NewRat(1000, 1))
			if candidate.Cost.Amount.Rat().Cmp(want) != 0 {
				t.Fatalf("candidate[%d] cost=%s want=%s", index, candidate.Cost.Amount, want)
			}
		}
		if opportunity.SelectedIndex >= 0 {
			selectedDirections++
			if got := opportunity.Candidates[opportunity.SelectedIndex].Input.Rat().RatString(); got != "750" {
				t.Fatalf("selected input=%s want=750", got)
			}
		}
	}
	if selectedDirections != 1 {
		t.Fatalf("selected directions=%d want=1", selectedDirections)
	}
}

func TestLiveEvaluatorNormalizesDistinctQuoteTokensWithFixedConversion(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	registry, setup, snapshots := liveRegistry(t, now)
	evaluator, err := strategy.NewLiveEvaluator(strategy.LiveConfig{Setup: setup, Registry: registry,
		Sources: map[market.MarketID]quoteport.Source{"market-a": &recordingSource{id: "a"}, "market-b": &recordingSource{id: "b"}},
		Clock:   func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	quoteIn, _ := market.NewTokenAmount("quote-a", big.NewInt(100_000_000))
	baseOut, _ := market.NewTokenAmount("base-a", big.NewInt(100))
	baseIn, _ := market.NewTokenAmount("base-b", big.NewInt(100))
	quoteOut, _ := market.NewTokenAmount("quote-b", big.NewInt(102_000_000))
	buy, _ := market.NewQuote(market.Quote{Source: "a", Market: "market-a", SnapshotVersion: 1,
		Purpose: market.QuotePurposeLiveDiscovery, Mode: market.QuoteModeExactInput, Quality: market.QuoteQualityExact,
		AmountIn: quoteIn, AmountOut: baseOut, QuotedAt: now})
	sell, _ := market.NewQuote(market.Quote{Source: "b", Market: "market-b", SnapshotVersion: 2,
		Purpose: market.QuotePurposeLiveDiscovery, Mode: market.QuoteModeExactInput, Quality: market.QuoteQualityExact,
		AmountIn: baseIn, AmountOut: quoteOut, QuotedAt: now})
	fxIn, _ := market.NewTokenAmount("quote-b", big.NewInt(500_000_000))
	fxOut, _ := market.NewTokenAmount("quote-a", big.NewInt(490_000_000))
	fx, _ := market.NewQuoteConversionSnapshot("fx", fxIn, fxOut, now, now.Add(3*time.Second))
	notional, _ := market.NewAssetQuantity("quote", big.NewRat(100, 1))
	zero, _ := market.NewAssetQuantity("quote", new(big.Rat))
	maxCost, _ := market.NewAssetQuantity("quote", big.NewRat(10, 1))
	maxBase, _ := market.NewAssetQuantity("base", big.NewRat(1_000, 1))
	valuation, _ := arbitrage.NewValuationSnapshot(1, "base", "quote", big.NewRat(1, 1), 1, now)
	opportunity, err := evaluator.Value(strategy.LiveEvaluationRequest{ID: "fx", Direction: arbitrage.Direction{BuyMarket: "market-a", SellMarket: "market-b"},
		Snapshots: snapshots, Notional: notional, Valuation: valuation, Cost: zero, Threshold: zero,
		MaximumCost: maxCost, MaximumBaseExposure: maxBase, TriggeredAt: now, QuoteConversion: &fx}, buy, sell, now)
	if err != nil {
		t.Fatal(err)
	}
	if opportunity.GrossPnL.Rat().Cmp(big.NewRat(-1, 25)) != 0 {
		t.Fatalf("normalized gross=%s want=-0.04", opportunity.GrossPnL)
	}
}

func TestPrefundedParallelAppliesDirectionalThresholdsWithoutChangingFallback(t *testing.T) {
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	registry, setup, snapshots := liveRegistry(t, now)
	local := &recordingSource{id: "local"}
	remote := &recordingSource{id: "remote"}
	notional, _ := market.NewAssetQuantity("quote", big.NewRat(100, 1))
	fallback, _ := market.NewAssetQuantity("quote", big.NewRat(3, 4))
	directional, _ := market.NewAssetQuantity("quote", big.NewRat(1, 5))
	zero, _ := market.NewAssetQuantity("quote", new(big.Rat))
	overridden := arbitrage.Direction{BuyMarket: "market-a", SellMarket: "market-b"}
	evaluator, err := strategy.NewPrefundedParallel(strategy.PrefundedParallelConfig{
		ID: "prefunded", Setup: setup, Registry: registry,
		Sources:  map[market.MarketID]quoteport.Source{"market-a": local, "market-b": remote},
		Notional: notional, Threshold: fallback,
		DirectionalThresholds: map[arbitrage.Direction]market.AssetQuantity{overridden: directional},
		ThresholdFixed:        zero, ValuationMarket: "market-a", Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	evaluation, err := arbitrage.NewEvaluation("evaluation", "run", "prefunded", "hash",
		[]market.MarketSnapshot{snapshots["market-a"], snapshots["market-b"]},
		arbitrage.CostSnapshot{ID: "fixed", Amount: zero, CapturedAt: now}, now, now)
	if err != nil {
		t.Fatal(err)
	}
	opportunities, _, err := evaluator.EvaluateWithTiming(context.Background(), evaluation)
	if err != nil {
		t.Fatal(err)
	}
	for _, opportunity := range opportunities {
		if len(opportunity.Candidates) != 1 {
			t.Fatalf("direction %v has no candidate", opportunity.Direction)
		}
		want := fallback
		if opportunity.Direction == overridden {
			want = directional
		}
		if got := opportunity.Candidates[0].EffectiveThreshold; got.Rat().Cmp(want.Rat()) != 0 {
			t.Fatalf("direction %v threshold=%s want=%s", opportunity.Direction, got, want)
		}
	}
}

func (s *blockingSource) ID() market.SourceID { return s.id }
func (s *blockingSource) Quote(ctx context.Context, input quoteport.Input) (market.Quote, error) {
	select {
	case s.started <- input:
	case <-ctx.Done():
		return market.Quote{}, ctx.Err()
	}
	select {
	case <-s.release:
	case <-ctx.Done():
		return market.Quote{}, ctx.Err()
	}
	output, _ := market.NewTokenAmount(input.TokenOut, s.output)
	return market.NewQuote(market.Quote{
		Source: s.id, Market: input.Snapshot.Metadata().Market,
		SnapshotVersion: input.Snapshot.Metadata().Version, SnapshotHash: input.Snapshot.Metadata().StateHash,
		Purpose: input.Purpose, Mode: market.QuoteModeExactInput, Quality: market.QuoteQualityExact,
		AmountIn: input.AmountIn, AmountOut: output, QuotedAt: input.QuotedAt,
	})
}

func TestLiveEvaluatorFixesIndependentInputsAndQuotesConcurrently(t *testing.T) {
	now := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	registry, setup, snapshots := liveRegistry(t, now)
	release := make(chan struct{})
	started := make(chan quoteport.Input, 2)
	buySource := &blockingSource{id: "buy-source", started: started, release: release, output: big.NewInt(14_550)}
	sellSource := &blockingSource{id: "sell-source", started: started, release: release, output: big.NewInt(100_100_000)}
	evaluator, err := strategy.NewLiveEvaluator(strategy.LiveConfig{
		Setup: setup, Registry: registry,
		Sources: map[market.MarketID]quoteport.Source{"market-a": buySource, "market-b": sellSource},
		Clock:   func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	valuation, err := arbitrage.NewValuationSnapshot(1, "base", "quote", big.NewRat(100, 14_500), 2, now)
	if err != nil {
		t.Fatal(err)
	}
	notional, _ := market.NewAssetQuantity("quote", big.NewRat(100, 1))
	cost, _ := market.NewAssetQuantity("quote", big.NewRat(1, 100))
	threshold, _ := market.NewAssetQuantity("quote", big.NewRat(1, 5))
	maximumCost, _ := market.NewAssetQuantity("quote", big.NewRat(1, 1))
	maximumExposure, _ := market.NewAssetQuantity("base", big.NewRat(100, 1))
	request := strategy.LiveEvaluationRequest{
		ID: "evaluation-1", Direction: arbitrage.Direction{BuyMarket: "market-a", SellMarket: "market-b"},
		Snapshots: snapshots, Notional: notional, Valuation: valuation, Cost: cost,
		Threshold: threshold, MaximumCost: maximumCost,
		MaximumBaseExposure: maximumExposure, TriggeredAt: now,
	}
	result := make(chan arbitrage.LiveOpportunity, 1)
	failures := make(chan error, 1)
	go func() {
		opportunity, evaluateErr := evaluator.Evaluate(context.Background(), request)
		if evaluateErr != nil {
			failures <- evaluateErr
			return
		}
		result <- opportunity
	}()
	first := <-started
	second := <-started
	if first.TokenIn == second.TokenIn {
		t.Fatalf("expected buy and sell to start independently, got duplicate token %q", first.TokenIn)
	}
	var buyInput, sellInput market.TokenAmount
	for _, input := range []quoteport.Input{first, second} {
		if input.TokenIn == "quote-a" {
			buyInput = input.AmountIn
		} else {
			sellInput = input.AmountIn
		}
	}
	if buyInput.String() != "100000000" {
		t.Fatalf("buy input = %s", buyInput.String())
	}
	if sellInput.String() != "14500" {
		t.Fatalf("sell input = %s; it must come from cached valuation, not buy output", sellInput.String())
	}
	close(release)
	select {
	case err := <-failures:
		t.Fatal(err)
	case opportunity := <-result:
		if opportunity.SellQuote.AmountIn.String() != "14500" || opportunity.BuyQuote.AmountOut.String() != "14550" {
			t.Fatalf("dependent sell regression: sell=%s buy_out=%s", opportunity.SellQuote.AmountIn, opportunity.BuyQuote.AmountOut)
		}
		if got := opportunity.QuoteDelta.Decimal(6); got != "0.100000" {
			t.Fatalf("quote delta = %s", got)
		}
		if got := opportunity.BaseDelta.Decimal(0); got != "50" {
			t.Fatalf("base delta = %s", got)
		}
		if got := opportunity.GrossPnL.Decimal(6); got != "0.444828" {
			t.Fatalf("gross PnL = %s", got)
		}
		if !opportunity.Profitable() {
			t.Fatal("expected profitable opportunity")
		}
		request.MaximumBaseExposure, _ = market.NewAssetQuantity("base", big.NewRat(10, 1))
		blocked, err := evaluator.Value(request, opportunity.BuyQuote, opportunity.SellQuote, now)
		if err != nil {
			t.Fatal(err)
		}
		if blocked.RiskBlock != "base_exposure_limit" || blocked.Profitable() {
			t.Fatalf("exposure limit did not block execution: %+v", blocked)
		}
	}
}

func TestBaseValuationCacheAveragesLatestObservationsWithoutTTL(t *testing.T) {
	now := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	cache, err := strategy.NewBaseValuationCache("base", "quote")
	if err != nil {
		t.Fatal(err)
	}
	baseA := market.Token{ID: "base-a", Asset: "base", Chain: "a", Decimals: 0, Symbol: "BASE"}
	quoteA := market.Token{ID: "quote-a", Asset: "quote", Chain: "a", Decimals: 0, Symbol: "QUOTE"}
	baseB := market.Token{ID: "base-b", Asset: "base", Chain: "b", Decimals: 0, Symbol: "BASE"}
	quoteB := market.Token{ID: "quote-b", Asset: "quote", Chain: "b", Decimals: 0, Symbol: "QUOTE"}
	observeQuote(t, cache, "a/buy", quoteA, baseA, 100, 20, now)
	observeQuote(t, cache, "b/sell", baseB, quoteB, 10, 60, now)
	snapshot, err := cache.Snapshot(now.Add(24 * time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	// Prices are 5 and 6 quote/base; the mean remains valid regardless of age.
	if snapshot.Price().Cmp(big.NewRat(11, 2)) != 0 {
		t.Fatalf("price = %s", snapshot.Price())
	}
	if snapshot.Observations() != 2 {
		t.Fatalf("observations = %d", snapshot.Observations())
	}
}

func observeQuote(t *testing.T, cache *strategy.BaseValuationCache, key string, tokenIn, tokenOut market.Token, in, out int64, at time.Time) {
	t.Helper()
	amountIn, _ := market.NewTokenAmount(tokenIn.ID, big.NewInt(in))
	amountOut, _ := market.NewTokenAmount(tokenOut.ID, big.NewInt(out))
	quote, err := market.NewQuote(market.Quote{
		Source: market.SourceID(key), Market: market.MarketID(key), SnapshotVersion: 1,
		Purpose: market.QuotePurposeLiveDiscovery, Mode: market.QuoteModeExactInput,
		Quality: market.QuoteQualityExact, AmountIn: amountIn, AmountOut: amountOut, QuotedAt: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Observe(key, quote, tokenIn, tokenOut, at); err != nil {
		t.Fatal(err)
	}
}

func liveRegistry(t *testing.T, now time.Time) (*market.Registry, arbitrage.ArbitrageSetup, map[market.MarketID]market.MarketSnapshot) {
	t.Helper()
	catalog := market.Catalog{
		Chains: []market.Chain{{ID: "a"}, {ID: "b"}},
		Assets: []market.Asset{{ID: "base", Symbol: "BASE"}, {ID: "quote", Symbol: "QUOTE"}},
		Tokens: []market.Token{
			{ID: "base-a", Asset: "base", Chain: "a", Decimals: 0, Symbol: "BASE"},
			{ID: "quote-a", Asset: "quote", Chain: "a", Decimals: 6, Symbol: "QUOTE"},
			{ID: "base-b", Asset: "base", Chain: "b", Decimals: 0, Symbol: "BASE"},
			{ID: "quote-b", Asset: "quote", Chain: "b", Decimals: 6, Symbol: "QUOTE"},
		},
		Venues: []market.Venue{{ID: "venue-a"}, {ID: "venue-b"}},
		Pairs:  []market.Pair{{ID: "pair", BaseAsset: "base", QuoteAsset: "quote"}},
		Pools: []market.Pool{
			{ID: "pool-a", Venue: "venue-a", Chain: "a", Tokens: []market.TokenID{"base-a", "quote-a"}, Adapter: "test"},
			{ID: "pool-b", Venue: "venue-b", Chain: "b", Tokens: []market.TokenID{"base-b", "quote-b"}, Adapter: "test"},
		},
		Paths: []market.Path{
			{ID: "path-a", Chain: "a", Hops: []market.Hop{{Pool: "pool-a", TokenIn: "base-a", TokenOut: "quote-a"}}},
			{ID: "path-b", Chain: "b", Hops: []market.Hop{{Pool: "pool-b", TokenIn: "base-b", TokenOut: "quote-b"}}},
		},
		Markets: []market.Market{
			{ID: "market-a", Pair: "pair", Chain: "a", Path: "path-a", BaseToken: "base-a", QuoteToken: "quote-a"},
			{ID: "market-b", Pair: "pair", Chain: "b", Path: "path-b", BaseToken: "base-b", QuoteToken: "quote-b"},
		},
	}
	registry, err := market.NewRegistry(catalog)
	if err != nil {
		t.Fatal(err)
	}
	setup, err := arbitrage.NewArbitrageSetup("setup", "pair", []market.MarketID{"market-a", "market-b"}, registry)
	if err != nil {
		t.Fatal(err)
	}
	snapshots := make(map[market.MarketID]market.MarketSnapshot)
	for index, id := range []market.MarketID{"market-a", "market-b"} {
		hash := sha256.Sum256([]byte(id))
		snapshot, snapshotErr := market.NewMarketSnapshot(market.SnapshotMetadata{
			Market: id, Source: market.SourceID(id + "/source"), Version: uint64(index + 1),
			ReceivedAt: now, AppliedAt: now, Health: market.HealthHealthy, HealthChangedAt: now, StateHash: hash,
		}, snapshotData{})
		if snapshotErr != nil {
			t.Fatal(snapshotErr)
		}
		snapshots[id] = snapshot
	}
	return registry, setup, snapshots
}

var _ quoteport.Source = (*blockingSource)(nil)
