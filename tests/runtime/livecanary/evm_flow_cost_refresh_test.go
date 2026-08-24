package livecanary_test

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	evmadapter "github.com/VarozXYZ/vernier/adapters/chain/evm"
	acrossadapter "github.com/VarozXYZ/vernier/adapters/crosschain/across"
	sqlitestore "github.com/VarozXYZ/vernier/adapters/persistence/sqlite"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
	"github.com/VarozXYZ/vernier/runtime/configuration"
	"github.com/VarozXYZ/vernier/runtime/livecanary"
)

type evmCalibrationFake struct{ empty bool }

func (f evmCalibrationFake) LatestEVMFlowTransactions(_ context.Context, buy, sell market.MarketID,
	ordinal int, phase string, _ int) ([]sqlitestore.EVMFlowTransaction, error) {
	if f.empty {
		return nil, nil
	}
	chainFor := func(id market.MarketID) market.ChainID {
		if id == "market-b" {
			return "chain-b"
		}
		return "chain-a"
	}
	chain := chainFor(buy)
	if ordinal == 2 || phase == "wtt_redeem" || phase == "across_source" {
		chain = chainFor(sell)
	}
	return []sqlitestore.EVMFlowTransaction{{Chain: chain,
		Identity: string(buy) + "/" + string(sell) + "/" + phase, UpdatedAt: time.Now().UTC()}}, nil
}

type evmFeeFake struct {
	mu    sync.Mutex
	calls int
	now   time.Time
}

func (f *evmFeeFake) FeeSnapshot() (evmadapter.FeeSnapshot, bool) {
	return evmadapter.FeeSnapshot{BaseFee: big.NewInt(1_000_000_000), TipCap: big.NewInt(100_000_000),
		FeeCap: big.NewInt(2_000_000_000), CapturedAt: f.now}, true
}

func (f *evmFeeFake) ConfirmedGasUsed(context.Context, string) (uint64, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	return 200_000, nil
}

type evmAcrossFake struct{ omitGas bool }

func (f evmAcrossFake) CostApproval(_ context.Context, source market.ChainID, input *big.Int) (acrossadapter.EVMCostApproval, error) {
	var output *big.Int
	if source == "chain-a" {
		output = new(big.Int).Mul(new(big.Int).Sub(input, big.NewInt(500_000)), big.NewInt(1_000_000_000_000))
	} else {
		output = new(big.Int).Quo(input, big.NewInt(1_000_000_000_000))
		output.Sub(output, big.NewInt(500_000))
	}
	gas := uint64(175_000)
	if f.omitGas {
		gas = 0
	}
	return acrossadapter.EVMCostApproval{InputUnits: new(big.Int).Set(input), ExpectedOutputUnits: output,
		SourceGas: gas, ObservedAt: time.Date(2026, 8, 13, 8, 0, 0, 0, time.UTC)}, nil
}

type evmWTTFake struct{ now time.Time }

func (f evmWTTFake) MessageFee(context.Context, market.ChainID) (*big.Int, time.Time, error) {
	return big.NewInt(10_000_000_000_000), f.now, nil
}

func (evmWTTFake) EstimateTransferGas(context.Context, market.ChainID, market.ChainID, market.TokenAmount) (uint64, error) {
	return 180_000, nil
}

func (evmWTTFake) RedemptionGasFloor(market.ChainID) (uint64, error) { return 300_000, nil }

type fixedConversionBook struct {
	snapshot market.QuoteConversionSnapshot
}

func (b fixedConversionBook) Snapshot(input, output market.TokenID, at time.Time) (market.QuoteConversionSnapshot, bool) {
	return b.snapshot, b.snapshot.Input.Token() == input && b.snapshot.Output.Token() == output && b.snapshot.ValidAt(at)
}

func TestEVMObservedFlowCostsUseReceiptsCurrentCachesAndAcrossSpread(t *testing.T) {
	now := time.Date(2026, 8, 13, 8, 0, 0, 0, time.UTC)
	a, b := market.MarketID("market-a"), market.MarketID("market-b")
	quoteA := market.Token{ID: "quote-a", Asset: "quote", Chain: "chain-a", Decimals: 6}
	quoteB := market.Token{ID: "quote-b", Asset: "quote", Chain: "chain-b", Decimals: 18}
	baseA := market.Token{ID: "base-a", Asset: "base", Chain: "chain-a", Decimals: 18}
	baseB := market.Token{ID: "base-b", Asset: "base", Chain: "chain-b", Decimals: 18}
	markets := map[market.MarketID]configuration.ResolvedMarket{
		a: {ID: a, Chain: "chain-a", Base: configuration.ResolvedToken{Token: baseA}, Quote: configuration.ResolvedToken{Token: quoteA}},
		b: {ID: b, Chain: "chain-b", Base: configuration.ResolvedToken{Token: baseB}, Quote: configuration.ResolvedToken{Token: quoteB}},
	}
	valuator, err := livecanary.NewCostValuator("quote", func(context.Context) (map[market.AssetID]livecanary.CostAssetPrice, error) {
		return map[market.AssetID]livecanary.CostAssetPrice{
			"native-a": {Value: big.NewRat(2_000, 1), CapturedAt: now, Source: "test"},
			"native-b": {Value: big.NewRat(500, 1), CapturedAt: now, Source: "test"},
		}, nil
	}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if err := valuator.Warm(context.Background()); err != nil {
		t.Fatal(err)
	}
	feeA, feeB := &evmFeeFake{now: now}, &evmFeeFake{now: now}
	refresh, err := livecanary.NewEVMObservedFlowCostRefresh(livecanary.EVMObservedFlowCostRefreshConfig{
		Markets: markets, Valuator: valuator, Calibration: evmCalibrationFake{}, Across: evmAcrossFake{}, WTT: evmWTTFake{now: now},
		Fees:           map[market.ChainID]livecanary.EVMFeeCostSource{"chain-a": feeA, "chain-b": feeB},
		NativeAssets:   map[market.ChainID]market.AssetID{"chain-a": "native-a", "chain-b": "native-b"},
		NativeDecimals: map[market.ChainID]uint8{"chain-a": 18, "chain-b": 18},
		RemoteSwapGas:  map[market.MarketID]uint64{b: 1_000_000}, CalibrationTTL: time.Minute,
		Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	directions := []arbitrage.Direction{{BuyMarket: a, SellMarket: b}, {BuyMarket: b, SellMarket: a}}
	opportunities := make([]arbitrage.Opportunity, 0, 2)
	for _, direction := range directions {
		token, units := quoteB.ID, new(big.Int).Mul(big.NewInt(500), big.NewInt(1_000_000_000_000_000_000))
		if direction.SellMarket == a {
			token, units = quoteA.ID, big.NewInt(500_000_000)
		}
		amount, _ := market.NewTokenAmount(token, units)
		baseToken := baseA.ID
		if direction.BuyMarket == b {
			baseToken = baseB.ID
		}
		baseAmount, _ := market.NewTokenAmount(baseToken, big.NewInt(500_000_000_000_000_000))
		opportunities = append(opportunities, arbitrage.Opportunity{Direction: direction,
			Candidates: []arbitrage.Candidate{{BuyQuote: market.Quote{AmountOut: baseAmount}, SellQuote: market.Quote{AmountOut: amount}}}})
	}
	estimates, err := refresh(context.Background(), opportunities)
	if err != nil {
		t.Fatal(err)
	}
	if len(estimates) != 2 {
		t.Fatalf("estimates=%d, want 2", len(estimates))
	}
	for _, estimate := range estimates {
		if len(estimate.Components) != 7 {
			t.Fatalf("direction %v components=%d, want 7", estimate.Direction, len(estimate.Components))
		}
		if got := estimate.Components[5].Amount.Rat(); got.Cmp(big.NewRat(1, 2)) != 0 {
			t.Fatalf("Across spread=%s, want 0.5", got)
		}
		remoteSwap := 1
		if estimate.Direction.BuyMarket == b {
			remoteSwap = 0
		}
		if !strings.Contains(estimate.Components[remoteSwap].Evidence, "configured_expected_gas") {
			t.Fatalf("remote swap evidence=%q", estimate.Components[remoteSwap].Evidence)
		}
	}
	firstCalls := feeA.calls + feeB.calls
	if _, err := refresh(context.Background(), opportunities); err != nil {
		t.Fatal(err)
	}
	if got := feeA.calls + feeB.calls; got != firstCalls {
		t.Fatalf("receipt calibration calls repeated inside TTL: %d -> %d", firstCalls, got)
	}

	bootstrapRefresh, err := livecanary.NewEVMObservedFlowCostRefresh(livecanary.EVMObservedFlowCostRefreshConfig{
		Markets: markets, Valuator: valuator, Calibration: evmCalibrationFake{empty: true},
		Across: evmAcrossFake{omitGas: true}, WTT: evmWTTFake{now: now},
		Fees:           map[market.ChainID]livecanary.EVMFeeCostSource{"chain-a": feeA, "chain-b": feeB},
		NativeAssets:   map[market.ChainID]market.AssetID{"chain-a": "native-a", "chain-b": "native-b"},
		NativeDecimals: map[market.ChainID]uint8{"chain-a": 18, "chain-b": 18},
		RemoteSwapGas:  map[market.MarketID]uint64{a: 500_000, b: 500_000},
		AcrossGasFloor: map[market.ChainID]uint64{"chain-a": 400_000, "chain-b": 400_000},
		Clock:          func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	bootstrapEstimates, err := bootstrapRefresh(context.Background(), opportunities)
	if err != nil {
		t.Fatal(err)
	}
	for _, estimate := range bootstrapEstimates {
		if len(estimate.Components) != 7 ||
			!strings.Contains(estimate.Components[2].Evidence, "configured_expected_gas") ||
			!strings.Contains(estimate.Components[3].Evidence, "configured_expected_gas") ||
			!strings.Contains(estimate.Components[6].Evidence, "configured") {
			t.Fatalf("bootstrap cost evidence is incomplete: %+v", estimate.Components)
		}
	}
}

func TestEVMObservedFlowCostsRetainSuccessfulSizesWhenOneProbeFails(t *testing.T) {
	now := time.Date(2026, 8, 13, 8, 0, 0, 0, time.UTC)
	a, b := market.MarketID("market-a"), market.MarketID("market-b")
	quoteA := market.Token{ID: "quote-a", Asset: "quote", Chain: "chain-a", Decimals: 6}
	quoteB := market.Token{ID: "quote-b", Asset: "quote", Chain: "chain-b", Decimals: 18}
	baseA := market.Token{ID: "base-a", Asset: "base", Chain: "chain-a", Decimals: 18}
	baseB := market.Token{ID: "base-b", Asset: "base", Chain: "chain-b", Decimals: 18}
	markets := map[market.MarketID]configuration.ResolvedMarket{
		a: {ID: a, Chain: "chain-a", Base: configuration.ResolvedToken{Token: baseA}, Quote: configuration.ResolvedToken{Token: quoteA}},
		b: {ID: b, Chain: "chain-b", Base: configuration.ResolvedToken{Token: baseB}, Quote: configuration.ResolvedToken{Token: quoteB}},
	}
	valuator, err := livecanary.NewCostValuator("quote", func(context.Context) (map[market.AssetID]livecanary.CostAssetPrice, error) {
		return map[market.AssetID]livecanary.CostAssetPrice{
			"native-a": {Value: big.NewRat(2_000, 1), CapturedAt: now, Source: "test"},
			"native-b": {Value: big.NewRat(500, 1), CapturedAt: now, Source: "test"},
		}, nil
	}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if err = valuator.Warm(context.Background()); err != nil {
		t.Fatal(err)
	}
	failedOutput := big.NewInt(2_000_000_000_000_000_000)
	refresh, err := livecanary.NewEVMObservedFlowCostRefresh(livecanary.EVMObservedFlowCostRefreshConfig{
		Markets: markets, Valuator: valuator, Calibration: evmCalibrationFake{}, Across: evmAcrossFake{}, WTT: evmWTTFake{now: now},
		Fees: map[market.ChainID]livecanary.EVMFeeCostSource{
			"chain-a": &evmFeeFake{now: now}, "chain-b": &evmFeeFake{now: now},
		},
		NativeAssets:   map[market.ChainID]market.AssetID{"chain-a": "native-a", "chain-b": "native-b"},
		NativeDecimals: map[market.ChainID]uint8{"chain-a": 18, "chain-b": 18},
		RemoteSwapGas:  map[market.MarketID]uint64{a: 500_000, b: 500_000},
		SwapGasProbes: map[market.MarketID]livecanary.EVMObservedSwapGasProbe{a: func(_ context.Context, quote market.Quote) (uint64, error) {
			if quote.AmountOut.Units().Cmp(failedOutput) == 0 {
				return 0, errors.New("synthetic size failure")
			}
			return 200_000, nil
		}},
		Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	inputA, _ := market.ParseAssetQuantity("quote", "250")
	inputB, _ := market.ParseAssetQuantity("quote", "500")
	makeCandidate := func(input market.AssetQuantity, baseToken market.TokenID, baseUnits *big.Int, quoteToken market.TokenID, quoteUnits *big.Int) arbitrage.Candidate {
		return arbitrage.Candidate{Input: input, BuyQuote: market.Quote{AmountOut: mustAmount(t, baseToken, baseUnits)},
			SellQuote: market.Quote{AmountOut: mustAmount(t, quoteToken, quoteUnits)}}
	}
	opportunities := []arbitrage.Opportunity{
		{Direction: arbitrage.Direction{BuyMarket: a, SellMarket: b}, Candidates: []arbitrage.Candidate{
			makeCandidate(inputA, baseA.ID, big.NewInt(1_000_000_000_000_000_000), quoteB.ID,
				new(big.Int).Mul(big.NewInt(250), big.NewInt(1_000_000_000_000_000_000))),
			makeCandidate(inputB, baseA.ID, failedOutput, quoteB.ID,
				new(big.Int).Mul(big.NewInt(500), big.NewInt(1_000_000_000_000_000_000))),
		}},
		{Direction: arbitrage.Direction{BuyMarket: b, SellMarket: a}, Candidates: []arbitrage.Candidate{
			makeCandidate(inputA, baseB.ID, big.NewInt(1_000_000_000_000_000_000), quoteA.ID, big.NewInt(250_000_000)),
			makeCandidate(inputB, baseB.ID, big.NewInt(2_000_000_000_000_000_001), quoteA.ID, big.NewInt(500_000_000)),
		}},
	}
	estimates, err := refresh(context.Background(), opportunities)
	if err != nil {
		t.Fatal(err)
	}
	if len(estimates) != 3 {
		t.Fatalf("successful estimates=%d, want 3", len(estimates))
	}
}

func TestEVMObservedFlowCostsConvertOperationalQuoteBeforeAcross(t *testing.T) {
	now := time.Date(2026, 8, 13, 8, 0, 0, 0, time.UTC)
	a, b := market.MarketID("market-a"), market.MarketID("market-b")
	quoteA := market.Token{ID: "quote-a", Asset: "quote", Chain: "chain-a", Decimals: 6}
	quoteB := market.Token{ID: "operational-b", Asset: "quote", Chain: "chain-b", Decimals: 18}
	transitB := market.Token{ID: "transit-b", Asset: "quote", Chain: "chain-b", Decimals: 18}
	baseA := market.Token{ID: "base-a", Asset: "base", Chain: "chain-a", Decimals: 18}
	baseB := market.Token{ID: "base-b", Asset: "base", Chain: "chain-b", Decimals: 18}
	markets := map[market.MarketID]configuration.ResolvedMarket{
		a: {ID: a, Chain: "chain-a", Base: configuration.ResolvedToken{Token: baseA}, Quote: configuration.ResolvedToken{Token: quoteA}},
		b: {ID: b, Chain: "chain-b", Base: configuration.ResolvedToken{Token: baseB}, Quote: configuration.ResolvedToken{Token: quoteB}},
	}
	valuator, _ := livecanary.NewCostValuator("quote", func(context.Context) (map[market.AssetID]livecanary.CostAssetPrice, error) {
		return map[market.AssetID]livecanary.CostAssetPrice{
			"native-a": {Value: big.NewRat(2_000, 1), CapturedAt: now, Source: "test"},
			"native-b": {Value: big.NewRat(500, 1), CapturedAt: now, Source: "test"},
		}, nil
	}, func() time.Time { return now })
	if err := valuator.Warm(context.Background()); err != nil {
		t.Fatal(err)
	}
	fxIn, _ := market.NewTokenAmount(quoteB.ID, new(big.Int).Mul(big.NewInt(500), big.NewInt(1e18)))
	fxOut, _ := market.NewTokenAmount(transitB.ID, new(big.Int).Mul(big.NewInt(4995), big.NewInt(1e17)))
	fx, _ := market.NewQuoteConversionSnapshot("conversion", fxIn, fxOut, now, now.Add(3*time.Second))
	feeA, feeB := &evmFeeFake{now: now}, &evmFeeFake{now: now}
	refresh, err := livecanary.NewEVMObservedFlowCostRefresh(livecanary.EVMObservedFlowCostRefreshConfig{
		Markets: markets, Valuator: valuator, Calibration: evmCalibrationFake{}, Across: evmAcrossFake{}, WTT: evmWTTFake{now: now},
		Fees:           map[market.ChainID]livecanary.EVMFeeCostSource{"chain-a": feeA, "chain-b": feeB},
		NativeAssets:   map[market.ChainID]market.AssetID{"chain-a": "native-a", "chain-b": "native-b"},
		NativeDecimals: map[market.ChainID]uint8{"chain-a": 18, "chain-b": 18},
		RemoteSwapGas:  map[market.MarketID]uint64{b: 1_000_000}, QuoteConversions: fixedConversionBook{snapshot: fx},
		BridgeTokens:    map[market.ChainID]market.Token{"chain-a": quoteA, "chain-b": transitB},
		ConversionChain: "chain-b", ConversionGas: 250_000, Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	input, _ := market.NewTokenAmount(quoteB.ID, new(big.Int).Mul(big.NewInt(500), big.NewInt(1e18)))
	baseAInput := mustAmount(t, baseA.ID, big.NewInt(500_000_000_000_000_000))
	baseBInput := mustAmount(t, baseB.ID, big.NewInt(500_000_000_000_000_000))
	opportunities := []arbitrage.Opportunity{{Direction: arbitrage.Direction{BuyMarket: a, SellMarket: b},
		Candidates: []arbitrage.Candidate{{BuyQuote: market.Quote{AmountOut: baseAInput}, SellQuote: market.Quote{AmountOut: input}}}},
		{Direction: arbitrage.Direction{BuyMarket: b, SellMarket: a},
			Candidates: []arbitrage.Candidate{{BuyQuote: market.Quote{AmountOut: baseBInput}, SellQuote: market.Quote{AmountOut: mustAmount(t, quoteA.ID, big.NewInt(500_000_000))}}}}}
	estimates, err := refresh(context.Background(), opportunities)
	if err != nil {
		t.Fatal(err)
	}
	for _, estimate := range estimates {
		if len(estimate.Components) != 8 || estimate.Components[7].Kind != "quote_conversion_swap" {
			t.Fatalf("conversion-aware components=%#v", estimate.Components)
		}
	}
}

func mustAmount(t *testing.T, token market.TokenID, units *big.Int) market.TokenAmount {
	t.Helper()
	amount, err := market.NewTokenAmount(token, units)
	if err != nil {
		t.Fatal(err)
	}
	return amount
}
