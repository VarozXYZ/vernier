package livecanary_test

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	"github.com/VarozXYZ/vernier/runtime/livecanary"
)

func TestCostValuatorUsesPreloadedExactPriceWithoutIO(t *testing.T) {
	now := time.Now().UTC()
	refreshes := 0
	valuator, err := livecanary.NewCostValuator(
		"usdc",
		func(context.Context) (map[market.AssetID]livecanary.CostAssetPrice, error) {
			refreshes++
			return map[market.AssetID]livecanary.CostAssetPrice{
				"sol": {
					Value: big.NewRat(150, 1), CapturedAt: now,
					Source: "test_sol_usdc",
				},
			}, nil
		},
		func() time.Time { return now },
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := valuator.Warm(context.Background()); err != nil {
		t.Fatal(err)
	}
	amount, _ := market.NewAssetQuantity("sol", big.NewRat(1, 100))
	valued, err := valuator.Value(execution.CostComponent{
		Kind: "network_fee", Chain: "solana", Amount: amount,
		Evidence: "receipt",
	})
	if err != nil {
		t.Fatal(err)
	}
	if refreshes != 1 {
		t.Fatalf("Value performed I/O: refreshes=%d", refreshes)
	}
	if valued.QuoteValue.Asset() != "usdc" ||
		valued.QuoteValue.Rat().Cmp(big.NewRat(3, 2)) != 0 {
		t.Fatalf("quote value=%s %s", valued.QuoteValue, valued.QuoteValue.Asset())
	}
}

func TestCostValuatorDoesNotDoubleConvertQuoteAsset(t *testing.T) {
	now := time.Now().UTC()
	valuator, _ := livecanary.NewCostValuator(
		"usdc",
		func(context.Context) (map[market.AssetID]livecanary.CostAssetPrice, error) {
			return map[market.AssetID]livecanary.CostAssetPrice{
				"sol": {Value: big.NewRat(100, 1), CapturedAt: now, Source: "test"},
			}, nil
		},
		time.Now,
	)
	if err := valuator.Warm(context.Background()); err != nil {
		t.Fatal(err)
	}
	amount, _ := market.NewAssetQuantity("usdc", big.NewRat(1, 10_000))
	valued, err := valuator.Value(execution.CostComponent{
		Kind: "bridge_spread", Chain: "polygon", Amount: amount,
		IncludedInOutput: true, Evidence: "balance_delta",
	})
	if err != nil {
		t.Fatal(err)
	}
	if valued.QuoteValue.Rat().Cmp(amount.Rat()) != 0 ||
		!valued.IncludedInOutput {
		t.Fatalf("unexpected quote-asset valuation: %+v", valued)
	}
}
