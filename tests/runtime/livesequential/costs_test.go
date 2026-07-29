package livesequential_test

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	"github.com/VarozXYZ/vernier/runtime/livesequential"
)

func TestCostValuatorUsesPreloadedExactPriceWithoutIO(t *testing.T) {
	now := time.Now().UTC()
	refreshes := 0
	valuator, err := livesequential.NewCostValuator(
		"quote",
		func(
			context.Context,
		) (map[market.AssetID]livesequential.CostAssetPrice, error) {
			refreshes++
			return map[market.AssetID]livesequential.CostAssetPrice{
				"native": {
					Value:      big.NewRat(150, 1),
					CapturedAt: now,
					Source:     "synthetic_native_quote",
				},
			}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := valuator.Warm(context.Background()); err != nil {
		t.Fatal(err)
	}
	amount, _ := market.NewAssetQuantity(
		"native",
		big.NewRat(1, 100),
	)
	valued, err := valuator.Value(execution.CostComponent{
		Kind:     "network_fee",
		Chain:    "chain-a",
		Amount:   amount,
		Evidence: "synthetic_receipt",
	})
	if err != nil {
		t.Fatal(err)
	}
	if refreshes != 1 {
		t.Fatalf("Value performed I/O: refreshes=%d", refreshes)
	}
	if valued.QuoteValue.Asset() != "quote" ||
		valued.QuoteValue.Rat().Cmp(big.NewRat(3, 2)) != 0 {
		t.Fatalf("quote value=%s", valued.QuoteValue)
	}
}

func TestCostValuatorDoesNotDoubleConvertQuoteAsset(t *testing.T) {
	now := time.Now().UTC()
	valuator, err := livesequential.NewCostValuator(
		"quote",
		func(
			context.Context,
		) (map[market.AssetID]livesequential.CostAssetPrice, error) {
			return map[market.AssetID]livesequential.CostAssetPrice{
				"native": {
					Value:      big.NewRat(100, 1),
					CapturedAt: now,
					Source:     "synthetic",
				},
			}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := valuator.Warm(context.Background()); err != nil {
		t.Fatal(err)
	}
	amount, _ := market.NewAssetQuantity(
		"quote",
		big.NewRat(1, 10_000),
	)
	valued, err := valuator.Value(execution.CostComponent{
		Kind:             "transfer_spread",
		Chain:            "chain-b",
		Amount:           amount,
		IncludedInOutput: true,
		Evidence:         "synthetic_balance_delta",
	})
	if err != nil {
		t.Fatal(err)
	}
	if valued.QuoteValue.Rat().Cmp(amount.Rat()) != 0 ||
		!valued.IncludedInOutput {
		t.Fatalf("unexpected quote valuation: %+v", valued)
	}
}
