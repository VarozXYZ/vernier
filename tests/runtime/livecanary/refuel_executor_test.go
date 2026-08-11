package livecanary_test

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	"github.com/VarozXYZ/vernier/runtime/livecanary"
)

type refuelNetworkStub struct{}

func (refuelNetworkStub) NativeBalance(context.Context) (*big.Int, error) {
	return big.NewInt(1_000_000_000), nil
}

func (refuelNetworkStub) AwaitRefuel(
	context.Context,
	execution.TransactionIdentity,
	*big.Int,
) (*big.Int, *big.Int, *big.Int, error) {
	return nil, nil, nil, nil
}

func (refuelNetworkStub) ReconcileRefuel(
	context.Context,
	execution.TransactionIdentity,
	*big.Int,
) (bool, bool, *big.Int, *big.Int, *big.Int, error) {
	return false, false, nil, nil, nil, nil
}

func TestRefuelPreviewCompactsOversizedSolanaArtifact(t *testing.T) {
	now := time.Now().UTC()
	validator := &compactingValidator{
		now: now, compactLimit: "32",
	}
	manager := &settledTxManager{oversizedAt: "64"}
	valuator, err := livecanary.NewCostValuator(
		"quote",
		func(context.Context) (map[market.AssetID]livecanary.CostAssetPrice, error) {
			return map[market.AssetID]livecanary.CostAssetPrice{
				"native": {
					Value: big.NewRat(1, 1), CapturedAt: now, Source: "synthetic",
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
	executor, err := livecanary.NewSwapRefuelExecutor(
		livecanary.SwapRefuelExecutorConfig{
			Chain: "chain-a", Market: "market-a", Account: "account",
			QuoteToken: market.Token{
				ID: "quote-token", Asset: "quote", Chain: "chain-a", Decimals: 6,
			},
			NativeToken: market.Token{
				ID: "native-token", Asset: "native", Chain: "chain-a", Decimals: 9,
			},
			NativeAsset: "native",
			Binding: livecanary.SwapBinding{
				Account: "account", Validator: validator, TxManager: manager,
			},
			Network: refuelNetworkStub{}, Prices: valuator,
			Clock: func() time.Time { return now },
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	spend, _ := market.NewAssetQuantity("quote", big.NewRat(10, 1))
	record, err := executor.Preview(context.Background(), spend)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != executionport.RefuelPrepared {
		t.Fatalf("state = %s", record.State)
	}
	if validator.compact != 1 || manager.prepareCalls != 2 {
		t.Fatalf(
			"compact validations=%d prepare calls=%d",
			validator.compact,
			manager.prepareCalls,
		)
	}
}
