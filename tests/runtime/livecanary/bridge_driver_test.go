package livecanary_test

import (
	"context"
	"errors"
	"math/big"
	"testing"

	domainexecution "github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	crosschainport "github.com/VarozXYZ/vernier/ports/crosschain"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	"github.com/VarozXYZ/vernier/runtime/livecanary"
)

type interruptedBridgeProvider struct {
	cost domainexecution.CostComponent
}

func (p interruptedBridgeProvider) Transfer(
	context.Context,
	domainexecution.SequentialStageRequest,
	executionport.SequentialJournal,
) (crosschainport.LiveTransferResult, error) {
	return crosschainport.LiveTransferResult{},
		executionport.NewStageErrorWithCosts(
			executionport.DispositionPossible,
			[]domainexecution.CostComponent{p.cost},
			errors.New("interrupted while awaiting destination"),
		)
}

type bridgeCostValuator struct{}

func (bridgeCostValuator) Value(
	component domainexecution.CostComponent,
) (domainexecution.CostComponent, error) {
	value, err := market.NewAssetQuantity("usd", big.NewRat(2, 1))
	if err != nil {
		return domainexecution.CostComponent{}, err
	}
	return component.WithQuoteValue(value)
}

func TestBridgeDriverValuesCostsReturnedWithInterruptedTransfer(t *testing.T) {
	t.Parallel()

	input, err := market.NewTokenAmount("quote-a", big.NewInt(1_000_000))
	if err != nil {
		t.Fatal(err)
	}
	nativeCost, err := market.NewAssetQuantity("native-a", big.NewRat(1, 10))
	if err != nil {
		t.Fatal(err)
	}
	driver := livecanary.BridgeDriver{
		Stage: domainexecution.StageBridgeQuoteReturn,
		Provider: interruptedBridgeProvider{cost: domainexecution.CostComponent{
			Kind: "network_fee", Chain: "chain-a", Amount: nativeCost,
			Evidence: "confirmed_source",
		}},
		Costs: bridgeCostValuator{},
	}
	_, err = driver.ExecuteStage(
		context.Background(),
		domainexecution.SequentialStageRequest{
			Operation: "operation", Plan: "plan",
			Stage: domainexecution.SequentialStagePlan{
				Ordinal: 4, Stage: domainexecution.StageBridgeQuoteReturn,
				SourceChain: "chain-a", DestinationChain: "chain-b",
				InputToken: "quote-a", OutputToken: "quote-b",
			},
			Input: input,
		},
		nil,
	)
	if err == nil {
		t.Fatal("expected interrupted bridge error")
	}
	costs := executionport.ErrorCosts(err)
	if len(costs) != 1 || costs[0].QuoteValue.Asset() != "usd" ||
		costs[0].QuoteValue.Rat().Cmp(big.NewRat(2, 1)) != 0 {
		t.Fatalf("unexpected valued costs: %+v", costs)
	}
	if executionport.ErrorDisposition(err) != executionport.DispositionPossible {
		t.Fatalf("unexpected disposition: %s", executionport.ErrorDisposition(err))
	}
}
