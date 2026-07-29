package livesequential_test

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	crosschainport "github.com/VarozXYZ/vernier/ports/crosschain"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	"github.com/VarozXYZ/vernier/runtime/livesequential"
)

type syntheticTransferService struct {
	result crosschainport.LiveTransferResult
}

func (s syntheticTransferService) Transfer(
	context.Context,
	execution.SequentialStageRequest,
	executionport.SequentialJournal,
) (crosschainport.LiveTransferResult, error) {
	return s.result, nil
}

func TestTransferDriverAdaptsSettledTransferAndValuesCosts(
	t *testing.T,
) {
	now := time.Now().UTC()
	input, _ := market.NewTokenAmount("base-a", big.NewInt(400))
	output, _ := market.NewTokenAmount("base-b", big.NewInt(399))
	nativeCost, _ := market.NewAssetQuantity(
		"native-a",
		big.NewRat(1, 1000),
	)
	valuator, err := livesequential.NewCostValuator(
		"quote",
		func(
			context.Context,
		) (map[market.AssetID]livesequential.CostAssetPrice, error) {
			return map[market.AssetID]livesequential.CostAssetPrice{
				"native-a": {
					Value:      big.NewRat(10, 1),
					CapturedAt: now,
					Source:     "synthetic_price",
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
	request := execution.SequentialStageRequest{
		Operation: "operation",
		Plan:      "plan",
		Stage: execution.SequentialStagePlan{
			Ordinal:          2,
			Stage:            execution.StageBridgeBase,
			SourceChain:      "chain-a",
			DestinationChain: "chain-b",
			InputToken:       "base-a",
			OutputToken:      "base-b",
		},
		Input: input,
	}
	driver := livesequential.TransferDriver{
		Stage: execution.StageBridgeBase,
		Service: syntheticTransferService{
			result: crosschainport.LiveTransferResult{
				ActualInput:  input,
				ActualOutput: output,
				Costs: []execution.CostComponent{{
					Kind:     "network_fee",
					Chain:    "chain-a",
					Amount:   nativeCost,
					Evidence: "synthetic_receipt",
				}},
				SourceIdentity: execution.TransactionIdentity{
					Chain:   "chain-a",
					Account: "account-a",
					Hash:    "source-transaction",
				},
				DestinationIdentity: execution.TransactionIdentity{
					Chain:   "chain-b",
					Account: "account-b",
					Hash:    "destination-transaction",
				},
				ObservedAt: now,
				Evidence:   "synthetic_destination_observation",
			},
		},
		Costs: valuator,
	}
	settlement, err := driver.ExecuteStage(
		context.Background(),
		request,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if settlement.ActualOutput.Units().Cmp(big.NewInt(399)) != 0 ||
		len(settlement.Costs) != 1 ||
		settlement.Costs[0].QuoteValue.Rat().Cmp(
			big.NewRat(1, 100),
		) != 0 {
		t.Fatalf("unexpected transfer settlement: %+v", settlement)
	}
}
