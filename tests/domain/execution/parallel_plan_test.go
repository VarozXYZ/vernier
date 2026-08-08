package execution_test

import (
	"math/big"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
)

func TestPrefundedParallelPlanCompilesIndependentEconomicLegsAndBridges(t *testing.T) {
	plan := parallelPlan(t)
	plan.BaseAsset = "base"
	plan.QuoteAsset = "quote"
	plan.TokenDecimals = map[market.TokenID]uint8{
		"quote-a": 6, "quote-b": 6, "base-a": 9, "base-b": 18,
	}
	if err := plan.Validate(); err != nil {
		t.Fatal(err)
	}
	wantDependencies := [][]int{nil, nil, {1}, {2}}
	wantInputs := []int{0, 0, 1, 2}
	for index, stage := range plan.Stages {
		if !equalInts(stage.DependsOn, wantDependencies[index]) {
			t.Fatalf("stage %d dependencies=%v", stage.Ordinal, stage.DependsOn)
		}
		if stage.InputFromOrdinal != wantInputs[index] {
			t.Fatalf("stage %d input_from=%d", stage.Ordinal, stage.InputFromOrdinal)
		}
	}
	sellInput, err := plan.InputFor(plan.Stages[1], nil)
	if err != nil {
		t.Fatal(err)
	}
	if sellInput.Token() != "base-b" || sellInput.Units().Cmp(big.NewInt(4_000_000_000_000_000_000)) != 0 {
		t.Fatalf("independent sell input=%s/%s", sellInput.Token(), sellInput)
	}
}

func TestPrefundedParallelPlanRejectsSequentialBridgeDependencies(t *testing.T) {
	plan := parallelPlan(t)
	plan.BaseAsset = "base"
	plan.QuoteAsset = "quote"
	plan.TokenDecimals = map[market.TokenID]uint8{
		"quote-a": 6, "quote-b": 6, "base-a": 9, "base-b": 18,
	}
	plan.Stages[3].DependsOn = []int{2, 3}
	if err := plan.Validate(); err == nil {
		t.Fatal("expected sequential bridge dependency to be rejected")
	}
}

func parallelPlan(t *testing.T) execution.SequentialPlan {
	t.Helper()
	amount := func(token market.TokenID, units int64) market.TokenAmount {
		value, err := market.NewTokenAmount(token, big.NewInt(units))
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	input := amount("quote-a", 1_000_000)
	inputValue, _ := market.NewAssetQuantity("quote", big.NewRat(1, 1))
	outputValue, _ := market.NewAssetQuantity("quote", big.NewRat(101, 100))
	opportunity := arbitrage.Opportunity{
		Evaluation: "evaluation", ConfigHash: "config",
		Classification: arbitrage.ClassificationPolicyQualified,
		Direction:      arbitrage.Direction{BuyMarket: "buy", SellMarket: "sell"},
		Candidates: []arbitrage.Candidate{{
			Input: inputValue, Output: outputValue,
			BuyQuote: market.Quote{
				Market: "buy", AmountIn: input,
				AmountOut: amount("base-a", 4_000_000_000),
			},
			SellQuote: market.Quote{
				Market:    "sell",
				AmountIn:  amount("base-b", 4_000_000_000_000_000_000),
				AmountOut: amount("quote-b", 1_010_000),
			},
		}},
		SelectedIndex: 0,
	}
	plan, err := execution.NewPrefundedParallelPlan(
		"parallel-plan", opportunity, input, "chain-a", "chain-b", time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func equalInts(left, right []int) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
