package livesequential_test

import (
	"math/big"
	"testing"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
)

func syntheticOpportunity(t *testing.T) arbitrage.Opportunity {
	t.Helper()
	amount := func(
		token market.TokenID,
		units int64,
	) market.TokenAmount {
		value, err := market.NewTokenAmount(token, big.NewInt(units))
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	return arbitrage.Opportunity{
		Evaluation:     "evaluation",
		ConfigHash:     "synthetic-config",
		Classification: arbitrage.ClassificationPolicyQualified,
		Direction: arbitrage.Direction{
			BuyMarket:  "market-a",
			SellMarket: "market-b",
		},
		Candidates: []arbitrage.Candidate{{
			BuyQuote: market.Quote{
				AmountIn:  amount("quote-a", 100_000_000),
				AmountOut: amount("base-a", 400_000_000),
			},
			SellQuote: market.Quote{
				AmountIn:  amount("base-b", 400_000_000),
				AmountOut: amount("quote-b", 101_000_000),
			},
		}},
		SelectedIndex: 0,
	}
}
