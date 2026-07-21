package market_test

import (
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/domain/market"
)

func TestQuoteSupportsImmutableMultiTokenCostsAndCredits(t *testing.T) {
	input, _ := market.ParseTokenAmount("input", "100")
	output, _ := market.ParseTokenAmount("output", "95")
	venueAmount, _ := market.ParseTokenAmount("output", "2")
	rebateAmount, _ := market.ParseTokenAmount("rebate-token", "1")
	venueFee, _ := market.NewQuoteFee("venue", market.QuoteFeeCost, venueAmount, true)
	rebate, _ := market.NewQuoteFee("maker", market.QuoteFeeCredit, rebateAmount, false)
	quote, err := market.NewQuote(market.Quote{
		Source: "source", Market: "market", SnapshotVersion: 1,
		Purpose: market.QuotePurposeResearchDiscovery, Mode: market.QuoteModeExactInput,
		AmountIn: input, AmountOut: output, QuotedAt: time.Now(),
	}, venueFee, rebate)
	if err != nil {
		t.Fatal(err)
	}
	fees := quote.Fees()
	fees[0] = rebate
	if got := quote.Fees(); len(got) != 2 || got[0].Kind() != "venue" || got[1].Effect() != market.QuoteFeeCredit || got[1].IncludedInAmounts() {
		t.Fatalf("quote fees were mutated or lost: %+v", got)
	}
}

func TestQuoteRequiresExecutionMode(t *testing.T) {
	input, _ := market.ParseTokenAmount("input", "100")
	output, _ := market.ParseTokenAmount("output", "95")
	_, err := market.NewQuote(market.Quote{
		Source: "source", Market: "market", SnapshotVersion: 1,
		Purpose:  market.QuotePurposeResearchDiscovery,
		AmountIn: input, AmountOut: output, QuotedAt: time.Now(),
	})
	if err == nil {
		t.Fatal("quote without exact-input/exact-output mode was accepted")
	}
}
