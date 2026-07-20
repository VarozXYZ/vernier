package strategy

import (
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/domain/market"
)

func TestUnmodeledQuoteFeeClosesClassification(t *testing.T) {
	input, _ := market.ParseTokenAmount("input", "100")
	output, _ := market.ParseTokenAmount("output", "95")
	feeAmount, _ := market.ParseTokenAmount("third-token", "1")
	included, _ := market.NewQuoteFee("venue", market.QuoteFeeCost, feeAmount, true)
	external, _ := market.NewQuoteFee("venue", market.QuoteFeeCost, feeAmount, false)
	base := market.Quote{
		Source: "source", Market: "market", SnapshotVersion: 1,
		Purpose: market.QuotePurposeResearchDiscovery, AmountIn: input, AmountOut: output, QuotedAt: time.Now(),
	}
	fullyModeled, _ := market.NewQuote(base, included)
	unmodeled, _ := market.NewQuote(base, external)
	if hasUnmodeledFee(fullyModeled) || !hasUnmodeledFee(unmodeled) {
		t.Fatal("quote fee modeling boundary was not enforced")
	}
}
