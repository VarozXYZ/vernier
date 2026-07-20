package market

import (
	"fmt"
	"time"
)

type QuotePurpose string

const (
	QuotePurposeResearchDiscovery QuotePurpose = "research_discovery"
)

type Quote struct {
	Source          SourceID
	Market          MarketID
	SnapshotVersion uint64
	SnapshotHash    [32]byte
	Purpose         QuotePurpose
	AmountIn        TokenAmount
	AmountOut       TokenAmount
	Fee             TokenAmount
	QuotedAt        time.Time
}

func NewQuote(quote Quote) (Quote, error) {
	if quote.Source == "" || quote.Market == "" || quote.SnapshotVersion == 0 {
		return Quote{}, fmt.Errorf("quote source, market, and snapshot version are required")
	}
	if quote.Purpose == "" || quote.QuotedAt.IsZero() {
		return Quote{}, fmt.Errorf("quote purpose and timestamp are required")
	}
	if quote.AmountIn.Token() == "" || quote.AmountOut.Token() == "" || quote.Fee.Token() == "" {
		return Quote{}, fmt.Errorf("quote amounts are required")
	}
	if quote.Fee.Token() != quote.AmountIn.Token() {
		return Quote{}, fmt.Errorf("fee token must match input token")
	}
	quote.QuotedAt = quote.QuotedAt.UTC()
	return quote, nil
}
