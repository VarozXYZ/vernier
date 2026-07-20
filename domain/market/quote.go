package market

import (
	"fmt"
	"time"
)

type QuotePurpose string

const (
	QuotePurposeResearchDiscovery QuotePurpose = "research_discovery"
)

type QuoteFeeEffect string

const (
	QuoteFeeCost   QuoteFeeEffect = "cost"
	QuoteFeeCredit QuoteFeeEffect = "credit"
)

type QuoteFee struct {
	kind              string
	effect            QuoteFeeEffect
	amount            TokenAmount
	includedInAmounts bool
}

// NewQuoteFee creates a typed quote component. includedInAmounts reports
// whether AmountIn or AmountOut already reflects the component.
func NewQuoteFee(kind string, effect QuoteFeeEffect, amount TokenAmount, includedInAmounts bool) (QuoteFee, error) {
	if kind == "" || amount.Token() == "" {
		return QuoteFee{}, fmt.Errorf("quote fee kind and amount are required")
	}
	if effect != QuoteFeeCost && effect != QuoteFeeCredit {
		return QuoteFee{}, fmt.Errorf("invalid quote fee effect %q", effect)
	}
	return QuoteFee{kind: kind, effect: effect, amount: amount, includedInAmounts: includedInAmounts}, nil
}

func (f QuoteFee) Kind() string            { return f.kind }
func (f QuoteFee) Effect() QuoteFeeEffect  { return f.effect }
func (f QuoteFee) Amount() TokenAmount     { return f.amount }
func (f QuoteFee) IncludedInAmounts() bool { return f.includedInAmounts }

type Quote struct {
	Source          SourceID
	Market          MarketID
	SnapshotVersion uint64
	SnapshotHash    [32]byte
	Purpose         QuotePurpose
	AmountIn        TokenAmount
	AmountOut       TokenAmount
	QuotedAt        time.Time
	fees            []QuoteFee
}

func NewQuote(quote Quote, fees ...QuoteFee) (Quote, error) {
	if quote.Source == "" || quote.Market == "" || quote.SnapshotVersion == 0 {
		return Quote{}, fmt.Errorf("quote source, market, and snapshot version are required")
	}
	if quote.Purpose == "" || quote.QuotedAt.IsZero() {
		return Quote{}, fmt.Errorf("quote purpose and timestamp are required")
	}
	if quote.AmountIn.Token() == "" || quote.AmountOut.Token() == "" {
		return Quote{}, fmt.Errorf("quote amounts are required")
	}
	for index, fee := range fees {
		if fee.kind == "" || fee.amount.Token() == "" || fee.effect != QuoteFeeCost && fee.effect != QuoteFeeCredit {
			return Quote{}, fmt.Errorf("invalid quote fee component %d", index)
		}
	}
	quote.QuotedAt = quote.QuotedAt.UTC()
	quote.fees = append([]QuoteFee(nil), fees...)
	return quote, nil
}

func (q Quote) Fees() []QuoteFee { return append([]QuoteFee(nil), q.fees...) }
