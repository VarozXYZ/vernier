package market

import (
	"errors"
	"fmt"
	"time"
)

var ErrQuoteOutputRoundsToZero = errors.New("quote output rounds to zero")

type QuotePurpose string

const (
	QuotePurposeResearchDiscovery QuotePurpose = "research_discovery"
	QuotePurposeLiveDiscovery     QuotePurpose = "live_discovery"
	QuotePurposeLiveValidation    QuotePurpose = "live_validation"
)

type QuoteMode string

const (
	QuoteModeExactInput  QuoteMode = "exact_input"
	QuoteModeExactOutput QuoteMode = "exact_output"
)

// QuoteQuality describes whether the amounts came directly from the quoted
// snapshot/provider or were reused as an interim value. The empty value is
// normalized to exact for backwards-compatible local quote constructors.
type QuoteQuality string

const (
	QuoteQualityExact                QuoteQuality = "exact"
	QuoteQualityCachedExact          QuoteQuality = "cached_exact"
	QuoteQualityProportionalEstimate QuoteQuality = "proportional_estimate"
)

func (q QuoteQuality) RequiresRefresh() bool {
	return q == QuoteQualityCachedExact || q == QuoteQualityProportionalEstimate
}

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
	SourcePosition  SourcePosition
	ResponseHash    [32]byte
	Purpose         QuotePurpose
	Mode            QuoteMode
	Quality         QuoteQuality
	AmountIn        TokenAmount
	AmountOut       TokenAmount
	QuotedAt        time.Time
	fees            []QuoteFee
}

func NewQuote(quote Quote, fees ...QuoteFee) (Quote, error) {
	if quote.Source == "" || quote.Market == "" || quote.SnapshotVersion == 0 {
		return Quote{}, fmt.Errorf("quote source, market, and snapshot version are required")
	}
	if err := quote.SourcePosition.Validate(); err != nil {
		return Quote{}, fmt.Errorf("quote source position: %w", err)
	}
	if quote.Purpose == "" || quote.QuotedAt.IsZero() || quote.Mode != QuoteModeExactInput && quote.Mode != QuoteModeExactOutput {
		return Quote{}, fmt.Errorf("quote purpose, mode, and timestamp are required")
	}
	if quote.Quality == "" {
		quote.Quality = QuoteQualityExact
	}
	if quote.Quality != QuoteQualityExact && quote.Quality != QuoteQualityCachedExact &&
		quote.Quality != QuoteQualityProportionalEstimate {
		return Quote{}, fmt.Errorf("invalid quote quality %q", quote.Quality)
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
