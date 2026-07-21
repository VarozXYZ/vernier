package sizing

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

// ExactOutputRequest describes a protocol-neutral search for the smallest
// exact-input quote that covers TargetOut. It intentionally depends only on
// the generic quote.Source contract.
type ExactOutputRequest struct {
	Snapshot    market.MarketSnapshot
	TokenIn     market.TokenID
	TokenOut    market.TokenID
	TargetOut   market.TokenAmount
	InitialHigh market.TokenAmount
	Purpose     market.QuotePurpose
	QuotedAt    time.Time
}

// MinimumInputForOutput resolves an exact-output research size using only
// deterministic exact-input quotes. The returned quote may exceed TargetOut
// by integer rounding, but no smaller raw input unit can meet the target.
func MinimumInputForOutput(
	ctx context.Context,
	source quoteport.Source,
	request ExactOutputRequest,
) (market.Quote, error) {
	if source == nil || request.TokenIn == "" || request.TokenOut == "" ||
		request.TokenIn == request.TokenOut || request.TargetOut.Token() != request.TokenOut ||
		request.TargetOut.IsZero() || request.InitialHigh.Token() != request.TokenIn ||
		request.InitialHigh.IsZero() || request.Purpose == "" || request.QuotedAt.IsZero() {
		return market.Quote{}, fmt.Errorf("invalid exact-output sizing request")
	}
	if native, ok := source.(quoteport.ExactOutputSource); ok {
		candidate, err := native.QuoteExactOutput(ctx, quoteport.ExactOutputInput{
			Snapshot: request.Snapshot, TokenIn: request.TokenIn, TokenOut: request.TokenOut,
			AmountOut: request.TargetOut, Purpose: request.Purpose, QuotedAt: request.QuotedAt,
		})
		if err != nil {
			return market.Quote{}, err
		}
		if candidate.AmountIn.Token() != request.TokenIn || candidate.AmountIn.IsZero() ||
			candidate.AmountOut.Token() != request.TokenOut ||
			candidate.AmountOut.Units().Cmp(request.TargetOut.Units()) != 0 {
			return market.Quote{}, fmt.Errorf("native source returned inconsistent exact-output evidence")
		}
		return candidate, nil
	}

	quoteAt := func(units *big.Int) (market.Quote, error) {
		amount, err := market.NewTokenAmount(request.TokenIn, units)
		if err != nil {
			return market.Quote{}, err
		}
		candidate, err := source.Quote(ctx, quoteport.Input{
			Snapshot: request.Snapshot, TokenIn: request.TokenIn, TokenOut: request.TokenOut,
			AmountIn: amount, Purpose: request.Purpose, QuotedAt: request.QuotedAt,
		})
		if err != nil {
			return market.Quote{}, err
		}
		if candidate.AmountIn.Token() != request.TokenIn || candidate.AmountIn.Units().Cmp(units) != 0 ||
			candidate.AmountOut.Token() != request.TokenOut {
			return market.Quote{}, fmt.Errorf("quote source returned inconsistent exact-output evidence")
		}
		return candidate, nil
	}

	target := request.TargetOut.Units()
	low := new(big.Int)
	high := request.InitialHigh.Units()
	highQuote, err := quoteAt(high)
	if err != nil {
		return market.Quote{}, err
	}
	for highQuote.AmountOut.Units().Cmp(target) < 0 {
		low.Set(high)
		high.Lsh(high, 1)
		if high.BitLen() > 256 {
			return market.Quote{}, fmt.Errorf("exact-output sizing exceeds uint256 input")
		}
		highQuote, err = quoteAt(high)
		if err != nil {
			return market.Quote{}, err
		}
	}

	one := big.NewInt(1)
	for new(big.Int).Sub(high, low).Cmp(one) > 0 {
		middle := new(big.Int).Rsh(new(big.Int).Add(low, high), 1)
		candidate, quoteErr := quoteAt(middle)
		if quoteErr != nil {
			return market.Quote{}, quoteErr
		}
		if candidate.AmountOut.Units().Cmp(target) >= 0 {
			high.Set(middle)
			highQuote = candidate
		} else {
			low.Set(middle)
		}
	}
	return highQuote, nil
}
