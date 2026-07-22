package dlmm

import (
	"context"
	"fmt"
	"math/big"

	"github.com/VarozXYZ/vernier/adapters/market/liquiditycurve"
	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

type Quoter struct {
	id     market.SourceID
	market market.Market
	tokenX market.TokenID
	tokenY market.TokenID
}

func NewQuoter(id market.SourceID, candidate market.Market, tokenX, tokenY market.TokenID) (*Quoter, error) {
	if id == "" || candidate.ID == "" || tokenX == "" || tokenY == "" || tokenX == tokenY || !matches(candidate, tokenX, tokenY) {
		return nil, fmt.Errorf("source, market, and Meteora token endpoints are required")
	}
	return &Quoter{id: id, market: candidate, tokenX: tokenX, tokenY: tokenY}, nil
}
func (q *Quoter) ID() market.SourceID { return q.id }

func (q *Quoter) Quote(ctx context.Context, input quoteport.Input) (market.Quote, error) {
	if err := ctx.Err(); err != nil {
		return market.Quote{}, err
	}
	state, err := q.state(input.Snapshot)
	if err != nil {
		return market.Quote{}, err
	}
	segments, err := q.segments(state, input.TokenIn, input.TokenOut)
	if err != nil {
		return market.Quote{}, err
	}
	amountOut, fee, err := liquiditycurve.ExactInput(segments, input.AmountIn.Units(), state.feeBPS)
	if err != nil {
		return market.Quote{}, err
	}
	return q.result(input, market.QuoteModeExactInput, input.AmountIn, input.TokenOut, amountOut, fee)
}

func (q *Quoter) QuoteExactOutput(ctx context.Context, input quoteport.ExactOutputInput) (market.Quote, error) {
	if err := ctx.Err(); err != nil {
		return market.Quote{}, err
	}
	state, err := q.state(input.Snapshot)
	if err != nil {
		return market.Quote{}, err
	}
	segments, err := q.segments(state, input.TokenIn, input.TokenOut)
	if err != nil {
		return market.Quote{}, err
	}
	amountIn, fee, err := liquiditycurve.ExactOutput(segments, input.AmountOut.Units(), state.feeBPS)
	if err != nil {
		return market.Quote{}, err
	}
	amount, err := market.NewTokenAmount(input.TokenIn, amountIn)
	if err != nil {
		return market.Quote{}, err
	}
	return q.result(quoteport.Input{Snapshot: input.Snapshot, TokenIn: input.TokenIn, TokenOut: input.TokenOut, AmountIn: amount, Purpose: input.Purpose, QuotedAt: input.QuotedAt}, market.QuoteModeExactOutput, amount, input.TokenOut, input.AmountOut.Units(), fee)
}

func (q *Quoter) state(snapshot market.MarketSnapshot) (Snapshot, error) {
	if snapshot.Metadata().Market != q.market.ID {
		return Snapshot{}, fmt.Errorf("snapshot belongs to market %q, expected %q", snapshot.Metadata().Market, q.market.ID)
	}
	state, ok := snapshot.Data().(Snapshot)
	if !ok || state.schemaVersion != snapshotSchemaVersion {
		return Snapshot{}, fmt.Errorf("incompatible meteora DLMM snapshot %T", snapshot.Data())
	}
	return state, nil
}

func (q *Quoter) segments(state Snapshot, tokenIn, tokenOut market.TokenID) ([]liquiditycurve.Segment, error) {
	if tokenIn != q.tokenX && tokenIn != q.tokenY || tokenOut != q.tokenX && tokenOut != q.tokenY || tokenIn == tokenOut {
		return nil, fmt.Errorf("unsupported Meteora token direction")
	}
	result := make([]liquiditycurve.Segment, 0, len(state.bins))
	scale := new(big.Int).Lsh(big.NewInt(1), priceScaleBits)
	if tokenIn == q.tokenX {
		for _, bin := range state.bins {
			if bin.id < state.activeID {
				continue
			}
			if bin.reserveY.Sign() > 0 {
				// The protocol consumes Y liquidity at the bin price. The
				// corresponding X input capacity is rounded up.
				input := ceilMulDiv(bin.reserveY, scale, bin.priceX64)
				result = append(result, liquiditycurve.Segment{In: input, Out: bin.reserveY})
			}
		}
	} else {
		for i := len(state.bins) - 1; i >= 0; i-- {
			bin := state.bins[i]
			if bin.id > state.activeID {
				continue
			}
			if bin.reserveX.Sign() > 0 {
				input := ceilMulDiv(bin.reserveX, bin.priceX64, scale)
				result = append(result, liquiditycurve.Segment{In: input, Out: bin.reserveX})
			}
		}
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("meteora DLMM has no active liquidity")
	}
	return result, nil
}

func ceilMulDiv(value, multiplier, divisor *big.Int) *big.Int {
	product := new(big.Int).Mul(value, multiplier)
	product.Add(product, new(big.Int).Sub(new(big.Int).Set(divisor), big.NewInt(1)))
	return product.Quo(product, divisor)
}

func (q *Quoter) result(input quoteport.Input, mode market.QuoteMode, amountIn market.TokenAmount, outputToken market.TokenID, outputUnits, feeUnits *big.Int) (market.Quote, error) {
	amountOut, err := market.NewTokenAmount(outputToken, outputUnits)
	if err != nil {
		return market.Quote{}, err
	}
	fee, err := market.NewTokenAmount(input.TokenIn, feeUnits)
	if err != nil {
		return market.Quote{}, err
	}
	feeComponent, err := market.NewQuoteFee("liquidity_provider", market.QuoteFeeCost, fee, true)
	if err != nil {
		return market.Quote{}, err
	}
	metadata := input.Snapshot.Metadata()
	return market.NewQuote(market.Quote{Source: q.id, Market: q.market.ID, SnapshotVersion: metadata.Version, SnapshotHash: metadata.StateHash, Purpose: input.Purpose, Mode: mode, AmountIn: amountIn, AmountOut: amountOut, QuotedAt: input.QuotedAt}, feeComponent)
}

func matches(candidate market.Market, first, second market.TokenID) bool {
	return candidate.BaseToken == first && candidate.QuoteToken == second || candidate.BaseToken == second && candidate.QuoteToken == first
}

var _ quoteport.Source = (*Quoter)(nil)
var _ quoteport.ExactOutputSource = (*Quoter)(nil)
