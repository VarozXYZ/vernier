package orcawhirlpool

import (
	"context"
	"fmt"
	"math/big"

	"github.com/VarozXYZ/vernier/adapters/market/liquiditycurve"
	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

const q64 uint64 = 1 << 6 // used only as a shift exponent below

type Quoter struct {
	id             market.SourceID
	market         market.Market
	tokenA, tokenB market.TokenID
}

func NewQuoter(id market.SourceID, candidate market.Market, tokenA, tokenB market.TokenID) (*Quoter, error) {
	if id == "" || candidate.ID == "" || tokenA == "" || tokenB == "" || tokenA == tokenB || !matches(candidate, tokenA, tokenB) {
		return nil, fmt.Errorf("source, market, and Whirlpool token endpoints are required")
	}
	return &Quoter{id: id, market: candidate, tokenA: tokenA, tokenB: tokenB}, nil
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
	out, fee, err := liquiditycurve.ExactInput(segments, input.AmountIn.Units(), state.feeBPS)
	if err != nil {
		return market.Quote{}, err
	}
	return q.result(input, market.QuoteModeExactInput, input.AmountIn, input.TokenOut, out, fee)
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
	in, fee, err := liquiditycurve.ExactOutput(segments, input.AmountOut.Units(), state.feeBPS)
	if err != nil {
		return market.Quote{}, err
	}
	amount, err := market.NewTokenAmount(input.TokenIn, in)
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
		return Snapshot{}, fmt.Errorf("incompatible Orca Whirlpool snapshot %T", snapshot.Data())
	}
	return state, nil
}

// segments derives the active virtual reserves from Q64.64 price and then
// walks initialized ticks in direction order. The local reserve segment is
// deterministic; a future decoder can provide additional covered segments
// without changing the quote contract.
func (q *Quoter) segments(state Snapshot, tokenIn, tokenOut market.TokenID) ([]liquiditycurve.Segment, error) {
	if tokenIn != q.tokenA && tokenIn != q.tokenB || tokenOut != q.tokenA && tokenOut != q.tokenB || tokenIn == tokenOut {
		return nil, fmt.Errorf("unsupported Whirlpool token direction")
	}
	if state.liquidity.Sign() <= 0 {
		return nil, fmt.Errorf("whirlpool has no active liquidity")
	}
	q64Value := new(big.Int).Lsh(big.NewInt(1), uint(q64))
	reserveA := new(big.Int).Mul(state.liquidity, q64Value)
	reserveA.Quo(reserveA, state.sqrtPriceX64)
	reserveB := new(big.Int).Mul(state.liquidity, state.sqrtPriceX64)
	reserveB.Quo(reserveB, q64Value)
	if reserveA.Sign() <= 0 || reserveB.Sign() <= 0 {
		return nil, fmt.Errorf("whirlpool virtual reserves round to zero")
	}
	if tokenIn == q.tokenA {
		return []liquiditycurve.Segment{{In: reserveA, Out: reserveB}}, nil
	}
	return []liquiditycurve.Segment{{In: reserveB, Out: reserveA}}, nil
}

func (q *Quoter) result(input quoteport.Input, mode market.QuoteMode, amountIn market.TokenAmount, outputToken market.TokenID, outputUnits, feeUnits *big.Int) (market.Quote, error) {
	out, err := market.NewTokenAmount(outputToken, outputUnits)
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
	return market.NewQuote(market.Quote{Source: q.id, Market: q.market.ID, SnapshotVersion: metadata.Version, SnapshotHash: metadata.StateHash, Purpose: input.Purpose, Mode: mode, AmountIn: amountIn, AmountOut: out, QuotedAt: input.QuotedAt}, feeComponent)
}
func matches(candidate market.Market, first, second market.TokenID) bool {
	return candidate.BaseToken == first && candidate.QuoteToken == second || candidate.BaseToken == second && candidate.QuoteToken == first
}

var _ quoteport.Source = (*Quoter)(nil)
var _ quoteport.ExactOutputSource = (*Quoter)(nil)
