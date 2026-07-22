package orcawhirlpool

import (
	"context"
	"fmt"
	"math/big"

	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

// Whirlpool prices are encoded as Q64.64 fixed-point values. q64 is the
// fractional-bit count, not the fixed-point scale itself.
const q64 uint = 64

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
	reserveIn, reserveOut, err := q.virtualReserves(state, input.TokenIn, input.TokenOut)
	if err != nil {
		return market.Quote{}, err
	}
	out, fee, err := exactInput(reserveIn, reserveOut, input.AmountIn.Units(), state.feeBPS)
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
	reserveIn, reserveOut, err := q.virtualReserves(state, input.TokenIn, input.TokenOut)
	if err != nil {
		return market.Quote{}, err
	}
	in, fee, err := exactOutput(reserveIn, reserveOut, input.AmountOut.Units(), state.feeBPS)
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
func (q *Quoter) virtualReserves(state Snapshot, tokenIn, tokenOut market.TokenID) (*big.Int, *big.Int, error) {
	if tokenIn != q.tokenA && tokenIn != q.tokenB || tokenOut != q.tokenA && tokenOut != q.tokenB || tokenIn == tokenOut {
		return nil, nil, fmt.Errorf("unsupported Whirlpool token direction")
	}
	if state.liquidity.Sign() <= 0 {
		return nil, nil, fmt.Errorf("whirlpool has no active liquidity")
	}
	q64Value := new(big.Int).Lsh(big.NewInt(1), q64)
	reserveA := new(big.Int).Mul(state.liquidity, q64Value)
	reserveA.Quo(reserveA, state.sqrtPriceX64)
	reserveB := new(big.Int).Mul(state.liquidity, state.sqrtPriceX64)
	reserveB.Quo(reserveB, q64Value)
	if reserveA.Sign() <= 0 || reserveB.Sign() <= 0 {
		return nil, nil, fmt.Errorf("whirlpool virtual reserves round to zero")
	}
	if tokenIn == q.tokenA {
		return reserveA, reserveB, nil
	}
	return reserveB, reserveA, nil
}

func exactInput(reserveIn, reserveOut, amount *big.Int, feeBPS uint16) (*big.Int, *big.Int, error) {
	if amount == nil || amount.Sign() <= 0 || reserveIn.Sign() <= 0 || reserveOut.Sign() <= 0 || feeBPS >= 10_000 {
		return nil, nil, fmt.Errorf("invalid whirlpool exact-input request")
	}
	feeBase := big.NewInt(10_000)
	feeRate := new(big.Int).SetUint64(uint64(feeBPS))
	afterFee := new(big.Int).Mul(amount, new(big.Int).Sub(feeBase, feeRate))
	afterFee.Quo(afterFee, feeBase)
	if afterFee.Sign() <= 0 {
		return nil, nil, fmt.Errorf("whirlpool input rounds to zero after fee")
	}
	denominator := new(big.Int).Add(reserveIn, afterFee)
	out := new(big.Int).Mul(afterFee, reserveOut)
	out.Quo(out, denominator)
	if out.Sign() <= 0 || out.Cmp(reserveOut) >= 0 {
		return nil, nil, fmt.Errorf("whirlpool output is outside active liquidity")
	}
	fee := new(big.Int).Sub(amount, afterFee)
	return out, fee, nil
}

func exactOutput(reserveIn, reserveOut, amountOut *big.Int, feeBPS uint16) (*big.Int, *big.Int, error) {
	if amountOut == nil || amountOut.Sign() <= 0 || reserveIn.Sign() <= 0 || reserveOut.Sign() <= 0 || feeBPS >= 10_000 || amountOut.Cmp(reserveOut) >= 0 {
		return nil, nil, fmt.Errorf("invalid whirlpool exact-output request")
	}
	feeBase := big.NewInt(10_000)
	denominator := new(big.Int).Sub(reserveOut, amountOut)
	afterFee := new(big.Int).Mul(amountOut, reserveIn)
	afterFee.Add(afterFee, new(big.Int).Sub(denominator, big.NewInt(1)))
	afterFee.Quo(afterFee, denominator)
	gross := new(big.Int).Mul(afterFee, feeBase)
	feeRate := new(big.Int).SetUint64(uint64(feeBPS))
	gross.Add(gross, new(big.Int).Sub(new(big.Int).Sub(feeBase, feeRate), big.NewInt(1)))
	gross.Quo(gross, new(big.Int).Sub(feeBase, feeRate))
	fee := new(big.Int).Sub(gross, afterFee)
	return gross, fee, nil
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
