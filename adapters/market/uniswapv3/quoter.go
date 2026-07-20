package uniswapv3

import (
	"context"
	"fmt"

	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

type Quoter struct {
	id     market.SourceID
	market market.Market
	token0 market.TokenID
	token1 market.TokenID
}

func NewQuoter(id market.SourceID, candidate market.Market, token0, token1 market.TokenID) (*Quoter, error) {
	if id == "" || candidate.ID == "" || token0 == "" || token1 == "" || token0 == token1 {
		return nil, fmt.Errorf("source, market, and distinct pool tokens are required")
	}
	if !matchesEndpoints(candidate, token0, token1) {
		return nil, fmt.Errorf("uniswap V3 pool tokens do not match market endpoints")
	}
	return &Quoter{id: id, market: candidate, token0: token0, token1: token1}, nil
}

func (q *Quoter) ID() market.SourceID { return q.id }

func (q *Quoter) Quote(ctx context.Context, input quoteport.Input) (market.Quote, error) {
	if err := ctx.Err(); err != nil {
		return market.Quote{}, err
	}
	metadata := input.Snapshot.Metadata()
	if metadata.Market != q.market.ID {
		return market.Quote{}, fmt.Errorf("snapshot belongs to market %q, expected %q", metadata.Market, q.market.ID)
	}
	state, ok := input.Snapshot.Data().(Snapshot)
	if !ok || state.schemaVersion != snapshotSchemaVersion {
		return market.Quote{}, fmt.Errorf("incompatible Uniswap V3 snapshot %T", input.Snapshot.Data())
	}
	if input.AmountIn.Token() != input.TokenIn || input.AmountIn.IsZero() {
		return market.Quote{}, fmt.Errorf("positive input amount must match input token")
	}
	zeroForOne := input.TokenIn == q.token0 && input.TokenOut == q.token1
	oneForZero := input.TokenIn == q.token1 && input.TokenOut == q.token0
	if !zeroForOne && !oneForZero {
		return market.Quote{}, fmt.Errorf("unsupported token direction %q -> %q", input.TokenIn, input.TokenOut)
	}
	result, err := quoteExactInput(state, zeroForOne, input.AmountIn.Units())
	if err != nil {
		return market.Quote{}, err
	}
	amountOut, err := market.NewTokenAmount(input.TokenOut, result.amountOut)
	if err != nil {
		return market.Quote{}, err
	}
	fee, err := market.NewTokenAmount(input.TokenIn, result.fee)
	if err != nil {
		return market.Quote{}, err
	}
	feeComponent, err := market.NewQuoteFee("liquidity_provider", market.QuoteFeeCost, fee, true)
	if err != nil {
		return market.Quote{}, err
	}
	return market.NewQuote(market.Quote{
		Source: q.id, Market: q.market.ID, SnapshotVersion: metadata.Version, SnapshotHash: metadata.StateHash,
		Purpose: input.Purpose, AmountIn: input.AmountIn, AmountOut: amountOut, QuotedAt: input.QuotedAt,
	}, feeComponent)
}

func matchesEndpoints(candidate market.Market, token0, token1 market.TokenID) bool {
	return candidate.BaseToken == token0 && candidate.QuoteToken == token1 ||
		candidate.BaseToken == token1 && candidate.QuoteToken == token0
}
