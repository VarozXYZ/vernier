package dlmm

import (
	"context"
	"fmt"
	"math/big"
	"time"

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
	swapForY, err := q.direction(input.TokenIn, input.TokenOut)
	if err != nil {
		return market.Quote{}, err
	}
	quotedAt := quoteTime(input.QuotedAt, input.Snapshot.Metadata().AppliedAt, stateUpdateTime(state))
	protocolResult, err := quoteExactIn(state, input.AmountIn.Units(), swapForY, quotedAt.Unix())
	if err != nil {
		return market.Quote{}, err
	}
	feeToken := input.TokenIn
	if !state.feeOnInput(swapForY) {
		feeToken = input.TokenOut
	}
	input.QuotedAt = quotedAt
	return q.result(input, market.QuoteModeExactInput, input.AmountIn, input.TokenOut, protocolResult.amountOut, feeToken, protocolResult.fee)
}

func (q *Quoter) QuoteExactOutput(ctx context.Context, input quoteport.ExactOutputInput) (market.Quote, error) {
	if err := ctx.Err(); err != nil {
		return market.Quote{}, err
	}
	state, err := q.state(input.Snapshot)
	if err != nil {
		return market.Quote{}, err
	}
	swapForY, err := q.direction(input.TokenIn, input.TokenOut)
	if err != nil {
		return market.Quote{}, err
	}
	quotedAt := quoteTime(input.QuotedAt, input.Snapshot.Metadata().AppliedAt, stateUpdateTime(state))
	protocolResult, err := quoteExactOut(state, input.AmountOut.Units(), swapForY, quotedAt.Unix())
	if err != nil {
		return market.Quote{}, err
	}
	amount, err := market.NewTokenAmount(input.TokenIn, protocolResult.amountIn)
	if err != nil {
		return market.Quote{}, err
	}
	feeToken := input.TokenIn
	if !state.feeOnInput(swapForY) {
		feeToken = input.TokenOut
	}
	return q.result(quoteport.Input{Snapshot: input.Snapshot, TokenIn: input.TokenIn, TokenOut: input.TokenOut, AmountIn: amount, Purpose: input.Purpose, QuotedAt: quotedAt}, market.QuoteModeExactOutput, amount, input.TokenOut, input.AmountOut.Units(), feeToken, protocolResult.fee)
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

func (q *Quoter) direction(tokenIn, tokenOut market.TokenID) (bool, error) {
	if tokenIn != q.tokenX && tokenIn != q.tokenY || tokenOut != q.tokenX && tokenOut != q.tokenY || tokenIn == tokenOut {
		return false, fmt.Errorf("unsupported Meteora token direction")
	}
	return tokenIn == q.tokenX, nil
}

func quoteTime(quotedAt time.Time, stateTimes ...time.Time) time.Time {
	latest := quotedAt.UTC()
	for _, candidate := range stateTimes {
		candidate = candidate.UTC()
		if !candidate.IsZero() && (latest.IsZero() || candidate.After(latest)) {
			latest = candidate
		}
	}
	if latest.IsZero() {
		return time.Now().UTC()
	}
	// Snapshot application and Meteora's embedded protocol timestamp come
	// from different clocks. Dynamic fees must never be evaluated before
	// either state boundary.
	return latest
}

func stateUpdateTime(state Snapshot) time.Time {
	if state.lastUpdateTime <= 0 {
		return time.Time{}
	}
	return time.Unix(state.lastUpdateTime, 0).UTC()
}

func (q *Quoter) result(input quoteport.Input, mode market.QuoteMode, amountIn market.TokenAmount, outputToken market.TokenID, outputUnits *big.Int, feeToken market.TokenID, feeUnits *big.Int) (market.Quote, error) {
	amountOut, err := market.NewTokenAmount(outputToken, outputUnits)
	if err != nil {
		return market.Quote{}, err
	}
	fee, err := market.NewTokenAmount(feeToken, feeUnits)
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
