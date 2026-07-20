package constantproduct

import (
	"context"
	"fmt"
	"math/big"

	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

const basisPoints = 10_000

type Quoter struct {
	id     market.SourceID
	market market.Market
}

func NewQuoter(id market.SourceID, candidate market.Market) (*Quoter, error) {
	if id == "" || candidate.ID == "" || candidate.BaseToken == "" || candidate.QuoteToken == "" {
		return nil, fmt.Errorf("source and complete market are required")
	}
	return &Quoter{id: id, market: candidate}, nil
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
		return market.Quote{}, fmt.Errorf("incompatible constant-product snapshot %T", input.Snapshot.Data())
	}
	if input.AmountIn.Token() != input.TokenIn || input.AmountIn.IsZero() {
		return market.Quote{}, fmt.Errorf("positive input amount must match input token")
	}

	var reserveIn, reserveOut *big.Int
	switch {
	case input.TokenIn == q.market.QuoteToken && input.TokenOut == q.market.BaseToken:
		reserveIn, reserveOut = state.QuoteReserve(), state.BaseReserve()
	case input.TokenIn == q.market.BaseToken && input.TokenOut == q.market.QuoteToken:
		reserveIn, reserveOut = state.BaseReserve(), state.QuoteReserve()
	default:
		return market.Quote{}, fmt.Errorf("unsupported token direction %q -> %q", input.TokenIn, input.TokenOut)
	}

	amountIn := input.AmountIn.Units()
	feeMultiplier := big.NewInt(int64(basisPoints - int(state.feeBPS)))
	amountInWithFee := new(big.Int).Mul(amountIn, feeMultiplier)
	numerator := new(big.Int).Mul(amountInWithFee, reserveOut)
	denominator := new(big.Int).Add(new(big.Int).Mul(reserveIn, big.NewInt(basisPoints)), amountInWithFee)
	amountOutUnits := new(big.Int).Quo(numerator, denominator)
	if amountOutUnits.Sign() <= 0 {
		return market.Quote{}, fmt.Errorf("quote output rounds to zero")
	}
	amountOut, err := market.NewTokenAmount(input.TokenOut, amountOutUnits)
	if err != nil {
		return market.Quote{}, err
	}
	feeUnits := new(big.Int).Quo(new(big.Int).Mul(amountIn, big.NewInt(int64(state.feeBPS))), big.NewInt(basisPoints))
	fee, err := market.NewTokenAmount(input.TokenIn, feeUnits)
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
