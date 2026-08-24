package quote

import (
	"context"

	"github.com/VarozXYZ/vernier/domain/market"
)

type ConversionRequest struct {
	Input       market.TokenAmount
	OutputToken market.TokenID
}

type ConversionProvider interface {
	ID() market.SourceID
	QuoteConversion(context.Context, ConversionRequest) (market.TokenAmount, error)
}
