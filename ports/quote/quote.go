// Package quote defines quote-source contracts consumed by the core.
package quote

import (
	"context"
	"time"

	"github.com/VarozXYZ/vernier/domain/market"
)

type Input struct {
	Snapshot market.MarketSnapshot
	TokenIn  market.TokenID
	TokenOut market.TokenID
	AmountIn market.TokenAmount
	Purpose  market.QuotePurpose
	QuotedAt time.Time
}

type Source interface {
	ID() market.SourceID
	Quote(context.Context, Input) (market.Quote, error)
}
