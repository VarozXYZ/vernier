// Package price defines external price-source contracts consumed by the core.
package price

import (
	"context"

	"github.com/VarozXYZ/vernier/domain/market"
)

type Request struct {
	Base  market.AssetID
	Quote market.AssetID
}

type Source interface {
	ID() market.SourceID
	Observe(context.Context, Request) (market.PriceObservation, error)
}
