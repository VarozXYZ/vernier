package costing

import (
	"context"
	"fmt"

	"github.com/VarozXYZ/vernier/domain/market"
	priceport "github.com/VarozXYZ/vernier/ports/price"
)

type FallbackPriceSource struct {
	id       market.SourceID
	primary  priceport.Source
	fallback priceport.Source
}

func NewFallbackPriceSource(id market.SourceID, primary, fallback priceport.Source) (*FallbackPriceSource, error) {
	if id == "" || primary == nil || fallback == nil || primary.ID() == fallback.ID() {
		return nil, fmt.Errorf("fallback price source requires an ID and distinct providers")
	}
	return &FallbackPriceSource{id: id, primary: primary, fallback: fallback}, nil
}

func (s *FallbackPriceSource) ID() market.SourceID { return s.id }

func (s *FallbackPriceSource) Observe(ctx context.Context, request priceport.Request) (market.PriceObservation, error) {
	observation, primaryErr := s.primary.Observe(ctx, request)
	if primaryErr == nil {
		return observation, nil
	}
	if err := ctx.Err(); err != nil {
		return market.PriceObservation{}, err
	}
	observation, fallbackErr := s.fallback.Observe(ctx, request)
	if fallbackErr != nil {
		return market.PriceObservation{}, fmt.Errorf("primary price source %q failed: %v; fallback %q failed: %w", s.primary.ID(), primaryErr, s.fallback.ID(), fallbackErr)
	}
	return observation, nil
}
