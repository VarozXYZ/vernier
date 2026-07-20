// Package synthetic provides deterministic in-memory market feeds.
package synthetic

import (
	"context"
	"fmt"

	"github.com/VarozXYZ/vernier/domain/market"
	feedport "github.com/VarozXYZ/vernier/ports/feed"
)

type Feed struct {
	market market.MarketID
	events []market.MarketEvent
}

func New(marketID market.MarketID, events []market.MarketEvent) (*Feed, error) {
	if marketID == "" {
		return nil, fmt.Errorf("market ID is required")
	}
	for index, event := range events {
		if event.Market != marketID {
			return nil, fmt.Errorf("event %d belongs to market %q, expected %q", index, event.Market, marketID)
		}
	}
	return &Feed{market: marketID, events: append([]market.MarketEvent(nil), events...)}, nil
}

func (f *Feed) MarketID() market.MarketID { return f.market }

func (f *Feed) Run(ctx context.Context, sink feedport.Sink) error {
	if sink == nil {
		return fmt.Errorf("event sink is required")
	}
	for _, event := range f.events {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := sink.Publish(ctx, event); err != nil {
			return err
		}
	}
	return nil
}
