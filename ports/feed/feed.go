// Package feed defines market-data ingestion and mirror contracts.
package feed

import (
	"context"
	"fmt"

	"github.com/VarozXYZ/vernier/domain/market"
)

type EventSink interface {
	Publish(context.Context, market.MarketEvent) error
}

type Feed interface {
	MarketID() market.MarketID
	Run(context.Context, EventSink) error
}

type Mirror interface {
	MarketID() market.MarketID
	Apply(context.Context, market.MarketEvent) (market.MarketSnapshot, error)
	Current() (market.MarketSnapshot, bool)
	Health() market.Health
}

type SequenceViolation struct {
	Market   market.MarketID
	Expected uint64
	Actual   uint64
}

func (e SequenceViolation) Error() string {
	return fmt.Sprintf("market %q expected sequence %d, received %d", e.Market, e.Expected, e.Actual)
}

func (e SequenceViolation) IsGap() bool { return e.Actual > e.Expected }
