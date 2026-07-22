// Package crosschain composes configured pool mirrors and protocol-neutral
// quoters into a route market. It is a runtime composition helper; it owns no
// RPC, signer, or persistence concerns.
package crosschain

import (
	"context"
	"fmt"
	"time"

	quotemarket "github.com/VarozXYZ/vernier/adapters/quote/route"
	"github.com/VarozXYZ/vernier/core/marketstate"
	"github.com/VarozXYZ/vernier/domain/market"
	feedport "github.com/VarozXYZ/vernier/ports/feed"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

type Child struct {
	Market market.Market
	Mirror *marketstate.Mirror
	Source quoteport.Source
}

type Route struct {
	Market market.Market
	Mirror *marketstate.RouteMirror
	Source quoteport.Source
}

func NewRoute(candidate market.Market, sourceID market.SourceID, children []Child, clock marketstate.Clock) (*Route, error) {
	if candidate.ID == "" || sourceID == "" || len(children) == 0 || clock == nil {
		return nil, fmt.Errorf("route candidate, source, children, and clock are required")
	}
	mirrors := make([]feedport.Mirror, 0, len(children))
	hops := make([]quotemarket.Hop, 0, len(children))
	for _, child := range children {
		if child.Market.ID == "" || child.Mirror == nil || child.Source == nil {
			return nil, fmt.Errorf("route child is incomplete")
		}
		mirrors = append(mirrors, child.Mirror)
		hops = append(hops, quotemarket.Hop{Market: child.Market.ID, In: child.Market.BaseToken, Out: child.Market.QuoteToken, Source: child.Source})
	}
	mirror, err := marketstate.NewRouteMirror(candidate.ID, sourceID, mirrors, clock)
	if err != nil {
		return nil, err
	}
	source, err := quotemarket.New(sourceID+"/local", candidate, hops)
	if err != nil {
		return nil, err
	}
	return &Route{Market: candidate, Mirror: mirror, Source: source}, nil
}

func (r *Route) Apply(ctx context.Context, event market.MarketEvent) (feedport.ApplyResult, error) {
	return r.Mirror.Apply(ctx, event)
}
func (r *Route) Reset(ctx context.Context, event market.MarketEvent) (feedport.ApplyResult, error) {
	return r.Mirror.Reset(ctx, event)
}
func (r *Route) SetChildHealth(ctx context.Context, child market.MarketID, update feedport.HealthUpdate) error {
	return r.Mirror.SetChildHealth(ctx, child, update)
}
func (r *Route) Snapshot() (market.MarketSnapshot, bool) { return r.Mirror.Current() }

func FixedClock(at time.Time) marketstate.Clock { return func() time.Time { return at.UTC() } }
