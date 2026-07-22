// Package route composes protocol-neutral hop quoters into one market quote
// source. The caller provides the configured hop order; no DEX-specific type
// leaks through this package.
package route

import (
	"context"
	"fmt"

	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

type Hop struct {
	Market market.MarketID
	In     market.TokenID
	Out    market.TokenID
	Source quoteport.Source
}

type Source struct {
	id     market.SourceID
	market market.Market
	hops   []Hop
}

func New(id market.SourceID, candidate market.Market, hops []Hop) (*Source, error) {
	if id == "" || candidate.ID == "" || len(hops) == 0 {
		return nil, fmt.Errorf("route source, market, and hops are required")
	}
	previous := candidate.BaseToken
	for index, hop := range hops {
		if hop.Market == "" || hop.In == "" || hop.Out == "" || hop.In == hop.Out || hop.Source == nil {
			return nil, fmt.Errorf("route hop %d is incomplete", index)
		}
		if hop.In != previous && index == 0 {
			return nil, fmt.Errorf("route first hop input does not match market base token")
		}
		if index > 0 && hop.In != hops[index-1].Out {
			return nil, fmt.Errorf("route hops are discontinuous at %d", index)
		}
		previous = hop.Out
	}
	if previous != candidate.QuoteToken {
		return nil, fmt.Errorf("route final hop output does not match market quote token")
	}
	return &Source{id: id, market: candidate, hops: append([]Hop(nil), hops...)}, nil
}

func (s *Source) ID() market.SourceID { return s.id }

func (s *Source) Quote(ctx context.Context, input quoteport.Input) (market.Quote, error) {
	if err := ctx.Err(); err != nil {
		return market.Quote{}, err
	}
	if input.Snapshot.Metadata().Market != s.market.ID {
		return market.Quote{}, fmt.Errorf("snapshot belongs to market %q, expected %q", input.Snapshot.Metadata().Market, s.market.ID)
	}
	bundle, ok := input.Snapshot.Data().(market.SnapshotBundle)
	if !ok {
		return market.Quote{}, fmt.Errorf("route quote requires a snapshot bundle")
	}
	current := input.AmountIn
	var first market.Quote
	for index, hop := range s.hops {
		snapshot, ok := child(bundle, hop.Market)
		if !ok {
			return market.Quote{}, fmt.Errorf("route snapshot is missing hop %q", hop.Market)
		}
		if current.Token() != hop.In {
			return market.Quote{}, fmt.Errorf("route amount token mismatch at hop %d", index)
		}
		result, err := hop.Source.Quote(ctx, quoteport.Input{Snapshot: snapshot, TokenIn: hop.In, TokenOut: hop.Out, AmountIn: current, Purpose: input.Purpose, QuotedAt: input.QuotedAt})
		if err != nil {
			return market.Quote{}, fmt.Errorf("quote route hop %d: %w", index, err)
		}
		if index == 0 {
			first = result
		}
		current = result.AmountOut
	}
	return market.NewQuote(market.Quote{Source: s.id, Market: s.market.ID, SnapshotVersion: input.Snapshot.Metadata().Version, SnapshotHash: input.Snapshot.Metadata().StateHash, Purpose: input.Purpose, Mode: market.QuoteModeExactInput, AmountIn: input.AmountIn, AmountOut: current, QuotedAt: input.QuotedAt}, first.Fees()...)
}

func (s *Source) QuoteExactOutput(ctx context.Context, input quoteport.ExactOutputInput) (market.Quote, error) {
	if err := ctx.Err(); err != nil {
		return market.Quote{}, err
	}
	bundle, ok := input.Snapshot.Data().(market.SnapshotBundle)
	if !ok {
		return market.Quote{}, fmt.Errorf("route quote requires a snapshot bundle")
	}
	current := input.AmountOut
	var final market.Quote
	for index := len(s.hops) - 1; index >= 0; index-- {
		hop := s.hops[index]
		source, ok := hop.Source.(quoteport.ExactOutputSource)
		if !ok {
			return market.Quote{}, fmt.Errorf("route hop %d does not support exact output", index)
		}
		snapshot, ok := child(bundle, hop.Market)
		if !ok {
			return market.Quote{}, fmt.Errorf("route snapshot is missing hop %q", hop.Market)
		}
		result, err := source.QuoteExactOutput(ctx, quoteport.ExactOutputInput{Snapshot: snapshot, TokenIn: hop.In, TokenOut: hop.Out, AmountOut: current, Purpose: input.Purpose, QuotedAt: input.QuotedAt})
		if err != nil {
			return market.Quote{}, fmt.Errorf("quote route hop %d: %w", index, err)
		}
		if index == len(s.hops)-1 {
			final = result
		}
		current = result.AmountIn
	}
	return market.NewQuote(market.Quote{Source: s.id, Market: s.market.ID, SnapshotVersion: input.Snapshot.Metadata().Version, SnapshotHash: input.Snapshot.Metadata().StateHash, Purpose: input.Purpose, Mode: market.QuoteModeExactOutput, AmountIn: current, AmountOut: input.AmountOut, QuotedAt: input.QuotedAt}, final.Fees()...)
}

func child(bundle market.SnapshotBundle, id market.MarketID) (market.MarketSnapshot, bool) {
	for _, snapshot := range bundle.Snapshots() {
		if snapshot.Metadata().Market == id {
			return snapshot, true
		}
	}
	return market.MarketSnapshot{}, false
}

var _ quoteport.Source = (*Source)(nil)
var _ quoteport.ExactOutputSource = (*Source)(nil)
