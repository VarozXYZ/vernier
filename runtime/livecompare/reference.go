package livecompare

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/VarozXYZ/vernier/core/strategy"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
	runtimeresearch "github.com/VarozXYZ/vernier/runtime/research"
)

// ReferenceComparison is the auditable difference between one selected local
// leg and its optional external validation. The local sizing curve is not
// repeated here: it remains in the Opportunity candidates and this record
// points to the selected input/output only.
type ReferenceComparison struct {
	Direction          arbitrage.Direction
	Market             market.MarketID
	Leg                string
	Provider           market.SourceID
	SnapshotVersion    uint64
	Input              market.TokenAmount
	LocalOutput        market.TokenAmount
	ReferenceOutput    market.TokenAmount
	OutputDeltaRaw     string
	Status             quoteport.ReferenceStatus
	ContextSlot        uint64
	LocalQuoteDuration time.Duration
	ReferenceLatency   time.Duration
	TotalDuration      time.Duration
	Error              string
}

// validateReferences runs after local evaluation and never feeds a result
// back into the strategy classification. Only the selected leg is sent to an
// external source, at most once per direction. A failed provider therefore
// leaves the local opportunity intact and is represented as unavailable
// evidence.
func validateReferences(
	ctx context.Context,
	opportunities []arbitrage.Opportunity,
	snapshots []market.MarketSnapshot,
	sources map[market.MarketID]quoteport.Source,
	localTiming runtimeresearch.Report,
	clock func() time.Time,
) []ReferenceComparison {
	if clock == nil {
		clock = time.Now
	}
	snapshotByMarket := make(map[market.MarketID]market.MarketSnapshot, len(snapshots))
	for _, snapshot := range snapshots {
		snapshotByMarket[snapshot.Metadata().Market] = snapshot
	}
	results := make([]ReferenceComparison, 0, len(opportunities))
	for _, opportunity := range opportunities {
		if opportunity.SelectedIndex < 0 || opportunity.SelectedIndex >= len(opportunity.Candidates) {
			continue
		}
		candidate := opportunity.Candidates[opportunity.SelectedIndex]
		legs := []struct {
			name   string
			market market.MarketID
			quote  market.Quote
		}{
			{name: "buy", market: opportunity.Direction.BuyMarket, quote: candidate.BuyQuote},
			{name: "sell", market: opportunity.Direction.SellMarket, quote: candidate.SellQuote},
		}
		for _, leg := range legs {
			source, ok := sources[leg.market].(quoteport.ExternalReferenceSource)
			if !ok {
				continue
			}
			snapshot, ok := snapshotByMarket[leg.market]
			if !ok {
				continue
			}
			input := quoteport.Input{Snapshot: snapshot, TokenIn: leg.quote.AmountIn.Token(), TokenOut: leg.quote.AmountOut.Token(), AmountIn: leg.quote.AmountIn, Purpose: leg.quote.Purpose, QuotedAt: leg.quote.QuotedAt}
			started := clock().UTC()
			evidence, err := source.Reference(ctx, input, leg.quote)
			total := clock().UTC().Sub(started)
			comparison := ReferenceComparison{
				Direction: opportunity.Direction, Market: leg.market, Leg: leg.name, Provider: evidence.Provider,
				SnapshotVersion: snapshot.Metadata().Version, Input: leg.quote.AmountIn, LocalOutput: leg.quote.AmountOut,
				ReferenceOutput: evidence.AmountOut, Status: evidence.Status, ContextSlot: evidence.ContextSlot,
				LocalQuoteDuration: quoteDuration(localTiming.LocalTiming, opportunity.Direction, leg.name),
				ReferenceLatency:   evidence.Latency, TotalDuration: nonNegativeDuration(total), Error: evidence.Error,
			}
			if err != nil && comparison.Error == "" {
				comparison.Error = err.Error()
			}
			if evidence.Status == quoteport.ReferenceAvailable {
				if delta, deltaErr := signedDelta(leg.quote.AmountOut, evidence.AmountOut); deltaErr != nil {
					comparison.Status = quoteport.ReferenceUnavailable
					comparison.Error = deltaErr.Error()
				} else {
					comparison.OutputDeltaRaw = delta
				}
			}
			results = append(results, comparison)
			// A direction has one selected external route. The opposite direction
			// is evaluated independently and may produce its own validation.
			break
		}
	}
	return results
}

func quoteDuration(timing strategy.EvaluationTiming, direction arbitrage.Direction, leg string) time.Duration {
	for _, item := range timing.Directions {
		if item.Direction != direction {
			continue
		}
		for _, quote := range item.Quotes {
			if quote.Leg == leg {
				return quote.Duration
			}
		}
	}
	return 0
}

func signedDelta(local, reference market.TokenAmount) (string, error) {
	if local.Token() != reference.Token() {
		return "", fmt.Errorf("reference output token %q differs from local token %q", reference.Token(), local.Token())
	}
	return new(big.Int).Sub(reference.Units(), local.Units()).String(), nil
}

func nonNegativeDuration(value time.Duration) time.Duration {
	if value < 0 {
		return 0
	}
	return value
}
