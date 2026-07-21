package sizing_test

import (
	"context"
	"crypto/sha256"
	"math/big"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/core/sizing"
	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

type linearSource struct{}

func (linearSource) ID() market.SourceID { return "linear" }

func (linearSource) Quote(_ context.Context, input quoteport.Input) (market.Quote, error) {
	output, _ := market.NewTokenAmount(input.TokenOut, new(big.Int).Mul(input.AmountIn.Units(), big.NewInt(3)))
	return market.NewQuote(market.Quote{
		Source: "linear", Market: "market", SnapshotVersion: 1,
		Purpose: input.Purpose, Mode: market.QuoteModeExactInput,
		AmountIn: input.AmountIn, AmountOut: output, QuotedAt: input.QuotedAt,
	})
}

type nativeSource struct {
	linearSource
	calls int
}

func (s *nativeSource) QuoteExactOutput(_ context.Context, input quoteport.ExactOutputInput) (market.Quote, error) {
	s.calls++
	amountIn, _ := market.NewTokenAmount(input.TokenIn, new(big.Int).Quo(input.AmountOut.Units(), big.NewInt(2)))
	return market.NewQuote(market.Quote{
		Source: "native", Market: input.Snapshot.Metadata().Market,
		SnapshotVersion: input.Snapshot.Metadata().Version, SnapshotHash: input.Snapshot.Metadata().StateHash,
		Purpose: input.Purpose, Mode: market.QuoteModeExactOutput,
		AmountIn: amountIn, AmountOut: input.AmountOut, QuotedAt: input.QuotedAt,
	})
}

func TestMinimumInputForOutputPrefersNativeCapability(t *testing.T) {
	target, _ := market.NewTokenAmount("out", big.NewInt(20))
	high, _ := market.NewTokenAmount("in", big.NewInt(100))
	metadata := market.SnapshotMetadata{
		Market: "market", Source: "feed", Version: 1, EventPosition: market.SourcePosition{Kind: "block", Value: 1},
		Finality: market.FinalityConfirmed, ReceivedAt: time.Now(), AppliedAt: time.Now(),
		Health: market.HealthHealthy, HealthChangedAt: time.Now(), StateHash: sha256.Sum256([]byte("native")),
	}
	snapshot, err := market.NewMarketSnapshot(metadata, sizingSnapshot{})
	if err != nil {
		t.Fatal(err)
	}
	source := &nativeSource{}
	quote, err := sizing.MinimumInputForOutput(context.Background(), source, sizing.ExactOutputRequest{
		Snapshot: snapshot, TokenIn: "in", TokenOut: "out", TargetOut: target, InitialHigh: high,
		Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if source.calls != 1 || quote.AmountIn.Units().Cmp(big.NewInt(10)) != 0 {
		t.Fatalf("native calls=%d amount=%s", source.calls, quote.AmountIn.String())
	}
}

func TestMinimumInputForOutputReturnsSmallestRawInput(t *testing.T) {
	target, _ := market.ParseTokenAmount("out", "100")
	initial, _ := market.ParseTokenAmount("in", "4")
	metadata := market.SnapshotMetadata{
		Market: "market", Source: "feed", Version: 1, EventPosition: market.SourcePosition{Kind: "block", Value: 1},
		Finality: market.FinalityConfirmed, ReceivedAt: time.Now(), AppliedAt: time.Now(),
		Health: market.HealthHealthy, HealthChangedAt: time.Now(), StateHash: sha256.Sum256([]byte("state")),
	}
	snapshot, err := market.NewMarketSnapshot(metadata, sizingSnapshot{})
	if err != nil {
		t.Fatal(err)
	}
	quote, err := sizing.MinimumInputForOutput(context.Background(), linearSource{}, sizing.ExactOutputRequest{
		Snapshot: snapshot, TokenIn: "in", TokenOut: "out", TargetOut: target, InitialHigh: initial,
		Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if quote.AmountIn.Units().Cmp(big.NewInt(34)) != 0 || quote.AmountOut.Units().Cmp(big.NewInt(102)) != 0 {
		t.Fatalf("unexpected resolved quote %s -> %s", quote.AmountIn, quote.AmountOut)
	}
}

type sizingSnapshot struct{}

func (sizingSnapshot) SnapshotKind() string { return "test/v1" }
