package kyberswap_test

import (
	"context"
	"crypto/sha256"
	"math/big"
	"net/http"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/adapters/quote/kyberswap"
	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

type remoteSnapshotData struct{}

func (remoteSnapshotData) SnapshotKind() string { return "synthetic_remote/v1" }

func TestMarketSourceAlwaysRequestsAndPreservesEvidence(t *testing.T) {
	requests := 0
	direct, err := kyberswap.New(kyberswap.Config{
		BaseURL: "https://kyberswap.test", ClientID: "vernier-test",
		Client: clientFunc(func(request *http.Request) (*http.Response, error) {
			requests++
			time.Sleep(time.Millisecond)
			return response(http.StatusOK, routeResponse()), nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	source, err := kyberswap.NewMarketSource(kyberswap.MarketSourceConfig{
		ID: "kyber", Market: market.Market{ID: "remote", BaseToken: "base", QuoteToken: "quote"},
		TokenAddresses: map[market.TokenID]string{"base": tokenOut, "quote": tokenIn},
		Chain:          "polygon", Client: direct,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 27, 10, 0, 0, 0, time.UTC)
	snapshot, err := market.NewMarketSnapshot(market.SnapshotMetadata{
		Market: "remote", Source: "events", Version: 1,
		EventPosition:  market.SourcePosition{Kind: "block", Value: 10},
		EventReference: market.SourceReference{Kind: "transaction", Value: "synthetic"},
		Finality:       market.FinalityPreconfirmed, ReceivedAt: now, AppliedAt: now,
		Health: market.HealthHealthy, HealthChangedAt: now, StateHash: sha256.Sum256([]byte("state")),
	}, remoteSnapshotData{})
	if err != nil {
		t.Fatal(err)
	}
	amount, _ := market.NewTokenAmount("quote", big.NewInt(1_000_000))
	input := quoteport.Input{
		Snapshot: snapshot, TokenIn: "quote", TokenOut: "base", AmountIn: amount,
		Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: now,
	}
	first, err := source.Quote(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	second, err := source.Quote(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if requests != 2 || first.AmountOut.Units().String() != "2500000000000000000" ||
		first.ResponseHash == ([sha256.Size]byte{}) || second.ResponseHash != first.ResponseHash ||
		source.LastTiming().Duration <= 0 {
		t.Fatalf("unexpected market quote evidence: requests=%d first=%+v timing=%+v", requests, first, source.LastTiming())
	}
}
