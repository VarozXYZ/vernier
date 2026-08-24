package kyberswap_test

import (
	"context"
	"crypto/sha256"
	"math/big"
	"net/http"
	"strings"
	"sync/atomic"
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

func TestMarketSourceLiveCacheInvalidatesAndFreshBypasses(t *testing.T) {
	requests := 0
	direct, err := kyberswap.New(kyberswap.Config{
		BaseURL: "https://kyberswap.test", ClientID: "vernier-test",
		Client: clientFunc(func(request *http.Request) (*http.Response, error) {
			requests++
			return response(http.StatusOK, routeResponse()), nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	source, err := kyberswap.NewMarketSource(kyberswap.MarketSourceConfig{
		ID: "kyber", Market: market.Market{ID: "remote", BaseToken: "base", QuoteToken: "quote"},
		TokenAddresses: map[market.TokenID]string{"base": tokenOut, "quote": tokenIn},
		Chain:          "synthetic", Client: direct, CacheEnabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 27, 10, 0, 0, 0, time.UTC)
	snapshot, err := market.NewMarketSnapshot(market.SnapshotMetadata{Market: "remote", Source: "events", Version: 1,
		EventPosition: market.SourcePosition{Kind: "block", Value: 1}, ReceivedAt: now, AppliedAt: now,
		Health: market.HealthHealthy, HealthChangedAt: now, StateHash: sha256.Sum256([]byte("state"))}, remoteSnapshotData{})
	if err != nil {
		t.Fatal(err)
	}
	amount, _ := market.NewTokenAmount("quote", big.NewInt(1_000_000))
	input := quoteport.Input{Snapshot: snapshot, TokenIn: "quote", TokenOut: "base", AmountIn: amount, Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: now}
	if _, err = source.QuoteFresh(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	if _, err = source.Quote(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	if requests != 1 {
		t.Fatalf("cached quote made %d requests", requests)
	}
	if _, err = source.QuoteFresh(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	if requests != 2 {
		t.Fatalf("fresh quote made %d requests", requests)
	}
	source.Invalidate()
	if _, err = source.Quote(context.Background(), input); err == nil {
		t.Fatal("invalidated cache unexpectedly served a quote")
	}
	if err = source.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if requests != 3 {
		t.Fatalf("invalidated cache made %d requests", requests)
	}
}

func TestMarketSourceLiveCacheBootstrapsUnknownInputOffHotPath(t *testing.T) {
	var requests atomic.Int32
	direct, err := kyberswap.New(kyberswap.Config{
		BaseURL: "https://kyberswap.test", ClientID: "vernier-test",
		Client: clientFunc(func(request *http.Request) (*http.Response, error) {
			requests.Add(1)
			return response(http.StatusOK, routeResponse()), nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	source, err := kyberswap.NewMarketSource(kyberswap.MarketSourceConfig{
		ID: "kyber", Market: market.Market{ID: "remote", BaseToken: "base", QuoteToken: "quote"},
		TokenAddresses: map[market.TokenID]string{"base": tokenOut, "quote": tokenIn},
		Chain:          "synthetic", Client: direct, CacheEnabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 27, 10, 0, 0, 0, time.UTC)
	snapshot, err := market.NewMarketSnapshot(market.SnapshotMetadata{Market: "remote", Source: "events", Version: 1,
		EventPosition: market.SourcePosition{Kind: "block", Value: 1}, ReceivedAt: now, AppliedAt: now,
		Health: market.HealthHealthy, HealthChangedAt: now, StateHash: sha256.Sum256([]byte("state"))}, remoteSnapshotData{})
	if err != nil {
		t.Fatal(err)
	}
	amount, _ := market.NewTokenAmount("quote", big.NewInt(1_000_000))
	input := quoteport.Input{Snapshot: snapshot, TokenIn: "quote", TokenOut: "base", AmountIn: amount,
		Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: now}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source.StartRefresh(ctx, 50*time.Millisecond)

	if _, err = source.Quote(ctx, input); err == nil {
		t.Fatal("empty live cache unexpectedly served a quote")
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("cache miss performed %d HTTP requests on the hot path", got)
	}

	deadline := time.Now().Add(time.Second)
	for requests.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("background bootstrap performed %d HTTP requests", got)
	}
	if _, err = source.Quote(ctx, input); err != nil {
		t.Fatalf("background bootstrap did not populate the cache: %v", err)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("cached quote performed another HTTP request: %d", got)
	}
}

func TestMarketSourceLiveCacheReusesAndReplacesLatestInput(t *testing.T) {
	requests := 0
	direct, err := kyberswap.New(kyberswap.Config{
		BaseURL: "https://kyberswap.test", ClientID: "vernier-test",
		Client: clientFunc(func(request *http.Request) (*http.Response, error) {
			requests++
			amount := request.URL.Query().Get("amountIn")
			body := strings.ReplaceAll(routeResponse(), `"amountIn":"1000000"`, `"amountIn":"`+amount+`"`)
			body = strings.ReplaceAll(body, `"swapAmount":"1000000"`, `"swapAmount":"`+amount+`"`)
			return response(http.StatusOK, body), nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	source, err := kyberswap.NewMarketSource(kyberswap.MarketSourceConfig{
		ID: "kyber", Market: market.Market{ID: "remote", BaseToken: "base", QuoteToken: "quote"},
		TokenAddresses: map[market.TokenID]string{"base": tokenOut, "quote": tokenIn},
		Chain:          "synthetic", Client: direct, CacheEnabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 27, 10, 0, 0, 0, time.UTC)
	snapshot, err := market.NewMarketSnapshot(market.SnapshotMetadata{Market: "remote", Source: "events", Version: 1,
		EventPosition: market.SourcePosition{Kind: "block", Value: 1}, ReceivedAt: now, AppliedAt: now,
		Health: market.HealthHealthy, HealthChangedAt: now, StateHash: sha256.Sum256([]byte("state"))}, remoteSnapshotData{})
	if err != nil {
		t.Fatal(err)
	}
	firstAmount, _ := market.NewTokenAmount("quote", big.NewInt(1_000_000))
	secondAmount, _ := market.NewTokenAmount("quote", big.NewInt(2_000_000))
	first := quoteport.Input{Snapshot: snapshot, TokenIn: "quote", TokenOut: "base", AmountIn: firstAmount,
		Purpose: market.QuotePurposeLiveDiscovery, QuotedAt: now}
	second := first
	second.AmountIn = secondAmount

	if _, err = source.QuoteFresh(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	estimated, err := source.Quote(context.Background(), second)
	if err != nil {
		t.Fatal(err)
	}
	if requests != 1 {
		t.Fatalf("changed local input performed %d provider requests", requests)
	}
	if estimated.Quality != market.QuoteQualityProportionalEstimate || estimated.AmountOut.Units().String() != "5000000000000000000" {
		t.Fatalf("unexpected proportional cached quote: %+v", estimated)
	}

	source.Invalidate()
	if err = source.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if requests != 2 {
		t.Fatalf("refresh retained historical inputs: requests=%d", requests)
	}
	if _, err = source.Quote(context.Background(), second); err != nil {
		t.Fatalf("latest input was not refreshed: %v", err)
	}
}

func TestMarketSourceLiveCacheAmountChangePreservesFailureBackoff(t *testing.T) {
	var requests atomic.Int32
	direct, err := kyberswap.New(kyberswap.Config{
		BaseURL: "https://kyberswap.test", ClientID: "vernier-test",
		Client: clientFunc(func(request *http.Request) (*http.Response, error) {
			requests.Add(1)
			return response(http.StatusServiceUnavailable, `{"message":"unavailable"}`), nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	source, err := kyberswap.NewMarketSource(kyberswap.MarketSourceConfig{
		ID: "kyber", Market: market.Market{ID: "remote", BaseToken: "base", QuoteToken: "quote"},
		TokenAddresses: map[market.TokenID]string{"base": tokenOut, "quote": tokenIn},
		Chain:          "synthetic", Client: direct, CacheEnabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 27, 10, 0, 0, 0, time.UTC)
	snapshot, err := market.NewMarketSnapshot(market.SnapshotMetadata{Market: "remote", Source: "events", Version: 1,
		EventPosition: market.SourcePosition{Kind: "block", Value: 1}, ReceivedAt: now, AppliedAt: now,
		Health: market.HealthHealthy, HealthChangedAt: now, StateHash: sha256.Sum256([]byte("state"))}, remoteSnapshotData{})
	if err != nil {
		t.Fatal(err)
	}
	firstAmount, _ := market.NewTokenAmount("quote", big.NewInt(1_000_000))
	secondAmount, _ := market.NewTokenAmount("quote", big.NewInt(2_000_000))
	first := quoteport.Input{Snapshot: snapshot, TokenIn: "quote", TokenOut: "base", AmountIn: firstAmount,
		Purpose: market.QuotePurposeLiveDiscovery, QuotedAt: now}
	second := first
	second.AmountIn = secondAmount

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source.StartRefresh(ctx, 250*time.Millisecond)
	if _, err = source.Quote(ctx, first); err == nil {
		t.Fatal("empty cache unexpectedly served a quote")
	}
	deadline := time.Now().Add(time.Second)
	for requests.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(25 * time.Millisecond)
	if _, err = source.Quote(ctx, second); err == nil {
		t.Fatal("failed cache unexpectedly served a changed amount")
	}
	time.Sleep(100 * time.Millisecond)
	if got := requests.Load(); got != 1 {
		t.Fatalf("amount churn bypassed provider failure backoff: requests=%d", got)
	}
}
