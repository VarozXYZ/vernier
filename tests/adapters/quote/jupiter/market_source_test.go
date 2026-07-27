package jupiter_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/adapters/quote/jupiter"
	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

type causalData struct{}

func (causalData) SnapshotKind() string { return "causal/test" }

func TestMarketSourceBindsJupiterQuoteToCausalSnapshot(t *testing.T) {
	calls := 0
	client := quoteClientFunc(func(request *http.Request) (*http.Response, error) {
		calls++
		query := request.URL.Query()
		if query.Get("inputMint") != "mint-quote" || query.Get("outputMint") != "mint-base" || query.Get("amount") != "1250000" {
			t.Fatalf("unexpected Jupiter query: %s", request.URL.RawQuery)
		}
		return &http.Response{
			StatusCode: http.StatusOK, Status: "200 OK",
			Body: io.NopCloser(strings.NewReader(`{"inputMint":"mint-quote","inAmount":"1250000","outputMint":"mint-base","outAmount":"42000000","contextSlot":1234}`)),
		}, nil
	})
	direct, err := jupiter.NewQuoteSource(jupiter.QuoteConfig{
		ID: "http", BaseURL: "https://jupiter.test", Client: client,
		Limiter: jupiter.ImmediateLimiter{},
	})
	if err != nil {
		t.Fatal(err)
	}
	candidate := market.Market{ID: "remote-market", BaseToken: "base", QuoteToken: "quote"}
	source, err := jupiter.NewMarketSource(jupiter.MarketSourceConfig{
		ID: "jupiter", Market: candidate, Client: direct,
		TokenMints: map[market.TokenID]string{"base": "mint-base", "quote": "mint-quote"},
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	hash := sha256.Sum256([]byte("generation-7"))
	snapshot, err := market.NewMarketSnapshot(market.SnapshotMetadata{
		Market: "remote-market", Source: "pool-events", Version: 7,
		ReceivedAt: now, AppliedAt: now, Health: market.HealthHealthy,
		HealthChangedAt: now, StateHash: hash,
	}, causalData{})
	if err != nil {
		t.Fatal(err)
	}
	amount, _ := market.NewTokenAmount("quote", big.NewInt(1_250_000))
	result, err := source.Quote(context.Background(), quoteport.Input{
		Snapshot: snapshot, TokenIn: "quote", TokenOut: "base", AmountIn: amount,
		Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.AmountOut.Units().Cmp(big.NewInt(42_000_000)) != 0 ||
		result.SnapshotVersion != 7 || result.SnapshotHash != hash ||
		result.Market != "remote-market" || result.Source != "jupiter" ||
		result.SourcePosition.Kind != jupiter.ContextSlotPositionKind || result.SourcePosition.Value != 1234 ||
		result.ResponseHash == ([32]byte{}) {
		t.Fatalf("unexpected Research quote: %+v", result)
	}
	if source.CacheQuotes() {
		t.Fatal("Jupiter generation cache must remain owned by the adapter")
	}
	cached, err := source.Quote(context.Background(), quoteport.Input{
		Snapshot: snapshot, TokenIn: "quote", TokenOut: "base", AmountIn: amount,
		Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: now.Add(time.Millisecond),
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || cached.Quality != market.QuoteQualityCachedExact {
		t.Fatalf("same Solana generation did not reuse exact quote: calls=%d quality=%s", calls, cached.Quality)
	}
}

func TestMarketSourceUsesTriggerPositionAndReportsNonFatalSwapV2ModeMismatch(t *testing.T) {
	client := quoteClientFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body: io.NopCloser(strings.NewReader(
				`{"mode":"ultra","router":"metis","feeBps":10,"inputMint":"mint-quote","inAmount":"1250000","outputMint":"mint-base","outAmount":"42000000"}`,
			)),
		}, nil
	})
	direct, err := jupiter.NewQuoteSource(jupiter.QuoteConfig{
		ID: "http", BaseURL: "https://jupiter.test", QuotePath: jupiter.DefaultOrderPath,
		ExpectedMode: "manual", Client: client, Limiter: jupiter.ImmediateLimiter{},
	})
	if err != nil {
		t.Fatal(err)
	}
	source, err := jupiter.NewMarketSource(jupiter.MarketSourceConfig{
		ID: "jupiter", Market: market.Market{ID: "remote-market", BaseToken: "base", QuoteToken: "quote"},
		TokenMints: map[market.TokenID]string{"base": "mint-base", "quote": "mint-quote"},
		Client:     direct, FreshOnly: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 27, 10, 0, 0, 0, time.UTC)
	snapshot, err := market.NewMarketSnapshot(market.SnapshotMetadata{
		Market: "remote-market", Source: "pool-events", Version: 8,
		EventPosition: market.SourcePosition{Kind: "solana_slot", Value: 5678},
		ReceivedAt:    now, AppliedAt: now, Health: market.HealthHealthy,
		HealthChangedAt: now, StateHash: sha256.Sum256([]byte("generation-8")),
	}, causalData{})
	if err != nil {
		t.Fatal(err)
	}
	amount, _ := market.NewTokenAmount("quote", big.NewInt(1_250_000))
	result, err := source.Quote(context.Background(), quoteport.Input{
		Snapshot: snapshot, TokenIn: "quote", TokenOut: "base", AmountIn: amount,
		Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.SourcePosition.Kind != "solana_slot" || result.SourcePosition.Value != 5678 {
		t.Fatalf("Swap V2 order lost causal trigger position: %+v", result.SourcePosition)
	}
	warnings := source.TakeOperationalWarnings()
	if len(warnings) != 1 || warnings[0].Code != "jupiter_order_mode_mismatch" ||
		warnings[0].Expected != "manual" || warnings[0].Observed != "ultra" ||
		warnings[0].Provider != "jupiter" || warnings[0].Market != "remote-market" ||
		warnings[0].Details["router"] != "metis" {
		t.Fatalf("unexpected operational warnings: %+v", warnings)
	}
	if remaining := source.TakeOperationalWarnings(); len(remaining) != 0 {
		t.Fatalf("operational warnings were not drained: %+v", remaining)
	}
}

func TestMarketSourceFreshOnlyNeverUsesGenerationOrRateLimitCache(t *testing.T) {
	calls := 0
	client := quoteClientFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"inputMint":"mint-quote","inAmount":"100","outputMint":"mint-base","outAmount":"42","contextSlot":1234}`)),
		}, nil
	})
	direct, err := jupiter.NewQuoteSource(jupiter.QuoteConfig{
		ID: "http", BaseURL: "https://jupiter.test", Client: client, Limiter: jupiter.ImmediateLimiter{},
	})
	if err != nil {
		t.Fatal(err)
	}
	source, err := jupiter.NewMarketSource(jupiter.MarketSourceConfig{
		ID: "jupiter", Market: market.Market{ID: "remote-market", BaseToken: "base", QuoteToken: "quote"},
		TokenMints: map[market.TokenID]string{"base": "mint-base", "quote": "mint-quote"},
		Client:     direct, FreshOnly: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 27, 10, 0, 0, 0, time.UTC)
	amount, _ := market.NewTokenAmount("quote", big.NewInt(100))
	input := quoteport.Input{
		Snapshot: causalSnapshot(t, 1, now, "fresh-only"), TokenIn: "quote", TokenOut: "base",
		AmountIn: amount, Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: now,
	}
	if _, err := source.Quote(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Quote(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || source.LastTiming().Cached {
		t.Fatalf("fresh-only source reused a quote: calls=%d timing=%+v", calls, source.LastTiming())
	}
}

func TestMarketSourceScalesChangedAmountWithinGenerationAndFreshBypassesEstimate(t *testing.T) {
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	calls := 0
	client := quoteClientFunc(func(request *http.Request) (*http.Response, error) {
		calls++
		amount := request.URL.Query().Get("amount")
		output := "42"
		if amount == "200" {
			output = "70"
		}
		body := fmt.Sprintf(`{"inputMint":"mint-quote","inAmount":%q,"outputMint":"mint-base","outAmount":%q,"contextSlot":1234}`, amount, output)
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(body))}, nil
	})
	direct, err := jupiter.NewQuoteSource(jupiter.QuoteConfig{
		ID: "http", BaseURL: "https://jupiter.test", Client: client, Limiter: jupiter.ImmediateLimiter{},
	})
	if err != nil {
		t.Fatal(err)
	}
	source, err := jupiter.NewMarketSource(jupiter.MarketSourceConfig{
		ID: "jupiter", Market: market.Market{ID: "remote-market", BaseToken: "base", QuoteToken: "quote"},
		TokenMints: map[market.TokenID]string{"base": "mint-base", "quote": "mint-quote"}, Client: direct,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := causalSnapshot(t, 1, now, "generation")
	firstAmount, _ := market.NewTokenAmount("quote", big.NewInt(100))
	input := quoteport.Input{
		Snapshot: snapshot, TokenIn: "quote", TokenOut: "base", AmountIn: firstAmount,
		Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: now,
	}
	if _, err := source.Quote(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	secondAmount, _ := market.NewTokenAmount("quote", big.NewInt(200))
	input.AmountIn = secondAmount
	input.QuotedAt = now.Add(time.Millisecond)
	estimated, err := source.Quote(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || estimated.AmountOut.Units().Cmp(big.NewInt(84)) != 0 ||
		estimated.Quality != market.QuoteQualityProportionalEstimate {
		t.Fatalf("unexpected proportional estimate: calls=%d quote=%+v", calls, estimated)
	}
	confirmed, err := source.QuoteFresh(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 || confirmed.AmountOut.Units().Cmp(big.NewInt(70)) != 0 ||
		confirmed.Quality != market.QuoteQualityExact {
		t.Fatalf("fresh confirmation did not bypass estimate: calls=%d quote=%+v", calls, confirmed)
	}
}

func TestMarketSourceRejectsSnapshotFromAnotherMarket(t *testing.T) {
	direct, err := jupiter.NewQuoteSource(jupiter.QuoteConfig{
		ID: "http", BaseURL: "https://jupiter.test", Limiter: jupiter.ImmediateLimiter{},
		Client: quoteClientFunc(func(*http.Request) (*http.Response, error) {
			t.Fatal("invalid input reached Jupiter")
			return nil, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	source, err := jupiter.NewMarketSource(jupiter.MarketSourceConfig{
		ID: "jupiter", Market: market.Market{ID: "expected", BaseToken: "base", QuoteToken: "quote"},
		TokenMints: map[market.TokenID]string{"base": "mint-base", "quote": "mint-quote"}, Client: direct,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	snapshot, _ := market.NewMarketSnapshot(market.SnapshotMetadata{
		Market: "other", Source: "events", Version: 1, ReceivedAt: now, AppliedAt: now,
		Health: market.HealthHealthy, HealthChangedAt: now, StateHash: sha256.Sum256([]byte("other")),
	}, causalData{})
	amount, _ := market.NewTokenAmount("quote", big.NewInt(1))
	if _, err := source.Quote(context.Background(), quoteport.Input{
		Snapshot: snapshot, TokenIn: "quote", TokenOut: "base", AmountIn: amount,
		Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: now,
	}); err == nil {
		t.Fatal("wrong-market snapshot was accepted")
	}
}

func TestMarketSourceUsesYoungCompatibleQuoteOnlyAfterRateLimit(t *testing.T) {
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	calls := 0
	client := quoteClientFunc(func(*http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return &http.Response{
				StatusCode: http.StatusOK, Status: "200 OK",
				Body: io.NopCloser(strings.NewReader(`{"inputMint":"mint-quote","inAmount":"100","outputMint":"mint-base","outAmount":"42","contextSlot":1234}`)),
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusTooManyRequests, Status: "429 Too Many Requests",
			Body: io.NopCloser(strings.NewReader(`{"error":"rate limit"}`)),
		}, nil
	})
	direct, err := jupiter.NewQuoteSource(jupiter.QuoteConfig{
		ID: "http", BaseURL: "https://jupiter.test", Client: client, Limiter: jupiter.ImmediateLimiter{},
	})
	if err != nil {
		t.Fatal(err)
	}
	source, err := jupiter.NewMarketSource(jupiter.MarketSourceConfig{
		ID: "jupiter", Market: market.Market{ID: "remote-market", BaseToken: "base", QuoteToken: "quote"},
		TokenMints: map[market.TokenID]string{"base": "mint-base", "quote": "mint-quote"},
		Client:     direct, Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	firstSnapshot := causalSnapshot(t, 1, now, "first")
	amount, _ := market.NewTokenAmount("quote", big.NewInt(100))
	input := quoteport.Input{
		Snapshot: firstSnapshot, TokenIn: "quote", TokenOut: "base", AmountIn: amount,
		Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: now,
	}
	first, err := source.Quote(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(jupiter.RateLimitFallbackMaxAge)
	input.Snapshot = causalSnapshot(t, 2, now, "second")
	input.QuotedAt = now
	fallback, err := source.Quote(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 || fallback.AmountOut.Units().Cmp(first.AmountOut.Units()) != 0 ||
		fallback.SnapshotVersion != 2 || fallback.SnapshotHash != input.Snapshot.Metadata().StateHash ||
		!source.LastTiming().Cached || source.TakeRateLimitRetry() {
		t.Fatalf("unexpected bounded rate-limit fallback: calls=%d quote=%+v timing=%+v", calls, fallback, source.LastTiming())
	}
}

func TestMarketSourceRejectsStaleRateLimitFallbackAndRequestsOneStreamRetry(t *testing.T) {
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	calls := 0
	client := quoteClientFunc(func(*http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return &http.Response{
				StatusCode: http.StatusOK, Status: "200 OK",
				Body: io.NopCloser(strings.NewReader(`{"inputMint":"mint-quote","inAmount":"100","outputMint":"mint-base","outAmount":"42","contextSlot":1234}`)),
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusTooManyRequests, Status: "429 Too Many Requests",
			Body: io.NopCloser(strings.NewReader(`{"error":"rate limit"}`)),
		}, nil
	})
	direct, err := jupiter.NewQuoteSource(jupiter.QuoteConfig{
		ID: "http", BaseURL: "https://jupiter.test", Client: client, Limiter: jupiter.ImmediateLimiter{},
	})
	if err != nil {
		t.Fatal(err)
	}
	source, err := jupiter.NewMarketSource(jupiter.MarketSourceConfig{
		ID: "jupiter", Market: market.Market{ID: "remote-market", BaseToken: "base", QuoteToken: "quote"},
		TokenMints: map[market.TokenID]string{"base": "mint-base", "quote": "mint-quote"},
		Client:     direct, Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	amount, _ := market.NewTokenAmount("quote", big.NewInt(100))
	input := quoteport.Input{
		Snapshot: causalSnapshot(t, 1, now, "first"), TokenIn: "quote", TokenOut: "base", AmountIn: amount,
		Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: now,
	}
	if _, err := source.Quote(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	now = now.Add(jupiter.RateLimitFallbackMaxAge + time.Nanosecond)
	input.Snapshot = causalSnapshot(t, 2, now, "second")
	input.QuotedAt = now
	_, err = source.Quote(context.Background(), input)
	var apiErr *jupiter.APIError
	if !errors.As(err, &apiErr) || !apiErr.RateLimited() {
		t.Fatalf("stale fallback returned %T %v, want rate-limit error", err, err)
	}
	if source.LastTiming().Cached || !source.TakeRateLimitRetry() || source.TakeRateLimitRetry() {
		t.Fatalf("stale fallback did not expose exactly one retry marker: timing=%+v", source.LastTiming())
	}
}

func TestMarketSourceDoesNotUseFallbackForOtherClientErrors(t *testing.T) {
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	calls := 0
	client := quoteClientFunc(func(*http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return &http.Response{
				StatusCode: http.StatusOK, Status: "200 OK",
				Body: io.NopCloser(strings.NewReader(`{"inputMint":"mint-quote","inAmount":"100","outputMint":"mint-base","outAmount":"42","contextSlot":1234}`)),
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusBadRequest, Status: "400 Bad Request",
			Body: io.NopCloser(strings.NewReader(`{"error":"invalid request"}`)),
		}, nil
	})
	direct, err := jupiter.NewQuoteSource(jupiter.QuoteConfig{
		ID: "http", BaseURL: "https://jupiter.test", Client: client, Limiter: jupiter.ImmediateLimiter{},
	})
	if err != nil {
		t.Fatal(err)
	}
	source, err := jupiter.NewMarketSource(jupiter.MarketSourceConfig{
		ID: "jupiter", Market: market.Market{ID: "remote-market", BaseToken: "base", QuoteToken: "quote"},
		TokenMints: map[market.TokenID]string{"base": "mint-base", "quote": "mint-quote"},
		Client:     direct, Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	amount, _ := market.NewTokenAmount("quote", big.NewInt(100))
	input := quoteport.Input{
		Snapshot: causalSnapshot(t, 1, now, "first"), TokenIn: "quote", TokenOut: "base", AmountIn: amount,
		Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: now,
	}
	if _, err := source.Quote(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Millisecond)
	input.Snapshot = causalSnapshot(t, 2, now, "second")
	input.QuotedAt = now
	_, err = source.Quote(context.Background(), input)
	var apiErr *jupiter.APIError
	if !errors.As(err, &apiErr) || apiErr.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("client error = %T %v, want Jupiter 400", err, err)
	}
	if source.LastTiming().Cached || source.TakeRateLimitRetry() {
		t.Fatalf("non-rate-limit error used fallback or requested retry: timing=%+v", source.LastTiming())
	}
}

func TestMarketSourceDoesNotReuseYoungQuoteForAnotherAmount(t *testing.T) {
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	calls := 0
	client := quoteClientFunc(func(*http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return &http.Response{
				StatusCode: http.StatusOK, Status: "200 OK",
				Body: io.NopCloser(strings.NewReader(`{"inputMint":"mint-quote","inAmount":"100","outputMint":"mint-base","outAmount":"42","contextSlot":1234}`)),
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusTooManyRequests, Status: "429 Too Many Requests",
			Body: io.NopCloser(strings.NewReader(`{"error":"rate limit"}`)),
		}, nil
	})
	direct, err := jupiter.NewQuoteSource(jupiter.QuoteConfig{
		ID: "http", BaseURL: "https://jupiter.test", Client: client, Limiter: jupiter.ImmediateLimiter{},
	})
	if err != nil {
		t.Fatal(err)
	}
	source, err := jupiter.NewMarketSource(jupiter.MarketSourceConfig{
		ID: "jupiter", Market: market.Market{ID: "remote-market", BaseToken: "base", QuoteToken: "quote"},
		TokenMints: map[market.TokenID]string{"base": "mint-base", "quote": "mint-quote"},
		Client:     direct, Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	firstAmount, _ := market.NewTokenAmount("quote", big.NewInt(100))
	input := quoteport.Input{
		Snapshot: causalSnapshot(t, 1, now, "first"), TokenIn: "quote", TokenOut: "base", AmountIn: firstAmount,
		Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: now,
	}
	if _, err := source.Quote(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Millisecond)
	secondAmount, _ := market.NewTokenAmount("quote", big.NewInt(101))
	input.Snapshot = causalSnapshot(t, 2, now, "second")
	input.AmountIn = secondAmount
	input.QuotedAt = now
	_, err = source.Quote(context.Background(), input)
	var apiErr *jupiter.APIError
	if !errors.As(err, &apiErr) || !apiErr.RateLimited() {
		t.Fatalf("different-amount request returned %T %v, want rate-limit error", err, err)
	}
	if source.LastTiming().Cached || !source.TakeRateLimitRetry() {
		t.Fatalf("different amount reused cached quote: timing=%+v", source.LastTiming())
	}
}

func causalSnapshot(t *testing.T, version uint64, at time.Time, state string) market.MarketSnapshot {
	t.Helper()
	snapshot, err := market.NewMarketSnapshot(market.SnapshotMetadata{
		Market: "remote-market", Source: "events", Version: version,
		ReceivedAt: at, AppliedAt: at, Health: market.HealthHealthy,
		HealthChangedAt: at, StateHash: sha256.Sum256([]byte(state)),
	}, causalData{})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
