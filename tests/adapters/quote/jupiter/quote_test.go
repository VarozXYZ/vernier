package jupiter_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/VarozXYZ/vernier/adapters/quote/jupiter"
)

type quoteClientFunc func(*http.Request) (*http.Response, error)

func (f quoteClientFunc) Do(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestDirectQuoteUsesDefaultRouteAndParsesOutput(t *testing.T) {
	client := quoteClientFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != "/swap/v1/quote" {
			t.Fatalf("path = %q, want %q", request.URL.Path, "/swap/v1/quote")
		}
		query := request.URL.Query()
		if query.Get("inputMint") != "mint-in" || query.Get("outputMint") != "mint-out" || query.Get("amount") != "1000000" || query.Get("slippageBps") != "50" {
			t.Fatalf("unexpected query: %s", request.URL.RawQuery)
		}
		if query.Get("dexes") != "" {
			t.Fatalf("default quote unexpectedly restricted to dexes: %q", query.Get("dexes"))
		}
		if request.Header.Get("x-api-key") != "test-key" {
			t.Fatalf("missing Jupiter API key header")
		}
		body := `{"inputMint":"mint-in","inAmount":"1000000","outputMint":"mint-out","outAmount":"25500000","otherAmountThreshold":"25400000","priceImpactPct":"0.12","contextSlot":123,"routePlan":[{"percent":100,"swapInfo":{"ammKey":"pool","label":"Orca","inputMint":"mint-in","outputMint":"mint-out","inAmount":"1000000","outAmount":"25500000","feeAmount":"1000","feeMint":"mint-in"}}]}`
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(body))}, nil
	})
	limiter, err := jupiter.NewQuoteSpacingLimiter(0)
	if err != nil {
		t.Fatal(err)
	}
	source, err := jupiter.NewQuoteSource(jupiter.QuoteConfig{
		ID:          "jupiter-test",
		BaseURL:     "https://jupiter.test",
		APIKey:      "test-key",
		SlippageBPS: 50,
		Limiter:     limiter,
		Client:      client,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := source.Quote(context.Background(), jupiter.QuoteRequest{InputMint: "mint-in", OutputMint: "mint-out", Amount: "1000000"})
	if err != nil {
		t.Fatal(err)
	}
	if result.ToTokenAmount != "25500000" || result.ContextSlot != 123 || result.PriceImpactPercentage != "0.12" || len(result.RoutePlan) != 1 || result.RoutePlan[0].Label != "Orca" {
		t.Fatalf("unexpected quote result: %+v", result)
	}
}

func TestDirectQuoteSupportsSwapV2ManualOrder(t *testing.T) {
	useWSOL := false
	client := quoteClientFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != jupiter.DefaultOrderPath {
			t.Fatalf("path = %q, want %q", request.URL.Path, jupiter.DefaultOrderPath)
		}
		query := request.URL.Query()
		if query.Get("inputMint") != "mint-in" || query.Get("outputMint") != "mint-out" ||
			query.Get("amount") != "1000000" || query.Get("slippageBps") != "5" ||
			query.Get("taker") != "synthetic-taker" || query.Get("swapMode") != "ExactIn" ||
			query.Get("priorityFeeLamports") != "1000000" ||
			query.Get("broadcastFeeType") != "maxCap" ||
			query.Get("useWsol") != "false" ||
			query.Get("clientPlatform") != "synthetic.web" {
			t.Fatalf("unexpected manual order query: %s", request.URL.RawQuery)
		}
		body := `{"mode":"manual","router":"metis","feeBps":10,"inputMint":"mint-in","inAmount":"1000000","outputMint":"mint-out","outAmount":"25500000","otherAmountThreshold":"25372500","priceImpactPct":"0.12","routePlan":[{"percent":52.2,"bps":5220,"swapInfo":{"ammKey":"pool","label":"Raydium CLMM","inputMint":"mint-in","outputMint":"mint-out","inAmount":"1000000","outAmount":"25500000"}}]}`
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(body))}, nil
	})
	source, err := jupiter.NewQuoteSource(jupiter.QuoteConfig{
		ID: "jupiter-order", BaseURL: "https://jupiter.test",
		QuotePath: jupiter.DefaultOrderPath, ExpectedMode: "manual",
		APIKey: "test-key", SlippageBPS: 5, Taker: "synthetic-taker",
		SwapMode: "ExactIn", PriorityFeeLamports: 1_000_000,
		BroadcastFeeType: "maxCap", UseWSOL: &useWSOL, ClientPlatform: "synthetic.web",
		Limiter: jupiter.ImmediateLimiter{}, Client: client,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := source.Quote(context.Background(), jupiter.QuoteRequest{
		InputMint: "mint-in", OutputMint: "mint-out", Amount: "1000000",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.ToTokenAmount != "25500000" || result.Mode != "manual" ||
		result.ExpectedMode != "manual" || result.ModeMismatch ||
		result.Router != "metis" || result.FeeBPS != 10 ||
		len(result.RoutePlan) != 1 || result.RoutePlan[0].Percent != 52.2 ||
		result.RoutePlan[0].BPS != 5220 {
		t.Fatalf("unexpected manual order result: %+v", result)
	}
}

func TestDirectQuoteAcceptsUnexpectedSwapV2OrderModeAndMarksMismatch(t *testing.T) {
	source, err := jupiter.NewQuoteSource(jupiter.QuoteConfig{
		ID: "jupiter-order", BaseURL: "https://jupiter.test",
		QuotePath: jupiter.DefaultOrderPath, ExpectedMode: "manual",
		Limiter: jupiter.ImmediateLimiter{},
		Client: quoteClientFunc(func(*http.Request) (*http.Response, error) {
			body := `{"mode":"ultra","router":"metis","inputMint":"mint-in","inAmount":"1","outputMint":"mint-out","outAmount":"2"}`
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(body))}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := source.Quote(context.Background(), jupiter.QuoteRequest{
		InputMint: "mint-in", OutputMint: "mint-out", Amount: "1",
	})
	if err != nil {
		t.Fatalf("Ultra quote was rejected: %v", err)
	}
	if result.ToTokenAmount != "2" || result.Mode != "ultra" ||
		result.ExpectedMode != "manual" || !result.ModeMismatch {
		t.Fatalf("unexpected non-fatal mode mismatch result: %+v", result)
	}
}

func TestDirectQuotePassesDexFilter(t *testing.T) {
	limiter, err := jupiter.NewQuoteSpacingLimiter(0)
	if err != nil {
		t.Fatal(err)
	}
	source, err := jupiter.NewQuoteSource(jupiter.QuoteConfig{
		ID:      "jupiter-test",
		BaseURL: "https://jupiter.test",
		Limiter: limiter,
		Client: quoteClientFunc(func(request *http.Request) (*http.Response, error) {
			if request.URL.Query().Get("dexes") != "Meteora,Orca V2" {
				t.Fatalf("dexes = %q", request.URL.Query().Get("dexes"))
			}
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(`{"outAmount":"1"}`))}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := source.Quote(context.Background(), jupiter.QuoteRequest{InputMint: "mint-in", OutputMint: "mint-out", Amount: "1", Dexes: "Meteora,Orca V2"})
	if err != nil || result.ToTokenAmount != "1" {
		t.Fatalf("unexpected restricted quote: %+v err=%v", result, err)
	}
}

func TestDirectQuoteReturnsRateLimitError(t *testing.T) {
	limiter, err := jupiter.NewQuoteSpacingLimiter(0)
	if err != nil {
		t.Fatal(err)
	}
	source, err := jupiter.NewQuoteSource(jupiter.QuoteConfig{
		ID:      "jupiter-test",
		BaseURL: "https://jupiter.test",
		Limiter: limiter,
		Client: quoteClientFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusTooManyRequests, Status: "429 Too Many Requests", Body: io.NopCloser(strings.NewReader(`{"error":"rate limit","errorCode":"RATE_LIMIT"}`))}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = source.Quote(context.Background(), jupiter.QuoteRequest{InputMint: "mint-in", OutputMint: "mint-out", Amount: "1"})
	apiErr, ok := err.(*jupiter.APIError)
	if !ok || !apiErr.RateLimited() {
		t.Fatalf("error = %T %v, want rate-limited Jupiter API error", err, err)
	}
}

func TestAPIKeyPoolRotatesAcrossQuoteAndSwapSources(t *testing.T) {
	pool, err := jupiter.NewAPIKeyPool([]string{"key-0", "key-1"})
	if err != nil {
		t.Fatal(err)
	}
	quoteLimiter, err := jupiter.NewQuoteSpacingLimiter(0)
	if err != nil {
		t.Fatal(err)
	}
	quoteSource, err := jupiter.NewQuoteSource(jupiter.QuoteConfig{
		ID:         "jupiter-quote-test",
		BaseURL:    "https://jupiter.test",
		APIKeyPool: pool,
		Limiter:    quoteLimiter,
		Client: quoteClientFunc(func(request *http.Request) (*http.Response, error) {
			if request.Header.Get("x-api-key") != "key-0" {
				t.Fatalf("quote API key = %q, want key-0", request.Header.Get("x-api-key"))
			}
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(`{"outAmount":"1"}`))}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	swapLimiter, err := jupiter.NewQuoteSpacingLimiter(0)
	if err != nil {
		t.Fatal(err)
	}
	swapSource, err := jupiter.NewSwapSource(jupiter.SwapConfig{
		ID:         "jupiter-swap-test",
		BaseURL:    "https://jupiter.test",
		APIKeyPool: pool,
		Limiter:    swapLimiter,
		Client: quoteClientFunc(func(request *http.Request) (*http.Response, error) {
			if request.Header.Get("x-api-key") != "key-1" {
				t.Fatalf("swap API key = %q, want key-1", request.Header.Get("x-api-key"))
			}
			if request.URL.Path != "/swap/v1/swap" {
				t.Fatalf("swap path = %q", request.URL.Path)
			}
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(`{"swapTransaction":"unsigned","lastValidBlockHeight":10}`))}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	quote, err := quoteSource.Quote(context.Background(), jupiter.QuoteRequest{InputMint: "in", OutputMint: "out", Amount: "1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := swapSource.Swap(context.Background(), jupiter.SwapRequest{QuoteResponse: quote.RawResponse, UserPublicKey: "wallet"}); err != nil {
		t.Fatal(err)
	}
}

func TestAPIKeyPoolAssignsDifferentKeysToConcurrentQuotes(t *testing.T) {
	pool, err := jupiter.NewAPIKeyPool([]string{"key-0", "key-1"})
	if err != nil {
		t.Fatal(err)
	}
	var (
		mu   sync.Mutex
		keys []string
	)
	arrived := make(chan struct{}, 2)
	release := make(chan struct{})
	source, err := jupiter.NewQuoteSource(jupiter.QuoteConfig{
		ID: "jupiter-concurrent-test", BaseURL: "https://jupiter.test",
		APIKeyPool: pool, Limiter: jupiter.ImmediateLimiter{},
		Client: quoteClientFunc(func(request *http.Request) (*http.Response, error) {
			mu.Lock()
			keys = append(keys, request.Header.Get("x-api-key"))
			mu.Unlock()
			arrived <- struct{}{}
			<-release
			amount := request.URL.Query().Get("amount")
			body := `{"inputMint":"in","inAmount":"` + amount + `","outputMint":"out","outAmount":"1","contextSlot":1}`
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(body))}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errors := make(chan error, 2)
	for _, amount := range []string{"1", "2"} {
		amount := amount
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, quoteErr := source.Quote(context.Background(), jupiter.QuoteRequest{InputMint: "in", OutputMint: "out", Amount: amount})
			errors <- quoteErr
		}()
	}
	<-arrived
	<-arrived
	close(release)
	wg.Wait()
	close(errors)
	for quoteErr := range errors {
		if quoteErr != nil {
			t.Fatal(quoteErr)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(keys) != 2 || keys[0] == keys[1] {
		t.Fatalf("concurrent quotes did not reserve distinct keys: %v", keys)
	}
}
