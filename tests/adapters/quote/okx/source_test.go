package okx_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/adapters/quote/okx"
)

type clientFunc func(*http.Request) (*http.Response, error)

func (f clientFunc) Do(request *http.Request) (*http.Response, error) { return f(request) }

type noWaitLimiter struct{}

func (noWaitLimiter) Wait(context.Context) error { return nil }

func TestNewRequiresCredentialsAndUsesSolanaDefaults(t *testing.T) {
	if _, err := okx.New(okx.Config{ID: "okx"}); err == nil {
		t.Fatal("expected credentials validation error")
	}

	source, err := okx.New(okx.Config{
		ID:         "okx",
		APIKey:     "key",
		SecretKey:  "secret",
		Passphrase: "pass",
		Limiter:    noWaitLimiter{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if source.ID() != "okx" {
		t.Fatalf("source ID = %q", source.ID())
	}
}

func TestQuoteBuildsAuthenticatedRequestAndParsesRoute(t *testing.T) {
	var received *http.Request
	client := clientFunc(func(request *http.Request) (*http.Response, error) {
		received = request
		body := `{
          "code":"0",
          "msg":"",
          "data":[{
            "chainIndex":"501",
            "swapMode":"exactIn",
            "fromToken":{"tokenContractAddress":"from","tokenSymbol":"IN","decimal":"6"},
            "toToken":{"tokenContractAddress":"to","tokenSymbol":"OUT","decimal":"8"},
            "fromTokenAmount":"1000000",
            "toTokenAmount":"24680",
            "originToTokenAmount":"25000",
            "tradeFee":"0.001",
            "estimateGasFee":"5000",
            "priceImpactPercentage":"0.02",
            "dexRouterList":[{
              "router":"route-a",
              "routerPercent":"100",
              "dexProtocol":{"dexName":"Synthetic DEX","percent":"100"},
              "subRouterList":[{
                "dexProtocol":[{"dexName":"Synthetic Pool","percent":"100"}],
                "fromToken":{"tokenContractAddress":"from","tokenSymbol":"IN","decimal":"6"},
                "toToken":{"tokenContractAddress":"to","tokenSymbol":"OUT","decimal":"8"}
              }]
            }],
            "quoteCompareList":[{"dexName":"Alternative","amountOut":"24000","tradeFee":"0.002","priceImpactPercentage":"0.03"}]
          }]
        }`
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(body))}, nil
	})
	clock := func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 600000000, time.UTC) }
	source, err := okx.New(okx.Config{
		ID:         "okx",
		BaseURL:    "https://okx.test",
		APIKey:     "key",
		SecretKey:  "secret",
		Passphrase: "pass",
		ProjectID:  "project",
		Client:     client,
		Clock:      clock,
		Limiter:    noWaitLimiter{},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := source.Quote(context.Background(), okx.QuoteRequest{
		FromTokenAddress: "from",
		ToTokenAddress:   "to",
		Amount:           "1000000",
	})
	if err != nil {
		t.Fatal(err)
	}
	if received == nil {
		t.Fatal("client did not receive a request")
	}
	query := received.URL.Query()
	for key, want := range map[string]string{
		"chainIndex":       "501",
		"amount":           "1000000",
		"swapMode":         "exactIn",
		"fromTokenAddress": "from",
		"toTokenAddress":   "to",
	} {
		if got := query.Get(key); got != want {
			t.Fatalf("query %s = %q, want %q", key, got, want)
		}
	}
	if got := received.Header.Get("OK-ACCESS-KEY"); got != "key" || received.Header.Get("OK-ACCESS-PASSPHRASE") != "pass" || received.Header.Get("OK-ACCESS-PROJECT") != "project" {
		t.Fatalf("unexpected auth headers: %#v", received.Header)
	}
	timestamp := clock().UTC().Format("2006-01-02T15:04:05.000Z")
	path := received.URL.EscapedPath() + "?" + received.URL.RawQuery
	hasher := hmac.New(sha256.New, []byte("secret"))
	_, _ = hasher.Write([]byte(timestamp + http.MethodGet + path))
	wantSignature := base64.StdEncoding.EncodeToString(hasher.Sum(nil))
	if got := received.Header.Get("OK-ACCESS-SIGN"); got != wantSignature {
		t.Fatalf("signature = %q, want %q", got, wantSignature)
	}

	if result.ResponseCode != "0" || result.ChainIndex != "501" || result.SwapMode != okx.SwapModeExactInput || result.ToTokenAmount != "24680" || result.OriginToTokenAmount != "25000" || result.ToToken.Decimal != "8" || result.ToToken.Symbol != "OUT" || len(result.Routes) != 1 || len(result.Routes[0].Protocols) != 1 || len(result.Routes[0].SubRoutes) != 1 || len(result.Comparisons) != 1 {
		t.Fatalf("unexpected parsed quote: %+v", result)
	}
	if result.HTTPDuration < 0 || result.TotalDuration < result.HTTPDuration {
		t.Fatalf("invalid timing: %+v", result)
	}
}

func TestLiquiditySourcesParsesCurrentIDs(t *testing.T) {
	var received *http.Request
	source := mustSource(t, okx.Config{
		Limiter: noWaitLimiter{},
		Client: clientFunc(func(request *http.Request) (*http.Response, error) {
			received = request
			body := `{"code":"0","data":[{"id":"101","name":"Meteora DLMM"},{"id":"202","name":"Orca Whirlpool"}]}`
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(body))}, nil
		}),
	})
	sources, err := source.LiquiditySources(context.Background(), okx.SolanaChainIndex)
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != 2 || sources[0].ID != "101" || sources[0].Name != "Meteora DLMM" || sources[1].ID != "202" || sources[1].Name != "Orca Whirlpool" {
		t.Fatalf("unexpected liquidity sources: %+v", sources)
	}
	if received == nil || received.URL.Path != okx.DefaultLiquidityPath || received.URL.Query().Get("chainIndex") != okx.SolanaChainIndex {
		t.Fatalf("unexpected liquidity request: %v", received)
	}
}

func TestQuotePreservesProviderDefaultsAndSupportsSpecificOptions(t *testing.T) {
	var requests []*http.Request
	var mu sync.Mutex
	client := clientFunc(func(request *http.Request) (*http.Response, error) {
		mu.Lock()
		requests = append(requests, request)
		mu.Unlock()
		amountOut := "100"
		if request.URL.Query().Get("directRoute") == "true" {
			amountOut = "90"
		}
		body := `{"code":"0","data":[{"chainIndex":"501","swapMode":"exactIn","fromTokenAmount":"1","toTokenAmount":"` + amountOut + `"}]}`
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(body))}, nil
	})
	source := mustSource(t, okx.Config{Client: client, Limiter: noWaitLimiter{}})
	defaultResult, err := source.Quote(context.Background(), okx.QuoteRequest{FromTokenAddress: "from", ToTokenAddress: "to", Amount: "1"})
	if err != nil {
		t.Fatal(err)
	}
	direct := true
	specificResult, err := source.Quote(context.Background(), okx.QuoteRequest{FromTokenAddress: "from", ToTokenAddress: "to", Amount: "1", DirectRoute: &direct, DexIDs: "7,8", PriceImpactProtectionPercent: "0.25"})
	if err != nil {
		t.Fatal(err)
	}
	if defaultResult.ToTokenAmount != "100" || specificResult.ToTokenAmount != "90" {
		t.Fatalf("configuration did not affect output: default=%s specific=%s", defaultResult.ToTokenAmount, specificResult.ToTokenAmount)
	}
	if len(requests) != 2 {
		t.Fatalf("captured %d requests, want 2", len(requests))
	}
	defaultQuery := requests[0].URL.Query()
	if defaultQuery.Get("directRoute") != "" || defaultQuery.Get("dexIds") != "" || defaultQuery.Get("priceImpactProtectionPercentage") != "" {
		t.Fatalf("default request unexpectedly set optional parameters: %s", requests[0].URL.RawQuery)
	}
	specificQuery := requests[1].URL.Query()
	if specificQuery.Get("directRoute") != "true" || specificQuery.Get("dexIds") != "7,8" || specificQuery.Get("priceImpactProtectionPercentage") != "0.25" {
		t.Fatalf("specific request omitted configured parameters: %s", requests[1].URL.RawQuery)
	}
}

func TestParallelQuoteComparisonSharesRateLimiter(t *testing.T) {
	limiter, err := okx.NewSpacingLimiter(20 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	type observation struct {
		at          time.Time
		directRoute string
	}
	var observations []observation
	var mu sync.Mutex
	client := clientFunc(func(request *http.Request) (*http.Response, error) {
		mu.Lock()
		observations = append(observations, observation{at: time.Now(), directRoute: request.URL.Query().Get("directRoute")})
		mu.Unlock()
		amountOut := "100"
		if request.URL.Query().Get("directRoute") == "true" {
			amountOut = "90"
		}
		body := `{"code":"0","data":[{"chainIndex":"501","swapMode":"exactIn","fromTokenAmount":"1","toTokenAmount":"` + amountOut + `"}]}`
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(body))}, nil
	})
	source := mustSource(t, okx.Config{Client: client, Limiter: limiter})
	direct := true
	inputs := []okx.QuoteRequest{
		{FromTokenAddress: "from", ToTokenAddress: "to", Amount: "1"},
		{FromTokenAddress: "from", ToTokenAddress: "to", Amount: "1", DirectRoute: &direct},
	}
	results := make([]okx.Result, len(inputs))
	errs := make([]error, len(inputs))
	var group sync.WaitGroup
	for index := range inputs {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			results[index], errs[index] = source.Quote(context.Background(), inputs[index])
		}(index)
	}
	group.Wait()
	for index, err := range errs {
		if err != nil {
			t.Fatalf("parallel quote %d failed: %v", index, err)
		}
	}
	if results[0].ToTokenAmount == results[1].ToTokenAmount {
		t.Fatalf("parallel configurations produced the same output: %+v", results)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(observations) != 2 {
		t.Fatalf("observed %d HTTP calls, want 2", len(observations))
	}
	delta := observations[0].at.Sub(observations[1].at)
	if delta < 0 {
		delta = -delta
	}
	if delta < 12*time.Millisecond {
		t.Fatalf("parallel calls were closer than the configured rate interval: %s", delta)
	}
}

func TestQuoteReturnsProviderRateLimitError(t *testing.T) {
	source := mustSource(t, okx.Config{Client: clientFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusTooManyRequests, Status: "429 Too Many Requests", Body: io.NopCloser(strings.NewReader(`{"code":"50011","msg":"rate limit reached","data":[]}`))}, nil
	}), Limiter: noWaitLimiter{}})
	_, err := source.Quote(context.Background(), okx.QuoteRequest{FromTokenAddress: "from", ToTokenAddress: "to", Amount: "1"})
	apiErr, ok := err.(*okx.APIError)
	if !ok || !apiErr.RateLimited() || apiErr.Code != "50011" {
		t.Fatalf("expected rate limit API error, got %T %v", err, err)
	}
}

func TestQuoteRejectsExactOutputForSolana(t *testing.T) {
	source := mustSource(t, okx.Config{Limiter: noWaitLimiter{}})
	_, err := source.Quote(context.Background(), okx.QuoteRequest{FromTokenAddress: "from", ToTokenAddress: "to", Amount: "1", SwapMode: okx.SwapModeExactOutput})
	if err == nil || !strings.Contains(err.Error(), "not supported for Solana") {
		t.Fatalf("expected Solana exactOut validation error, got %v", err)
	}
}

func mustSource(t *testing.T, config okx.Config) *okx.Source {
	t.Helper()
	config.ID = "okx"
	config.APIKey = "key"
	config.SecretKey = "secret"
	config.Passphrase = "pass"
	if config.Client == nil {
		config.Client = clientFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(`{"code":"0","data":[{"chainIndex":"501","swapMode":"exactIn","fromTokenAmount":"1","toTokenAmount":"1"}]}`))}, nil
		})
	}
	source, err := okx.New(config)
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func TestURLQueryEncodingUsesProviderEscaping(t *testing.T) {
	values := url.Values{"dexIds": {"7,8"}, "fromTokenAddress": {"from"}}
	if got := values.Encode(); !strings.Contains(got, "dexIds=7%2C8") {
		t.Fatalf("unexpected query encoding: %s", got)
	}
}
