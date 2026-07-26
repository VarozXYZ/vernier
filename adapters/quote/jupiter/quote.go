package jupiter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VarozXYZ/vernier/domain/market"
)

const (
	// DefaultQuotePath is Jupiter's current Metis Swap v1 quote endpoint.
	DefaultQuotePath = "/swap/v1/quote"

	// DefaultSlippageBPS is sent explicitly so both runners use a reproducible
	// quote configuration.
	DefaultSlippageBPS = 50

	// DefaultRequestInterval keeps the standalone experiment within the
	// one-request-per-second allowance used by the free Jupiter API plan.
	DefaultRequestInterval = time.Second

	maxQuoteResponseBytes = 4 << 20
)

// QuoteConfig contains settings for the direct, read-only Jupiter quote
// client. It is deliberately separate from Source, which is the Research
// reference wrapper around a local quote source.
type QuoteConfig struct {
	ID              market.SourceID
	BaseURL         string
	QuotePath       string
	APIKey          string
	APIKeys         []string
	APIKeyPool      *APIKeyPool
	APIKeyHeader    string
	SlippageBPS     uint16
	RequestInterval time.Duration
	Limiter         QuoteLimiter
	Client          Client
	Clock           Clock
}

// QuoteRequest is the provider-facing Jupiter input. Amount is the integer
// number of minimum token units. No dexes or route filters are included: this
// client intentionally measures Jupiter's default route.
type QuoteRequest struct {
	InputMint   string
	OutputMint  string
	Amount      string
	SlippageBPS uint16
	Dexes       string
}

// RoutePlan is one Jupiter route leg returned by the quote endpoint.
type RoutePlan struct {
	Percent    int
	AMMKey     string
	Label      string
	InputMint  string
	OutputMint string
	InAmount   string
	OutAmount  string
	FeeAmount  string
	FeeMint    string
}

// QuoteResult is the parsed Jupiter quote and its transport timings.
type QuoteResult struct {
	Request               QuoteRequest
	InputMint             string
	OutputMint            string
	InTokenAmount         string
	ToTokenAmount         string
	OtherAmountThreshold  string
	PriceImpactPercentage string
	ContextSlot           uint64
	RoutePlan             []RoutePlan
	RawResponse           []byte
	HTTPStatus            int
	QueueDuration         time.Duration
	HTTPDuration          time.Duration
	TotalDuration         time.Duration
}

// APIError represents an HTTP or Jupiter-level error.
type APIError struct {
	Operation  string
	HTTPStatus int
	Message    string
	Code       string
}

func (e *APIError) Error() string {
	operation := e.Operation
	if operation == "" {
		operation = "request"
	}
	if e.HTTPStatus != 0 && e.Code != "" {
		return fmt.Sprintf("jupiter %s failed: code=%s http=%d: %s", operation, e.Code, e.HTTPStatus, e.Message)
	}
	if e.HTTPStatus != 0 {
		return fmt.Sprintf("jupiter %s failed: http=%d: %s", operation, e.HTTPStatus, e.Message)
	}
	return fmt.Sprintf("jupiter %s failed: %s", operation, e.Message)
}

// RateLimited reports the HTTP signal used by the Jupiter API gateway.
func (e *APIError) RateLimited() bool {
	return e != nil && e.HTTPStatus == http.StatusTooManyRequests
}

// QuoteSource is a direct Jupiter quote adapter for standalone experiments.
// It never builds, signs, or broadcasts a swap transaction.
type QuoteSource struct {
	id         market.SourceID
	baseURL    string
	quotePath  string
	keyPool    *APIKeyPool
	apiKeyHead string
	slippage   uint16
	client     Client
	limiter    QuoteLimiter
	clock      Clock
}

// NewQuoteSource constructs a direct Jupiter quote client.
func NewQuoteSource(config QuoteConfig) (*QuoteSource, error) {
	if config.ID == "" {
		return nil, fmt.Errorf("jupiter quote source requires an id")
	}
	if config.BaseURL == "" {
		config.BaseURL = DefaultBaseURL
	}
	baseURL, err := url.Parse(strings.TrimRight(config.BaseURL, "/"))
	if err != nil || baseURL.Scheme == "" || baseURL.Host == "" || baseURL.RawQuery != "" || baseURL.Fragment != "" {
		return nil, fmt.Errorf("invalid jupiter base URL")
	}
	if config.QuotePath == "" {
		config.QuotePath = DefaultQuotePath
	}
	if !strings.HasPrefix(config.QuotePath, "/") {
		return nil, fmt.Errorf("jupiter quote path must start with /")
	}
	pathURL, err := url.Parse(config.QuotePath)
	if err != nil || pathURL.RawQuery != "" || pathURL.Fragment != "" {
		return nil, fmt.Errorf("invalid jupiter quote path")
	}
	if config.SlippageBPS > 10_000 {
		return nil, fmt.Errorf("jupiter slippage must be <= 10000 basis points")
	}
	if config.SlippageBPS == 0 {
		config.SlippageBPS = DefaultSlippageBPS
	}
	if config.Client == nil {
		config.Client = &http.Client{Timeout: 8 * time.Second}
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	if config.Limiter == nil {
		interval := config.RequestInterval
		if interval == 0 {
			interval = DefaultRequestInterval
		}
		config.Limiter, err = NewQuoteSpacingLimiter(interval)
		if err != nil {
			return nil, err
		}
	}
	keyPool := config.APIKeyPool
	if keyPool == nil {
		keys := append([]string(nil), config.APIKeys...)
		if len(keys) == 0 && strings.TrimSpace(config.APIKey) != "" {
			keys = []string{config.APIKey}
		}
		if len(keys) > 0 {
			keyPool, err = NewAPIKeyPool(keys)
			if err != nil {
				return nil, err
			}
		}
	}
	return &QuoteSource{
		id:         config.ID,
		baseURL:    strings.TrimRight(baseURL.String(), "/"),
		quotePath:  config.QuotePath,
		keyPool:    keyPool,
		apiKeyHead: config.APIKeyHeader,
		slippage:   config.SlippageBPS,
		client:     config.Client,
		limiter:    config.Limiter,
		clock:      config.Clock,
	}, nil
}

// ID returns the configured source identity.
func (s *QuoteSource) ID() market.SourceID { return s.id }

// Quote performs one default-route, read-only Jupiter quote request.
func (s *QuoteSource) Quote(ctx context.Context, input QuoteRequest) (QuoteResult, error) {
	resolved, err := s.resolve(input)
	if err != nil {
		return QuoteResult{}, err
	}

	started := time.Now()
	queueStarted := started
	if err := s.limiter.Wait(ctx); err != nil {
		return QuoteResult{Request: resolved, QueueDuration: time.Since(queueStarted), TotalDuration: time.Since(started)}, err
	}
	queueDuration := time.Since(queueStarted)
	query := url.Values{}
	query.Set("inputMint", resolved.InputMint)
	query.Set("outputMint", resolved.OutputMint)
	query.Set("amount", resolved.Amount)
	query.Set("slippageBps", strconv.FormatUint(uint64(resolved.SlippageBPS), 10))
	if resolved.Dexes != "" {
		query.Set("dexes", resolved.Dexes)
	}
	requestURL := s.baseURL + s.quotePath + "?" + query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return QuoteResult{Request: resolved, QueueDuration: queueDuration, TotalDuration: time.Since(started)}, err
	}
	if s.keyPool != nil {
		apiKey := s.keyPool.Next()
		header := s.apiKeyHead
		if header == "" {
			header = "x-api-key"
		}
		request.Header.Set(header, apiKey)
	}

	httpStarted := time.Now()
	response, err := s.client.Do(request)
	if err != nil {
		return QuoteResult{Request: resolved, QueueDuration: queueDuration, HTTPDuration: time.Since(httpStarted), TotalDuration: time.Since(started)}, err
	}
	if response == nil || response.Body == nil {
		return QuoteResult{Request: resolved, HTTPStatus: responseStatus(response), QueueDuration: queueDuration, HTTPDuration: time.Since(httpStarted), TotalDuration: time.Since(started)}, fmt.Errorf("jupiter HTTP client returned an empty response")
	}
	defer response.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(response.Body, maxQuoteResponseBytes+1))
	result := QuoteResult{
		Request:       resolved,
		HTTPStatus:    response.StatusCode,
		QueueDuration: queueDuration,
		HTTPDuration:  time.Since(httpStarted),
		TotalDuration: time.Since(started),
	}
	if readErr != nil {
		return result, readErr
	}
	result.RawResponse = append([]byte(nil), body...)
	if len(body) > maxQuoteResponseBytes {
		return result, fmt.Errorf("jupiter response exceeds %d bytes", maxQuoteResponseBytes)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return result, parseAPIErrorFor("quote", response.StatusCode, body)
	}
	var payload quoteResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		return result, fmt.Errorf("decode jupiter quote response: %w", err)
	}
	if strings.TrimSpace(payload.OutAmount) == "" {
		return result, &APIError{Operation: "quote", HTTPStatus: response.StatusCode, Message: "jupiter response has no outAmount"}
	}
	outAmount, ok := new(big.Int).SetString(payload.OutAmount, 10)
	if !ok || outAmount.Sign() < 0 {
		return result, &APIError{Operation: "quote", HTTPStatus: response.StatusCode, Message: "jupiter outAmount is not a non-negative integer"}
	}
	result.InputMint = payload.InputMint
	result.OutputMint = payload.OutputMint
	result.InTokenAmount = payload.InAmount
	result.ToTokenAmount = payload.OutAmount
	result.OtherAmountThreshold = payload.OtherAmountThreshold
	result.PriceImpactPercentage = payload.PriceImpactPct
	result.ContextSlot = payload.ContextSlot
	for _, leg := range payload.RoutePlan {
		result.RoutePlan = append(result.RoutePlan, RoutePlan{
			Percent:    leg.Percent,
			AMMKey:     leg.SwapInfo.AMMKey,
			Label:      leg.SwapInfo.Label,
			InputMint:  leg.SwapInfo.InputMint,
			OutputMint: leg.SwapInfo.OutputMint,
			InAmount:   leg.SwapInfo.InAmount,
			OutAmount:  leg.SwapInfo.OutAmount,
			FeeAmount:  leg.SwapInfo.FeeAmount,
			FeeMint:    leg.SwapInfo.FeeMint,
		})
	}
	return result, nil
}

func (s *QuoteSource) resolve(input QuoteRequest) (QuoteRequest, error) {
	input.InputMint = strings.TrimSpace(input.InputMint)
	input.OutputMint = strings.TrimSpace(input.OutputMint)
	input.Amount = strings.TrimSpace(input.Amount)
	input.Dexes = strings.TrimSpace(input.Dexes)
	if input.InputMint == "" || input.OutputMint == "" || input.Amount == "" || input.InputMint == input.OutputMint {
		return QuoteRequest{}, fmt.Errorf("jupiter quote requires distinct input/output mints and amount")
	}
	amount, ok := new(big.Int).SetString(input.Amount, 10)
	if !ok || amount.Sign() <= 0 {
		return QuoteRequest{}, fmt.Errorf("jupiter quote amount must be a positive integer in minimum units")
	}
	if input.SlippageBPS == 0 {
		input.SlippageBPS = s.slippage
	}
	if input.SlippageBPS > 10_000 {
		return QuoteRequest{}, fmt.Errorf("jupiter slippage must be <= 10000 basis points")
	}
	return input, nil
}

type quoteResponse struct {
	InputMint            string `json:"inputMint"`
	InAmount             string `json:"inAmount"`
	OutputMint           string `json:"outputMint"`
	OutAmount            string `json:"outAmount"`
	OtherAmountThreshold string `json:"otherAmountThreshold"`
	PriceImpactPct       string `json:"priceImpactPct"`
	ContextSlot          uint64 `json:"contextSlot"`
	RoutePlan            []struct {
		Percent  int `json:"percent"`
		SwapInfo struct {
			AMMKey     string `json:"ammKey"`
			Label      string `json:"label"`
			InputMint  string `json:"inputMint"`
			OutputMint string `json:"outputMint"`
			InAmount   string `json:"inAmount"`
			OutAmount  string `json:"outAmount"`
			FeeAmount  string `json:"feeAmount"`
			FeeMint    string `json:"feeMint"`
		} `json:"swapInfo"`
	} `json:"routePlan"`
}

func parseAPIErrorFor(operation string, status int, body []byte) *APIError {
	var payload struct {
		Error     string `json:"error"`
		ErrorCode string `json:"errorCode"`
		Message   string `json:"message"`
	}
	message := strings.TrimSpace(string(body))
	if json.Unmarshal(body, &payload) == nil {
		if payload.Error != "" {
			message = payload.Error
		} else if payload.Message != "" {
			message = payload.Message
		}
	}
	if message == "" {
		message = http.StatusText(status)
	}
	return &APIError{Operation: operation, HTTPStatus: status, Code: payload.ErrorCode, Message: message}
}

func responseStatus(response *http.Response) int {
	if response == nil {
		return 0
	}
	return response.StatusCode
}

// QuoteLimiter spaces requests made by one QuoteSource.
type QuoteLimiter interface {
	Wait(context.Context) error
}

// ImmediateLimiter performs no local request spacing. Provider quotas remain
// an operational concern of the configured API-key pool and provider account.
type ImmediateLimiter struct{}

func (ImmediateLimiter) Wait(ctx context.Context) error { return ctx.Err() }

// QuoteSpacingLimiter schedules starts with a minimum interval. It does not
// retry failed requests and is shared by all callers of one source.
type QuoteSpacingLimiter struct {
	interval time.Duration
	mu       sync.Mutex
	next     time.Time
}

func NewQuoteSpacingLimiter(interval time.Duration) (*QuoteSpacingLimiter, error) {
	if interval < 0 {
		return nil, fmt.Errorf("jupiter rate-limit interval cannot be negative")
	}
	return &QuoteSpacingLimiter{interval: interval}, nil
}

func (l *QuoteSpacingLimiter) Wait(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := time.Now()
	l.mu.Lock()
	scheduled := now
	if l.next.After(now) {
		scheduled = l.next
	}
	l.next = scheduled.Add(l.interval)
	l.mu.Unlock()
	delay := time.Until(scheduled)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

var _ QuoteLimiter = ImmediateLimiter{}
var _ QuoteLimiter = (*QuoteSpacingLimiter)(nil)

// APIKeyPool rotates through a caller-owned pool of Jupiter API keys. The
// counter is shared by every source that receives the same pool, so quote and
// swap requests participate in one round-robin sequence.
type APIKeyPool struct {
	keys []string
	next atomic.Uint64
}

func NewAPIKeyPool(keys []string) (*APIKeyPool, error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("jupiter API key pool cannot be empty")
	}
	cleaned := make([]string, len(keys))
	for index, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, fmt.Errorf("jupiter API key pool contains an empty key at index %d", index)
		}
		cleaned[index] = key
	}
	return &APIKeyPool{keys: cleaned}, nil
}

func (p *APIKeyPool) Next() string {
	if p == nil || len(p.keys) == 0 {
		return ""
	}
	index := p.next.Add(1) - 1
	return p.keys[index%uint64(len(p.keys))]
}

func (p *APIKeyPool) Len() int {
	if p == nil {
		return 0
	}
	return len(p.keys)
}
