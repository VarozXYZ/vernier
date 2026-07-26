package jupiter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/VarozXYZ/vernier/domain/market"
)

const DefaultSwapPath = "/swap/v1/swap"

// SwapConfig configures Jupiter's read-only transaction-construction request.
// The returned transaction is unsigned; this source never signs or submits it.
type SwapConfig struct {
	ID              market.SourceID
	BaseURL         string
	SwapPath        string
	APIKey          string
	APIKeys         []string
	APIKeyPool      *APIKeyPool
	APIKeyHeader    string
	RequestInterval time.Duration
	Limiter         QuoteLimiter
	Client          Client
}

// SwapRequest contains the quote response and public wallet address required
// by Jupiter's v1 swap endpoint.
type SwapRequest struct {
	QuoteResponse       []byte
	UserPublicKey       string
	DynamicComputeUnits bool
}

// SwapResult contains transaction-construction output and transport timings.
// SwapTransaction is serialized but unsigned and must never be broadcast by
// this adapter.
type SwapResult struct {
	HTTPStatus           int
	QueueDuration        time.Duration
	HTTPDuration         time.Duration
	TotalDuration        time.Duration
	ResponseBytes        int
	SwapTransaction      string
	LastValidBlockHeight uint64
}

// SwapSource requests an unsigned Jupiter swap transaction.
type SwapSource struct {
	id         market.SourceID
	baseURL    string
	swapPath   string
	keyPool    *APIKeyPool
	apiKeyHead string
	client     Client
	limiter    QuoteLimiter
}

func NewSwapSource(config SwapConfig) (*SwapSource, error) {
	if config.ID == "" {
		return nil, fmt.Errorf("jupiter swap source requires an id")
	}
	if config.BaseURL == "" {
		config.BaseURL = DefaultBaseURL
	}
	baseURL, err := url.Parse(strings.TrimRight(config.BaseURL, "/"))
	if err != nil || baseURL.Scheme == "" || baseURL.Host == "" || baseURL.RawQuery != "" || baseURL.Fragment != "" {
		return nil, fmt.Errorf("invalid jupiter base URL")
	}
	if config.SwapPath == "" {
		config.SwapPath = DefaultSwapPath
	}
	if !strings.HasPrefix(config.SwapPath, "/") {
		return nil, fmt.Errorf("jupiter swap path must start with /")
	}
	pathURL, err := url.Parse(config.SwapPath)
	if err != nil || pathURL.RawQuery != "" || pathURL.Fragment != "" {
		return nil, fmt.Errorf("invalid jupiter swap path")
	}
	if config.Client == nil {
		config.Client = &http.Client{Timeout: 15 * time.Second}
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
	return &SwapSource{
		id:         config.ID,
		baseURL:    strings.TrimRight(baseURL.String(), "/"),
		swapPath:   config.SwapPath,
		keyPool:    keyPool,
		apiKeyHead: config.APIKeyHeader,
		client:     config.Client,
		limiter:    config.Limiter,
	}, nil
}

func (s *SwapSource) ID() market.SourceID { return s.id }

func (s *SwapSource) Swap(ctx context.Context, input SwapRequest) (SwapResult, error) {
	if strings.TrimSpace(input.UserPublicKey) == "" {
		return SwapResult{}, fmt.Errorf("jupiter swap requires a user public key")
	}
	if len(input.QuoteResponse) == 0 || !json.Valid(input.QuoteResponse) {
		return SwapResult{}, fmt.Errorf("jupiter swap requires a valid quote response")
	}

	started := time.Now()
	queueStarted := started
	if err := s.limiter.Wait(ctx); err != nil {
		return SwapResult{QueueDuration: time.Since(queueStarted), TotalDuration: time.Since(started)}, err
	}
	queueDuration := time.Since(queueStarted)
	payload := struct {
		UserPublicKey       string          `json:"userPublicKey"`
		QuoteResponse       json.RawMessage `json:"quoteResponse"`
		DynamicComputeUnits bool            `json:"dynamicComputeUnitLimit,omitempty"`
	}{
		UserPublicKey:       strings.TrimSpace(input.UserPublicKey),
		QuoteResponse:       json.RawMessage(input.QuoteResponse),
		DynamicComputeUnits: input.DynamicComputeUnits,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return SwapResult{QueueDuration: queueDuration, TotalDuration: time.Since(started)}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.baseURL+s.swapPath, bytes.NewReader(body))
	if err != nil {
		return SwapResult{QueueDuration: queueDuration, TotalDuration: time.Since(started)}, err
	}
	request.Header.Set("Content-Type", "application/json")
	if s.keyPool != nil {
		header := s.apiKeyHead
		if header == "" {
			header = "x-api-key"
		}
		request.Header.Set(header, s.keyPool.Next())
	}

	httpStarted := time.Now()
	response, err := s.client.Do(request)
	if err != nil {
		return SwapResult{QueueDuration: queueDuration, HTTPDuration: time.Since(httpStarted), TotalDuration: time.Since(started)}, err
	}
	if response == nil || response.Body == nil {
		return SwapResult{HTTPStatus: responseStatus(response), QueueDuration: queueDuration, HTTPDuration: time.Since(httpStarted), TotalDuration: time.Since(started)}, fmt.Errorf("jupiter HTTP client returned an empty response")
	}
	defer response.Body.Close()
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, maxQuoteResponseBytes+1))
	result := SwapResult{
		HTTPStatus:    response.StatusCode,
		QueueDuration: queueDuration,
		HTTPDuration:  time.Since(httpStarted),
		TotalDuration: time.Since(started),
		ResponseBytes: len(responseBody),
	}
	if readErr != nil {
		return result, readErr
	}
	if len(responseBody) > maxQuoteResponseBytes {
		return result, fmt.Errorf("jupiter swap response exceeds %d bytes", maxQuoteResponseBytes)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return result, parseAPIErrorFor("swap", response.StatusCode, responseBody)
	}
	var payloadResponse struct {
		SwapTransaction      string `json:"swapTransaction"`
		LastValidBlockHeight uint64 `json:"lastValidBlockHeight"`
	}
	if err := json.Unmarshal(responseBody, &payloadResponse); err != nil {
		return result, fmt.Errorf("decode jupiter swap response: %w", err)
	}
	if strings.TrimSpace(payloadResponse.SwapTransaction) == "" {
		return result, &APIError{Operation: "swap", HTTPStatus: response.StatusCode, Message: "jupiter response has no swap transaction"}
	}
	result.SwapTransaction = payloadResponse.SwapTransaction
	result.LastValidBlockHeight = payloadResponse.LastValidBlockHeight
	return result, nil
}
