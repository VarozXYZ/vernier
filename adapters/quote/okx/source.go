// Package okx provides a read-only client for the OKX DEX aggregator quote API.
// It does not create instructions, sign transactions, or broadcast swaps.
package okx

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/domain/market"
)

const (
	// DefaultBaseURL is the host documented by OKX for the DEX API.
	DefaultBaseURL = "https://web3.okx.com"
	// DefaultQuotePath follows the v6 classic Swap API documented by OKX.
	DefaultQuotePath = "/api/v6/dex/aggregator/quote"
	// DefaultLiquidityPath returns the current liquidity-source IDs for a
	// chain. It is used by live experiments to avoid hard-coding stale IDs.
	DefaultLiquidityPath = "/api/v6/dex/aggregator/get-liquidity"
	// SolanaChainIndex is OKX's public chain index for Solana mainnet.
	SolanaChainIndex = "501"
	// SwapModeExactInput asks OKX for the amount received for a fixed input.
	SwapModeExactInput = "exactIn"
	// SwapModeExactOutput asks OKX for the input required for a fixed output.
	SwapModeExactOutput = "exactOut"
	// DefaultRequestInterval is the conservative Trial-tier spacing published
	// by OKX. Accounts with an approved higher limit must configure their own
	// interval rather than assuming this value applies to every account.
	DefaultRequestInterval = time.Second
)

const maxResponseBytes = 4 << 20

// Client is the small HTTP boundary used by Source. It makes request and
// response behavior deterministic in tests without exporting transport types.
type Client interface {
	Do(*http.Request) (*http.Response, error)
}

// Clock supplies the timestamp used for OKX authentication. It is injectable
// so signature construction can be tested without depending on wall-clock
// time.
type Clock func() time.Time

// Limiter spaces requests made by one Source. A limiter is shared by all
// goroutines using that Source, so parallel callers cannot create a burst.
type Limiter interface {
	Wait(context.Context) error
}

// Config contains only read-only quote client settings. Credentials stay in
// the caller's environment or secret provider and are never logged or stored
// in quote results.
type Config struct {
	ID        market.SourceID
	BaseURL   string
	QuotePath string

	ChainIndex string
	APIKey     string
	SecretKey  string
	Passphrase string
	ProjectID  string

	// RequestInterval creates a shared spacing limiter when Limiter is nil.
	// Zero selects DefaultRequestInterval. Supplying Limiter is useful for
	// tests or for an account whose approved RPS is known by the caller.
	RequestInterval time.Duration
	Limiter         Limiter
	Client          Client
	Clock           Clock
}

// LiquiditySource is one currently available OKX liquidity source for a
// chain. IDs are provider data and may change; callers should resolve them
// through LiquiditySources instead of assuming a permanent mapping.
type LiquiditySource struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Logo string `json:"logo"`
}

// QuoteRequest is the provider-facing quote input. Amount is always a string
// containing the integer number of minimum token units, as required by OKX.
// Empty optional fields preserve the provider's defaults.
type QuoteRequest struct {
	ChainIndex       string
	FromTokenAddress string
	ToTokenAddress   string
	Amount           string
	SwapMode         string

	DexIDs                       string
	ExcludeDexIDs                string
	ForJitoBundle                *bool
	ExcludePoolAddresses         []string
	DirectRoute                  *bool
	PriceImpactProtectionPercent string
	FeePercent                   string
}

// Protocol describes one liquidity protocol in an OKX route.
type Protocol struct {
	Name    string `json:"dexName"`
	Percent string `json:"percent"`
}

// TokenInfo is the token metadata returned by OKX. Decimal and price remain
// strings because provider values must not be rounded through floating point.
type TokenInfo struct {
	Address  string `json:"tokenContractAddress"`
	Symbol   string `json:"tokenSymbol"`
	PriceUSD string `json:"tokenUnitPrice"`
	Decimal  string `json:"decimal"`
	TaxRate  string `json:"taxRate"`
	HoneyPot bool   `json:"isHoneyPot"`
}

// SubRoute is a lower-level route component returned by the aggregator.
type SubRoute struct {
	Protocols []Protocol `json:"dexProtocol"`
	FromToken TokenInfo  `json:"fromToken"`
	ToToken   TokenInfo  `json:"toToken"`
}

// Route describes one main path and its route split.
type Route struct {
	Router    string `json:"router"`
	Percent   string `json:"routerPercent"`
	Protocols []Protocol
	SubRoutes []SubRoute `json:"subRouterList"`
}

// RouteComparison is the provider's comparison set for alternative DEX
// routes. It is useful for measuring routing quality without reproducing OKX's
// proprietary routing algorithm locally.
type RouteComparison struct {
	DEXName               string `json:"dexName"`
	DEXLogo               string `json:"dexLogo"`
	TradeFee              string `json:"tradeFee"`
	AmountOut             string `json:"amountOut"`
	PriceImpactPercentage string `json:"priceImpactPercentage"`
}

// Result is the parsed quote plus transport measurements. HTTPDuration covers
// the outbound HTTP call and response body read. TotalDuration also includes
// the rate-limit queue; QueueDuration lets callers separate the two.
type Result struct {
	Request QuoteRequest

	ResponseCode string
	Message      string
	ChainIndex   string
	SwapMode     string

	FromTokenAmount       string
	ToTokenAmount         string
	OriginToTokenAmount   string
	FromToken             TokenInfo
	ToToken               TokenInfo
	TradeFeeUSD           string
	EstimateGasFee        string
	PriceImpactPercentage string
	Routes                []Route
	Comparisons           []RouteComparison

	HTTPStatus    int
	QueueDuration time.Duration
	HTTPDuration  time.Duration
	TotalDuration time.Duration
}

// APIError represents an HTTP or provider-level error. OKX can return a
// non-zero code with HTTP 200, so callers must inspect Code as well as the HTTP
// status. Code 50011 and HTTP 429 indicate rate limiting.
type APIError struct {
	Operation  string
	Code       string
	HTTPStatus int
	Message    string
}

func (e *APIError) Error() string {
	operation := e.Operation
	if operation == "" {
		operation = "quote"
	}
	if e.HTTPStatus != 0 && e.Code != "" {
		return fmt.Sprintf("OKX %s failed: code=%s http=%d: %s", operation, e.Code, e.HTTPStatus, e.Message)
	}
	if e.HTTPStatus != 0 {
		return fmt.Sprintf("OKX %s failed: http=%d: %s", operation, e.HTTPStatus, e.Message)
	}
	return fmt.Sprintf("OKX %s failed: code=%s: %s", operation, e.Code, e.Message)
}

// RateLimited reports the two rate-limit signals documented by OKX.
func (e *APIError) RateLimited() bool {
	return e != nil && (e.HTTPStatus == http.StatusTooManyRequests || e.Code == "50011")
}

// Source is a read-only OKX quote adapter.
type Source struct {
	id        market.SourceID
	baseURL   string
	quotePath string
	chain     string
	apiKey    string
	secret    string
	pass      string
	project   string
	client    Client
	clock     Clock
	limiter   Limiter
}

// New validates and constructs an authenticated quote source. The credentials
// are required because OKX documents all DEX API requests as authenticated.
func New(config Config) (*Source, error) {
	if config.ID == "" || strings.TrimSpace(config.APIKey) == "" || strings.TrimSpace(config.SecretKey) == "" || strings.TrimSpace(config.Passphrase) == "" {
		return nil, fmt.Errorf("OKX source requires id, API key, secret key, and passphrase")
	}
	if config.BaseURL == "" {
		config.BaseURL = DefaultBaseURL
	}
	baseURL, err := url.Parse(strings.TrimRight(config.BaseURL, "/"))
	if err != nil || baseURL.Scheme == "" || baseURL.Host == "" || baseURL.RawQuery != "" {
		return nil, fmt.Errorf("invalid OKX base URL")
	}
	if config.QuotePath == "" {
		config.QuotePath = DefaultQuotePath
	}
	if !strings.HasPrefix(config.QuotePath, "/") {
		return nil, fmt.Errorf("OKX quote path must start with /")
	}
	pathURL, err := url.Parse(config.QuotePath)
	if err != nil || pathURL.RawQuery != "" || pathURL.Fragment != "" {
		return nil, fmt.Errorf("invalid OKX quote path")
	}
	if config.ChainIndex == "" {
		config.ChainIndex = SolanaChainIndex
	}
	if config.Client == nil {
		config.Client = &http.Client{Timeout: 10 * time.Second}
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	if config.Limiter == nil {
		interval := config.RequestInterval
		if interval == 0 {
			interval = DefaultRequestInterval
		}
		config.Limiter, err = NewSpacingLimiter(interval)
		if err != nil {
			return nil, err
		}
	}
	return &Source{
		id:        config.ID,
		baseURL:   strings.TrimRight(baseURL.String(), "/"),
		quotePath: config.QuotePath,
		chain:     config.ChainIndex,
		apiKey:    config.APIKey,
		secret:    config.SecretKey,
		pass:      config.Passphrase,
		project:   config.ProjectID,
		client:    config.Client,
		clock:     config.Clock,
		limiter:   config.Limiter,
	}, nil
}

// ID returns the configured source identity.
func (s *Source) ID() market.SourceID { return s.id }

// Quote performs one authenticated read-only quote request.
func (s *Source) Quote(ctx context.Context, input QuoteRequest) (Result, error) {
	resolved, err := s.resolve(input)
	if err != nil {
		return Result{}, err
	}

	query := url.Values{}
	query.Set("chainIndex", resolved.ChainIndex)
	query.Set("amount", resolved.Amount)
	query.Set("swapMode", resolved.SwapMode)
	query.Set("fromTokenAddress", resolved.FromTokenAddress)
	query.Set("toTokenAddress", resolved.ToTokenAddress)
	if resolved.DexIDs != "" {
		query.Set("dexIds", resolved.DexIDs)
	}
	if resolved.ExcludeDexIDs != "" {
		query.Set("excludeDexIds", resolved.ExcludeDexIDs)
	}
	if resolved.ForJitoBundle != nil {
		query.Set("forJitoBundle", strconv.FormatBool(*resolved.ForJitoBundle))
	}
	if len(resolved.ExcludePoolAddresses) > 0 {
		query.Set("excludePoolAddresses", strings.Join(resolved.ExcludePoolAddresses, ","))
	}
	if resolved.DirectRoute != nil {
		query.Set("directRoute", strconv.FormatBool(*resolved.DirectRoute))
	}
	if resolved.PriceImpactProtectionPercent != "" {
		query.Set("priceImpactProtectionPercentage", resolved.PriceImpactProtectionPercent)
	}
	if resolved.FeePercent != "" {
		query.Set("feePercent", resolved.FeePercent)
	}

	transport, err := s.doGET(ctx, s.quotePath, query)
	result := Result{Request: resolved, HTTPStatus: transport.status, QueueDuration: transport.queueDuration, HTTPDuration: transport.httpDuration, TotalDuration: transport.totalDuration}
	if err != nil {
		return result, err
	}
	var envelope apiEnvelope
	if err := json.Unmarshal(transport.body, &envelope); err != nil {
		if transport.status < 200 || transport.status >= 300 {
			return result, &APIError{HTTPStatus: transport.status, Message: strings.TrimSpace(string(transport.body))}
		}
		return result, fmt.Errorf("decode OKX quote response: %w", err)
	}
	result.ResponseCode = envelope.Code
	result.Message = envelope.Message
	if transport.status < 200 || transport.status >= 300 || envelope.Code != "" && envelope.Code != "0" {
		return result, &APIError{Code: envelope.Code, HTTPStatus: transport.status, Message: envelope.Message}
	}
	if len(envelope.Data) == 0 {
		return result, &APIError{Code: "empty_data", HTTPStatus: transport.status, Message: "OKX returned no quote data"}
	}
	result.apply(envelope.Data[0])
	return result, nil
}

// LiquiditySources returns the current liquidity-source catalog for a chain.
// The request consumes the same shared rate-limit slot as a quote.
func (s *Source) LiquiditySources(ctx context.Context, chainIndex string) ([]LiquiditySource, error) {
	chainIndex = strings.TrimSpace(chainIndex)
	if chainIndex == "" {
		chainIndex = s.chain
	}
	if chainIndex == "" {
		return nil, fmt.Errorf("OKX liquidity lookup requires a chain index")
	}
	query := url.Values{}
	query.Set("chainIndex", chainIndex)
	transport, err := s.doGET(ctx, DefaultLiquidityPath, query)
	if err != nil {
		return nil, err
	}
	var envelope liquidityEnvelope
	if err := json.Unmarshal(transport.body, &envelope); err != nil {
		if transport.status < 200 || transport.status >= 300 {
			return nil, &APIError{HTTPStatus: transport.status, Message: strings.TrimSpace(string(transport.body))}
		}
		return nil, fmt.Errorf("decode OKX liquidity response: %w", err)
	}
	if transport.status < 200 || transport.status >= 300 || envelope.Code != "" && envelope.Code != "0" {
		return nil, &APIError{Code: envelope.Code, HTTPStatus: transport.status, Message: envelope.Message}
	}
	return envelope.Data, nil
}

type transportResult struct {
	status        int
	body          []byte
	queueDuration time.Duration
	httpDuration  time.Duration
	totalDuration time.Duration
}

func (s *Source) doGET(ctx context.Context, path string, query url.Values) (transportResult, error) {
	started := time.Now()
	queueStarted := started
	if err := s.limiter.Wait(ctx); err != nil {
		return transportResult{queueDuration: time.Since(queueStarted), totalDuration: time.Since(started)}, err
	}
	queueDuration := time.Since(queueStarted)
	requestURL := s.baseURL + path
	if len(query) > 0 {
		requestURL += "?" + query.Encode()
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return transportResult{queueDuration: queueDuration, totalDuration: time.Since(started)}, err
	}
	// OKX documents ISO 8601 timestamps with millisecond precision. Keep the
	// exact value used for the header and signature; RFC3339Nano can emit more
	// fractional digits than OKX accepts and results in error 50112.
	timestamp := s.clock().UTC().Format("2006-01-02T15:04:05.000Z")
	requestPath := request.URL.EscapedPath()
	if request.URL.RawQuery != "" {
		requestPath += "?" + request.URL.RawQuery
	}
	request.Header.Set("OK-ACCESS-KEY", s.apiKey)
	request.Header.Set("OK-ACCESS-TIMESTAMP", timestamp)
	request.Header.Set("OK-ACCESS-PASSPHRASE", s.pass)
	request.Header.Set("OK-ACCESS-SIGN", sign(timestamp+http.MethodGet+requestPath, s.secret))
	if s.project != "" {
		request.Header.Set("OK-ACCESS-PROJECT", s.project)
	}

	httpStarted := time.Now()
	response, err := s.client.Do(request)
	if err != nil {
		return transportResult{queueDuration: queueDuration, httpDuration: time.Since(httpStarted), totalDuration: time.Since(started)}, err
	}
	if response == nil || response.Body == nil {
		return transportResult{status: responseStatus(response), queueDuration: queueDuration, httpDuration: time.Since(httpStarted), totalDuration: time.Since(started)}, fmt.Errorf("OKX HTTP client returned an empty response")
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	transport := transportResult{status: response.StatusCode, body: body, queueDuration: queueDuration, httpDuration: time.Since(httpStarted), totalDuration: time.Since(started)}
	if err != nil {
		return transport, err
	}
	if len(body) > maxResponseBytes {
		return transport, fmt.Errorf("OKX response exceeds %d bytes", maxResponseBytes)
	}
	return transport, nil
}

func responseStatus(response *http.Response) int {
	if response == nil {
		return 0
	}
	return response.StatusCode
}

func (s *Source) resolve(input QuoteRequest) (QuoteRequest, error) {
	input.ChainIndex = strings.TrimSpace(input.ChainIndex)
	if input.ChainIndex == "" {
		input.ChainIndex = s.chain
	}
	input.FromTokenAddress = strings.TrimSpace(input.FromTokenAddress)
	input.ToTokenAddress = strings.TrimSpace(input.ToTokenAddress)
	input.Amount = strings.TrimSpace(input.Amount)
	if input.ChainIndex == "" || input.FromTokenAddress == "" || input.ToTokenAddress == "" || input.Amount == "" || input.FromTokenAddress == input.ToTokenAddress {
		return QuoteRequest{}, fmt.Errorf("OKX quote requires distinct chain, token addresses, and amount")
	}
	amount, ok := new(big.Int).SetString(input.Amount, 10)
	if !ok || amount.Sign() <= 0 {
		return QuoteRequest{}, fmt.Errorf("OKX quote amount must be a positive integer in minimum units")
	}
	if input.SwapMode == "" {
		input.SwapMode = SwapModeExactInput
	}
	if input.SwapMode != SwapModeExactInput && input.SwapMode != SwapModeExactOutput {
		return QuoteRequest{}, fmt.Errorf("unsupported OKX swap mode %q", input.SwapMode)
	}
	if input.ChainIndex == SolanaChainIndex && input.SwapMode == SwapModeExactOutput {
		return QuoteRequest{}, fmt.Errorf("OKX exactOut quotes are not supported for Solana")
	}
	return input, nil
}

func (r *Result) apply(quote apiQuote) {
	r.ChainIndex = quote.ChainIndex
	r.SwapMode = quote.SwapMode
	r.FromTokenAmount = quote.FromTokenAmount
	r.ToTokenAmount = quote.ToTokenAmount
	r.OriginToTokenAmount = quote.OriginToTokenAmount
	r.FromToken = quote.FromToken
	r.ToToken = quote.ToToken
	r.TradeFeeUSD = quote.TradeFee
	r.EstimateGasFee = quote.EstimateGasFee
	r.PriceImpactPercentage = quote.PriceImpactPercentage
	r.Comparisons = append([]RouteComparison(nil), quote.Comparisons...)
	for _, route := range quote.Routes {
		r.Routes = append(r.Routes, route.toRoute())
	}
}

func sign(message, secret string) string {
	hasher := hmac.New(sha256.New, []byte(secret))
	_, _ = hasher.Write([]byte(message))
	return base64.StdEncoding.EncodeToString(hasher.Sum(nil))
}

// SpacingLimiter schedules one request at a time with a minimum interval
// between scheduled starts. It does not retry failed calls.
type SpacingLimiter struct {
	interval time.Duration
	mu       sync.Mutex
	next     time.Time
}

// NewSpacingLimiter creates a limiter for a known account allowance. An
// interval of zero is valid and means no spacing.
func NewSpacingLimiter(interval time.Duration) (*SpacingLimiter, error) {
	if interval < 0 {
		return nil, fmt.Errorf("rate-limit interval cannot be negative")
	}
	return &SpacingLimiter{interval: interval}, nil
}

func (l *SpacingLimiter) Wait(ctx context.Context) error {
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

type apiEnvelope struct {
	Code    string     `json:"code"`
	Message string     `json:"msg"`
	Data    []apiQuote `json:"data"`
}

type liquidityEnvelope struct {
	Code    string            `json:"code"`
	Message string            `json:"msg"`
	Data    []LiquiditySource `json:"data"`
}

type apiQuote struct {
	ChainIndex            string            `json:"chainIndex"`
	SwapMode              string            `json:"swapMode"`
	Routes                []apiRoute        `json:"dexRouterList"`
	FromToken             TokenInfo         `json:"fromToken"`
	ToToken               TokenInfo         `json:"toToken"`
	FromTokenAmount       string            `json:"fromTokenAmount"`
	ToTokenAmount         string            `json:"toTokenAmount"`
	OriginToTokenAmount   string            `json:"originToTokenAmount"`
	TradeFee              string            `json:"tradeFee"`
	EstimateGasFee        string            `json:"estimateGasFee"`
	PriceImpactPercentage string            `json:"priceImpactPercentage"`
	Comparisons           []RouteComparison `json:"quoteCompareList"`
}

type apiRoute struct {
	Router    string        `json:"router"`
	Percent   string        `json:"routerPercent"`
	Protocols flexProtocols `json:"dexProtocol"`
	SubRoutes []apiSubRoute `json:"subRouterList"`
}

type apiSubRoute struct {
	Protocols flexProtocols `json:"dexProtocol"`
	FromToken TokenInfo     `json:"fromToken"`
	ToToken   TokenInfo     `json:"toToken"`
}

// flexProtocols accepts both the v6 object form and the v5 array form seen in
// OKX's published examples.
type flexProtocols []Protocol

func (p *flexProtocols) UnmarshalJSON(data []byte) error {
	if string(data) == "null" || len(strings.TrimSpace(string(data))) == 0 {
		return nil
	}
	var list []Protocol
	if err := json.Unmarshal(data, &list); err == nil {
		*p = list
		return nil
	}
	var one Protocol
	if err := json.Unmarshal(data, &one); err != nil {
		return fmt.Errorf("decode OKX protocol list: %w", err)
	}
	*p = []Protocol{one}
	return nil
}

func (r apiRoute) toRoute() Route {
	converted := Route{Router: r.Router, Percent: r.Percent, Protocols: append([]Protocol(nil), r.Protocols...)}
	for _, sub := range r.SubRoutes {
		converted.SubRoutes = append(converted.SubRoutes, SubRoute{Protocols: append([]Protocol(nil), sub.Protocols...), FromToken: sub.FromToken, ToToken: sub.ToToken})
	}
	return converted
}

var _ Limiter = (*SpacingLimiter)(nil)
