// Package kyberswap provides a read-only client for the KyberSwap Aggregator
// API v1. It can request routes and unsigned calldata, but it never signs,
// approves tokens, submits, or broadcasts transactions.
package kyberswap

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

const (
	DefaultBaseURL   = "https://aggregator-api.kyberswap.com"
	DefaultChain     = "robinhood"
	DefaultRoutePath = "/api/v1/routes"
	DefaultBuildPath = "/api/v1/route/build"

	maxResponseBytes = 4 << 20
)

var chainName = regexp.MustCompile(`^[a-z0-9-]+$`)

// Client is the HTTP boundary used by Source.
type Client interface {
	Do(*http.Request) (*http.Response, error)
}

// Config contains identification and transport settings. ClientID is sent as
// x-client-id; KyberSwap applies stricter rate limits when it is absent.
type Config struct {
	BaseURL  string
	ClientID string
	Client   Client
	Timeout  time.Duration
}

// RouteRequest contains one exact-input route request. AmountIn is expressed
// in the input token's integer minimum units.
type RouteRequest struct {
	Chain    string
	TokenIn  string
	TokenOut string
	AmountIn string
	Origin   string
}

// RouteStep describes one liquidity source in a returned path.
type RouteStep struct {
	Pool       string `json:"pool"`
	TokenIn    string `json:"tokenIn"`
	TokenOut   string `json:"tokenOut"`
	SwapAmount string `json:"swapAmount"`
	AmountOut  string `json:"amountOut"`
	Exchange   string `json:"exchange"`
	PoolType   string `json:"poolType"`
}

// RouteResult contains KyberSwap's best route plus transport timings.
type RouteResult struct {
	Request RouteRequest

	RouterAddress string
	TokenIn       string
	TokenOut      string
	AmountIn      string
	AmountOut     string
	AmountInUSD   string
	AmountOutUSD  string
	Gas           string
	GasPrice      string
	GasUSD        string
	L1FeeUSD      string
	RouteID       string
	Paths         [][]RouteStep

	HTTPStatus    int
	HTTPDuration  time.Duration
	TotalDuration time.Duration
	ResponseBytes int
	RawResponse   []byte

	routeSummary json.RawMessage
}

// BuildRequest contains the transaction-specific fields required to turn a
// fresh route into unsigned calldata.
type BuildRequest struct {
	Route               RouteResult
	Sender              string
	Recipient           string
	Origin              string
	SlippageBPS         uint16
	EnableGasEstimation bool
}

// BuildResult contains unsigned transaction data returned by KyberSwap.
type BuildResult struct {
	AmountIn         string
	AmountOut        string
	AmountInUSD      string
	AmountOutUSD     string
	Gas              string
	GasUSD           string
	OutputChange     string
	RouterAddress    string
	Data             string
	TransactionValue string

	HTTPStatus    int
	HTTPDuration  time.Duration
	TotalDuration time.Duration
	ResponseBytes int
	RawResponse   []byte
}

// APIError represents an HTTP or provider-level failure.
type APIError struct {
	Operation  string
	HTTPStatus int
	Code       string
	Message    string
	retryAfter time.Duration
}

func (e *APIError) Error() string {
	operation := e.Operation
	if operation == "" {
		operation = "request"
	}
	if e.Code != "" {
		return fmt.Sprintf("KyberSwap %s failed: code=%s http=%d: %s", operation, e.Code, e.HTTPStatus, e.Message)
	}
	return fmt.Sprintf("KyberSwap %s failed: http=%d: %s", operation, e.HTTPStatus, e.Message)
}

func (e *APIError) RateLimited() bool {
	return e != nil && e.HTTPStatus == http.StatusTooManyRequests
}

func (e *APIError) HTTPStatusCode() int {
	if e == nil {
		return 0
	}
	return e.HTTPStatus
}

func (e *APIError) RetryAfter() time.Duration {
	if e == nil {
		return 0
	}
	return e.retryAfter
}

// Source is a direct KyberSwap Aggregator API v1 client.
type Source struct {
	baseURL  string
	clientID string
	client   Client
}

func New(config Config) (*Source, error) {
	if strings.TrimSpace(config.ClientID) == "" {
		return nil, fmt.Errorf("KyberSwap source requires a client ID")
	}
	if config.BaseURL == "" {
		config.BaseURL = DefaultBaseURL
	}
	parsed, err := url.Parse(strings.TrimRight(config.BaseURL, "/"))
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("invalid KyberSwap base URL")
	}
	if config.Client == nil {
		timeout := config.Timeout
		if timeout <= 0 {
			timeout = 15 * time.Second
		}
		config.Client = &http.Client{Timeout: timeout}
	}
	return &Source{
		baseURL:  strings.TrimRight(parsed.String(), "/"),
		clientID: strings.TrimSpace(config.ClientID),
		client:   config.Client,
	}, nil
}

// Route requests KyberSwap's best current exact-input route.
func (s *Source) Route(ctx context.Context, input RouteRequest) (RouteResult, error) {
	started := time.Now()
	resolved, err := resolveRoute(input)
	if err != nil {
		return RouteResult{TotalDuration: time.Since(started)}, err
	}
	query := url.Values{}
	query.Set("tokenIn", resolved.TokenIn)
	query.Set("tokenOut", resolved.TokenOut)
	query.Set("amountIn", resolved.AmountIn)
	if resolved.Origin != "" {
		query.Set("origin", resolved.Origin)
	}
	endpoint := s.baseURL + "/" + resolved.Chain + DefaultRoutePath + "?" + query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return RouteResult{Request: resolved, TotalDuration: time.Since(started)}, err
	}
	s.setHeaders(request)

	status, httpDuration, headers, body, err := s.do(request)
	result := RouteResult{
		Request:       resolved,
		HTTPStatus:    status,
		HTTPDuration:  httpDuration,
		TotalDuration: time.Since(started),
		ResponseBytes: len(body),
		RawResponse:   append([]byte(nil), body...),
	}
	if err != nil {
		return result, err
	}
	if status < 200 || status >= 300 {
		apiErr := parseAPIError("route", status, body)
		apiErr.retryAfter = parseRetryAfter(headers.Get("Retry-After"), time.Now())
		return result, apiErr
	}

	var envelope routeEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return result, fmt.Errorf("decode KyberSwap route response: %w", err)
	}
	if envelope.Code != 0 {
		return result, providerError("route", status, envelope.Code, envelope.Message)
	}
	var summary routeSummary
	if len(envelope.Data.RouteSummary) == 0 || json.Unmarshal(envelope.Data.RouteSummary, &summary) != nil {
		return result, &APIError{Operation: "route", HTTPStatus: status, Code: "INVALID_ROUTE", Message: "response has no valid route summary"}
	}
	result.apply(envelope.Data.RouterAddress, envelope.Data.RouteSummary, summary)
	result.TotalDuration = time.Since(started)
	if !positiveInteger(result.AmountIn) || !positiveInteger(result.AmountOut) {
		return result, &APIError{Operation: "route", HTTPStatus: status, Code: "INVALID_AMOUNT", Message: "response has no positive input/output amount"}
	}
	if !strings.EqualFold(result.TokenIn, resolved.TokenIn) ||
		!strings.EqualFold(result.TokenOut, resolved.TokenOut) ||
		result.AmountIn != resolved.AmountIn {
		return result, &APIError{Operation: "route", HTTPStatus: status, Code: "MISMATCHED_ROUTE", Message: "response does not match the requested tokens and amount"}
	}
	if !validAddress(result.RouterAddress, true) {
		return result, &APIError{Operation: "route", HTTPStatus: status, Code: "INVALID_ROUTER", Message: "response has no valid router address"}
	}
	return result, nil
}

// Build converts a fresh Route result into unsigned transaction calldata.
func (s *Source) Build(ctx context.Context, input BuildRequest) (BuildResult, error) {
	started := time.Now()
	resolved, err := resolveBuild(input)
	if err != nil {
		return BuildResult{TotalDuration: time.Since(started)}, err
	}
	payload := struct {
		RouteSummary      json.RawMessage `json:"routeSummary"`
		Sender            string          `json:"sender"`
		Recipient         string          `json:"recipient"`
		Origin            string          `json:"origin,omitempty"`
		SlippageTolerance uint16          `json:"slippageTolerance"`
		EnableGasEstimate bool            `json:"enableGasEstimation"`
		Source            string          `json:"source"`
	}{
		RouteSummary:      resolved.Route.routeSummary,
		Sender:            resolved.Sender,
		Recipient:         resolved.Recipient,
		Origin:            resolved.Origin,
		SlippageTolerance: resolved.SlippageBPS,
		EnableGasEstimate: resolved.EnableGasEstimation,
		Source:            s.clientID,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return BuildResult{TotalDuration: time.Since(started)}, err
	}
	endpoint := s.baseURL + "/" + resolved.Route.Request.Chain + DefaultBuildPath
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return BuildResult{TotalDuration: time.Since(started)}, err
	}
	s.setHeaders(request)
	request.Header.Set("Content-Type", "application/json")

	status, httpDuration, headers, responseBody, err := s.do(request)
	result := BuildResult{
		HTTPStatus:    status,
		HTTPDuration:  httpDuration,
		TotalDuration: time.Since(started),
		ResponseBytes: len(responseBody),
		RawResponse:   append([]byte(nil), responseBody...),
	}
	if err != nil {
		return result, err
	}
	if status < 200 || status >= 300 {
		apiErr := parseAPIError("build", status, responseBody)
		apiErr.retryAfter = parseRetryAfter(headers.Get("Retry-After"), time.Now())
		return result, apiErr
	}
	var envelope buildEnvelope
	if err := json.Unmarshal(responseBody, &envelope); err != nil {
		return result, fmt.Errorf("decode KyberSwap build response: %w", err)
	}
	if envelope.Code != 0 {
		return result, providerError("build", status, envelope.Code, envelope.Message)
	}
	result.apply(envelope.Data)
	result.TotalDuration = time.Since(started)
	if !validAddress(result.RouterAddress, true) || !validCalldata(result.Data) {
		return result, &APIError{Operation: "build", HTTPStatus: status, Code: "MISSING_TRANSACTION", Message: "response has no executable unsigned transaction"}
	}
	if !strings.EqualFold(result.RouterAddress, resolved.Route.RouterAddress) {
		return result, &APIError{Operation: "build", HTTPStatus: status, Code: "MISMATCHED_ROUTER", Message: "transaction router does not match the quoted route"}
	}
	if result.AmountIn != resolved.Route.AmountIn ||
		!positiveInteger(result.AmountOut) {
		return result, &APIError{Operation: "build", HTTPStatus: status, Code: "MISMATCHED_AMOUNT", Message: "transaction amounts do not match the quoted route"}
	}
	return result, nil
}

type routeEnvelope struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		RouteSummary  json.RawMessage `json:"routeSummary"`
		RouterAddress string          `json:"routerAddress"`
	} `json:"data"`
}

type routeSummary struct {
	TokenIn      string        `json:"tokenIn"`
	TokenOut     string        `json:"tokenOut"`
	AmountIn     string        `json:"amountIn"`
	AmountOut    string        `json:"amountOut"`
	AmountInUSD  string        `json:"amountInUsd"`
	AmountOutUSD string        `json:"amountOutUsd"`
	Gas          string        `json:"gas"`
	GasPrice     string        `json:"gasPrice"`
	GasUSD       string        `json:"gasUsd"`
	L1FeeUSD     string        `json:"l1FeeUsd"`
	RouteID      string        `json:"routeID"`
	Route        [][]RouteStep `json:"route"`
}

type buildEnvelope struct {
	Code    int       `json:"code"`
	Message string    `json:"message"`
	Data    buildData `json:"data"`
}

type buildData struct {
	AmountIn         string          `json:"amountIn"`
	AmountOut        string          `json:"amountOut"`
	AmountInUSD      string          `json:"amountInUsd"`
	AmountOutUSD     string          `json:"amountOutUsd"`
	Gas              string          `json:"gas"`
	GasUSD           string          `json:"gasUsd"`
	OutputChange     json.RawMessage `json:"outputChange"`
	RouterAddress    string          `json:"routerAddress"`
	Data             string          `json:"data"`
	TransactionValue string          `json:"transactionValue"`
}

func (r *RouteResult) apply(routerAddress string, raw json.RawMessage, summary routeSummary) {
	r.RouterAddress = routerAddress
	r.TokenIn = summary.TokenIn
	r.TokenOut = summary.TokenOut
	r.AmountIn = summary.AmountIn
	r.AmountOut = summary.AmountOut
	r.AmountInUSD = summary.AmountInUSD
	r.AmountOutUSD = summary.AmountOutUSD
	r.Gas = summary.Gas
	r.GasPrice = summary.GasPrice
	r.GasUSD = summary.GasUSD
	r.L1FeeUSD = summary.L1FeeUSD
	r.RouteID = summary.RouteID
	r.Paths = summary.Route
	r.routeSummary = append(json.RawMessage(nil), raw...)
}

func (r *BuildResult) apply(data buildData) {
	r.AmountIn = data.AmountIn
	r.AmountOut = data.AmountOut
	r.AmountInUSD = data.AmountInUSD
	r.AmountOutUSD = data.AmountOutUSD
	r.Gas = data.Gas
	r.GasUSD = data.GasUSD
	r.OutputChange = normalizeJSONText(data.OutputChange)
	r.RouterAddress = data.RouterAddress
	r.Data = data.Data
	r.TransactionValue = data.TransactionValue
}

func normalizeJSONText(raw json.RawMessage) string {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return ""
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return text
	}
	var compact bytes.Buffer
	if json.Compact(&compact, raw) == nil {
		return compact.String()
	}
	return string(raw)
}

func (s *Source) setHeaders(request *http.Request) {
	request.Header.Set("x-client-id", s.clientID)
	request.Header.Set("Accept", "application/json")
}

func (s *Source) do(request *http.Request) (int, time.Duration, http.Header, []byte, error) {
	httpStarted := time.Now()
	response, err := s.client.Do(request)
	if err != nil {
		return 0, time.Since(httpStarted), nil, nil, err
	}
	if response == nil || response.Body == nil {
		return responseStatus(response), time.Since(httpStarted), nil, nil, fmt.Errorf("KyberSwap HTTP client returned an empty response")
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	httpDuration := time.Since(httpStarted)
	if err != nil {
		return response.StatusCode, httpDuration, response.Header.Clone(), body, err
	}
	if len(body) > maxResponseBytes {
		return response.StatusCode, httpDuration, response.Header.Clone(), body, fmt.Errorf("KyberSwap response exceeds %d bytes", maxResponseBytes)
	}
	return response.StatusCode, httpDuration, response.Header.Clone(), body, nil
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil && at.After(now) {
		return at.Sub(now)
	}
	return 0
}

func resolveRoute(input RouteRequest) (RouteRequest, error) {
	input.Chain = strings.ToLower(strings.TrimSpace(input.Chain))
	input.TokenIn = strings.TrimSpace(input.TokenIn)
	input.TokenOut = strings.TrimSpace(input.TokenOut)
	input.AmountIn = strings.TrimSpace(input.AmountIn)
	input.Origin = strings.TrimSpace(input.Origin)
	if input.Chain == "" {
		input.Chain = DefaultChain
	}
	if !chainName.MatchString(input.Chain) {
		return RouteRequest{}, fmt.Errorf("KyberSwap chain must be a lowercase chain slug")
	}
	if !validAddress(input.TokenIn, true) || !validAddress(input.TokenOut, true) || strings.EqualFold(input.TokenIn, input.TokenOut) {
		return RouteRequest{}, fmt.Errorf("KyberSwap route requires distinct non-zero input/output token addresses")
	}
	if !positiveInteger(input.AmountIn) {
		return RouteRequest{}, fmt.Errorf("KyberSwap input amount must be a positive integer in minimum units")
	}
	if input.Origin != "" && !validAddress(input.Origin, true) {
		return RouteRequest{}, fmt.Errorf("KyberSwap origin must be a valid non-zero address")
	}
	return input, nil
}

func resolveBuild(input BuildRequest) (BuildRequest, error) {
	input.Sender = strings.TrimSpace(input.Sender)
	input.Recipient = strings.TrimSpace(input.Recipient)
	input.Origin = strings.TrimSpace(input.Origin)
	if len(input.Route.routeSummary) == 0 || input.Route.Request.Chain == "" {
		return BuildRequest{}, fmt.Errorf("KyberSwap build requires a route returned by this source")
	}
	if !validAddress(input.Sender, true) || !validAddress(input.Recipient, true) {
		return BuildRequest{}, fmt.Errorf("KyberSwap build requires valid non-zero sender and recipient addresses")
	}
	if input.Origin != "" && !validAddress(input.Origin, true) {
		return BuildRequest{}, fmt.Errorf("KyberSwap build origin must be a valid non-zero address")
	}
	if input.SlippageBPS > 2_000 {
		return BuildRequest{}, fmt.Errorf("KyberSwap slippage must be <= 2000 basis points")
	}
	return input, nil
}

func validAddress(value string, rejectZero bool) bool {
	if !common.IsHexAddress(value) {
		return false
	}
	return !rejectZero || common.HexToAddress(value) != (common.Address{})
}

func validCalldata(value string) bool {
	value = strings.TrimSpace(value)
	return strings.HasPrefix(value, "0x") && len(value) > 2
}

func positiveInteger(value string) bool {
	parsed, ok := new(big.Int).SetString(strings.TrimSpace(value), 10)
	return ok && parsed.Sign() > 0
}

func parseAPIError(operation string, status int, body []byte) *APIError {
	var payload struct {
		Code    json.RawMessage `json:"code"`
		Message string          `json:"message"`
	}
	message := strings.TrimSpace(string(body))
	code := ""
	if json.Unmarshal(body, &payload) == nil {
		code = rawCode(payload.Code)
		if payload.Message != "" {
			message = payload.Message
		}
	}
	if message == "" {
		message = http.StatusText(status)
	}
	return &APIError{Operation: operation, HTTPStatus: status, Code: code, Message: message}
}

func providerError(operation string, status, code int, message string) *APIError {
	if strings.TrimSpace(message) == "" {
		message = "provider returned a non-zero response code"
	}
	return &APIError{Operation: operation, HTTPStatus: status, Code: strconv.Itoa(code), Message: message}
}

func rawCode(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return text
	}
	var number json.Number
	if json.Unmarshal(raw, &number) == nil {
		return number.String()
	}
	return strings.Trim(string(raw), `"`)
}

func responseStatus(response *http.Response) int {
	if response == nil {
		return 0
	}
	return response.StatusCode
}
