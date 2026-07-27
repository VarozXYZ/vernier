// Package zerox provides a read-only client for the 0x Swap API v2.
// It can request indicative prices and firm quotes with executable calldata,
// but it never signs, approves tokens, submits, or broadcasts transactions.
package zerox

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
	"time"

	"github.com/ethereum/go-ethereum/common"
)

const (
	DefaultBaseURL   = "https://api.0x.org"
	DefaultPricePath = "/swap/allowance-holder/price"
	DefaultQuotePath = "/swap/allowance-holder/quote"
	APIVersion       = "v2"

	RobinhoodChainID = "4663"

	maxResponseBytes = 4 << 20
)

// Client is the HTTP boundary used by Source.
type Client interface {
	Do(*http.Request) (*http.Response, error)
}

// Config contains authentication and transport settings.
type Config struct {
	BaseURL string
	APIKey  string
	Client  Client
	Timeout time.Duration
}

// Request contains one exact-input swap request. SellAmount is expressed in
// the sell token's integer minimum units.
type Request struct {
	ChainID     string
	SellToken   string
	BuyToken    string
	SellAmount  string
	Taker       string
	SlippageBPS uint16
}

// RouteFill describes one source used by the 0x routing engine.
type RouteFill struct {
	From          string `json:"from"`
	To            string `json:"to"`
	Source        string `json:"source"`
	ProportionBPS string `json:"proportionBps"`
}

// Transaction is the unsigned EVM transaction returned by a firm quote.
type Transaction struct {
	To       string `json:"to"`
	Data     string `json:"data"`
	Value    string `json:"value"`
	Gas      string `json:"gas"`
	GasPrice string `json:"gasPrice"`
}

// Issues reports execution preconditions detected by 0x. Missing allowance or
// balance does not invalidate a quote measurement, but callers should inspect
// these fields before any real execution workflow.
type Issues struct {
	AllowanceSpender     string
	AllowanceActual      string
	BalanceToken         string
	BalanceActual        string
	BalanceExpected      string
	SimulationIncomplete bool
}

// Result contains a parsed price or quote plus transport timings.
type Result struct {
	Request Request

	BlockNumber        string
	SellAmount         string
	BuyAmount          string
	MinBuyAmount       string
	SellToken          string
	BuyToken           string
	LiquidityAvailable bool
	AllowanceTarget    string
	TotalNetworkFee    string
	Gas                string
	GasPrice           string
	Route              []RouteFill
	Issues             Issues
	Transaction        Transaction

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
}

func (e *APIError) Error() string {
	operation := e.Operation
	if operation == "" {
		operation = "request"
	}
	if e.Code != "" {
		return fmt.Sprintf("0x %s failed: code=%s http=%d: %s", operation, e.Code, e.HTTPStatus, e.Message)
	}
	return fmt.Sprintf("0x %s failed: http=%d: %s", operation, e.HTTPStatus, e.Message)
}

func (e *APIError) RateLimited() bool {
	return e != nil && e.HTTPStatus == http.StatusTooManyRequests
}

// Source is a direct 0x Swap API client.
type Source struct {
	baseURL string
	apiKey  string
	client  Client
}

func New(config Config) (*Source, error) {
	if strings.TrimSpace(config.APIKey) == "" {
		return nil, fmt.Errorf("0x source requires an API key")
	}
	if config.BaseURL == "" {
		config.BaseURL = DefaultBaseURL
	}
	parsed, err := url.Parse(strings.TrimRight(config.BaseURL, "/"))
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("invalid 0x base URL")
	}
	if config.Client == nil {
		timeout := config.Timeout
		if timeout <= 0 {
			timeout = 15 * time.Second
		}
		config.Client = &http.Client{Timeout: timeout}
	}
	return &Source{
		baseURL: strings.TrimRight(parsed.String(), "/"),
		apiKey:  strings.TrimSpace(config.APIKey),
		client:  config.Client,
	}, nil
}

// Price requests an indicative, read-only price.
func (s *Source) Price(ctx context.Context, input Request) (Result, error) {
	return s.get(ctx, "price", DefaultPricePath, input, false)
}

// Quote requests a firm quote and unsigned transaction calldata.
func (s *Source) Quote(ctx context.Context, input Request) (Result, error) {
	return s.get(ctx, "quote", DefaultQuotePath, input, true)
}

func (s *Source) get(ctx context.Context, operation, path string, input Request, requireTransaction bool) (Result, error) {
	started := time.Now()
	resolved, err := resolve(input)
	if err != nil {
		return Result{TotalDuration: time.Since(started)}, err
	}
	query := url.Values{}
	query.Set("chainId", resolved.ChainID)
	query.Set("sellToken", resolved.SellToken)
	query.Set("buyToken", resolved.BuyToken)
	query.Set("sellAmount", resolved.SellAmount)
	query.Set("taker", resolved.Taker)
	if resolved.SlippageBPS != 0 {
		query.Set("slippageBps", strconv.FormatUint(uint64(resolved.SlippageBPS), 10))
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, s.baseURL+path+"?"+query.Encode(), nil)
	if err != nil {
		return Result{Request: resolved, TotalDuration: time.Since(started)}, err
	}
	request.Header.Set("0x-api-key", s.apiKey)
	request.Header.Set("0x-version", APIVersion)
	request.Header.Set("Accept", "application/json")

	httpStarted := time.Now()
	response, err := s.client.Do(request)
	if err != nil {
		return Result{Request: resolved, HTTPDuration: time.Since(httpStarted), TotalDuration: time.Since(started)}, err
	}
	if response == nil || response.Body == nil {
		return Result{Request: resolved, HTTPStatus: responseStatus(response), HTTPDuration: time.Since(httpStarted), TotalDuration: time.Since(started)}, fmt.Errorf("0x HTTP client returned an empty response")
	}
	defer response.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	result := Result{
		Request:       resolved,
		HTTPStatus:    response.StatusCode,
		HTTPDuration:  time.Since(httpStarted),
		TotalDuration: time.Since(started),
		ResponseBytes: len(body),
		RawResponse:   append([]byte(nil), body...),
	}
	if readErr != nil {
		return result, readErr
	}
	if len(body) > maxResponseBytes {
		return result, fmt.Errorf("0x response exceeds %d bytes", maxResponseBytes)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return result, parseAPIError(operation, response.StatusCode, body)
	}

	var payload responsePayload
	if err := json.Unmarshal(body, &payload); err != nil {
		return result, fmt.Errorf("decode 0x %s response: %w", operation, err)
	}
	result.apply(payload)
	result.TotalDuration = time.Since(started)
	if !result.LiquidityAvailable {
		return result, &APIError{Operation: operation, HTTPStatus: response.StatusCode, Code: "NO_LIQUIDITY", Message: "0x returned no available liquidity"}
	}
	if !positiveInteger(result.BuyAmount) || !positiveInteger(result.SellAmount) {
		return result, &APIError{Operation: operation, HTTPStatus: response.StatusCode, Code: "INVALID_AMOUNT", Message: "0x response has no positive buy/sell amount"}
	}
	if requireTransaction && (!validAddress(result.Transaction.To, true) || strings.TrimSpace(result.Transaction.Data) == "") {
		return result, &APIError{Operation: operation, HTTPStatus: response.StatusCode, Code: "MISSING_TRANSACTION", Message: "0x firm quote has no executable transaction"}
	}
	return result, nil
}

type responsePayload struct {
	BlockNumber        string `json:"blockNumber"`
	SellAmount         string `json:"sellAmount"`
	BuyAmount          string `json:"buyAmount"`
	MinBuyAmount       string `json:"minBuyAmount"`
	SellToken          string `json:"sellToken"`
	BuyToken           string `json:"buyToken"`
	LiquidityAvailable bool   `json:"liquidityAvailable"`
	AllowanceTarget    string `json:"allowanceTarget"`
	TotalNetworkFee    string `json:"totalNetworkFee"`
	Gas                string `json:"gas"`
	GasPrice           string `json:"gasPrice"`
	Route              struct {
		Fills []RouteFill `json:"fills"`
	} `json:"route"`
	Issues struct {
		Allowance *struct {
			Actual  string `json:"actual"`
			Spender string `json:"spender"`
		} `json:"allowance"`
		Balance *struct {
			Token    string `json:"token"`
			Actual   string `json:"actual"`
			Expected string `json:"expected"`
		} `json:"balance"`
		SimulationIncomplete bool `json:"simulationIncomplete"`
	} `json:"issues"`
	Transaction Transaction `json:"transaction"`
}

func (r *Result) apply(payload responsePayload) {
	r.BlockNumber = payload.BlockNumber
	r.SellAmount = payload.SellAmount
	r.BuyAmount = payload.BuyAmount
	r.MinBuyAmount = payload.MinBuyAmount
	r.SellToken = payload.SellToken
	r.BuyToken = payload.BuyToken
	r.LiquidityAvailable = payload.LiquidityAvailable
	r.AllowanceTarget = payload.AllowanceTarget
	r.TotalNetworkFee = payload.TotalNetworkFee
	r.Gas = payload.Gas
	r.GasPrice = payload.GasPrice
	r.Route = append([]RouteFill(nil), payload.Route.Fills...)
	r.Transaction = payload.Transaction
	r.Issues.SimulationIncomplete = payload.Issues.SimulationIncomplete
	if payload.Issues.Allowance != nil {
		r.Issues.AllowanceActual = payload.Issues.Allowance.Actual
		r.Issues.AllowanceSpender = payload.Issues.Allowance.Spender
	}
	if payload.Issues.Balance != nil {
		r.Issues.BalanceToken = payload.Issues.Balance.Token
		r.Issues.BalanceActual = payload.Issues.Balance.Actual
		r.Issues.BalanceExpected = payload.Issues.Balance.Expected
	}
}

func resolve(input Request) (Request, error) {
	input.ChainID = strings.TrimSpace(input.ChainID)
	input.SellToken = strings.TrimSpace(input.SellToken)
	input.BuyToken = strings.TrimSpace(input.BuyToken)
	input.SellAmount = strings.TrimSpace(input.SellAmount)
	input.Taker = strings.TrimSpace(input.Taker)
	if input.ChainID == "" {
		input.ChainID = RobinhoodChainID
	}
	chainID, ok := new(big.Int).SetString(input.ChainID, 10)
	if !ok || chainID.Sign() <= 0 {
		return Request{}, fmt.Errorf("0x chain ID must be a positive integer")
	}
	if !validAddress(input.SellToken, true) || !validAddress(input.BuyToken, true) || strings.EqualFold(input.SellToken, input.BuyToken) {
		return Request{}, fmt.Errorf("0x request requires distinct non-zero sell/buy token addresses")
	}
	if !validAddress(input.Taker, true) {
		return Request{}, fmt.Errorf("0x request requires a valid non-zero taker address")
	}
	if !positiveInteger(input.SellAmount) {
		return Request{}, fmt.Errorf("0x sell amount must be a positive integer in minimum units")
	}
	if input.SlippageBPS > 10_000 {
		return Request{}, fmt.Errorf("0x slippage must be <= 10000 basis points")
	}
	return input, nil
}

func validAddress(value string, rejectZero bool) bool {
	if !common.IsHexAddress(value) {
		return false
	}
	return !rejectZero || common.HexToAddress(value) != (common.Address{})
}

func positiveInteger(value string) bool {
	parsed, ok := new(big.Int).SetString(strings.TrimSpace(value), 10)
	return ok && parsed.Sign() > 0
}

func parseAPIError(operation string, status int, body []byte) *APIError {
	var payload struct {
		Name    string `json:"name"`
		Code    string `json:"code"`
		Message string `json:"message"`
		Reason  string `json:"reason"`
	}
	message := strings.TrimSpace(string(body))
	if json.Unmarshal(body, &payload) == nil {
		if payload.Message != "" {
			message = payload.Message
		} else if payload.Reason != "" {
			message = payload.Reason
		}
	}
	if message == "" {
		message = http.StatusText(status)
	}
	code := payload.Code
	if code == "" {
		code = payload.Name
	}
	return &APIError{Operation: operation, HTTPStatus: status, Code: code, Message: message}
}

func responseStatus(response *http.Response) int {
	if response == nil {
		return 0
	}
	return response.StatusCode
}
