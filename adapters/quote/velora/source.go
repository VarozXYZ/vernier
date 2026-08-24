// Package velora provides a read-only Velora Market API v6.2 client. It can
// retrieve routes and unsigned transaction calldata, but it never signs,
// approves, submits, or broadcasts transactions.
package velora

import (
	"bytes"
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
	DefaultBaseURL = "https://api.velora.xyz"
	Version        = "6.2"

	maxResponseBytes = 4 << 20
)

type Client interface {
	Do(*http.Request) (*http.Response, error)
}

type Config struct {
	BaseURL string
	Client  Client
	Timeout time.Duration
}

type Source struct {
	baseURL string
	client  Client
}

type PriceRequest struct {
	Network     uint64
	SourceToken string
	SourceUnits uint8
	DestToken   string
	DestUnits   uint8
	Amount      string
	UserAddress string
	Partner     string
	ExcludeRFQ  bool
}

type PriceResult struct {
	Request       PriceRequest
	SourceAmount  string
	DestAmount    string
	GasCost       string
	GasCostUSD    string
	Contract      string
	Method        string
	PriceRoute    json.RawMessage
	HTTPStatus    int
	HTTPDuration  time.Duration
	TotalDuration time.Duration
	ResponseBytes int
	RawResponse   []byte
}

type TransactionRequest struct {
	Price        PriceResult
	UserAddress  string
	SlippageBPS  uint16
	Partner      string
	IgnoreChecks bool
}

type TransactionResult struct {
	From          string
	To            string
	Value         string
	Data          string
	Gas           string
	GasPrice      string
	ChainID       uint64
	HTTPStatus    int
	HTTPDuration  time.Duration
	TotalDuration time.Duration
	ResponseBytes int
	RawResponse   []byte
}

type SwapRequest struct {
	PriceRequest
	SlippageBPS uint16
}

type SwapResult struct {
	Price         PriceResult
	Transaction   TransactionResult
	HTTPStatus    int
	HTTPDuration  time.Duration
	TotalDuration time.Duration
	ResponseBytes int
	RawResponse   []byte
}

type APIError struct {
	Operation  string
	HTTPStatus int
	Type       string
	Message    string
}

func (e *APIError) Error() string {
	if e.Type != "" {
		return fmt.Sprintf("Velora %s failed: type=%s http=%d: %s", e.Operation, e.Type, e.HTTPStatus, e.Message)
	}
	return fmt.Sprintf("Velora %s failed: http=%d: %s", e.Operation, e.HTTPStatus, e.Message)
}

func New(config Config) (*Source, error) {
	if config.BaseURL == "" {
		config.BaseURL = DefaultBaseURL
	}
	parsed, err := url.Parse(strings.TrimRight(config.BaseURL, "/"))
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("invalid Velora base URL")
	}
	if config.Client == nil {
		timeout := config.Timeout
		if timeout <= 0 {
			timeout = 15 * time.Second
		}
		config.Client = &http.Client{Timeout: timeout}
	}
	return &Source{baseURL: strings.TrimRight(parsed.String(), "/"), client: config.Client}, nil
}

// Price retrieves an exact-input (SELL) Market route.
func (s *Source) Price(ctx context.Context, input PriceRequest) (PriceResult, error) {
	started := time.Now()
	resolved, err := resolvePrice(input)
	if err != nil {
		return PriceResult{TotalDuration: time.Since(started)}, err
	}
	query := priceQuery(resolved)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, s.baseURL+"/prices?"+query.Encode(), nil)
	if err != nil {
		return PriceResult{Request: resolved, TotalDuration: time.Since(started)}, err
	}
	status, httpDuration, body, err := s.do(request)
	result := PriceResult{Request: resolved, HTTPStatus: status, HTTPDuration: httpDuration, TotalDuration: time.Since(started), ResponseBytes: len(body), RawResponse: append([]byte(nil), body...)}
	if err != nil {
		return result, err
	}
	if status < 200 || status >= 300 {
		return result, parseAPIError("prices", status, body)
	}
	var envelope struct {
		PriceRoute json.RawMessage `json:"priceRoute"`
	}
	if json.Unmarshal(body, &envelope) != nil || len(envelope.PriceRoute) == 0 {
		return result, fmt.Errorf("decode Velora prices response: missing priceRoute")
	}
	if err := applyPrice(&result, envelope.PriceRoute); err != nil {
		return result, err
	}
	result.TotalDuration = time.Since(started)
	return result, nil
}

// Transaction builds unsigned calldata from a Price result.
func (s *Source) Transaction(ctx context.Context, input TransactionRequest) (TransactionResult, error) {
	started := time.Now()
	resolved, err := resolveTransaction(input)
	if err != nil {
		return TransactionResult{TotalDuration: time.Since(started)}, err
	}
	payload := struct {
		PriceRoute   json.RawMessage `json:"priceRoute"`
		SourceToken  string          `json:"srcToken"`
		DestToken    string          `json:"destToken"`
		UserAddress  string          `json:"userAddress"`
		SourceUnits  uint8           `json:"srcDecimals"`
		DestUnits    uint8           `json:"destDecimals"`
		SourceAmount string          `json:"srcAmount"`
		SlippageBPS  uint16          `json:"slippage"`
		Partner      string          `json:"partner,omitempty"`
	}{
		PriceRoute: resolved.Price.PriceRoute, SourceToken: resolved.Price.Request.SourceToken,
		DestToken: resolved.Price.Request.DestToken, UserAddress: resolved.UserAddress,
		SourceUnits: resolved.Price.Request.SourceUnits, DestUnits: resolved.Price.Request.DestUnits,
		SourceAmount: resolved.Price.SourceAmount,
		SlippageBPS:  resolved.SlippageBPS, Partner: resolved.Partner,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return TransactionResult{TotalDuration: time.Since(started)}, err
	}
	query := url.Values{}
	if resolved.IgnoreChecks {
		query.Set("ignoreChecks", "true")
	}
	endpoint := s.baseURL + "/transactions/" + strconv.FormatUint(resolved.Price.Request.Network, 10)
	if encoded := query.Encode(); encoded != "" {
		endpoint += "?" + encoded
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return TransactionResult{TotalDuration: time.Since(started)}, err
	}
	request.Header.Set("Content-Type", "application/json")
	status, httpDuration, responseBody, err := s.do(request)
	result := TransactionResult{HTTPStatus: status, HTTPDuration: httpDuration, TotalDuration: time.Since(started), ResponseBytes: len(responseBody), RawResponse: append([]byte(nil), responseBody...)}
	if err != nil {
		return result, err
	}
	if status < 200 || status >= 300 {
		return result, parseAPIError("transactions", status, responseBody)
	}
	if err := json.Unmarshal(responseBody, &result); err != nil {
		return result, fmt.Errorf("decode Velora transactions response: %w", err)
	}
	if err := validateTransaction(result, resolved.UserAddress, resolved.Price.Request.Network); err != nil {
		return result, err
	}
	result.HTTPStatus, result.HTTPDuration, result.TotalDuration = status, httpDuration, time.Since(started)
	result.ResponseBytes, result.RawResponse = len(responseBody), append([]byte(nil), responseBody...)
	return result, nil
}

// Swap retrieves a Market route and unsigned transaction calldata in one call.
func (s *Source) Swap(ctx context.Context, input SwapRequest) (SwapResult, error) {
	started := time.Now()
	priceRequest, err := resolvePrice(input.PriceRequest)
	if err != nil || input.SlippageBPS > 10_000 {
		if err == nil {
			err = fmt.Errorf("velora slippage must be <= 10000 basis points")
		}
		return SwapResult{TotalDuration: time.Since(started)}, err
	}
	query := priceQuery(priceRequest)
	query.Set("slippage", strconv.Itoa(int(input.SlippageBPS)))
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, s.baseURL+"/swap?"+query.Encode(), nil)
	if err != nil {
		return SwapResult{TotalDuration: time.Since(started)}, err
	}
	status, httpDuration, body, err := s.do(request)
	result := SwapResult{HTTPStatus: status, HTTPDuration: httpDuration, TotalDuration: time.Since(started), ResponseBytes: len(body), RawResponse: append([]byte(nil), body...)}
	if err != nil {
		return result, err
	}
	if status < 200 || status >= 300 {
		return result, parseAPIError("swap", status, body)
	}
	var envelope struct {
		PriceRoute json.RawMessage `json:"priceRoute"`
		TxParams   json.RawMessage `json:"txParams"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil || len(envelope.PriceRoute) == 0 || len(envelope.TxParams) == 0 {
		return result, fmt.Errorf("decode Velora swap response: missing priceRoute or txParams")
	}
	result.Price = PriceResult{Request: priceRequest, PriceRoute: append(json.RawMessage(nil), envelope.PriceRoute...)}
	if err := applyPrice(&result.Price, envelope.PriceRoute); err != nil {
		return result, err
	}
	if err := json.Unmarshal(envelope.TxParams, &result.Transaction); err != nil {
		return result, fmt.Errorf("decode Velora swap transaction: %w", err)
	}
	if err := validateTransaction(result.Transaction, priceRequest.UserAddress, priceRequest.Network); err != nil {
		return result, err
	}
	result.TotalDuration = time.Since(started)
	return result, nil
}

func priceQuery(input PriceRequest) url.Values {
	query := url.Values{}
	query.Set("srcToken", input.SourceToken)
	query.Set("srcDecimals", strconv.Itoa(int(input.SourceUnits)))
	query.Set("destToken", input.DestToken)
	query.Set("destDecimals", strconv.Itoa(int(input.DestUnits)))
	query.Set("amount", input.Amount)
	query.Set("side", "SELL")
	query.Set("network", strconv.FormatUint(input.Network, 10))
	query.Set("version", Version)
	query.Set("userAddress", input.UserAddress)
	if input.Partner != "" {
		query.Set("partner", input.Partner)
	}
	if input.ExcludeRFQ {
		query.Set("excludeRFQ", "true")
	}
	return query
}

func resolvePrice(input PriceRequest) (PriceRequest, error) {
	input.SourceToken, input.DestToken = strings.TrimSpace(input.SourceToken), strings.TrimSpace(input.DestToken)
	input.UserAddress, input.Partner = strings.TrimSpace(input.UserAddress), strings.TrimSpace(input.Partner)
	if input.Network == 0 {
		return PriceRequest{}, fmt.Errorf("velora network must be positive")
	}
	if !validAddress(input.SourceToken) || !validAddress(input.DestToken) || strings.EqualFold(input.SourceToken, input.DestToken) {
		return PriceRequest{}, fmt.Errorf("velora price requires distinct non-zero token addresses")
	}
	if !validAddress(input.UserAddress) {
		return PriceRequest{}, fmt.Errorf("velora price requires a valid non-zero user address")
	}
	if !positiveInteger(input.Amount) {
		return PriceRequest{}, fmt.Errorf("velora amount must be a positive integer in minimum units")
	}
	return input, nil
}

func resolveTransaction(input TransactionRequest) (TransactionRequest, error) {
	input.UserAddress, input.Partner = strings.TrimSpace(input.UserAddress), strings.TrimSpace(input.Partner)
	if len(input.Price.PriceRoute) == 0 || input.Price.Request.Network == 0 {
		return TransactionRequest{}, fmt.Errorf("velora transaction requires a price returned by this source")
	}
	if !validAddress(input.UserAddress) {
		return TransactionRequest{}, fmt.Errorf("velora transaction requires a valid non-zero user address")
	}
	if input.SlippageBPS > 10_000 {
		return TransactionRequest{}, fmt.Errorf("velora slippage must be <= 10000 basis points")
	}
	return input, nil
}

func applyPrice(result *PriceResult, raw json.RawMessage) error {
	var route struct {
		Network      uint64 `json:"network"`
		SourceToken  string `json:"srcToken"`
		DestToken    string `json:"destToken"`
		SourceAmount string `json:"srcAmount"`
		DestAmount   string `json:"destAmount"`
		GasCost      string `json:"gasCost"`
		GasCostUSD   string `json:"gasCostUSD"`
		Contract     string `json:"contractAddress"`
		Method       string `json:"contractMethod"`
	}
	if err := json.Unmarshal(raw, &route); err != nil {
		return fmt.Errorf("decode Velora priceRoute: %w", err)
	}
	if route.Network != result.Request.Network || !strings.EqualFold(route.SourceToken, result.Request.SourceToken) || !strings.EqualFold(route.DestToken, result.Request.DestToken) || route.SourceAmount != result.Request.Amount || !positiveInteger(route.DestAmount) {
		return fmt.Errorf("velora priceRoute does not match the request")
	}
	result.SourceAmount, result.DestAmount = route.SourceAmount, route.DestAmount
	result.GasCost, result.GasCostUSD, result.Contract, result.Method = route.GasCost, route.GasCostUSD, route.Contract, route.Method
	result.PriceRoute = append(json.RawMessage(nil), raw...)
	return nil
}

func validateTransaction(result TransactionResult, user string, network uint64) error {
	if !strings.EqualFold(result.From, user) || !validAddress(result.To) || !strings.HasPrefix(result.Data, "0x") || len(result.Data) <= 2 || result.ChainID != network {
		return fmt.Errorf("velora response has no matching unsigned transaction")
	}
	return nil
}

func (s *Source) do(request *http.Request) (int, time.Duration, []byte, error) {
	request.Header.Set("Accept", "application/json")
	started := time.Now()
	response, err := s.client.Do(request)
	if err != nil {
		return 0, time.Since(started), nil, err
	}
	if response == nil || response.Body == nil {
		return 0, time.Since(started), nil, fmt.Errorf("velora HTTP client returned an empty response")
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	duration := time.Since(started)
	if err != nil {
		return response.StatusCode, duration, body, err
	}
	if len(body) > maxResponseBytes {
		return response.StatusCode, duration, body, fmt.Errorf("velora response exceeds %d bytes", maxResponseBytes)
	}
	return response.StatusCode, duration, body, nil
}

func parseAPIError(operation string, status int, body []byte) *APIError {
	var payload struct {
		Type    string `json:"errorType"`
		Details string `json:"details"`
		Message string `json:"message"`
	}
	_ = json.Unmarshal(body, &payload)
	message := strings.TrimSpace(payload.Details)
	if message == "" {
		message = strings.TrimSpace(payload.Message)
	}
	if message == "" {
		message = strings.TrimSpace(string(body))
	}
	if message == "" {
		message = http.StatusText(status)
	}
	return &APIError{Operation: operation, HTTPStatus: status, Type: payload.Type, Message: message}
}

func validAddress(value string) bool {
	return common.IsHexAddress(value) && common.HexToAddress(value) != (common.Address{})
}
func positiveInteger(value string) bool {
	parsed, ok := new(big.Int).SetString(strings.TrimSpace(value), 10)
	return ok && parsed.Sign() > 0
}
