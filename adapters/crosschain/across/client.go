// Package across implements the authenticated Across Swap API boundary.
//
// The returned source transaction is intentionally kept opaque. Callers must
// verify the economic fields and the origin-chain envelope before signing it;
// they must not reconstruct the route or destination caller locally.
package across

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultBaseURL = "https://app.across.to/api"
	SolanaChainID  = uint64(34268394551451)
)

var integratorIDPattern = regexp.MustCompile(`^0x[0-9a-fA-F]{4}$`)

type Config struct {
	BaseURL      string
	APIKey       string
	IntegratorID string
	Client       *http.Client
	Clock        func() time.Time
}

type Client struct {
	baseURL      *url.URL
	apiKey       string
	integratorID string
	client       *http.Client
	clock        func() time.Time
}

func New(config Config) (*Client, error) {
	if strings.TrimSpace(config.BaseURL) == "" {
		config.BaseURL = DefaultBaseURL
	}
	baseURL, err := url.Parse(config.BaseURL)
	if err != nil || baseURL.Scheme == "" || baseURL.Host == "" {
		return nil, fmt.Errorf("across base URL is invalid")
	}
	if strings.TrimSpace(config.APIKey) == "" {
		return nil, fmt.Errorf("across API key is required")
	}
	if !integratorIDPattern.MatchString(config.IntegratorID) {
		return nil, fmt.Errorf("across integrator ID must be a 2-byte 0x-prefixed hex value")
	}
	if config.Client == nil {
		config.Client = &http.Client{Timeout: 5 * time.Second}
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	return &Client{
		baseURL: baseURL, apiKey: strings.TrimSpace(config.APIKey),
		integratorID: strings.ToLower(config.IntegratorID),
		client:       config.Client, clock: config.Clock,
	}, nil
}

type ApprovalRequest struct {
	OriginChainID      uint64
	DestinationChainID uint64
	InputToken         string
	OutputToken        string
	Amount             string
	Depositor          string
	Recipient          string
	RefundAddress      string
	Slippage           string
	// CostOnly keeps economic and artifact validation but ignores wallet
	// balance/simulation failures caused solely by probing the configured
	// notional without holding it at refresh time.
	CostOnly bool
}

func (r ApprovalRequest) validate() error {
	if r.OriginChainID == 0 || r.DestinationChainID == 0 || r.OriginChainID == r.DestinationChainID {
		return fmt.Errorf("across approval requires distinct non-zero chains")
	}
	if strings.TrimSpace(r.InputToken) == "" || strings.TrimSpace(r.OutputToken) == "" ||
		strings.TrimSpace(r.Depositor) == "" || strings.TrimSpace(r.Recipient) == "" {
		return fmt.Errorf("across approval requires tokens, depositor, and recipient")
	}
	amount, ok := new(big.Int).SetString(r.Amount, 10)
	if !ok || amount.Sign() <= 0 {
		return fmt.Errorf("across approval amount must be a positive integer")
	}
	if r.Slippage != "" && r.Slippage != "auto" {
		value, err := strconv.ParseFloat(r.Slippage, 64)
		if err != nil || value < 0 || value > 1 {
			return fmt.Errorf("across slippage must be auto or a decimal between 0 and 1")
		}
	}
	return nil
}

type Transaction struct {
	SimulationSuccess *bool
	SimulationError   string
	ChainID           uint64
	To                string
	Data              string
	Value             string
	Gas               string
	MaxFeePerGas      string
	MaxPriorityFee    string
	Serialized        string
	Raw               json.RawMessage
}

func (t Transaction) FieldNames() []string {
	var fields map[string]json.RawMessage
	if json.Unmarshal(t.Raw, &fields) != nil {
		return nil
	}
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

type Approval struct {
	ID                   string
	CrossSwapType        string
	AmountType           string
	InputAmount          string
	MaxInputAmount       string
	ExpectedOutputAmount string
	MinimumOutputAmount  string
	ExpectedFillTime     time.Duration
	ExpiresAt            time.Time
	BridgeProvider       string
	Allowance            AllowanceCheck
	Balance              BalanceCheck
	ApprovalTransactions []Transaction
	SwapTransaction      Transaction
	ResponseHash         string
	ObservedAt           time.Time
}

type approvalEnvelope struct {
	ID                   string            `json:"id"`
	CrossSwapType        string            `json:"crossSwapType"`
	AmountType           string            `json:"amountType"`
	InputAmount          string            `json:"inputAmount"`
	MaxInputAmount       string            `json:"maxInputAmount"`
	ExpectedOutputAmount string            `json:"expectedOutputAmount"`
	MinimumOutputAmount  string            `json:"minOutputAmount"`
	ExpectedFillTime     json.Number       `json:"expectedFillTime"`
	QuoteExpiryTimestamp json.Number       `json:"quoteExpiryTimestamp"`
	ApprovalTransactions []json.RawMessage `json:"approvalTxns"`
	SwapTransaction      json.RawMessage   `json:"swapTx"`
	Steps                approvalSteps     `json:"steps"`
	Checks               struct {
		Allowance AllowanceCheck `json:"allowance"`
		Balance   BalanceCheck   `json:"balance"`
	} `json:"checks"`
}

type AllowanceCheck struct {
	Token    string `json:"token"`
	Spender  string `json:"spender"`
	Actual   string `json:"actual"`
	Expected string `json:"expected"`
}

type BalanceCheck struct {
	Token    string `json:"token"`
	Actual   string `json:"actual"`
	Expected string `json:"expected"`
}

type approvalSteps struct {
	Bridge struct {
		Provider string `json:"provider"`
	} `json:"bridge"`
}

func (c *Client) Approval(ctx context.Context, input ApprovalRequest) (Approval, error) {
	if err := input.validate(); err != nil {
		return Approval{}, err
	}
	endpoint := *c.baseURL
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/swap/approval"
	query := endpoint.Query()
	query.Set("tradeType", "exactInput")
	query.Set("strictTradeType", "true")
	query.Set("amount", input.Amount)
	query.Set("inputToken", input.InputToken)
	query.Set("outputToken", input.OutputToken)
	query.Set("originChainId", strconv.FormatUint(input.OriginChainID, 10))
	query.Set("destinationChainId", strconv.FormatUint(input.DestinationChainID, 10))
	query.Set("depositor", input.Depositor)
	query.Set("recipient", input.Recipient)
	query.Set("integratorId", c.integratorID)
	if input.RefundAddress != "" {
		query.Set("refundAddress", input.RefundAddress)
	}
	query.Set("refundOnOrigin", "true")
	if input.Slippage != "" {
		query.Set("slippage", input.Slippage)
	}
	endpoint.RawQuery = query.Encode()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return Approval{}, err
	}
	request.Header.Set("Authorization", "Bearer "+c.apiKey)
	request.Header.Set("Accept", "application/json")
	response, err := c.client.Do(request)
	if err != nil {
		return Approval{}, fmt.Errorf("request Across approval: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if err != nil {
		return Approval{}, fmt.Errorf("read Across approval: %w", err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return Approval{}, decodeAPIError(response.StatusCode, body)
	}
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.UseNumber()
	var envelope approvalEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return Approval{}, fmt.Errorf("decode Across approval: %w", err)
	}
	approval, err := decodeApproval(envelope, body, c.clock().UTC())
	if err != nil {
		return Approval{}, err
	}
	if err := approval.Validate(input, c.clock().UTC()); err != nil {
		return Approval{}, err
	}
	return approval, nil
}

func decodeApproval(envelope approvalEnvelope, body []byte, observedAt time.Time) (Approval, error) {
	expires, err := jsonNumberInt64(envelope.QuoteExpiryTimestamp)
	if err != nil {
		return Approval{}, fmt.Errorf("across quote expiry is invalid: %w", err)
	}
	fillSeconds, err := jsonNumberInt64(envelope.ExpectedFillTime)
	if err != nil || fillSeconds < 0 {
		return Approval{}, fmt.Errorf("across expected fill time is invalid")
	}
	swap, err := decodeTransaction(envelope.SwapTransaction)
	if err != nil {
		return Approval{}, fmt.Errorf("decode Across swap transaction: %w", err)
	}
	approvals := make([]Transaction, 0, len(envelope.ApprovalTransactions))
	for _, raw := range envelope.ApprovalTransactions {
		transaction, decodeErr := decodeTransaction(raw)
		if decodeErr != nil {
			return Approval{}, fmt.Errorf("decode Across approval transaction: %w", decodeErr)
		}
		approvals = append(approvals, transaction)
	}
	hash := sha256.Sum256(body)
	expiry := time.Time{}
	if expires > 0 {
		expiry = time.Unix(expires, 0).UTC()
	}
	return Approval{
		ID: envelope.ID, CrossSwapType: envelope.CrossSwapType, AmountType: envelope.AmountType,
		InputAmount: envelope.InputAmount, MaxInputAmount: envelope.MaxInputAmount,
		ExpectedOutputAmount: envelope.ExpectedOutputAmount,
		MinimumOutputAmount:  envelope.MinimumOutputAmount,
		ExpectedFillTime:     time.Duration(fillSeconds) * time.Second,
		ExpiresAt:            expiry, BridgeProvider: envelope.Steps.Bridge.Provider,
		Allowance:            envelope.Checks.Allowance,
		Balance:              envelope.Checks.Balance,
		ApprovalTransactions: approvals, SwapTransaction: swap,
		ResponseHash: hex.EncodeToString(hash[:]), ObservedAt: observedAt,
	}, nil
}

func decodeTransaction(raw json.RawMessage) (Transaction, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return Transaction{}, fmt.Errorf("transaction is missing")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return Transaction{}, err
	}
	var result Transaction
	result.Raw = append(json.RawMessage(nil), raw...)
	decodeOptional(fields, "simulationSuccess", &result.SimulationSuccess)
	for _, key := range []string{"simulationError", "simulationErrorReason", "error"} {
		if result.SimulationError == "" {
			decodeOptional(fields, key, &result.SimulationError)
		}
	}
	decodeOptionalUint64(fields, "chainId", &result.ChainID)
	decodeOptional(fields, "to", &result.To)
	decodeOptional(fields, "data", &result.Data)
	decodeOptional(fields, "value", &result.Value)
	decodeOptional(fields, "gas", &result.Gas)
	decodeOptional(fields, "maxFeePerGas", &result.MaxFeePerGas)
	decodeOptional(fields, "maxPriorityFeePerGas", &result.MaxPriorityFee)
	for _, key := range []string{"serializedTransaction", "transaction", "tx"} {
		if result.Serialized == "" {
			decodeOptional(fields, key, &result.Serialized)
		}
	}
	return result, nil
}

func (a Approval) Validate(request ApprovalRequest, now time.Time) error {
	if a.ID == "" || a.AmountType != "exactInput" {
		return fmt.Errorf("across approval identity or amount type is invalid")
	}
	if a.InputAmount != request.Amount {
		return fmt.Errorf(
			"across approval input amount %q does not match requested amount %q",
			a.InputAmount, request.Amount,
		)
	}
	if !a.ExpiresAt.IsZero() && !a.ExpiresAt.After(now) {
		return fmt.Errorf(
			"across approval expired at %s before validation at %s",
			a.ExpiresAt.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano),
		)
	}
	expected, expectedOK := positiveInteger(a.ExpectedOutputAmount)
	minimum, minimumOK := positiveInteger(a.MinimumOutputAmount)
	if !expectedOK || !minimumOK || minimum.Cmp(expected) > 0 {
		return fmt.Errorf("across approval output amounts are invalid")
	}
	if !request.CostOnly && a.Balance.Actual != "" && a.Balance.Expected != "" {
		actualBalance, actualOK := new(big.Int).SetString(a.Balance.Actual, 10)
		expectedBalance, expectedOK := new(big.Int).SetString(a.Balance.Expected, 10)
		if !actualOK || !expectedOK || actualBalance.Sign() < 0 || expectedBalance.Sign() <= 0 {
			return fmt.Errorf("across balance check is invalid")
		}
		if actualBalance.Cmp(expectedBalance) < 0 {
			return fmt.Errorf(
				"across source balance is insufficient: actual=%s expected=%s",
				actualBalance, expectedBalance,
			)
		}
	}
	if !request.CostOnly &&
		a.SwapTransaction.SimulationSuccess != nil &&
		!*a.SwapTransaction.SimulationSuccess {
		if a.SwapTransaction.SimulationError != "" {
			return fmt.Errorf("across source transaction simulation failed: %s", a.SwapTransaction.SimulationError)
		}
		if request.OriginChainID != SolanaChainID {
			return fmt.Errorf("across source transaction simulation failed without a reason")
		}
	}
	if a.SwapTransaction.ChainID != request.OriginChainID {
		return fmt.Errorf("across source transaction targets chain %d, expected %d", a.SwapTransaction.ChainID, request.OriginChainID)
	}
	if request.OriginChainID == SolanaChainID {
		if a.SwapTransaction.Serialized == "" && a.SwapTransaction.Data == "" {
			return fmt.Errorf("across Solana source transaction has no executable payload")
		}
	} else if !isHexAddress(a.SwapTransaction.To) || !isHexBytes(a.SwapTransaction.Data) {
		return fmt.Errorf("across EVM source transaction envelope is invalid")
	}
	return nil
}

type DepositStatus string

const (
	DepositPending  DepositStatus = "pending"
	DepositReceived DepositStatus = "received"
	DepositFilled   DepositStatus = "filled"
	DepositExpired  DepositStatus = "expired"
	DepositRefunded DepositStatus = "refunded"
)

type Status struct {
	State             DepositStatus
	OriginTransaction string
	FillTransaction   string
	Raw               json.RawMessage
	ObservedAt        time.Time
}

func (c *Client) DepositStatus(ctx context.Context, transactionReference string) (Status, error) {
	if strings.TrimSpace(transactionReference) == "" {
		return Status{}, fmt.Errorf("across deposit transaction reference is required")
	}
	endpoint := *c.baseURL
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/deposit/status"
	query := endpoint.Query()
	query.Set("depositTxnRef", transactionReference)
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return Status{}, err
	}
	request.Header.Set("Authorization", "Bearer "+c.apiKey)
	request.Header.Set("Accept", "application/json")
	response, err := c.client.Do(request)
	if err != nil {
		return Status{}, fmt.Errorf("request Across deposit status: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return Status{}, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return Status{}, decodeAPIError(response.StatusCode, body)
	}
	var envelope struct {
		Status            DepositStatus `json:"status"`
		DepositTxnRef     string        `json:"depositTxnRef"`
		FillTxnRef        string        `json:"fillTxnRef"`
		DestinationTxnRef string        `json:"destinationTxnRef"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return Status{}, fmt.Errorf("decode Across deposit status: %w", err)
	}
	switch envelope.Status {
	case DepositPending, DepositReceived, DepositFilled, DepositExpired, DepositRefunded:
	default:
		return Status{}, fmt.Errorf("across returned unknown deposit status %q", envelope.Status)
	}
	fill := envelope.FillTxnRef
	if fill == "" {
		fill = envelope.DestinationTxnRef
	}
	return Status{
		State: envelope.Status, OriginTransaction: envelope.DepositTxnRef,
		FillTransaction: fill, Raw: append(json.RawMessage(nil), body...),
		ObservedAt: c.clock().UTC(),
	}, nil
}

type APIError struct {
	HTTPStatus int
	Code       string
	Message    string
	RequestID  string
}

func (e *APIError) Error() string {
	if e.Code == "" {
		return fmt.Sprintf("Across HTTP %d: %s", e.HTTPStatus, e.Message)
	}
	return fmt.Sprintf("Across %s HTTP %d: %s", e.Code, e.HTTPStatus, e.Message)
}

func decodeAPIError(status int, body []byte) error {
	var envelope struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		ID      string `json:"id"`
	}
	_ = json.Unmarshal(body, &envelope)
	if envelope.Message == "" {
		envelope.Message = strings.TrimSpace(string(body))
	}
	if envelope.Message == "" {
		envelope.Message = http.StatusText(status)
	}
	return &APIError{HTTPStatus: status, Code: envelope.Code, Message: envelope.Message, RequestID: envelope.ID}
}

func jsonNumberInt64(number json.Number) (int64, error) {
	if number == "" {
		return 0, errors.New("missing number")
	}
	return number.Int64()
}

func positiveInteger(text string) (*big.Int, bool) {
	value, ok := new(big.Int).SetString(text, 10)
	return value, ok && value.Sign() > 0
}

func isHexAddress(value string) bool {
	if len(value) != 42 || !strings.HasPrefix(value, "0x") {
		return false
	}
	_, err := hex.DecodeString(value[2:])
	return err == nil
}

func isHexBytes(value string) bool {
	if len(value) < 4 || !strings.HasPrefix(value, "0x") || len(value)%2 != 0 {
		return false
	}
	_, err := hex.DecodeString(value[2:])
	return err == nil
}

func decodeOptional(fields map[string]json.RawMessage, key string, target any) {
	if raw, ok := fields[key]; ok {
		_ = json.Unmarshal(raw, target)
	}
}

func decodeOptionalUint64(fields map[string]json.RawMessage, key string, target *uint64) {
	raw, ok := fields[key]
	if !ok {
		return
	}
	var number json.Number
	if err := json.Unmarshal(raw, &number); err == nil {
		*target, _ = strconv.ParseUint(number.String(), 10, 64)
	}
}
