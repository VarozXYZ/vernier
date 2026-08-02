// Package solana contains the read-only JSON-RPC and WebSocket capabilities
// shared by Solana feeds and market adapters. It deliberately has no signer
// or transaction submission surface.
package solana

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	chainport "github.com/VarozXYZ/vernier/ports/chain"
	"github.com/gorilla/websocket"
)

const (
	DefaultWebSocketPingInterval = 30 * time.Second
	DefaultWebSocketPongTimeout  = 90 * time.Second
	DefaultWebSocketWriteTimeout = 10 * time.Second
)

// WebSocketKeepalive configures transport-level ping/pong health checks for
// every Solana subscription connection. PongTimeout must exceed PingInterval
// so a healthy peer has time to acknowledge a ping before the read fails.
type WebSocketKeepalive struct {
	PingInterval time.Duration
	PongTimeout  time.Duration
	WriteTimeout time.Duration
}

func DefaultWebSocketKeepalive() WebSocketKeepalive {
	return WebSocketKeepalive{
		PingInterval: DefaultWebSocketPingInterval,
		PongTimeout:  DefaultWebSocketPongTimeout,
		WriteTimeout: DefaultWebSocketWriteTimeout,
	}
}

func (c WebSocketKeepalive) validate() error {
	if c.PingInterval <= 0 {
		return fmt.Errorf("WebSocket ping interval must be positive")
	}
	if c.PongTimeout <= c.PingInterval {
		return fmt.Errorf("WebSocket pong timeout must exceed ping interval")
	}
	if c.WriteTimeout <= 0 {
		return fmt.Errorf("WebSocket write timeout must be positive")
	}
	return nil
}

type ReadOnlyNetwork struct {
	id           string
	label        string
	httpURL      string
	websocketURL string
	httpClient   *http.Client
	dialer       *websocket.Dialer
	keepalive    WebSocketKeepalive
	requestID    atomic.Uint64
}

func DialReadOnlyNetwork(ctx context.Context, id, label, httpURL, websocketURL string) (*ReadOnlyNetwork, error) {
	network, err := NewReadOnlyNetwork(id, label, httpURL, websocketURL, nil, nil)
	if err != nil {
		return nil, err
	}
	if err := network.Validate(ctx); err != nil {
		return nil, err
	}
	return network, nil
}

func NewReadOnlyNetwork(id, label, httpURL, websocketURL string, httpClient *http.Client, dialer *websocket.Dialer) (*ReadOnlyNetwork, error) {
	return NewReadOnlyNetworkWithKeepalive(
		id,
		label,
		httpURL,
		websocketURL,
		httpClient,
		dialer,
		DefaultWebSocketKeepalive(),
	)
}

// NewReadOnlyNetworkWithKeepalive constructs a network with an explicit
// transport keepalive policy. Production callers normally use
// NewReadOnlyNetwork and its Helius-compatible defaults.
func NewReadOnlyNetworkWithKeepalive(
	id, label, httpURL, websocketURL string,
	httpClient *http.Client,
	dialer *websocket.Dialer,
	keepalive WebSocketKeepalive,
) (*ReadOnlyNetwork, error) {
	if id == "" || label == "" {
		return nil, fmt.Errorf("network id and label are required")
	}
	if err := validateEndpoint(httpURL, "HTTP"); err != nil {
		return nil, err
	}
	if err := validateEndpoint(websocketURL, "WebSocket"); err != nil {
		return nil, err
	}
	if err := keepalive.validate(); err != nil {
		return nil, err
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	if dialer == nil {
		dialer = websocket.DefaultDialer
	}
	return &ReadOnlyNetwork{
		id: id, label: label, httpURL: httpURL, websocketURL: websocketURL,
		httpClient: httpClient, dialer: dialer, keepalive: keepalive,
	}, nil
}

func validateEndpoint(raw, kind string) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return fmt.Errorf("invalid %s endpoint", kind)
	}
	if kind == "HTTP" && parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("HTTP endpoint must use http or https")
	}
	if kind == "WebSocket" && parsed.Scheme != "ws" && parsed.Scheme != "wss" {
		return fmt.Errorf("WebSocket endpoint must use ws or wss")
	}
	return nil
}

func (n *ReadOnlyNetwork) ID() string    { return n.id }
func (n *ReadOnlyNetwork) Label() string { return n.label }

func (n *ReadOnlyNetwork) Validate(ctx context.Context) error {
	var health string
	if err := n.callHTTP(ctx, "getHealth", nil, &health); err != nil {
		return fmt.Errorf("validate %s HTTP endpoint: %w", n.label, err)
	}
	if health != "ok" {
		return fmt.Errorf("validate %s HTTP endpoint: health %q", n.label, health)
	}
	if _, err := n.CurrentSlot(ctx); err != nil {
		return fmt.Errorf("validate %s RPC endpoint: %w", n.label, err)
	}
	return nil
}

func (n *ReadOnlyNetwork) CurrentSlot(ctx context.Context) (uint64, error) {
	var slot uint64
	if err := n.callHTTP(ctx, "getSlot", []any{map[string]string{"commitment": "processed"}}, &slot); err != nil {
		return 0, fmt.Errorf("read %s current slot: %w", n.label, err)
	}
	return slot, nil
}

func (n *ReadOnlyNetwork) NativeBalance(
	ctx context.Context,
	address string,
) (*big.Int, error) {
	if strings.TrimSpace(address) == "" {
		return nil, fmt.Errorf("native balance address is required")
	}
	var result struct {
		Value uint64 `json:"value"`
	}
	if err := n.callHTTP(
		ctx,
		"getBalance",
		[]any{
			address,
			map[string]string{"commitment": "confirmed"},
		},
		&result,
	); err != nil {
		return nil, fmt.Errorf("read %s native balance: %w", n.label, err)
	}
	return new(big.Int).SetUint64(result.Value), nil
}

func (n *ReadOnlyNetwork) FeeForMessage(
	ctx context.Context,
	messageBase64 string,
) (uint64, error) {
	if strings.TrimSpace(messageBase64) == "" {
		return 0, fmt.Errorf("read %s message fee: message is empty", n.label)
	}
	var result struct {
		Value *uint64 `json:"value"`
	}
	if err := n.callHTTP(
		ctx,
		"getFeeForMessage",
		[]any{
			messageBase64,
			map[string]string{"commitment": "confirmed"},
		},
		&result,
	); err != nil {
		return 0, fmt.Errorf("read %s message fee: %w", n.label, err)
	}
	if result.Value == nil {
		return 0, fmt.Errorf("read %s message fee: blockhash is no longer valid", n.label)
	}
	return *result.Value, nil
}

// SimulateSignedTransaction executes an exact signed transaction against the
// node's current confirmed state without broadcasting it.
func (n *ReadOnlyNetwork) SimulateSignedTransaction(
	ctx context.Context,
	raw []byte,
) error {
	return n.simulateTransaction(ctx, raw, true)
}

// SimulateSignedTransactionEconomic returns the post-simulation SPL account
// amount as part of the same RPC request. The caller supplies its versioned
// local pre-balance, avoiding a separate hot-path account read.
func (n *ReadOnlyNetwork) SimulateSignedTransactionEconomic(
	ctx context.Context,
	raw []byte,
	outputAccount string,
) (*big.Int, uint64, uint64, error) {
	if len(raw) == 0 || strings.TrimSpace(outputAccount) == "" {
		return nil, 0, 0, fmt.Errorf("simulate %s economic transaction: input is incomplete", n.label)
	}
	var result struct {
		Context struct {
			Slot uint64 `json:"slot"`
		} `json:"context"`
		Value *struct {
			Err           json.RawMessage `json:"err"`
			Logs          []string        `json:"logs"`
			UnitsConsumed *uint64         `json:"unitsConsumed"`
			Accounts      []*struct {
				Data []json.RawMessage `json:"data"`
			} `json:"accounts"`
		} `json:"value"`
	}
	if err := n.callHTTP(ctx, "simulateTransaction", []any{
		base64.StdEncoding.EncodeToString(raw),
		map[string]any{
			"encoding": "base64", "sigVerify": true,
			"commitment": "confirmed",
			"accounts": map[string]any{
				"encoding": "base64", "addresses": []string{outputAccount},
			},
		},
	}, &result); err != nil {
		return nil, 0, 0, fmt.Errorf("simulate %s economic transaction: %w", n.label, err)
	}
	if result.Value == nil {
		return nil, 0, result.Context.Slot, fmt.Errorf("simulate %s economic transaction: response has no value", n.label)
	}
	if len(result.Value.Err) > 0 && string(result.Value.Err) != "null" {
		logs := result.Value.Logs
		if len(logs) > 8 {
			logs = logs[len(logs)-8:]
		}
		return nil, 0, result.Context.Slot, fmt.Errorf(
			"simulate %s economic transaction failed: %s logs=%v",
			n.label, string(result.Value.Err), logs,
		)
	}
	if len(result.Value.Accounts) != 1 || result.Value.Accounts[0] == nil ||
		len(result.Value.Accounts[0].Data) == 0 {
		return nil, 0, result.Context.Slot, &chainport.EconomicOutputError{Err: fmt.Errorf("simulate %s economic transaction returned no output account", n.label)}
	}
	var encoded string
	if err := json.Unmarshal(result.Value.Accounts[0].Data[0], &encoded); err != nil {
		return nil, 0, result.Context.Slot, &chainport.EconomicOutputError{Err: fmt.Errorf("decode %s simulated output account: %w", n.label, err)}
	}
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(data) < 72 {
		return nil, 0, result.Context.Slot, &chainport.EconomicOutputError{Err: fmt.Errorf("decode %s simulated SPL balance", n.label)}
	}
	consumed := uint64(0)
	if result.Value.UnitsConsumed != nil {
		consumed = *result.Value.UnitsConsumed
	}
	return new(big.Int).SetUint64(binary.LittleEndian.Uint64(data[64:72])), consumed, result.Context.Slot, nil
}

// SimulateTransactionWithoutSignatureVerification is restricted to
// non-broadcastable preflight transactions whose account metas are exact but
// whose reference token owner is not controlled by this process.
func (n *ReadOnlyNetwork) SimulateTransactionWithoutSignatureVerification(
	ctx context.Context,
	raw []byte,
) error {
	return n.simulateTransaction(ctx, raw, false)
}

func (n *ReadOnlyNetwork) simulateTransaction(
	ctx context.Context,
	raw []byte,
	verifySignatures bool,
) error {
	if len(raw) == 0 {
		return fmt.Errorf("simulate %s transaction: payload is empty", n.label)
	}
	var result struct {
		Value *struct {
			Err           json.RawMessage `json:"err"`
			Logs          []string        `json:"logs"`
			UnitsConsumed *uint64         `json:"unitsConsumed"`
		} `json:"value"`
	}
	if err := n.callHTTP(
		ctx,
		"simulateTransaction",
		[]any{
			base64.StdEncoding.EncodeToString(raw),
			map[string]any{
				"encoding":   "base64",
				"sigVerify":  verifySignatures,
				"commitment": "confirmed",
			},
		},
		&result,
	); err != nil {
		return fmt.Errorf("simulate %s transaction: %w", n.label, err)
	}
	if result.Value == nil {
		return fmt.Errorf("simulate %s transaction: response has no value", n.label)
	}
	if len(result.Value.Err) > 0 &&
		string(result.Value.Err) != "null" {
		logs := result.Value.Logs
		if len(logs) > 8 {
			logs = logs[len(logs)-8:]
		}
		return fmt.Errorf(
			"simulate %s transaction failed: %s logs=%v",
			n.label,
			string(result.Value.Err),
			logs,
		)
	}
	return nil
}

type Account struct {
	Lamports   uint64
	Owner      string
	Executable bool
	RentEpoch  uint64
	Data       []byte
}

// AccountNotification contains the complete account value delivered by an
// accountSubscribe WebSocket notification. The slot is the only ordering
// evidence supplied by the node.
type AccountNotification struct {
	Slot    uint64
	Account string
	Value   Account
}

// ProgramFilter is the small subset of Solana program filters needed by
// protocol adapters. A filter is either a data-size constraint or a byte
// comparison at an account-data offset.
type ProgramFilter struct {
	DataSize *uint64
	Memcmp   *ProgramMemcmp
}

type ProgramMemcmp struct {
	Offset uint64
	Bytes  string
}

type ProgramAccount struct {
	Account string
	Value   Account
}

type ProgramSubscriptionRequest struct {
	Program string
	Filters []ProgramFilter
}

type ProgramNotification struct {
	Slot    uint64
	Account string
	Value   Account
}

func (n *ReadOnlyNetwork) ReadAccount(ctx context.Context, address string) (Account, error) {
	accounts, err := n.ReadMultipleAccounts(ctx, []string{address})
	if err != nil {
		return Account{}, err
	}
	if len(accounts) != 1 {
		return Account{}, fmt.Errorf("account response length mismatch")
	}
	return accounts[0], nil
}

func (n *ReadOnlyNetwork) ReadMultipleAccounts(ctx context.Context, addresses []string) ([]Account, error) {
	if len(addresses) == 0 {
		return nil, fmt.Errorf("at least one account is required")
	}
	var result struct {
		Value []*jsonAccountValue `json:"value"`
	}
	params := []any{addresses, map[string]any{"encoding": "base64", "commitment": "processed"}}
	if err := n.callHTTP(ctx, "getMultipleAccounts", params, &result); err != nil {
		return nil, fmt.Errorf("read %s accounts: %w", n.label, err)
	}
	accounts := make([]Account, len(result.Value))
	for i, value := range result.Value {
		if value == nil {
			continue
		}
		account, err := value.account()
		if err != nil {
			return nil, fmt.Errorf("decode account %d: %w", i, err)
		}
		accounts[i] = account
	}
	return accounts, nil
}

func (n *ReadOnlyNetwork) ReadProgramAccounts(ctx context.Context, program string, filters []ProgramFilter) ([]ProgramAccount, error) {
	if strings.TrimSpace(program) == "" {
		return nil, fmt.Errorf("program is required")
	}
	config := map[string]any{"encoding": "base64", "commitment": "processed"}
	if len(filters) > 0 {
		encoded := make([]map[string]any, 0, len(filters))
		for _, filter := range filters {
			switch {
			case filter.DataSize != nil && filter.Memcmp == nil:
				encoded = append(encoded, map[string]any{"dataSize": *filter.DataSize})
			case filter.DataSize == nil && filter.Memcmp != nil && filter.Memcmp.Bytes != "":
				encoded = append(encoded, map[string]any{"memcmp": map[string]any{"offset": filter.Memcmp.Offset, "bytes": filter.Memcmp.Bytes}})
			default:
				return nil, fmt.Errorf("invalid program filter")
			}
		}
		config["filters"] = encoded
	}
	var result []struct {
		Pubkey  string            `json:"pubkey"`
		Account *jsonAccountValue `json:"account"`
	}
	if err := n.callHTTP(ctx, "getProgramAccounts", []any{program, config}, &result); err != nil {
		return nil, fmt.Errorf("read %s program accounts: %w", n.label, err)
	}
	accounts := make([]ProgramAccount, 0, len(result))
	for _, item := range result {
		if item.Account == nil {
			continue
		}
		account, err := item.Account.account()
		if err != nil {
			return nil, fmt.Errorf("decode program account %s: %w", item.Pubkey, err)
		}
		accounts = append(accounts, ProgramAccount{Account: item.Pubkey, Value: account})
	}
	return accounts, nil
}

type Transaction struct {
	Signature   string
	Slot        uint64
	BlockTime   *int64
	Transaction json.RawMessage
	Meta        json.RawMessage
}

type SignatureStatus struct {
	Found              bool
	Slot               uint64
	ConfirmationStatus string
	Err                json.RawMessage
}

// ReadSignatureStatus is a reconciliation-only RPC fallback. It is never
// called while preparing or broadcasting a Live transaction.
func (n *ReadOnlyNetwork) ReadSignatureStatus(ctx context.Context, signature string) (SignatureStatus, error) {
	if strings.TrimSpace(signature) == "" {
		return SignatureStatus{}, fmt.Errorf("transaction signature is required")
	}
	var result struct {
		Value []*struct {
			Slot               uint64          `json:"slot"`
			Err                json.RawMessage `json:"err"`
			ConfirmationStatus string          `json:"confirmationStatus"`
		} `json:"value"`
	}
	if err := n.callHTTP(ctx, "getSignatureStatuses", []any{
		[]string{signature}, map[string]bool{"searchTransactionHistory": true},
	}, &result); err != nil {
		return SignatureStatus{}, fmt.Errorf("read %s signature status: %w", n.label, err)
	}
	if len(result.Value) != 1 || result.Value[0] == nil {
		return SignatureStatus{}, nil
	}
	value := result.Value[0]
	return SignatureStatus{
		Found: true, Slot: value.Slot, ConfirmationStatus: value.ConfirmationStatus,
		Err: append(json.RawMessage(nil), value.Err...),
	}, nil
}

func (n *ReadOnlyNetwork) CurrentBlockHeight(ctx context.Context) (uint64, error) {
	var height uint64
	if err := n.callHTTP(ctx, "getBlockHeight", []any{map[string]string{"commitment": "confirmed"}}, &height); err != nil {
		return 0, fmt.Errorf("read %s current block height: %w", n.label, err)
	}
	return height, nil
}

func (n *ReadOnlyNetwork) IsBlockhashValid(ctx context.Context, blockhash string) (bool, error) {
	if strings.TrimSpace(blockhash) == "" {
		return false, fmt.Errorf("blockhash is required")
	}
	var result struct {
		Value bool `json:"value"`
	}
	if err := n.callHTTP(ctx, "isBlockhashValid", []any{
		blockhash, map[string]string{"commitment": "confirmed"},
	}, &result); err != nil {
		return false, fmt.Errorf("read %s blockhash validity: %w", n.label, err)
	}
	return result.Value, nil
}

func (n *ReadOnlyNetwork) ReadTransaction(ctx context.Context, signature string) (Transaction, error) {
	var result *jsonTransaction
	if err := n.callHTTP(ctx, "getTransaction", []any{signature, map[string]any{"encoding": "jsonParsed", "commitment": "confirmed", "maxSupportedTransactionVersion": 0}}, &result); err != nil {
		return Transaction{}, fmt.Errorf("read %s transaction: %w", n.label, err)
	}
	if result == nil {
		return Transaction{}, fmt.Errorf("transaction %s was not found", signature)
	}
	return Transaction{
		Signature: signature, Slot: result.Slot, BlockTime: result.BlockTime,
		Transaction: result.Transaction, Meta: result.Meta,
	}, nil
}

// ConfirmedPayerDebit returns the source account's actual lamport decrease.
// For bridge calibration this includes signature/priority fees, rent and any
// explicit native debit made by the transaction.
func (n *ReadOnlyNetwork) ConfirmedPayerDebit(
	ctx context.Context,
	signature string,
) (uint64, error) {
	transaction, err := n.ReadTransaction(ctx, signature)
	if err != nil {
		return 0, err
	}
	var metadata struct {
		Err          json.RawMessage `json:"err"`
		PreBalances  []uint64        `json:"preBalances"`
		PostBalances []uint64        `json:"postBalances"`
	}
	if err := json.Unmarshal(transaction.Meta, &metadata); err != nil {
		return 0, fmt.Errorf("decode %s transaction balances: %w", n.label, err)
	}
	if len(metadata.Err) > 0 && string(metadata.Err) != "null" {
		return 0, fmt.Errorf("solana calibration transaction failed")
	}
	if len(metadata.PreBalances) == 0 ||
		len(metadata.PreBalances) != len(metadata.PostBalances) {
		return 0, fmt.Errorf("solana calibration transaction has no payer balances")
	}
	if metadata.PostBalances[0] >= metadata.PreBalances[0] {
		return 0, nil
	}
	return metadata.PreBalances[0] - metadata.PostBalances[0], nil
}

type jsonAccountValue struct {
	Lamports   uint64          `json:"lamports"`
	Owner      string          `json:"owner"`
	Executable bool            `json:"executable"`
	RentEpoch  uint64          `json:"rentEpoch"`
	Data       json.RawMessage `json:"data"`
}

func (a *jsonAccountValue) account() (Account, error) {
	if a == nil {
		return Account{}, nil
	}
	var encoded []any
	if err := json.Unmarshal(a.Data, &encoded); err != nil || len(encoded) == 0 {
		return Account{}, fmt.Errorf("account data is not base64 tuple")
	}
	text, ok := encoded[0].(string)
	if !ok {
		return Account{}, fmt.Errorf("account data encoding is not a string")
	}
	data, err := base64.StdEncoding.DecodeString(text)
	if err != nil {
		return Account{}, fmt.Errorf("decode account data: %w", err)
	}
	return Account{Lamports: a.Lamports, Owner: a.Owner, Executable: a.Executable, RentEpoch: a.RentEpoch, Data: data}, nil
}

type jsonTransaction struct {
	Slot        uint64          `json:"slot"`
	BlockTime   *int64          `json:"blockTime"`
	Transaction json.RawMessage `json:"transaction"`
	Meta        json.RawMessage `json:"meta"`
}

func (n *ReadOnlyNetwork) callHTTP(ctx context.Context, method string, params any, result any) error {
	id := n.requestID.Add(1)
	body, err := json.Marshal(rpcRequest{JSONRPC: "2.0", ID: id, Method: method, Params: params})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.httpURL, strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := n.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("HTTP status %s", response.Status)
	}
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	var envelope rpcResponse
	if err := json.Unmarshal(data, &envelope); err != nil {
		return err
	}
	if envelope.Error != nil {
		return envelope.Error
	}
	if result == nil {
		return nil
	}
	return json.Unmarshal(envelope.Result, result)
}

type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      uint64 `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type rpcResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *RPCError       `json:"error"`
}

type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *RPCError) Error() string { return fmt.Sprintf("RPC error %d: %s", e.Code, e.Message) }

func (n *ReadOnlyNetwork) Close() {}

type LogNotification struct {
	Slot      uint64
	Signature string
	Err       json.RawMessage
	Logs      []string
}

type LogsSubscription interface {
	Err() <-chan error
	Notifications() <-chan LogNotification
	Unsubscribe()
}

type AccountSubscription interface {
	Err() <-chan error
	Notifications() <-chan AccountNotification
	Unsubscribe()
}

type ProgramSubscription interface {
	Err() <-chan error
	Notifications() <-chan ProgramNotification
	Unsubscribe()
}

type TransactionNotification struct {
	Signature   string
	Slot        uint64
	Transaction json.RawMessage
	Meta        json.RawMessage
}

type TransactionSubscription interface {
	Err() <-chan error
	Notifications() <-chan TransactionNotification
	Unsubscribe()
}

func startWebSocketKeepalive(conn *websocket.Conn, done <-chan struct{}, config WebSocketKeepalive) {
	_ = conn.SetReadDeadline(time.Now().Add(config.PongTimeout))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(config.PongTimeout))
	})
	go func() {
		ticker := time.NewTicker(config.PingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case tick := <-ticker.C:
				if err := conn.WriteControl(
					websocket.PingMessage,
					nil,
					tick.Add(config.WriteTimeout),
				); err != nil {
					// Closing the connection unblocks the sole reader, which
					// publishes the terminal error through the subscription.
					_ = conn.Close()
					return
				}
			}
		}
	}()
}

func (n *ReadOnlyNetwork) SubscribeLogs(ctx context.Context, pool string) (LogsSubscription, error) {
	if strings.TrimSpace(pool) == "" {
		return nil, fmt.Errorf("pool account is required")
	}
	conn, _, err := n.dialer.DialContext(ctx, n.websocketURL, nil)
	if err != nil {
		return nil, fmt.Errorf("dial %s WebSocket endpoint: %w", n.label, err)
	}
	id := n.requestID.Add(1)
	request := rpcRequest{JSONRPC: "2.0", ID: id, Method: "logsSubscribe", Params: []any{map[string]any{"mentions": []string{pool}}, map[string]string{"commitment": "processed"}}}
	if err := conn.WriteJSON(request); err != nil {
		conn.Close()
		return nil, err
	}
	var response struct {
		ID     uint64    `json:"id"`
		Result uint64    `json:"result"`
		Error  *RPCError `json:"error"`
	}
	if err := conn.ReadJSON(&response); err != nil {
		conn.Close()
		return nil, err
	}
	if response.Error != nil {
		conn.Close()
		return nil, response.Error
	}
	if response.ID != id || response.Result == 0 {
		conn.Close()
		return nil, fmt.Errorf("invalid logsSubscribe response")
	}
	subscription := &logsSubscription{conn: conn, id: response.Result, errors: make(chan error, 1), notifications: make(chan LogNotification, 128), done: make(chan struct{})}
	startWebSocketKeepalive(conn, subscription.done, n.keepalive)
	go subscription.readLoop()
	return subscription, nil
}

func (n *ReadOnlyNetwork) SubscribeAccount(ctx context.Context, account string) (AccountSubscription, error) {
	if strings.TrimSpace(account) == "" {
		return nil, fmt.Errorf("account is required")
	}
	conn, _, err := n.dialer.DialContext(ctx, n.websocketURL, nil)
	if err != nil {
		return nil, fmt.Errorf("dial %s WebSocket endpoint: %w", n.label, err)
	}
	id := n.requestID.Add(1)
	request := rpcRequest{JSONRPC: "2.0", ID: id, Method: "accountSubscribe", Params: []any{account, map[string]string{"commitment": "processed", "encoding": "base64"}}}
	if err := conn.WriteJSON(request); err != nil {
		conn.Close()
		return nil, err
	}
	var response struct {
		ID     uint64    `json:"id"`
		Result uint64    `json:"result"`
		Error  *RPCError `json:"error"`
	}
	if err := conn.ReadJSON(&response); err != nil {
		conn.Close()
		return nil, err
	}
	if response.Error != nil {
		conn.Close()
		return nil, response.Error
	}
	if response.ID != id {
		conn.Close()
		return nil, fmt.Errorf("invalid accountSubscribe response")
	}
	subscription := &accountSubscription{conn: conn, account: account, id: response.Result, errors: make(chan error, 1), notifications: make(chan AccountNotification, 128), done: make(chan struct{})}
	startWebSocketKeepalive(conn, subscription.done, n.keepalive)
	go subscription.readLoop()
	return subscription, nil
}

func (n *ReadOnlyNetwork) SubscribeProgram(ctx context.Context, request ProgramSubscriptionRequest) (ProgramSubscription, error) {
	if strings.TrimSpace(request.Program) == "" {
		return nil, fmt.Errorf("program is required")
	}
	filters := make([]map[string]any, 0, len(request.Filters))
	for _, filter := range request.Filters {
		switch {
		case filter.DataSize != nil && filter.Memcmp == nil:
			filters = append(filters, map[string]any{"dataSize": *filter.DataSize})
		case filter.DataSize == nil && filter.Memcmp != nil && filter.Memcmp.Bytes != "":
			filters = append(filters, map[string]any{"memcmp": map[string]any{"offset": filter.Memcmp.Offset, "bytes": filter.Memcmp.Bytes}})
		default:
			return nil, fmt.Errorf("invalid program filter")
		}
	}
	config := map[string]any{"commitment": "processed", "encoding": "base64", "withContext": true}
	if len(filters) > 0 {
		config["filters"] = filters
	}
	conn, _, err := n.dialer.DialContext(ctx, n.websocketURL, nil)
	if err != nil {
		return nil, fmt.Errorf("dial %s WebSocket endpoint: %w", n.label, err)
	}
	id := n.requestID.Add(1)
	rpc := rpcRequest{JSONRPC: "2.0", ID: id, Method: "programSubscribe", Params: []any{request.Program, config}}
	if err := conn.WriteJSON(rpc); err != nil {
		conn.Close()
		return nil, err
	}
	var response struct {
		ID     uint64    `json:"id"`
		Result uint64    `json:"result"`
		Error  *RPCError `json:"error"`
	}
	if err := conn.ReadJSON(&response); err != nil {
		conn.Close()
		return nil, err
	}
	if response.Error != nil {
		conn.Close()
		return nil, response.Error
	}
	if response.ID != id || response.Result == 0 {
		conn.Close()
		return nil, fmt.Errorf("invalid programSubscribe response")
	}
	subscription := &programSubscription{conn: conn, id: response.Result, errors: make(chan error, 1), notifications: make(chan ProgramNotification, 128), done: make(chan struct{})}
	startWebSocketKeepalive(conn, subscription.done, n.keepalive)
	go subscription.readLoop()
	return subscription, nil
}

// SubscribeTransactions establishes the Helius transactionSubscribe stream
// at confirmed commitment before Live broadcast. Filtering by the signer
// account keeps one persistent subscription sufficient for all operations.
func (n *ReadOnlyNetwork) SubscribeTransactions(ctx context.Context, account string) (TransactionSubscription, error) {
	if strings.TrimSpace(account) == "" {
		return nil, fmt.Errorf("transaction subscription account is required")
	}
	conn, _, err := n.dialer.DialContext(ctx, n.websocketURL, nil)
	if err != nil {
		return nil, fmt.Errorf("dial %s WebSocket endpoint: %w", n.label, err)
	}
	id := n.requestID.Add(1)
	request := rpcRequest{
		JSONRPC: "2.0", ID: id, Method: "transactionSubscribe",
		Params: []any{
			map[string]any{
				"accountInclude": []string{account},
				"vote":           false,
			},
			map[string]any{
				"commitment":                     "confirmed",
				"encoding":                       "jsonParsed",
				"transactionDetails":             "full",
				"showRewards":                    false,
				"maxSupportedTransactionVersion": 0,
			},
		},
	}
	if err := conn.WriteJSON(request); err != nil {
		_ = conn.Close()
		return nil, err
	}
	var response struct {
		ID     uint64    `json:"id"`
		Result uint64    `json:"result"`
		Error  *RPCError `json:"error"`
	}
	if err := conn.ReadJSON(&response); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if response.Error != nil {
		_ = conn.Close()
		return nil, response.Error
	}
	if response.ID != id || response.Result == 0 {
		_ = conn.Close()
		return nil, fmt.Errorf("invalid transactionSubscribe response")
	}
	subscription := &transactionSubscription{
		conn: conn, id: response.Result, errors: make(chan error, 1),
		notifications: make(chan TransactionNotification, 16), done: make(chan struct{}),
	}
	startWebSocketKeepalive(conn, subscription.done, n.keepalive)
	go subscription.readLoop()
	return subscription, nil
}

type logsSubscription struct {
	mu            sync.Mutex
	conn          *websocket.Conn
	id            uint64
	errors        chan error
	notifications chan LogNotification
	done          chan struct{}
	once          sync.Once
}

func (s *logsSubscription) Err() <-chan error                     { return s.errors }
func (s *logsSubscription) Notifications() <-chan LogNotification { return s.notifications }

func (s *logsSubscription) Unsubscribe() {
	s.once.Do(func() {
		close(s.done)
		s.mu.Lock()
		_ = s.conn.WriteJSON(rpcRequest{JSONRPC: "2.0", ID: s.id + 1, Method: "logsUnsubscribe", Params: []any{s.id}})
		_ = s.conn.Close()
		s.mu.Unlock()
	})
}

func (s *logsSubscription) readLoop() {
	defer close(s.notifications)
	defer close(s.errors)
	for {
		var message struct {
			Method string `json:"method"`
			Params struct {
				Result struct {
					Context struct {
						Slot uint64 `json:"slot"`
					} `json:"context"`
					Value struct {
						Signature string          `json:"signature"`
						Err       json.RawMessage `json:"err"`
						Logs      []string        `json:"logs"`
					} `json:"value"`
				} `json:"result"`
			} `json:"params"`
		}
		if err := s.conn.ReadJSON(&message); err != nil {
			select {
			case <-s.done:
				return
			case s.errors <- err:
			}
			return
		}
		if message.Method != "logsNotification" {
			continue
		}
		notification := LogNotification{Slot: message.Params.Result.Context.Slot, Signature: message.Params.Result.Value.Signature, Err: message.Params.Result.Value.Err, Logs: append([]string(nil), message.Params.Result.Value.Logs...)}
		select {
		case <-s.done:
			return
		case s.notifications <- notification:
		}
	}
}

var _ LogsSubscription = (*logsSubscription)(nil)

type accountSubscription struct {
	mu            sync.Mutex
	conn          *websocket.Conn
	account       string
	id            uint64
	errors        chan error
	notifications chan AccountNotification
	done          chan struct{}
	once          sync.Once
}

func (s *accountSubscription) Err() <-chan error                         { return s.errors }
func (s *accountSubscription) Notifications() <-chan AccountNotification { return s.notifications }
func (s *accountSubscription) Unsubscribe() {
	s.once.Do(func() {
		close(s.done)
		s.mu.Lock()
		_ = s.conn.WriteJSON(rpcRequest{JSONRPC: "2.0", ID: s.id + 1, Method: "accountUnsubscribe", Params: []any{s.id}})
		_ = s.conn.Close()
		s.mu.Unlock()
	})
}

func (s *accountSubscription) readLoop() {
	defer close(s.notifications)
	defer close(s.errors)
	for {
		var message struct {
			Method string `json:"method"`
			Params struct {
				Result struct {
					Context struct {
						Slot uint64 `json:"slot"`
					} `json:"context"`
					Value *jsonAccountValue `json:"value"`
				} `json:"result"`
			} `json:"params"`
		}
		if err := s.conn.ReadJSON(&message); err != nil {
			select {
			case <-s.done:
				return
			case s.errors <- err:
			}
			return
		}
		if message.Method != "accountNotification" || message.Params.Result.Value == nil {
			continue
		}
		value, err := message.Params.Result.Value.account()
		if err != nil {
			select {
			case <-s.done:
				return
			case s.errors <- err:
			}
			return
		}
		notification := AccountNotification{Slot: message.Params.Result.Context.Slot, Account: s.account, Value: value}
		select {
		case <-s.done:
			return
		case s.notifications <- notification:
		}
	}
}

var _ AccountSubscription = (*accountSubscription)(nil)

type programSubscription struct {
	mu            sync.Mutex
	conn          *websocket.Conn
	id            uint64
	errors        chan error
	notifications chan ProgramNotification
	done          chan struct{}
	once          sync.Once
}

func (s *programSubscription) Err() <-chan error                         { return s.errors }
func (s *programSubscription) Notifications() <-chan ProgramNotification { return s.notifications }
func (s *programSubscription) Unsubscribe() {
	s.once.Do(func() {
		close(s.done)
		s.mu.Lock()
		_ = s.conn.WriteJSON(rpcRequest{JSONRPC: "2.0", ID: s.id + 1, Method: "programUnsubscribe", Params: []any{s.id}})
		_ = s.conn.Close()
		s.mu.Unlock()
	})
}

func (s *programSubscription) readLoop() {
	defer close(s.notifications)
	defer close(s.errors)
	for {
		var message struct {
			Method string `json:"method"`
			Params struct {
				Result struct {
					Context struct {
						Slot uint64 `json:"slot"`
					} `json:"context"`
					Value struct {
						Pubkey  string            `json:"pubkey"`
						Account *jsonAccountValue `json:"account"`
					} `json:"value"`
				} `json:"result"`
			} `json:"params"`
		}
		if err := s.conn.ReadJSON(&message); err != nil {
			select {
			case <-s.done:
				return
			case s.errors <- err:
			}
			return
		}
		if message.Method != "programNotification" || message.Params.Result.Value.Account == nil {
			continue
		}
		value, err := message.Params.Result.Value.Account.account()
		if err != nil {
			select {
			case <-s.done:
				return
			case s.errors <- err:
			}
			return
		}
		notification := ProgramNotification{Slot: message.Params.Result.Context.Slot, Account: message.Params.Result.Value.Pubkey, Value: value}
		select {
		case <-s.done:
			return
		case s.notifications <- notification:
		}
	}
}

var _ ProgramSubscription = (*programSubscription)(nil)

type transactionSubscription struct {
	mu            sync.Mutex
	conn          *websocket.Conn
	id            uint64
	errors        chan error
	notifications chan TransactionNotification
	done          chan struct{}
	once          sync.Once
}

func (s *transactionSubscription) Err() <-chan error { return s.errors }
func (s *transactionSubscription) Notifications() <-chan TransactionNotification {
	return s.notifications
}
func (s *transactionSubscription) Unsubscribe() {
	s.once.Do(func() {
		close(s.done)
		s.mu.Lock()
		_ = s.conn.WriteJSON(rpcRequest{
			JSONRPC: "2.0", ID: s.id + 1, Method: "transactionUnsubscribe", Params: []any{s.id},
		})
		_ = s.conn.Close()
		s.mu.Unlock()
	})
}

func (s *transactionSubscription) readLoop() {
	defer close(s.notifications)
	defer close(s.errors)
	for {
		var message struct {
			Method string `json:"method"`
			Params struct {
				Result struct {
					Signature   string `json:"signature"`
					Slot        uint64 `json:"slot"`
					Transaction struct {
						Transaction json.RawMessage `json:"transaction"`
						Meta        json.RawMessage `json:"meta"`
					} `json:"transaction"`
				} `json:"result"`
			} `json:"params"`
		}
		if err := s.conn.ReadJSON(&message); err != nil {
			select {
			case <-s.done:
				return
			case s.errors <- err:
			}
			return
		}
		if message.Method != "transactionNotification" ||
			strings.TrimSpace(message.Params.Result.Signature) == "" {
			continue
		}
		notification := TransactionNotification{
			Signature: message.Params.Result.Signature,
			Slot:      message.Params.Result.Slot,
			Transaction: append(
				json.RawMessage(nil), message.Params.Result.Transaction.Transaction...,
			),
			Meta: append(json.RawMessage(nil), message.Params.Result.Transaction.Meta...),
		}
		select {
		case <-s.done:
			return
		case s.notifications <- notification:
		}
	}
}

var _ TransactionSubscription = (*transactionSubscription)(nil)
