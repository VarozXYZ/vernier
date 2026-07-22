// Package solana contains the read-only JSON-RPC and WebSocket capabilities
// shared by Solana feeds and market adapters. It deliberately has no signer
// or transaction submission surface.
package solana

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

type ReadOnlyNetwork struct {
	id           string
	label        string
	httpURL      string
	websocketURL string
	httpClient   *http.Client
	dialer       *websocket.Dialer
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
	if id == "" || label == "" {
		return nil, fmt.Errorf("network id and label are required")
	}
	if err := validateEndpoint(httpURL, "HTTP"); err != nil {
		return nil, err
	}
	if err := validateEndpoint(websocketURL, "WebSocket"); err != nil {
		return nil, err
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	if dialer == nil {
		dialer = websocket.DefaultDialer
	}
	return &ReadOnlyNetwork{id: id, label: label, httpURL: httpURL, websocketURL: websocketURL, httpClient: httpClient, dialer: dialer}, nil
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

type Transaction struct {
	Slot        uint64
	BlockTime   *int64
	Transaction json.RawMessage
	Meta        json.RawMessage
}

func (n *ReadOnlyNetwork) ReadTransaction(ctx context.Context, signature string) (Transaction, error) {
	var result *jsonTransaction
	if err := n.callHTTP(ctx, "getTransaction", []any{signature, map[string]any{"encoding": "json", "commitment": "processed", "maxSupportedTransactionVersion": 0}}, &result); err != nil {
		return Transaction{}, fmt.Errorf("read %s transaction: %w", n.label, err)
	}
	if result == nil {
		return Transaction{}, fmt.Errorf("transaction %s was not found", signature)
	}
	return Transaction{Slot: result.Slot, BlockTime: result.BlockTime, Transaction: result.Transaction, Meta: result.Meta}, nil
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
