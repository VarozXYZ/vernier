package evm

import (
	"context"
	"fmt"
	"math/big"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

// Client is the JSON-RPC surface required by a read-only EVM network.
type Client interface {
	ChainID(context.Context) (*big.Int, error)
	HeaderByNumber(context.Context, *big.Int) (*types.Header, error)
	SubscribeFilterLogs(context.Context, geth.FilterQuery, chan<- types.Log) (geth.Subscription, error)
	FilterLogs(context.Context, geth.FilterQuery) ([]types.Log, error)
	CallContractAtHash(context.Context, geth.CallMsg, common.Hash) ([]byte, error)
	CodeAtHash(context.Context, common.Address, common.Hash) ([]byte, error)
	Close()
}

// ReadOnlyNetwork implements shared EVM mechanics. A named chain package must
// select its identity and chain ID explicitly before this capability is used.
type ReadOnlyNetwork struct {
	id        string
	label     string
	chainID   *big.Int
	http      Client
	websocket Client
}

func DialReadOnlyNetwork(ctx context.Context, id, label string, chainID *big.Int, httpURL, wsURL string) (*ReadOnlyNetwork, error) {
	if httpURL == "" || wsURL == "" {
		return nil, fmt.Errorf("%s HTTP and WebSocket endpoints are required", label)
	}
	httpClient, err := ethclient.DialContext(ctx, httpURL)
	if err != nil {
		return nil, fmt.Errorf("dial %s HTTP endpoint: connection failed", label)
	}
	wsClient, err := ethclient.DialContext(ctx, wsURL)
	if err != nil {
		httpClient.Close()
		return nil, fmt.Errorf("dial %s WebSocket endpoint: connection failed", label)
	}
	network, err := NewReadOnlyNetwork(id, label, chainID, httpClient, wsClient)
	if err != nil {
		httpClient.Close()
		wsClient.Close()
		return nil, err
	}
	if err := network.Validate(ctx); err != nil {
		network.Close()
		return nil, err
	}
	return network, nil
}

func NewReadOnlyNetwork(id, label string, chainID *big.Int, httpClient, wsClient Client) (*ReadOnlyNetwork, error) {
	if id == "" || label == "" || chainID == nil || chainID.Sign() <= 0 || httpClient == nil || wsClient == nil {
		return nil, fmt.Errorf("network identity, chain ID, and HTTP/WebSocket clients are required")
	}
	return &ReadOnlyNetwork{
		id: id, label: label, chainID: new(big.Int).Set(chainID), http: httpClient, websocket: wsClient,
	}, nil
}

func (n *ReadOnlyNetwork) ID() string { return n.id }

func (n *ReadOnlyNetwork) Validate(ctx context.Context) error {
	for name, client := range map[string]Client{"HTTP": n.http, "WebSocket": n.websocket} {
		chainID, err := client.ChainID(ctx)
		if err != nil {
			return fmt.Errorf("read %s %s chain ID: %w", n.label, name, err)
		}
		if chainID == nil || chainID.Cmp(n.chainID) != 0 {
			actual := "<nil>"
			if chainID != nil {
				actual = chainID.String()
			}
			return fmt.Errorf("%s %s endpoint returned chain ID %s, expected %s", n.label, name, actual, n.chainID)
		}
	}
	return nil
}

func (n *ReadOnlyNetwork) CurrentBlock(ctx context.Context) (BlockReference, error) {
	header, err := n.http.HeaderByNumber(ctx, nil)
	if err != nil {
		return BlockReference{}, fmt.Errorf("read current %s block: %w", n.label, err)
	}
	if header == nil || header.Number == nil || !header.Number.IsUint64() {
		return BlockReference{}, fmt.Errorf("%s returned an invalid current block", n.label)
	}
	return BlockReference{Number: header.Number.Uint64(), Hash: header.Hash()}, nil
}

func (n *ReadOnlyNetwork) SubscribeLogs(ctx context.Context, filter LogFilter, output chan<- types.Log) (Subscription, error) {
	if filter.Address == (common.Address{}) || output == nil {
		return nil, fmt.Errorf("%s log subscription requires an address and output channel", n.label)
	}
	subscription, err := n.websocket.SubscribeFilterLogs(ctx, filter.Query(nil), output)
	if err != nil {
		return nil, fmt.Errorf("subscribe to %s logs: %w", n.label, err)
	}
	return subscription, nil
}

func (n *ReadOnlyNetwork) LogsAt(ctx context.Context, block BlockReference, filter LogFilter) ([]types.Log, error) {
	logs, err := n.http.FilterLogs(ctx, filter.Query(&block.Hash))
	if err != nil {
		return nil, fmt.Errorf("read %s logs at block %d: %w", n.label, block.Number, err)
	}
	return logs, nil
}

func (n *ReadOnlyNetwork) CallContract(ctx context.Context, block BlockReference, call geth.CallMsg) ([]byte, error) {
	result, err := n.http.CallContractAtHash(ctx, call, block.Hash)
	if err != nil {
		return nil, fmt.Errorf("call %s contract at block %d: %w", n.label, block.Number, err)
	}
	return result, nil
}

func (n *ReadOnlyNetwork) CodeAt(ctx context.Context, block BlockReference, address common.Address) ([]byte, error) {
	code, err := n.http.CodeAtHash(ctx, address, block.Hash)
	if err != nil {
		return nil, fmt.Errorf("read %s contract code at block %d: %w", n.label, block.Number, err)
	}
	return code, nil
}

func (n *ReadOnlyNetwork) Close() {
	n.http.Close()
	if n.websocket != n.http {
		n.websocket.Close()
	}
}

var _ Network = (*ReadOnlyNetwork)(nil)
