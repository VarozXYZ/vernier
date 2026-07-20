// Package ethereum provides the canonical Ethereum mainnet network adapter.
package ethereum

import (
	"context"
	"fmt"
	"math/big"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/VarozXYZ/vernier/adapters/chain/evm"
)

const ID = "ethereum"

var mainnetChainID = big.NewInt(1)

type Client interface {
	ChainID(context.Context) (*big.Int, error)
	HeaderByNumber(context.Context, *big.Int) (*types.Header, error)
	SubscribeFilterLogs(context.Context, geth.FilterQuery, chan<- types.Log) (geth.Subscription, error)
	FilterLogs(context.Context, geth.FilterQuery) ([]types.Log, error)
	CallContractAtHash(context.Context, geth.CallMsg, common.Hash) ([]byte, error)
	CodeAtHash(context.Context, common.Address, common.Hash) ([]byte, error)
	Close()
}

type Adapter struct {
	http Client
	ws   Client
}

func Dial(ctx context.Context, httpURL, wsURL string) (*Adapter, error) {
	if httpURL == "" || wsURL == "" {
		return nil, fmt.Errorf("ethereum HTTP and WebSocket endpoints are required")
	}
	httpClient, err := ethclient.DialContext(ctx, httpURL)
	if err != nil {
		return nil, fmt.Errorf("dial Ethereum HTTP endpoint: connection failed")
	}
	wsClient, err := ethclient.DialContext(ctx, wsURL)
	if err != nil {
		httpClient.Close()
		return nil, fmt.Errorf("dial Ethereum WebSocket endpoint: connection failed")
	}
	adapter, err := New(httpClient, wsClient)
	if err != nil {
		httpClient.Close()
		wsClient.Close()
		return nil, err
	}
	if err := adapter.Validate(ctx); err != nil {
		adapter.Close()
		return nil, err
	}
	return adapter, nil
}

func New(httpClient, wsClient Client) (*Adapter, error) {
	if httpClient == nil || wsClient == nil {
		return nil, fmt.Errorf("ethereum HTTP and WebSocket clients are required")
	}
	return &Adapter{http: httpClient, ws: wsClient}, nil
}

func (a *Adapter) ID() string { return ID }

func (a *Adapter) Validate(ctx context.Context) error {
	for name, client := range map[string]Client{"HTTP": a.http, "WebSocket": a.ws} {
		chainID, err := client.ChainID(ctx)
		if err != nil {
			return fmt.Errorf("read Ethereum %s chain ID: %w", name, err)
		}
		if chainID.Cmp(mainnetChainID) != 0 {
			return fmt.Errorf("ethereum %s endpoint returned chain ID %s, expected 1", name, chainID)
		}
	}
	return nil
}

func (a *Adapter) CurrentBlock(ctx context.Context) (evm.BlockReference, error) {
	header, err := a.http.HeaderByNumber(ctx, nil)
	if err != nil {
		return evm.BlockReference{}, fmt.Errorf("read current Ethereum block: %w", err)
	}
	if header == nil || header.Number == nil || !header.Number.IsUint64() {
		return evm.BlockReference{}, fmt.Errorf("ethereum returned an invalid current block")
	}
	return evm.BlockReference{Number: header.Number.Uint64(), Hash: header.Hash()}, nil
}

func (a *Adapter) SubscribeLogs(ctx context.Context, filter evm.LogFilter, output chan<- types.Log) (evm.Subscription, error) {
	if filter.Address == (common.Address{}) || output == nil {
		return nil, fmt.Errorf("ethereum log subscription requires an address and output channel")
	}
	subscription, err := a.ws.SubscribeFilterLogs(ctx, filter.Query(nil), output)
	if err != nil {
		return nil, fmt.Errorf("subscribe to Ethereum logs: %w", err)
	}
	return subscription, nil
}

func (a *Adapter) LogsAt(ctx context.Context, block evm.BlockReference, filter evm.LogFilter) ([]types.Log, error) {
	logs, err := a.http.FilterLogs(ctx, filter.Query(&block.Hash))
	if err != nil {
		return nil, fmt.Errorf("read Ethereum logs at block %d: %w", block.Number, err)
	}
	return logs, nil
}

func (a *Adapter) CallContract(ctx context.Context, block evm.BlockReference, call geth.CallMsg) ([]byte, error) {
	result, err := a.http.CallContractAtHash(ctx, call, block.Hash)
	if err != nil {
		return nil, fmt.Errorf("call Ethereum contract at block %d: %w", block.Number, err)
	}
	return result, nil
}

func (a *Adapter) CodeAt(ctx context.Context, block evm.BlockReference, address common.Address) ([]byte, error) {
	code, err := a.http.CodeAtHash(ctx, address, block.Hash)
	if err != nil {
		return nil, fmt.Errorf("read Ethereum contract code at block %d: %w", block.Number, err)
	}
	return code, nil
}

func (a *Adapter) Close() {
	a.http.Close()
	if a.ws != a.http {
		a.ws.Close()
	}
}

var _ evm.Network = (*Adapter)(nil)
