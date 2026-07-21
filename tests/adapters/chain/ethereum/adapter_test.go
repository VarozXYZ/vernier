package ethereum_test

import (
	"context"
	"math/big"
	"testing"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/VarozXYZ/vernier/adapters/chain/ethereum"
	"github.com/VarozXYZ/vernier/adapters/chain/evm"
)

type subscription struct{ errors chan error }

func (s *subscription) Err() <-chan error { return s.errors }
func (*subscription) Unsubscribe()        {}

type client struct {
	chainID *big.Int
	header  *types.Header
	query   geth.FilterQuery
	closed  bool
}

func (c *client) ChainID(context.Context) (*big.Int, error) { return new(big.Int).Set(c.chainID), nil }
func (c *client) HeaderByNumber(context.Context, *big.Int) (*types.Header, error) {
	return c.header, nil
}
func (c *client) SubscribeFilterLogs(_ context.Context, query geth.FilterQuery, _ chan<- types.Log) (geth.Subscription, error) {
	c.query = query
	return &subscription{errors: make(chan error)}, nil
}
func (c *client) FilterLogs(_ context.Context, query geth.FilterQuery) ([]types.Log, error) {
	c.query = query
	return nil, nil
}
func (*client) CallContractAtHash(context.Context, geth.CallMsg, common.Hash) ([]byte, error) {
	return nil, nil
}
func (*client) CodeAtHash(context.Context, common.Address, common.Hash) ([]byte, error) {
	return []byte{1}, nil
}
func (c *client) Close() { c.closed = true }

func TestAdapterValidatesEthereumAndUsesNarrowLogQueries(t *testing.T) {
	httpClient := &client{chainID: big.NewInt(1), header: &types.Header{Number: big.NewInt(42)}}
	wsClient := &client{chainID: big.NewInt(1)}
	adapter, err := ethereum.New(httpClient, wsClient)
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Validate(context.Background()); err != nil {
		t.Fatal(err)
	}
	address := common.HexToAddress("0x1000000000000000000000000000000000000001")
	topics := []common.Hash{common.HexToHash("0x01"), common.HexToHash("0x02")}
	filter := evm.LogFilter{Address: address, Topics: topics}
	subscription, err := adapter.SubscribeLogs(context.Background(), filter, make(chan types.Log))
	if err != nil {
		t.Fatal(err)
	}
	subscription.Unsubscribe()
	if len(wsClient.query.Addresses) != 1 || wsClient.query.Addresses[0] != address ||
		len(wsClient.query.Topics) != 1 || len(wsClient.query.Topics[0]) != 2 || wsClient.query.BlockHash != nil {
		t.Fatalf("unexpected subscription query: %+v", wsClient.query)
	}
	block := evm.BlockReference{Number: 42, Hash: common.HexToHash("0x42")}
	if _, err := adapter.LogsAt(context.Background(), block, filter); err != nil {
		t.Fatal(err)
	}
	if httpClient.query.BlockHash == nil || *httpClient.query.BlockHash != block.Hash {
		t.Fatalf("log query did not use exact block hash: %+v", httpClient.query)
	}
	adapter.Close()
	if !httpClient.closed || !wsClient.closed {
		t.Fatal("adapter did not close both clients")
	}
}

func TestAdapterRejectsNonEthereumEndpoint(t *testing.T) {
	adapter, err := ethereum.New(&client{chainID: big.NewInt(1)}, &client{chainID: big.NewInt(8453)})
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Validate(context.Background()); err == nil {
		t.Fatal("expected chain ID mismatch")
	}
}
