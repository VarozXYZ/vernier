package evm_test

import (
	"context"
	"math/big"
	"testing"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/VarozXYZ/vernier/adapters/chain/evm"
)

type subscription struct{ errors chan error }

func (s *subscription) Err() <-chan error { return s.errors }
func (*subscription) Unsubscribe()        {}

type client struct {
	chainID *big.Int
	query   geth.FilterQuery
	closed  bool
}

func (c *client) ChainID(context.Context) (*big.Int, error)                     { return new(big.Int).Set(c.chainID), nil }
func (*client) HeaderByNumber(context.Context, *big.Int) (*types.Header, error) { return nil, nil }
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

func TestConfiguredNetworkValidatesIdentityAndUsesNarrowLogQueries(t *testing.T) {
	httpClient := &client{chainID: big.NewInt(8453)}
	wsClient := &client{chainID: big.NewInt(8453)}
	network, err := evm.NewReadOnlyNetwork("configured", "Configured Chain", big.NewInt(8453), httpClient, wsClient)
	if err != nil {
		t.Fatal(err)
	}
	if err := network.Validate(context.Background()); err != nil {
		t.Fatal(err)
	}
	address := common.HexToAddress("0x1000000000000000000000000000000000000001")
	filter := evm.LogFilter{Address: address, Topics: []common.Hash{common.HexToHash("0x01"), common.HexToHash("0x02")}}
	subscription, err := network.SubscribeLogs(context.Background(), filter, make(chan types.Log))
	if err != nil {
		t.Fatal(err)
	}
	subscription.Unsubscribe()
	if len(wsClient.query.Addresses) != 1 || wsClient.query.Addresses[0] != address || len(wsClient.query.Topics[0]) != 2 {
		t.Fatalf("unexpected subscription query: %+v", wsClient.query)
	}
	block := evm.BlockReference{Number: 42, Hash: common.HexToHash("0x42")}
	if _, err := network.LogsAt(context.Background(), block, filter); err != nil {
		t.Fatal(err)
	}
	if httpClient.query.BlockHash == nil || *httpClient.query.BlockHash != block.Hash {
		t.Fatalf("log query did not use exact block hash: %+v", httpClient.query)
	}
	wsClient.chainID = big.NewInt(1)
	if err := network.Validate(context.Background()); err == nil {
		t.Fatal("wrong configured chain ID was accepted")
	}
	network.Close()
	if !httpClient.closed || !wsClient.closed {
		t.Fatal("network did not close both clients")
	}
}
