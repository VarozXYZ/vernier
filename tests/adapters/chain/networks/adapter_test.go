package networks_test

import (
	"context"
	"math/big"
	"testing"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/VarozXYZ/vernier/adapters/chain/base"
	"github.com/VarozXYZ/vernier/adapters/chain/robinhood"
)

type client struct{ chainID *big.Int }

func (c *client) ChainID(context.Context) (*big.Int, error) {
	return new(big.Int).Set(c.chainID), nil
}
func (*client) HeaderByNumber(context.Context, *big.Int) (*types.Header, error) { return nil, nil }
func (*client) SubscribeFilterLogs(context.Context, geth.FilterQuery, chan<- types.Log) (geth.Subscription, error) {
	return nil, nil
}
func (*client) FilterLogs(context.Context, geth.FilterQuery) ([]types.Log, error) { return nil, nil }
func (*client) CallContractAtHash(context.Context, geth.CallMsg, common.Hash) ([]byte, error) {
	return nil, nil
}
func (*client) CodeAtHash(context.Context, common.Address, common.Hash) ([]byte, error) {
	return nil, nil
}
func (*client) Close() {}

func TestNamedNetworksValidateTheirOwnChainIDs(t *testing.T) {
	tests := []struct {
		name    string
		id      string
		chainID int64
		build   func(*client, *client) (interface {
			ID() string
			Validate(context.Context) error
		}, error)
	}{
		{name: "base", id: base.ID, chainID: 8453, build: func(http, ws *client) (interface {
			ID() string
			Validate(context.Context) error
		}, error) {
			return base.New(http, ws)
		}},
		{name: "robinhood", id: robinhood.ID, chainID: 4663, build: func(http, ws *client) (interface {
			ID() string
			Validate(context.Context) error
		}, error) {
			return robinhood.New(http, ws)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			http := &client{chainID: big.NewInt(test.chainID)}
			ws := &client{chainID: big.NewInt(test.chainID)}
			adapter, err := test.build(http, ws)
			if err != nil {
				t.Fatal(err)
			}
			if adapter.ID() != test.id {
				t.Fatalf("network ID = %q", adapter.ID())
			}
			if err := adapter.Validate(context.Background()); err != nil {
				t.Fatal(err)
			}
			ws.chainID = big.NewInt(1)
			if err := adapter.Validate(context.Background()); err == nil {
				t.Fatal("wrong WebSocket chain ID was accepted")
			}
		})
	}
}
