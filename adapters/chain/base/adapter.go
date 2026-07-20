// Package base provides the canonical Base mainnet network adapter.
package base

import (
	"context"
	"fmt"
	"math/big"

	"github.com/VarozXYZ/vernier/adapters/chain/evm"
)

const ID = "base"

var mainnetChainID = big.NewInt(8453)

type Client = evm.Client

type Adapter struct{ *evm.ReadOnlyNetwork }

func Dial(ctx context.Context, httpURL, wsURL string) (*Adapter, error) {
	network, err := evm.DialReadOnlyNetwork(ctx, ID, "Base", mainnetChainID, httpURL, wsURL)
	if err != nil {
		return nil, err
	}
	return &Adapter{ReadOnlyNetwork: network}, nil
}

func New(httpClient, wsClient Client) (*Adapter, error) {
	network, err := evm.NewReadOnlyNetwork(ID, "Base", mainnetChainID, httpClient, wsClient)
	if err != nil {
		return nil, fmt.Errorf("base: %w", err)
	}
	return &Adapter{ReadOnlyNetwork: network}, nil
}

var _ evm.Network = (*Adapter)(nil)
