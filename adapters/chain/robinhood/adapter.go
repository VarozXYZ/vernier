// Package robinhood provides the canonical Robinhood Chain mainnet adapter.
package robinhood

import (
	"context"
	"fmt"
	"math/big"

	"github.com/VarozXYZ/vernier/adapters/chain/evm"
)

const ID = "robinhood"

var mainnetChainID = big.NewInt(4663)

type Client = evm.Client

type Adapter struct{ *evm.ReadOnlyNetwork }

func Dial(ctx context.Context, httpURL, wsURL string) (*Adapter, error) {
	network, err := evm.DialReadOnlyNetwork(ctx, ID, "Robinhood Chain", mainnetChainID, httpURL, wsURL)
	if err != nil {
		return nil, err
	}
	return &Adapter{ReadOnlyNetwork: network}, nil
}

func New(httpClient, wsClient Client) (*Adapter, error) {
	network, err := evm.NewReadOnlyNetwork(ID, "Robinhood Chain", mainnetChainID, httpClient, wsClient)
	if err != nil {
		return nil, fmt.Errorf("robinhood: %w", err)
	}
	return &Adapter{ReadOnlyNetwork: network}, nil
}

var _ evm.Network = (*Adapter)(nil)
