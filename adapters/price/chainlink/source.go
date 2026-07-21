package chainlink

import (
	"context"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/VarozXYZ/vernier/adapters/chain/evm"
	"github.com/VarozXYZ/vernier/domain/market"
	priceport "github.com/VarozXYZ/vernier/ports/price"
)

type Source struct {
	id      market.SourceID
	base    market.AssetID
	quote   market.AssetID
	network evm.Network
	block   evm.BlockReference
	feed    common.Address
	clock   func() time.Time
}

func NewSource(id market.SourceID, base, quote market.AssetID, network evm.Network, block evm.BlockReference, feed common.Address, clock func() time.Time) (*Source, error) {
	if id == "" || base == "" || quote == "" || base == quote || network == nil ||
		block.Hash == (common.Hash{}) || feed == (common.Address{}) {
		return nil, fmt.Errorf("Chainlink source requires an ID, pair, network, exact block, and feed")
	}
	if clock == nil {
		clock = time.Now
	}
	return &Source{id: id, base: base, quote: quote, network: network, block: block, feed: feed, clock: clock}, nil
}

func (s *Source) ID() market.SourceID { return s.id }

func (s *Source) Observe(ctx context.Context, request priceport.Request) (market.PriceObservation, error) {
	if request.Base != s.base || request.Quote != s.quote {
		return market.PriceObservation{}, fmt.Errorf("Chainlink source %q does not provide %s/%s", s.id, request.Base, request.Quote)
	}
	value, err := Read(ctx, s.network, s.block, s.feed)
	if err != nil {
		return market.PriceObservation{}, err
	}
	reference := fmt.Sprintf("chainlink:%s/block/%d/round/%s", s.network.ID(), value.Block.Number, value.RoundID)
	return market.NewPriceObservation(s.id, s.base, s.quote, value.Value(), reference, value.UpdatedAt, s.clock().UTC())
}
