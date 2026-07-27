package uniswapv4

import (
	"github.com/VarozXYZ/vernier/adapters/market/uniswapv3"
	"github.com/VarozXYZ/vernier/domain/market"
)

// Reducer and Quoter deliberately reuse the proven concentrated-liquidity
// implementation. NewAdapter guarantees that only zero-hook, static-fee V4
// pools can produce this compatible state.
type Reducer = uniswapv3.Reducer
type Quoter = uniswapv3.Quoter

func NewQuoter(id market.SourceID, candidate market.Market, currency0, currency1 market.TokenID) (*Quoter, error) {
	return uniswapv3.NewQuoter(id, candidate, currency0, currency1)
}
