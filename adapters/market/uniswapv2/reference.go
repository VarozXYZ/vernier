package uniswapv2

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/VarozXYZ/vernier/adapters/chain/evm"
)

const routerABIJSON = `[
  {"type":"function","name":"getAmountsOut","stateMutability":"view","inputs":[{"name":"amountIn","type":"uint256"},{"name":"path","type":"address[]"}],"outputs":[{"name":"amounts","type":"uint256[]"}]}
]`

var routerABI = mustRouterABI(routerABIJSON)

type ReferenceQuoter struct{ router common.Address }

func NewReferenceQuoter(router common.Address) (*ReferenceQuoter, error) {
	if router == (common.Address{}) {
		return nil, fmt.Errorf("uniswap V2 router address is required")
	}
	return &ReferenceQuoter{router: router}, nil
}

func (q *ReferenceQuoter) QuoteExactInput(
	ctx context.Context,
	network evm.Network,
	block evm.BlockReference,
	tokenIn, tokenOut common.Address,
	amountIn *big.Int,
) (*big.Int, error) {
	if network == nil || tokenIn == (common.Address{}) || tokenOut == (common.Address{}) ||
		tokenIn == tokenOut || amountIn == nil || amountIn.Sign() <= 0 {
		return nil, fmt.Errorf("invalid Uniswap V2 router quote request")
	}
	input, err := routerABI.Pack("getAmountsOut", amountIn, []common.Address{tokenIn, tokenOut})
	if err != nil {
		return nil, fmt.Errorf("encode Uniswap V2 router request: %w", err)
	}
	result, err := network.CallContract(ctx, block, geth.CallMsg{To: &q.router, Data: input})
	if err != nil {
		return nil, err
	}
	values, err := routerABI.Unpack("getAmountsOut", result)
	if err != nil {
		return nil, fmt.Errorf("decode Uniswap V2 router response: %w", err)
	}
	amounts, ok := values[0].([]*big.Int)
	if !ok || len(amounts) != 2 || amounts[1] == nil || amounts[1].Sign() <= 0 {
		return nil, fmt.Errorf("uniswap V2 router returned invalid amounts")
	}
	return new(big.Int).Set(amounts[1]), nil
}

func mustRouterABI(definition string) abi.ABI {
	parsed, err := abi.JSON(strings.NewReader(definition))
	if err != nil {
		panic(err)
	}
	return parsed
}
