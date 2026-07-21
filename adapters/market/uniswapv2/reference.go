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
  {"type":"function","name":"getAmountsOut","stateMutability":"view","inputs":[{"name":"amountIn","type":"uint256"},{"name":"path","type":"address[]"}],"outputs":[{"name":"amounts","type":"uint256[]"}]},
  {"type":"function","name":"getAmountsIn","stateMutability":"view","inputs":[{"name":"amountOut","type":"uint256"},{"name":"path","type":"address[]"}],"outputs":[{"name":"amounts","type":"uint256[]"}]}
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
	return q.quote(ctx, network, block, "getAmountsOut", tokenIn, tokenOut, amountIn, 1)
}

func (q *ReferenceQuoter) QuoteExactOutput(
	ctx context.Context,
	network evm.Network,
	block evm.BlockReference,
	tokenIn, tokenOut common.Address,
	amountOut *big.Int,
) (*big.Int, error) {
	return q.quote(ctx, network, block, "getAmountsIn", tokenIn, tokenOut, amountOut, 0)
}

func (q *ReferenceQuoter) quote(
	ctx context.Context,
	network evm.Network,
	block evm.BlockReference,
	method string,
	tokenIn, tokenOut common.Address,
	amount *big.Int,
	resultIndex int,
) (*big.Int, error) {
	if network == nil || tokenIn == (common.Address{}) || tokenOut == (common.Address{}) ||
		tokenIn == tokenOut || amount == nil || amount.Sign() <= 0 {
		return nil, fmt.Errorf("invalid Uniswap V2 router quote request")
	}
	input, err := routerABI.Pack(method, amount, []common.Address{tokenIn, tokenOut})
	if err != nil {
		return nil, fmt.Errorf("encode Uniswap V2 router request: %w", err)
	}
	result, err := network.CallContract(ctx, block, geth.CallMsg{To: &q.router, Data: input})
	if err != nil {
		return nil, err
	}
	values, err := routerABI.Unpack(method, result)
	if err != nil {
		return nil, fmt.Errorf("decode Uniswap V2 router response: %w", err)
	}
	amounts, ok := values[0].([]*big.Int)
	if !ok || len(amounts) != 2 || amounts[resultIndex] == nil || amounts[resultIndex].Sign() <= 0 {
		return nil, fmt.Errorf("uniswap V2 router returned invalid amounts")
	}
	return new(big.Int).Set(amounts[resultIndex]), nil
}

func mustRouterABI(definition string) abi.ABI {
	parsed, err := abi.JSON(strings.NewReader(definition))
	if err != nil {
		panic(err)
	}
	return parsed
}
