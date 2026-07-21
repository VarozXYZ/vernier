package aerodrome

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
  {"type":"function","name":"getAmountsOut","stateMutability":"view","inputs":[{"name":"amountIn","type":"uint256"},{"name":"routes","type":"tuple[]","components":[{"name":"from","type":"address"},{"name":"to","type":"address"},{"name":"stable","type":"bool"},{"name":"factory","type":"address"}]}],"outputs":[{"name":"amounts","type":"uint256[]"}]}
]`

var routerABI = mustRouterABI(routerABIJSON)

type ReferenceQuoter struct {
	router  common.Address
	factory common.Address
	stable  bool
	from    common.Address
	to      common.Address
}

func NewReferenceQuoter(router, factory, base, quote common.Address, stable bool) (*ReferenceQuoter, error) {
	if router == (common.Address{}) || factory == (common.Address{}) || base == (common.Address{}) || quote == (common.Address{}) || base == quote {
		return nil, fmt.Errorf("aerodrome router, factory, and distinct market tokens are required")
	}
	return &ReferenceQuoter{router: router, factory: factory, stable: stable, from: base, to: quote}, nil
}

func (q *ReferenceQuoter) QuoteExactInput(ctx context.Context, network evm.Network, block evm.BlockReference, tokenIn, tokenOut common.Address, amountIn *big.Int) (*big.Int, error) {
	if network == nil || tokenIn == (common.Address{}) || tokenOut == (common.Address{}) || tokenIn == tokenOut || amountIn == nil || amountIn.Sign() <= 0 {
		return nil, fmt.Errorf("invalid aerodrome router quote request")
	}
	input, err := routerABI.Pack("getAmountsOut", amountIn, []route{{From: tokenIn, To: tokenOut, Stable: q.stable, Factory: q.factory}})
	if err != nil {
		return nil, fmt.Errorf("encode Aerodrome router request: %w", err)
	}
	result, err := network.CallContract(ctx, block, geth.CallMsg{To: &q.router, Data: input})
	if err != nil {
		return nil, err
	}
	values, err := routerABI.Unpack("getAmountsOut", result)
	if err != nil {
		return nil, fmt.Errorf("decode Aerodrome router response: %w", err)
	}
	amounts, ok := values[0].([]*big.Int)
	if !ok || len(amounts) != 2 || amounts[1] == nil || amounts[1].Sign() <= 0 {
		return nil, fmt.Errorf("aerodrome router returned invalid amounts")
	}
	return new(big.Int).Set(amounts[1]), nil
}

// QuoteExactOutput finds the smallest input by querying Aerodrome's exact-input
// router. The live default uses quote-sized exact-input evaluation; this path
// remains available for explicit base-sized compatibility.
func (q *ReferenceQuoter) QuoteExactOutput(ctx context.Context, network evm.Network, block evm.BlockReference, tokenIn, tokenOut common.Address, amountOut *big.Int) (*big.Int, error) {
	if amountOut == nil || amountOut.Sign() <= 0 {
		return nil, fmt.Errorf("invalid aerodrome exact-output request")
	}
	upper := big.NewInt(1)
	for i := 0; i < 256; i++ {
		output, err := q.QuoteExactInput(ctx, network, block, tokenIn, tokenOut, upper)
		if err == nil && output.Cmp(amountOut) >= 0 {
			break
		}
		upper.Lsh(upper, 1)
	}
	output, err := q.QuoteExactInput(ctx, network, block, tokenIn, tokenOut, upper)
	if err != nil || output.Cmp(amountOut) < 0 {
		return nil, fmt.Errorf("aerodrome exact output is unavailable")
	}
	lower := new(big.Int)
	for new(big.Int).Sub(upper, lower).Cmp(big.NewInt(1)) > 0 {
		mid := new(big.Int).Add(lower, upper)
		mid.Rsh(mid, 1)
		output, err := q.QuoteExactInput(ctx, network, block, tokenIn, tokenOut, mid)
		if err == nil && output.Cmp(amountOut) >= 0 {
			upper = mid
		} else {
			lower = mid
		}
	}
	return upper, nil
}

type route struct {
	From    common.Address
	To      common.Address
	Stable  bool
	Factory common.Address
}

func mustRouterABI(definition string) abi.ABI {
	parsed, err := abi.JSON(strings.NewReader(definition))
	if err != nil {
		panic(err)
	}
	return parsed
}
