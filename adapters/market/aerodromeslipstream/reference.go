package aerodromeslipstream

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

const quoterABIJSON = `[
  {
    "type":"function",
    "name":"quoteExactInputSingle",
    "stateMutability":"nonpayable",
    "inputs":[{"name":"params","type":"tuple","components":[
      {"name":"tokenIn","type":"address"},
      {"name":"tokenOut","type":"address"},
      {"name":"amountIn","type":"uint256"},
      {"name":"tickSpacing","type":"int24"},
      {"name":"sqrtPriceLimitX96","type":"uint160"}
    ]}],
    "outputs":[
      {"name":"amountOut","type":"uint256"},
      {"name":"sqrtPriceX96After","type":"uint160"},
      {"name":"initializedTicksCrossed","type":"uint32"},
      {"name":"gasEstimate","type":"uint256"}
    ]
  }
]`

var quoterABI = mustABI(quoterABIJSON)

type ExactInputSingleParams struct {
	TokenIn           common.Address
	TokenOut          common.Address
	AmountIn          *big.Int
	TickSpacing       *big.Int
	SqrtPriceLimitX96 *big.Int
}

type ReferenceQuoter struct {
	address common.Address
}

func NewReferenceQuoter(address common.Address) (*ReferenceQuoter, error) {
	if address == (common.Address{}) {
		return nil, fmt.Errorf("Slipstream quoter address is required")
	}
	return &ReferenceQuoter{address: address}, nil
}

func (q *ReferenceQuoter) QuoteExactInputSingle(
	ctx context.Context,
	network evm.Network,
	block evm.BlockReference,
	tokenIn common.Address,
	tokenOut common.Address,
	amountIn *big.Int,
	tickSpacing int32,
) (*big.Int, error) {
	if network == nil || tokenIn == (common.Address{}) || tokenOut == (common.Address{}) ||
		tokenIn == tokenOut || amountIn == nil || amountIn.Sign() <= 0 || tickSpacing <= 0 {
		return nil, fmt.Errorf("invalid Slipstream quoter request")
	}
	input, err := quoterABI.Pack("quoteExactInputSingle", ExactInputSingleParams{
		TokenIn: tokenIn, TokenOut: tokenOut, AmountIn: new(big.Int).Set(amountIn),
		TickSpacing: big.NewInt(int64(tickSpacing)), SqrtPriceLimitX96: new(big.Int),
	})
	if err != nil {
		return nil, fmt.Errorf("encode Slipstream quoter request: %w", err)
	}
	result, err := network.CallContract(ctx, block, geth.CallMsg{To: &q.address, Data: input})
	if err != nil {
		return nil, err
	}
	values, err := quoterABI.Unpack("quoteExactInputSingle", result)
	if err != nil {
		return nil, fmt.Errorf("decode Slipstream quoter response: %w", err)
	}
	amountOut, ok := values[0].(*big.Int)
	if !ok || amountOut == nil || amountOut.Sign() <= 0 {
		return nil, fmt.Errorf("Slipstream quoter returned a non-positive amount")
	}
	return new(big.Int).Set(amountOut), nil
}

func mustReferenceABI(definition string) abi.ABI {
	parsed, err := abi.JSON(strings.NewReader(definition))
	if err != nil {
		panic(err)
	}
	return parsed
}
