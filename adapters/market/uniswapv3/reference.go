package uniswapv3

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

const quoterV2ABIJSON = "[" +
	"{\"type\":\"function\",\"name\":\"quoteExactInputSingle\",\"stateMutability\":\"nonpayable\"," +
	"\"inputs\":[{\"name\":\"params\",\"type\":\"tuple\",\"components\":[" +
	"{\"name\":\"tokenIn\",\"type\":\"address\"},{\"name\":\"tokenOut\",\"type\":\"address\"}," +
	"{\"name\":\"amountIn\",\"type\":\"uint256\"},{\"name\":\"fee\",\"type\":\"uint24\"}," +
	"{\"name\":\"sqrtPriceLimitX96\",\"type\":\"uint160\"}]}]," +
	"\"outputs\":[{\"name\":\"amountOut\",\"type\":\"uint256\"},{\"name\":\"sqrtPriceX96After\",\"type\":\"uint160\"}," +
	"{\"name\":\"initializedTicksCrossed\",\"type\":\"uint32\"},{\"name\":\"gasEstimate\",\"type\":\"uint256\"}]}" +
	"]"

var quoterV2ABI = mustReferenceABI(quoterV2ABIJSON)

type ExactInputSingleParams struct {
	TokenIn           common.Address
	TokenOut          common.Address
	AmountIn          *big.Int
	Fee               *big.Int
	SqrtPriceLimitX96 *big.Int
}

type ReferenceQuoter struct {
	address common.Address
}

func NewReferenceQuoter(address common.Address) (*ReferenceQuoter, error) {
	if address == (common.Address{}) {
		return nil, fmt.Errorf("QuoterV2 address is required")
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
	fee uint32,
) (*big.Int, error) {
	if network == nil || tokenIn == (common.Address{}) || tokenOut == (common.Address{}) ||
		tokenIn == tokenOut || amountIn == nil || amountIn.Sign() <= 0 {
		return nil, fmt.Errorf("invalid QuoterV2 request")
	}
	input, err := quoterV2ABI.Pack("quoteExactInputSingle", ExactInputSingleParams{
		TokenIn: tokenIn, TokenOut: tokenOut, AmountIn: cloneInt(amountIn),
		Fee: big.NewInt(int64(fee)), SqrtPriceLimitX96: new(big.Int),
	})
	if err != nil {
		return nil, fmt.Errorf("encode QuoterV2 request: %w", err)
	}
	result, err := network.CallContract(ctx, block, geth.CallMsg{To: &q.address, Data: input})
	if err != nil {
		return nil, err
	}
	values, err := quoterV2ABI.Unpack("quoteExactInputSingle", result)
	if err != nil {
		return nil, fmt.Errorf("decode QuoterV2 response: %w", err)
	}
	amountOut, err := bigValue(values[0], "QuoterV2 amount out")
	if err != nil || amountOut.Sign() <= 0 {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("QuoterV2 returned a non-positive amount")
	}
	return amountOut, nil
}

func mustReferenceABI(definition string) abi.ABI {
	parsed, err := abi.JSON(strings.NewReader(definition))
	if err != nil {
		panic(err)
	}
	return parsed
}
