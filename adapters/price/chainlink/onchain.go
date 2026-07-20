// Package chainlink reads an immutable Chainlink data-feed observation at an
// exact EVM block for use as research cost evidence.
package chainlink

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/VarozXYZ/vernier/adapters/chain/evm"
)

const aggregatorABIJSON = `[
  {"type":"function","name":"decimals","stateMutability":"view","inputs":[],"outputs":[{"type":"uint8"}]},
  {"type":"function","name":"latestRoundData","stateMutability":"view","inputs":[],"outputs":[
    {"name":"roundId","type":"uint80"},
    {"name":"answer","type":"int256"},
    {"name":"startedAt","type":"uint256"},
    {"name":"updatedAt","type":"uint256"},
    {"name":"answeredInRound","type":"uint80"}
  ]}
]`

var aggregatorABI = mustABI(aggregatorABIJSON)

type Observation struct {
	Feed            common.Address
	Block           evm.BlockReference
	RoundID         *big.Int
	AnsweredInRound *big.Int
	Answer          *big.Int
	Decimals        uint8
	UpdatedAt       time.Time
}

func (o Observation) Value() *big.Rat {
	if o.Answer == nil {
		return new(big.Rat)
	}
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(o.Decimals)), nil)
	return new(big.Rat).SetFrac(new(big.Int).Set(o.Answer), scale)
}

func Read(
	ctx context.Context,
	network evm.Network,
	block evm.BlockReference,
	feed common.Address,
) (Observation, error) {
	if network == nil || feed == (common.Address{}) || block.Hash == (common.Hash{}) {
		return Observation{}, fmt.Errorf("network, feed, and exact block are required")
	}
	decimalsValues, err := call(ctx, network, block, feed, "decimals")
	if err != nil {
		return Observation{}, err
	}
	decimals, ok := decimalsValues[0].(uint8)
	if !ok || decimals > 36 {
		return Observation{}, fmt.Errorf("chainlink feed returned invalid decimals")
	}
	values, err := call(ctx, network, block, feed, "latestRoundData")
	if err != nil {
		return Observation{}, err
	}
	roundID, okRound := values[0].(*big.Int)
	answer, okAnswer := values[1].(*big.Int)
	updatedAt, okUpdated := values[3].(*big.Int)
	answeredInRound, okAnswered := values[4].(*big.Int)
	if !okRound || !okAnswer || !okUpdated || !okAnswered || roundID == nil || answer == nil ||
		updatedAt == nil || answeredInRound == nil || roundID.Sign() <= 0 || answer.Sign() <= 0 ||
		updatedAt.Sign() <= 0 || !updatedAt.IsInt64() || answeredInRound.Cmp(roundID) < 0 {
		return Observation{}, fmt.Errorf("chainlink feed returned an invalid round")
	}
	return Observation{
		Feed: feed, Block: block, RoundID: new(big.Int).Set(roundID),
		AnsweredInRound: new(big.Int).Set(answeredInRound), Answer: new(big.Int).Set(answer),
		Decimals: decimals, UpdatedAt: time.Unix(updatedAt.Int64(), 0).UTC(),
	}, nil
}

func call(
	ctx context.Context,
	network evm.Network,
	block evm.BlockReference,
	feed common.Address,
	method string,
) ([]any, error) {
	input, err := aggregatorABI.Pack(method)
	if err != nil {
		return nil, fmt.Errorf("encode Chainlink %s call: %w", method, err)
	}
	output, err := network.CallContract(ctx, block, geth.CallMsg{To: &feed, Data: input})
	if err != nil {
		return nil, err
	}
	values, err := aggregatorABI.Unpack(method, output)
	if err != nil {
		return nil, fmt.Errorf("decode Chainlink %s response: %w", method, err)
	}
	return values, nil
}

func mustABI(definition string) abi.ABI {
	parsed, err := abi.JSON(strings.NewReader(definition))
	if err != nil {
		panic(err)
	}
	return parsed
}
