package evm_test

import (
	"context"
	"math/big"
	"testing"
	"time"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	evmadapter "github.com/VarozXYZ/vernier/adapters/chain/evm"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
)

type confirmationSubscription struct {
	errors chan error
}

func (s *confirmationSubscription) Err() <-chan error { return s.errors }
func (*confirmationSubscription) Unsubscribe()        {}

type confirmationNetwork struct {
	output chan<- types.Log
}

func (*confirmationNetwork) ID() string { return "synthetic" }
func (*confirmationNetwork) CurrentBlock(context.Context) (evmadapter.BlockReference, error) {
	return evmadapter.BlockReference{}, nil
}
func (n *confirmationNetwork) SubscribeLogs(_ context.Context, _ evmadapter.LogFilter, output chan<- types.Log) (evmadapter.Subscription, error) {
	n.output = output
	return &confirmationSubscription{errors: make(chan error)}, nil
}
func (*confirmationNetwork) LogsAt(context.Context, evmadapter.BlockReference, evmadapter.LogFilter) ([]types.Log, error) {
	return nil, nil
}
func (*confirmationNetwork) CallContract(context.Context, evmadapter.BlockReference, geth.CallMsg) ([]byte, error) {
	return nil, nil
}
func (*confirmationNetwork) CodeAt(context.Context, evmadapter.BlockReference, common.Address) ([]byte, error) {
	return nil, nil
}
func (*confirmationNetwork) Close() {}

type confirmationDecoder struct {
	filter evmadapter.LogFilter
	now    time.Time
}

func (d confirmationDecoder) Filter() evmadapter.LogFilter { return d.filter }
func (d confirmationDecoder) DecodeLog(step execution.OperationStep, observed types.Log) (execution.Settlement, bool, error) {
	if len(observed.Data) != 1 || observed.Data[0] != 1 {
		return execution.Settlement{}, false, nil
	}
	return execution.Settlement{
		Technical:  execution.StateConfirmedSuccess,
		Economic:   execution.EconomicEffectVerified,
		ActualIn:   step.Leg.Input,
		ActualOut:  step.Leg.ExpectedOutput,
		ObservedAt: d.now,
	}, true, nil
}

func TestEVMConfirmationBuffersEventBeforeAwait(t *testing.T) {
	now := time.Date(2026, 7, 27, 20, 0, 0, 0, time.UTC)
	contract := common.HexToAddress("0x1000000000000000000000000000000000000001")
	topic := common.HexToHash("0x01")
	network := &confirmationNetwork{}
	source, err := evmadapter.NewConfirmationSource(evmadapter.ConfirmationSourceConfig{
		Network: network,
		Decoder: confirmationDecoder{
			filter: evmadapter.LogFilter{Address: contract, Topics: []common.Hash{topic}},
			now:    now,
		},
		Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := source.Warm(ctx); err != nil {
		t.Fatal(err)
	}
	hash := common.HexToHash("0x02")
	network.output <- types.Log{Address: contract, Topics: []common.Hash{topic}, TxHash: hash, Data: []byte{1}}
	input, _ := market.NewTokenAmount("quote", big.NewInt(100))
	output, _ := market.NewTokenAmount("base", big.NewInt(145))
	step := execution.OperationStep{
		Leg: execution.Leg{
			ID: "buy", Side: execution.LegBuy, Chain: "synthetic", Account: "account",
			Market: "market", Input: input, ExpectedOutput: output,
		},
		Identity: execution.TransactionIdentity{
			Chain: "synthetic", Account: "account", Hash: hash.Hex(),
		},
	}
	settlement, err := source.Await(context.Background(), step)
	if err != nil {
		t.Fatal(err)
	}
	if settlement.Technical != execution.StateConfirmedSuccess ||
		settlement.Economic != execution.EconomicEffectVerified ||
		settlement.ActualOut.Units().Cmp(output.Units()) != 0 {
		t.Fatalf("settlement = %+v", settlement)
	}
}

var _ evmadapter.Network = (*confirmationNetwork)(nil)
