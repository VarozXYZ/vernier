package livecanary_test

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	chainport "github.com/VarozXYZ/vernier/ports/chain"
	crosschainport "github.com/VarozXYZ/vernier/ports/crosschain"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	"github.com/VarozXYZ/vernier/runtime/livecanary"
)

type primaryConversionManager struct {
	*settledTxManager
	primary int
}

func (m *primaryConversionManager) BroadcastPrimary(ctx context.Context,
	prepared chainport.PreparedTransaction) (chainport.BroadcastResult, error) {
	m.primary++
	return m.settledTxManager.Broadcast(ctx, prepared)
}

type recordingBridge struct {
	requests []execution.SequentialStageRequest
}

func (b *recordingBridge) Transfer(_ context.Context, request execution.SequentialStageRequest,
	_ executionport.SequentialJournal) (crosschainport.LiveTransferResult, error) {
	b.requests = append(b.requests, request)
	output, _ := market.NewTokenAmount(request.Stage.OutputToken, big.NewInt(498_000_000))
	destination := execution.TransactionIdentity{Chain: request.Stage.DestinationChain, Account: "account", Hash: "bridge-destination"}
	return crosschainport.LiveTransferResult{ActualInput: request.Input, ActualOutput: output,
		SourceIdentity:      execution.TransactionIdentity{Chain: request.Stage.SourceChain, Account: "account", Hash: "bridge-source"},
		DestinationIdentity: destination, ObservedAt: time.Now().UTC(), Evidence: "bridge"}, nil
}

func (b *recordingBridge) RecoverTransfer(ctx context.Context, request execution.SequentialStageRequest,
	_ []executionport.SequentialTransactionRecord, journal executionport.SequentialJournal,
) (crosschainport.LiveTransferResult, error) {
	return b.Transfer(ctx, request, journal)
}

func TestConversionAwareTransferConvertsBeforeOutboundAcrossUsingPrimaryBroadcast(t *testing.T) {
	now := time.Now().UTC()
	journal := &durableJournal{}
	converted, _ := market.NewTokenAmount("transit-b", new(big.Int).Mul(big.NewInt(499), big.NewInt(1e18)))
	baseManager := &settledTxManager{journal: journal, actualOutput: converted}
	manager := &primaryConversionManager{settledTxManager: baseManager}
	validator := &fixedOutputValidator{now: now, output: converted.Units()}
	bridge := &recordingBridge{}
	service, err := livecanary.NewConversionAwareTransfer(livecanary.ConversionAwareTransfer{
		Bridge: bridge, ConversionChain: "chain-b", OperationalToken: "operational-b",
		TransitToken: "transit-b", Market: "conversion-b", SlippageBPS: 100,
		Binding: livecanary.SwapBinding{Account: "account", ConversionValidator: validator,
			ConversionEstimator: livecanary.SwapQuoteEstimatorFunc(func(context.Context, market.TokenAmount, market.TokenID) (market.TokenAmount, error) {
				return converted, nil
			}), TxManager: manager}, Clock: func() time.Time { return now }, PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	input, _ := market.NewTokenAmount("operational-b", new(big.Int).Mul(big.NewInt(500), big.NewInt(1e18)))
	request := execution.SequentialStageRequest{Operation: "operation", Plan: "plan", Input: input,
		Stage: execution.SequentialStagePlan{Ordinal: 4, Stage: execution.StageBridgeQuoteReturn,
			SourceChain: "chain-b", DestinationChain: "chain-a", InputToken: input.Token(), OutputToken: "quote-a"}}
	result, err := service.Transfer(context.Background(), request, journal)
	if err != nil {
		t.Fatal(err)
	}
	if manager.primary != 1 || manager.broadcasts != 1 {
		t.Fatalf("conversion broadcasts: primary=%d total=%d", manager.primary, manager.broadcasts)
	}
	if len(bridge.requests) != 1 || bridge.requests[0].Input.Token() != "transit-b" ||
		bridge.requests[0].Input.Units().Cmp(converted.Units()) != 0 {
		t.Fatalf("Across did not receive confirmed transit output: %#v", bridge.requests)
	}
	if result.ActualInput.Token() != "operational-b" || result.ActualOutput.Token() != "quote-a" ||
		len(journal.phases) != 1 || journal.phases[0] != "quote_convert_source" {
		t.Fatalf("unexpected wrapped transfer result: %#v phases=%v", result, journal.phases)
	}
}
