package wormholewtt

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"time"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	chainport "github.com/VarozXYZ/vernier/ports/chain"
	crosschainport "github.com/VarozXYZ/vernier/ports/crosschain"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

var (
	logMessagePublishedTopic = crypto.Keccak256Hash([]byte("LogMessagePublished(address,uint64,uint32,bytes,uint8)"))
	erc20TransferTopic       = crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))
)

type LiveChain struct {
	ID            market.ChainID
	WormholeID    uint16
	CoreBridge    common.Address
	TokenBridge   common.Address
	Token         market.Token
	TokenAddress  common.Address
	Owner         common.Address
	Manager       chainport.TxManager
	Client        *ethclient.Client
	NativeAsset   market.AssetID
	TransferGas   uint64
	RedemptionGas uint64
}

type LiveServiceConfig struct {
	Chains       map[market.ChainID]LiveChain
	Attestations crosschainport.AttestationSource
	Clock        func() time.Time
	PollInterval time.Duration
	Timeout      time.Duration
	Trace        func(string)
}

type LiveService struct{ config LiveServiceConfig }

// MessageFee returns the current source-chain Wormhole Core fee without
// preparing or broadcasting a transfer.
func (s *LiveService) MessageFee(ctx context.Context, source market.ChainID) (*big.Int, time.Time, error) {
	chain, ok := s.config.Chains[source]
	if !ok {
		return nil, time.Time{}, fmt.Errorf("WTT message-fee source chain is unavailable")
	}
	fee, err := readMessageFee(ctx, chain.Client, chain.CoreBridge)
	if err != nil {
		return nil, time.Time{}, err
	}
	return new(big.Int).Set(fee), s.config.Clock().UTC(), nil
}

// EstimateTransferGas builds the same source-chain transferTokens call used
// by Live and asks the node to execute a read-only gas estimate. It neither
// prepares a transaction nor consumes a nonce.
func (s *LiveService) EstimateTransferGas(ctx context.Context, sourceID, destinationID market.ChainID,
	amount market.TokenAmount) (uint64, error) {
	source, sourceOK := s.config.Chains[sourceID]
	destination, destinationOK := s.config.Chains[destinationID]
	if !sourceOK || !destinationOK || sourceID == destinationID || amount.Token() != source.Token.ID {
		return 0, fmt.Errorf("WTT gas probe does not match configured chains")
	}
	transferable, _, err := TrimTransferAmount(amount.Units(), source.Token.Decimals)
	if err != nil {
		return 0, err
	}
	var recipient [32]byte
	copy(recipient[12:], destination.Owner.Bytes())
	adapter, err := NewEVMAdapter(source.TokenBridge)
	if err != nil {
		return 0, err
	}
	payload, err := adapter.BuildTransfer(source.TokenAddress, transferable, destination.WormholeID, recipient, 0)
	if err != nil {
		return 0, err
	}
	messageFee, err := readMessageFee(ctx, source.Client, source.CoreBridge)
	if err != nil {
		return 0, err
	}
	return source.Client.EstimateGas(ctx, geth.CallMsg{From: source.Owner, To: &source.TokenBridge,
		Data: payload, Value: messageFee})
}

// RedemptionGasFloor is the configured technical limit used until a real
// confirmed redeem receipt is available for high-water calibration.
func (s *LiveService) RedemptionGasFloor(chainID market.ChainID) (uint64, error) {
	chain, ok := s.config.Chains[chainID]
	if !ok {
		return 0, fmt.Errorf("WTT redemption chain is unavailable")
	}
	if chain.RedemptionGas > 0 {
		return chain.RedemptionGas, nil
	}
	return 1_500_000, nil
}

func NewLiveService(config LiveServiceConfig) (*LiveService, error) {
	if len(config.Chains) != 2 || config.Attestations == nil {
		return nil, fmt.Errorf("WTT Live service requires two EVM chains and an attestation source")
	}
	for id, chain := range config.Chains {
		if id == "" || chain.ID != id || chain.WormholeID == 0 || chain.CoreBridge == (common.Address{}) ||
			chain.TokenBridge == (common.Address{}) || chain.Token.ID == "" || chain.Token.Chain != id || chain.TokenAddress == (common.Address{}) ||
			chain.Owner == (common.Address{}) || chain.Manager == nil || chain.Client == nil || chain.NativeAsset == "" {
			return nil, fmt.Errorf("WTT Live chain %q is incomplete", id)
		}
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	if config.PollInterval <= 0 {
		config.PollInterval = 500 * time.Millisecond
	}
	if config.Timeout <= 0 {
		config.Timeout = 10 * time.Minute
	}
	return &LiveService{config: config}, nil
}

func (s *LiveService) Transfer(ctx context.Context, request execution.SequentialStageRequest,
	journal executionport.SequentialJournal) (crosschainport.LiveTransferResult, error) {
	return s.transfer(ctx, request, nil, journal)
}

func (s *LiveService) RecoverTransfer(ctx context.Context, request execution.SequentialStageRequest,
	records []executionport.SequentialTransactionRecord, journal executionport.SequentialJournal,
) (crosschainport.LiveTransferResult, error) {
	return s.transfer(ctx, request, records, journal)
}

func (s *LiveService) transfer(ctx context.Context, request execution.SequentialStageRequest,
	records []executionport.SequentialTransactionRecord, journal executionport.SequentialJournal,
) (crosschainport.LiveTransferResult, error) {
	if err := request.Validate(); err != nil {
		return crosschainport.LiveTransferResult{}, err
	}
	s.trace("validated")
	source, sourceOK := s.config.Chains[request.Stage.SourceChain]
	destination, destinationOK := s.config.Chains[request.Stage.DestinationChain]
	if !sourceOK || !destinationOK || source.ID == destination.ID || request.Input.Token() != source.Token.ID ||
		request.Stage.OutputToken != destination.Token.ID || journal == nil {
		return crosschainport.LiveTransferResult{}, fmt.Errorf("WTT transfer request does not match configured chains")
	}
	transferable, _, err := TrimTransferAmount(request.Input.Units(), source.Token.Decimals)
	if err != nil {
		return crosschainport.LiveTransferResult{}, err
	}
	actualInput, _ := market.NewTokenAmount(source.Token.ID, transferable)
	expectedUnits := convertDecimals(transferable, source.Token.Decimals, destination.Token.Decimals)
	expectedOutput, _ := market.NewTokenAmount(destination.Token.ID, expectedUnits)
	var recipient [32]byte
	copy(recipient[12:], destination.Owner.Bytes())

	sourceRecord, hasSource := transactionByPhase(records, "wtt_source")
	var sourceIdentity execution.TransactionIdentity
	var sourceReceipt *types.Receipt
	if hasSource {
		s.trace("source_reconcile_started")
		sourceIdentity = sourceRecord.Identity
		sourceReceipt, err = s.awaitReceipt(ctx, source.Client, sourceIdentity.Hash)
	} else {
		messageFee, feeErr := readMessageFee(ctx, source.Client, source.CoreBridge)
		if feeErr != nil {
			return crosschainport.LiveTransferResult{}, feeErr
		}
		adapter, adapterErr := NewEVMAdapter(source.TokenBridge)
		if adapterErr != nil {
			return crosschainport.LiveTransferResult{}, adapterErr
		}
		var nonceBytes [4]byte
		if _, err = rand.Read(nonceBytes[:]); err != nil {
			return crosschainport.LiveTransferResult{}, err
		}
		payload, buildErr := adapter.BuildTransfer(source.TokenAddress, transferable, destination.WormholeID,
			recipient, binary.BigEndian.Uint32(nonceBytes[:]))
		if buildErr != nil {
			return crosschainport.LiveTransferResult{}, buildErr
		}
		gas := source.TransferGas
		if gas == 0 {
			gas = 500_000
		}
		artifact := bridgeArtifact(request, source, actualInput, expectedOutput, source.TokenBridge, payload, messageFee, gas, "wtt-source")
		prepared, prepareErr := source.Manager.Prepare(ctx, artifact)
		if prepareErr != nil {
			return crosschainport.LiveTransferResult{}, prepareErr
		}
		if err = journal.RecordPreparedTransaction(ctx, executionport.PreparedTransaction{Operation: request.Operation,
			Ordinal: request.Stage.Ordinal, Phase: "wtt_source", Identity: prepared.Identity, PreparedAt: prepared.PreparedAt}); err != nil {
			return crosschainport.LiveTransferResult{}, err
		}
		sourceIdentity = prepared.Identity
		broadcast, broadcastErr := chainport.BroadcastPrimary(ctx, source.Manager, prepared)
		if broadcast.Disposition == chainport.BroadcastRejected {
			_ = journal.MarkTransaction(context.WithoutCancel(ctx), request.Operation, request.Stage.Ordinal, "wtt_source", "broadcast_rejected")
			return crosschainport.LiveTransferResult{}, executionport.NewStageError(executionport.DispositionRejected, broadcastErr)
		}
		status := "broadcast"
		if broadcast.Disposition == chainport.BroadcastPossible {
			status = "outcome_unknown"
		}
		if err = journal.MarkTransaction(context.WithoutCancel(ctx), request.Operation, request.Stage.Ordinal, "wtt_source", status); err != nil {
			return crosschainport.LiveTransferResult{}, err
		}
		sourceReceipt, err = s.awaitReceipt(ctx, source.Client, sourceIdentity.Hash)
	}
	s.trace("source_receipt_confirmed")
	if err != nil {
		return crosschainport.LiveTransferResult{}, err
	}
	if sourceReceipt.Status != types.ReceiptStatusSuccessful {
		_ = journal.MarkTransaction(context.WithoutCancel(ctx), request.Operation, request.Stage.Ordinal, "wtt_source", "confirmed_revert")
		return crosschainport.LiveTransferResult{}, executionport.NewStageError(executionport.DispositionConfirmedFailure, fmt.Errorf("WTT source transaction reverted"))
	}
	_ = journal.MarkTransaction(context.WithoutCancel(ctx), request.Operation, request.Stage.Ordinal, "wtt_source", "confirmed")
	messageFeePaid, err := transactionValue(ctx, source.Client, sourceIdentity.Hash)
	if err != nil {
		return crosschainport.LiveTransferResult{}, fmt.Errorf("read confirmed WTT message fee: %w", err)
	}
	message, err := ParseMessageID(sourceReceipt, source.CoreBridge, source.TokenBridge, source.WormholeID)
	if err != nil {
		return crosschainport.LiveTransferResult{}, err
	}
	attestation, err := s.config.Attestations.Await(ctx, message)
	if err != nil {
		return crosschainport.LiveTransferResult{}, err
	}
	s.trace("attestation_ready")

	s.trace("destination_balance_before_started")
	readCtx, cancelRead := context.WithTimeout(ctx, 10*time.Second)
	before, err := readTokenBalance(readCtx, destination.Client, destination.TokenAddress, destination.Owner)
	cancelRead()
	if err != nil {
		return crosschainport.LiveTransferResult{}, err
	}
	s.trace("destination_balance_before_ready")
	destinationRecord, hasDestination := transactionByPhase(records, "wtt_redeem")
	var destinationIdentity execution.TransactionIdentity
	var destinationReceipt *types.Receipt
	if hasDestination {
		s.trace("destination_reconcile_started")
		destinationIdentity = destinationRecord.Identity
		destinationReceipt, err = s.awaitReceipt(ctx, destination.Client, destinationIdentity.Hash)
	} else {
		adapter, adapterErr := NewEVMAdapter(destination.TokenBridge)
		if adapterErr != nil {
			return crosschainport.LiveTransferResult{}, adapterErr
		}
		completed, checkErr := transferCompleted(ctx, destination.Client, adapter, attestation.Raw)
		if checkErr != nil {
			return crosschainport.LiveTransferResult{}, checkErr
		}
		if completed {
			return crosschainport.LiveTransferResult{}, executionport.NewRecoveryError(executionport.RecoveryFailureUncertain,
				fmt.Errorf("WTT VAA is consumed but destination transaction identity is unavailable"))
		}
		payload, buildErr := adapter.BuildRedeem(attestation.Raw)
		if buildErr != nil {
			return crosschainport.LiveTransferResult{}, buildErr
		}
		gas := destination.RedemptionGas
		if gas == 0 {
			gas = 1_500_000
		}
		artifact := bridgeArtifact(request, destination, actualInput, expectedOutput, destination.TokenBridge, payload, new(big.Int), gas, "wtt-redeem")
		prepared, prepareErr := destination.Manager.Prepare(ctx, artifact)
		if prepareErr != nil {
			return crosschainport.LiveTransferResult{}, prepareErr
		}
		if err = journal.RecordPreparedTransaction(ctx, executionport.PreparedTransaction{Operation: request.Operation,
			Ordinal: request.Stage.Ordinal, Phase: "wtt_redeem", Identity: prepared.Identity, PreparedAt: prepared.PreparedAt}); err != nil {
			return crosschainport.LiveTransferResult{}, err
		}
		destinationIdentity = prepared.Identity
		broadcast, broadcastErr := chainport.BroadcastPrimary(ctx, destination.Manager, prepared)
		if broadcast.Disposition == chainport.BroadcastRejected {
			_ = journal.MarkTransaction(context.WithoutCancel(ctx), request.Operation, request.Stage.Ordinal, "wtt_redeem", "broadcast_rejected")
			return crosschainport.LiveTransferResult{}, executionport.NewStageError(executionport.DispositionRejected, broadcastErr)
		}
		status := "broadcast"
		if broadcast.Disposition == chainport.BroadcastPossible {
			status = "outcome_unknown"
		}
		if err = journal.MarkTransaction(context.WithoutCancel(ctx), request.Operation, request.Stage.Ordinal, "wtt_redeem", status); err != nil {
			return crosschainport.LiveTransferResult{}, err
		}
		destinationReceipt, err = s.awaitReceipt(ctx, destination.Client, destinationIdentity.Hash)
	}
	if err != nil {
		return crosschainport.LiveTransferResult{}, err
	}
	if destinationReceipt.Status != types.ReceiptStatusSuccessful {
		_ = journal.MarkTransaction(context.WithoutCancel(ctx), request.Operation, request.Stage.Ordinal, "wtt_redeem", "confirmed_revert")
		return crosschainport.LiveTransferResult{}, executionport.NewStageError(executionport.DispositionConfirmedFailure, fmt.Errorf("WTT redeem transaction reverted"))
	}
	_ = journal.MarkTransaction(context.WithoutCancel(ctx), request.Operation, request.Stage.Ordinal, "wtt_redeem", "confirmed")
	s.trace("destination_receipt_confirmed")
	s.trace("destination_balance_after_started")
	readCtx, cancelRead = context.WithTimeout(ctx, 10*time.Second)
	after, err := readTokenBalance(readCtx, destination.Client, destination.TokenAddress, destination.Owner)
	cancelRead()
	if err != nil {
		return crosschainport.LiveTransferResult{}, err
	}
	s.trace("destination_balance_after_ready")
	delivered, err := deliveredTo(destinationReceipt, destination.TokenAddress, destination.Owner)
	if err != nil {
		return crosschainport.LiveTransferResult{}, err
	}
	if delivered.Cmp(expectedUnits) != 0 {
		return crosschainport.LiveTransferResult{}, fmt.Errorf("WTT redeem delivered %s units, expected %s", delivered, expectedUnits)
	}
	actualOutput, _ := market.NewTokenAmount(destination.Token.ID, delivered)
	messageFeeCost := nativeValueCost(messageFeePaid, source.NativeAsset, source.ID, "wtt_message_fee", "evm_transaction_value")
	return crosschainport.LiveTransferResult{ActualInput: actualInput, ActualOutput: actualOutput,
		Costs: []execution.CostComponent{receiptCost(sourceReceipt, source.NativeAsset, source.ID, "wtt_source_gas"),
			messageFeeCost, receiptCost(destinationReceipt, destination.NativeAsset, destination.ID, "wtt_redeem_gas")},
		SourceIdentity: sourceIdentity, DestinationIdentity: destinationIdentity,
		DestinationBalanceBefore: before, DestinationBalanceAfter: after,
		ObservedAt: s.config.Clock().UTC(), Evidence: fmt.Sprintf("wormhole_wtt:%d/%x/%d", message.EmitterChain, message.EmitterAddress, message.Sequence)}, nil
}

func (s *LiveService) trace(phase string) {
	if s.config.Trace != nil {
		s.config.Trace(phase)
	}
}

func deliveredTo(receipt *types.Receipt, token, recipient common.Address) (*big.Int, error) {
	total := new(big.Int)
	if receipt != nil {
		for _, log := range receipt.Logs {
			if log == nil || log.Address != token || len(log.Topics) < 3 || log.Topics[0] != erc20TransferTopic ||
				common.BytesToAddress(log.Topics[2].Bytes()[12:]) != recipient || len(log.Data) < 32 {
				continue
			}
			total.Add(total, new(big.Int).SetBytes(log.Data[:32]))
		}
	}
	if total.Sign() <= 0 {
		return nil, fmt.Errorf("WTT redeem receipt contains no attributable token delivery")
	}
	return total, nil
}

func bridgeArtifact(request execution.SequentialStageRequest, chain LiveChain, input, output market.TokenAmount,
	to common.Address, payload []byte, value *big.Int, gas uint64, id execution.StepID) executionport.Artifact {
	leg := execution.Leg{ID: id, Side: execution.LegSell, Chain: chain.ID, Account: chain.Manager.Account(),
		Market: market.MarketID(id), Input: input, ExpectedOutput: output}
	return executionport.Artifact{Leg: leg, Payload: payload, Metadata: map[string]string{
		"to": to.Hex(), "value": value.String(), "gas_limit": fmt.Sprint(gas),
	}, BuiltAt: time.Now().UTC()}
}

func (s *LiveService) awaitReceipt(ctx context.Context, client *ethclient.Client, hashText string) (*types.Receipt, error) {
	if !common.IsHexHash(hashText) {
		return nil, fmt.Errorf("WTT transaction hash is invalid")
	}
	waitCtx, cancel := context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()
	ticker := time.NewTicker(s.config.PollInterval)
	defer ticker.Stop()
	for {
		receipt, err := client.TransactionReceipt(waitCtx, common.HexToHash(hashText))
		if err == nil && receipt != nil {
			return receipt, nil
		}
		if err != nil && !errors.Is(err, geth.NotFound) {
			return nil, err
		}
		select {
		case <-waitCtx.Done():
			return nil, fmt.Errorf("WTT receipt outcome unknown: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func ParseMessageID(receipt *types.Receipt, core, emitter common.Address, wormholeChain uint16) (crosschainport.MessageID, error) {
	if receipt == nil || core == (common.Address{}) || emitter == (common.Address{}) || wormholeChain == 0 {
		return crosschainport.MessageID{}, fmt.Errorf("WTT message receipt configuration is incomplete")
	}
	for _, log := range receipt.Logs {
		if log == nil || log.Address != core || len(log.Topics) < 2 || log.Topics[0] != logMessagePublishedTopic ||
			common.BytesToAddress(log.Topics[1].Bytes()[12:]) != emitter || len(log.Data) < 32 {
			continue
		}
		var emitterAddress [32]byte
		copy(emitterAddress[12:], emitter.Bytes())
		return crosschainport.MessageID{EmitterChain: wormholeChain, EmitterAddress: emitterAddress,
			Sequence: new(big.Int).SetBytes(log.Data[:32]).Uint64()}, nil
	}
	return crosschainport.MessageID{}, fmt.Errorf("WTT source receipt contains no matching Wormhole message")
}

func readMessageFee(ctx context.Context, client *ethclient.Client, core common.Address) (*big.Int, error) {
	selector := crypto.Keccak256([]byte("messageFee()"))[:4]
	raw, err := client.CallContract(ctx, geth.CallMsg{To: &core, Data: selector}, nil)
	if err != nil {
		return nil, err
	}
	if len(raw) != 32 {
		return nil, fmt.Errorf("wormhole messageFee response is invalid")
	}
	return new(big.Int).SetBytes(raw), nil
}

func readTokenBalance(ctx context.Context, client *ethclient.Client, token, owner common.Address) (*big.Int, error) {
	payload := append(crypto.Keccak256([]byte("balanceOf(address)"))[:4], common.LeftPadBytes(owner.Bytes(), 32)...)
	raw, err := client.CallContract(ctx, geth.CallMsg{To: &token, Data: payload}, nil)
	if err != nil {
		return nil, err
	}
	if len(raw) != 32 {
		return nil, fmt.Errorf("ERC-20 balance response is invalid")
	}
	return new(big.Int).SetBytes(raw), nil
}

func transferCompleted(ctx context.Context, client *ethclient.Client, adapter *EVMAdapter, vaa []byte) (bool, error) {
	payload, _, err := adapter.BuildCompletionCheck(vaa)
	if err != nil {
		return false, err
	}
	bridge := adapter.Bridge()
	raw, err := client.CallContract(ctx, geth.CallMsg{To: &bridge, Data: payload}, nil)
	if err != nil {
		return false, err
	}
	if len(raw) != 32 {
		return false, fmt.Errorf("WTT completion response is invalid")
	}
	return new(big.Int).SetBytes(raw).Sign() != 0, nil
}

func convertDecimals(amount *big.Int, from, to uint8) *big.Int {
	result := new(big.Int).Set(amount)
	if from > to {
		return result.Quo(result, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(from-to)), nil))
	}
	if to > from {
		return result.Mul(result, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(to-from)), nil))
	}
	return result
}

func receiptCost(receipt *types.Receipt, asset market.AssetID, chain market.ChainID, kind string) execution.CostComponent {
	units := new(big.Int)
	if receipt != nil && receipt.EffectiveGasPrice != nil {
		units.Mul(new(big.Int).SetUint64(receipt.GasUsed), receipt.EffectiveGasPrice)
	}
	amount, _ := market.NewAssetQuantity(asset, new(big.Rat).SetFrac(units, new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)))
	return execution.CostComponent{Kind: kind, Chain: chain, Amount: amount, Evidence: "evm_receipt"}
}

func nativeValueCost(units *big.Int, asset market.AssetID, chain market.ChainID, kind, evidence string) execution.CostComponent {
	if units == nil {
		units = new(big.Int)
	}
	amount, _ := market.NewAssetQuantity(asset, new(big.Rat).SetFrac(new(big.Int).Set(units), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)))
	return execution.CostComponent{Kind: kind, Chain: chain, Amount: amount, Evidence: evidence}
}

func transactionValue(ctx context.Context, client *ethclient.Client, hashText string) (*big.Int, error) {
	if client == nil || !common.IsHexHash(hashText) {
		return nil, fmt.Errorf("WTT transaction identity is invalid")
	}
	transaction, _, err := client.TransactionByHash(ctx, common.HexToHash(hashText))
	if err != nil {
		return nil, err
	}
	if transaction == nil || transaction.Value() == nil {
		return nil, fmt.Errorf("WTT transaction value is unavailable")
	}
	return new(big.Int).Set(transaction.Value()), nil
}

func transactionByPhase(records []executionport.SequentialTransactionRecord, phase string) (executionport.SequentialTransactionRecord, bool) {
	for _, record := range records {
		if record.Phase == phase {
			return record, true
		}
	}
	return executionport.SequentialTransactionRecord{}, false
}

var _ crosschainport.RecoverableLiveTransferService = (*LiveService)(nil)
