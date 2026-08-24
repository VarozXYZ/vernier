package across

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
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

var erc20TransferTopic = crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))

const spokePoolDepositABI = `[
 {"type":"function","name":"depositV3","stateMutability":"payable","inputs":[{"name":"depositor","type":"address"},{"name":"recipient","type":"address"},{"name":"inputToken","type":"address"},{"name":"outputToken","type":"address"},{"name":"inputAmount","type":"uint256"},{"name":"outputAmount","type":"uint256"},{"name":"destinationChainId","type":"uint256"},{"name":"exclusiveRelayer","type":"address"},{"name":"quoteTimestamp","type":"uint32"},{"name":"fillDeadline","type":"uint32"},{"name":"exclusivityDeadline","type":"uint32"},{"name":"message","type":"bytes"}],"outputs":[]},
 {"type":"function","name":"deposit","stateMutability":"payable","inputs":[{"name":"depositor","type":"bytes32"},{"name":"recipient","type":"bytes32"},{"name":"inputToken","type":"bytes32"},{"name":"outputToken","type":"bytes32"},{"name":"inputAmount","type":"uint256"},{"name":"outputAmount","type":"uint256"},{"name":"destinationChainId","type":"uint256"},{"name":"exclusiveRelayer","type":"bytes32"},{"name":"quoteTimestamp","type":"uint32"},{"name":"fillDeadline","type":"uint32"},{"name":"exclusivityDeadline","type":"uint32"},{"name":"message","type":"bytes"}],"outputs":[]},
 {"type":"function","name":"depositV3Now","stateMutability":"payable","inputs":[{"name":"depositor","type":"address"},{"name":"recipient","type":"address"},{"name":"inputToken","type":"address"},{"name":"outputToken","type":"address"},{"name":"inputAmount","type":"uint256"},{"name":"outputAmount","type":"uint256"},{"name":"destinationChainId","type":"uint256"},{"name":"exclusiveRelayer","type":"address"},{"name":"fillDeadlineOffset","type":"uint32"},{"name":"exclusivityDeadline","type":"uint32"},{"name":"message","type":"bytes"}],"outputs":[]},
 {"type":"function","name":"depositNow","stateMutability":"payable","inputs":[{"name":"depositor","type":"bytes32"},{"name":"recipient","type":"bytes32"},{"name":"inputToken","type":"bytes32"},{"name":"outputToken","type":"bytes32"},{"name":"inputAmount","type":"uint256"},{"name":"outputAmount","type":"uint256"},{"name":"destinationChainId","type":"uint256"},{"name":"exclusiveRelayer","type":"bytes32"},{"name":"fillDeadlineOffset","type":"uint32"},{"name":"exclusivityDeadline","type":"uint32"},{"name":"message","type":"bytes"}],"outputs":[]}
]`

type EVMLiveChain struct {
	ID               market.ChainID
	ChainID          uint64
	Token            market.Token
	TokenAddress     common.Address
	Owner            common.Address
	AllowedContracts []common.Address
	Manager          chainport.TxManager
	Client           *ethclient.Client
	NativeAsset      market.AssetID
}

type EVMLiveServiceConfig struct {
	Client       *Client
	Chains       map[market.ChainID]EVMLiveChain
	Clock        func() time.Time
	PollInterval time.Duration
	Timeout      time.Duration
}

type EVMLiveService struct{ config EVMLiveServiceConfig }

// EVMCostApproval is the normalized read-only evidence needed by the
// complete-flow cost oracle. It deliberately excludes calldata and raw
// provider responses.
type EVMCostApproval struct {
	InputUnits          *big.Int
	ExpectedOutputUnits *big.Int
	SourceGas           uint64
	ObservedAt          time.Time
}

func NewEVMLiveService(config EVMLiveServiceConfig) (*EVMLiveService, error) {
	if config.Client == nil || len(config.Chains) != 2 {
		return nil, fmt.Errorf("across EVM Live service requires a client and two chains")
	}
	for id, chain := range config.Chains {
		if id == "" || chain.ID != id || chain.ChainID == 0 || chain.Token.ID == "" || chain.Token.Chain != id ||
			chain.TokenAddress == (common.Address{}) || chain.Owner == (common.Address{}) || len(chain.AllowedContracts) == 0 ||
			chain.Manager == nil || chain.Client == nil || chain.NativeAsset == "" {
			return nil, fmt.Errorf("across EVM Live chain %q is incomplete", id)
		}
		for _, allowed := range chain.AllowedContracts {
			if allowed == (common.Address{}) {
				return nil, fmt.Errorf("across contract allowlist is invalid")
			}
		}
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	if config.PollInterval <= 0 {
		config.PollInterval = time.Second
	}
	if config.Timeout <= 0 {
		config.Timeout = 20 * time.Minute
	}
	return &EVMLiveService{config: config}, nil
}

func (s *EVMLiveService) Transfer(ctx context.Context, request execution.SequentialStageRequest,
	journal executionport.SequentialJournal) (crosschainport.LiveTransferResult, error) {
	return s.transfer(ctx, request, nil, journal)
}

func (s *EVMLiveService) RecoverTransfer(ctx context.Context, request execution.SequentialStageRequest,
	records []executionport.SequentialTransactionRecord, journal executionport.SequentialJournal,
) (crosschainport.LiveTransferResult, error) {
	return s.transfer(ctx, request, records, journal)
}

// CostApproval requests and validates the same Across envelope used for
// execution without preparing, signing, or broadcasting it.
func (s *EVMLiveService) CostApproval(ctx context.Context, sourceID market.ChainID, amount *big.Int) (EVMCostApproval, error) {
	if amount == nil || amount.Sign() <= 0 {
		return EVMCostApproval{}, fmt.Errorf("across cost amount must be positive")
	}
	source, ok := s.config.Chains[sourceID]
	if !ok {
		return EVMCostApproval{}, fmt.Errorf("across cost source chain is unavailable")
	}
	var destination EVMLiveChain
	for id, candidate := range s.config.Chains {
		if id != sourceID {
			destination = candidate
			break
		}
	}
	if destination.ID == "" {
		return EVMCostApproval{}, fmt.Errorf("across cost destination chain is unavailable")
	}
	approval, err := s.config.Client.Approval(ctx, ApprovalRequest{
		OriginChainID: source.ChainID, DestinationChainID: destination.ChainID,
		InputToken: source.TokenAddress.Hex(), OutputToken: destination.TokenAddress.Hex(),
		Amount: amount.String(), Depositor: source.Owner.Hex(), Recipient: destination.Owner.Hex(),
		RefundAddress: source.Owner.Hex(), Slippage: "auto", CostOnly: true,
	})
	if err != nil {
		return EVMCostApproval{}, err
	}
	if err := validateAcrossEnvelope(approval, source, destination, amount); err != nil {
		return EVMCostApproval{}, err
	}
	expected, ok := new(big.Int).SetString(approval.ExpectedOutputAmount, 10)
	if !ok || expected.Sign() <= 0 {
		return EVMCostApproval{}, fmt.Errorf("across cost expected output is invalid")
	}
	inputHuman := new(big.Rat).SetFrac(amount, tokenDecimalScale(source.Token.Decimals))
	outputHuman := new(big.Rat).SetFrac(expected, tokenDecimalScale(destination.Token.Decimals))
	if outputHuman.Cmp(inputHuman) > 0 {
		return EVMCostApproval{}, fmt.Errorf("across cost expected output exceeds its human input")
	}
	gasValue, gasOK := parseRPCInteger(approval.SwapTransaction.Gas)
	var gas uint64
	if gasOK && gasValue.IsUint64() {
		gas = gasValue.Uint64()
	}
	return EVMCostApproval{InputUnits: new(big.Int).Set(amount), ExpectedOutputUnits: expected,
		SourceGas: gas, ObservedAt: approval.ObservedAt.UTC()}, nil
}

func (s *EVMLiveService) transfer(ctx context.Context, request execution.SequentialStageRequest,
	records []executionport.SequentialTransactionRecord, journal executionport.SequentialJournal,
) (crosschainport.LiveTransferResult, error) {
	if err := request.Validate(); err != nil {
		return crosschainport.LiveTransferResult{}, err
	}
	source, sourceOK := s.config.Chains[request.Stage.SourceChain]
	destination, destinationOK := s.config.Chains[request.Stage.DestinationChain]
	if !sourceOK || !destinationOK || source.ID == destination.ID || request.Input.Token() != source.Token.ID ||
		request.Stage.OutputToken != destination.Token.ID || journal == nil {
		return crosschainport.LiveTransferResult{}, fmt.Errorf("across transfer request does not match configured chains")
	}
	before, err := evmTokenBalance(ctx, destination.Client, destination.TokenAddress, destination.Owner)
	if err != nil {
		return crosschainport.LiveTransferResult{}, err
	}
	record, hasRecord := acrossTransactionByPhase(records, "across_source")
	var identity execution.TransactionIdentity
	var sourceReceipt *types.Receipt
	if hasRecord {
		identity = record.Identity
		sourceReceipt, err = s.awaitReceipt(ctx, source.Client, identity.Hash)
	} else {
		approval, approvalErr := s.config.Client.Approval(ctx, ApprovalRequest{OriginChainID: source.ChainID,
			DestinationChainID: destination.ChainID, InputToken: source.TokenAddress.Hex(), OutputToken: destination.TokenAddress.Hex(),
			Amount: request.Input.Units().String(), Depositor: source.Owner.Hex(), Recipient: destination.Owner.Hex(),
			RefundAddress: source.Owner.Hex(), Slippage: "auto"})
		if approvalErr != nil {
			return crosschainport.LiveTransferResult{}, approvalErr
		}
		if err = validateAcrossEnvelope(approval, source, destination, request.Input.Units()); err != nil {
			return crosschainport.LiveTransferResult{}, err
		}
		payload, _ := hex.DecodeString(strings.TrimPrefix(approval.SwapTransaction.Data, "0x"))
		valueText := strings.TrimSpace(approval.SwapTransaction.Value)
		value, ok := parseRPCInteger(valueText)
		if valueText == "" {
			value, ok = new(big.Int), true
		}
		if !ok || value.Sign() < 0 {
			return crosschainport.LiveTransferResult{}, fmt.Errorf("across transaction value is invalid: %q", approval.SwapTransaction.Value)
		}
		if value.Sign() != 0 {
			return crosschainport.LiveTransferResult{}, fmt.Errorf("across ERC-20 deposit transaction has non-zero native value")
		}
		gas, ok := parseRPCInteger(approval.SwapTransaction.Gas)
		if !ok || !gas.IsUint64() || gas.Sign() <= 0 {
			return crosschainport.LiveTransferResult{}, fmt.Errorf("across transaction gas is invalid")
		}
		expectedUnits, ok := new(big.Int).SetString(approval.MinimumOutputAmount, 10)
		if !ok || expectedUnits.Sign() <= 0 {
			return crosschainport.LiveTransferResult{}, fmt.Errorf("across minimum output is invalid")
		}
		expected, _ := market.NewTokenAmount(destination.Token.ID, expectedUnits)
		leg := execution.Leg{ID: "across-source", Side: execution.LegSell, Chain: source.ID, Account: source.Manager.Account(),
			Market: "across-source", Input: request.Input, ExpectedOutput: expected}
		artifact := executionport.Artifact{Leg: leg, Payload: payload, Metadata: map[string]string{
			"to": approval.SwapTransaction.To, "value": value.String(), "gas_limit": gas.String(),
		}, BuiltAt: s.config.Clock().UTC()}
		prepared, prepareErr := source.Manager.Prepare(ctx, artifact)
		if prepareErr != nil {
			return crosschainport.LiveTransferResult{}, prepareErr
		}
		if err = journal.RecordPreparedTransaction(ctx, executionport.PreparedTransaction{Operation: request.Operation,
			Ordinal: request.Stage.Ordinal, Phase: "across_source", Identity: prepared.Identity, PreparedAt: prepared.PreparedAt}); err != nil {
			return crosschainport.LiveTransferResult{}, err
		}
		identity = prepared.Identity
		broadcast, broadcastErr := chainport.BroadcastPrimary(ctx, source.Manager, prepared)
		if broadcast.Disposition == chainport.BroadcastRejected {
			_ = journal.MarkTransaction(context.WithoutCancel(ctx), request.Operation, request.Stage.Ordinal, "across_source", "broadcast_rejected")
			return crosschainport.LiveTransferResult{}, executionport.NewStageError(executionport.DispositionRejected, broadcastErr)
		}
		state := "broadcast"
		if broadcast.Disposition == chainport.BroadcastPossible {
			state = "outcome_unknown"
		}
		if err = journal.MarkTransaction(context.WithoutCancel(ctx), request.Operation, request.Stage.Ordinal, "across_source", state); err != nil {
			return crosschainport.LiveTransferResult{}, err
		}
		sourceReceipt, err = s.awaitReceipt(ctx, source.Client, identity.Hash)
	}
	if err != nil {
		return crosschainport.LiveTransferResult{}, err
	}
	if sourceReceipt.Status != types.ReceiptStatusSuccessful {
		_ = journal.MarkTransaction(context.WithoutCancel(ctx), request.Operation, request.Stage.Ordinal, "across_source", "confirmed_revert")
		return crosschainport.LiveTransferResult{}, executionport.NewStageError(executionport.DispositionConfirmedFailure, fmt.Errorf("across source transaction reverted"))
	}
	_ = journal.MarkTransaction(context.WithoutCancel(ctx), request.Operation, request.Stage.Ordinal, "across_source", "confirmed")
	status, err := s.awaitFill(ctx, identity.Hash)
	if err != nil {
		return crosschainport.LiveTransferResult{}, err
	}
	if !common.IsHexHash(status.FillTransaction) {
		return crosschainport.LiveTransferResult{}, fmt.Errorf("across filled deposit has no EVM fill identity")
	}
	fillReceipt, err := s.awaitReceipt(ctx, destination.Client, status.FillTransaction)
	if err != nil {
		return crosschainport.LiveTransferResult{}, err
	}
	outputUnits, err := transferTo(fillReceipt, destination.TokenAddress, destination.Owner)
	if err != nil {
		return crosschainport.LiveTransferResult{}, err
	}
	after, balanceErr := evmTokenBalance(ctx, destination.Client, destination.TokenAddress, destination.Owner)
	if balanceErr != nil {
		return crosschainport.LiveTransferResult{}, balanceErr
	}
	actualOutput, _ := market.NewTokenAmount(destination.Token.ID, outputUnits)
	destinationIdentity := execution.TransactionIdentity{Chain: destination.ID, Account: destination.Manager.Account(), Hash: status.FillTransaction}
	spreadCost, err := acrossSpreadCost(source, destination, request.Input.Units(), outputUnits)
	if err != nil {
		return crosschainport.LiveTransferResult{}, err
	}
	return crosschainport.LiveTransferResult{ActualInput: request.Input, ActualOutput: actualOutput,
		Costs: []execution.CostComponent{spreadCost,
			acrossReceiptCost(sourceReceipt, source.NativeAsset, source.ID)},
		SourceIdentity: identity, DestinationIdentity: destinationIdentity, DestinationBalanceBefore: before,
		DestinationBalanceAfter: after, ObservedAt: s.config.Clock().UTC(), Evidence: "across_v4_fill"}, nil
}

func parseRPCInteger(text string) (*big.Int, bool) {
	trimmed := strings.TrimSpace(text)
	base := 10
	if strings.HasPrefix(trimmed, "0x") || strings.HasPrefix(trimmed, "0X") {
		trimmed = trimmed[2:]
		base = 16
	}
	if trimmed == "" {
		return nil, false
	}
	value, ok := new(big.Int).SetString(trimmed, base)
	return value, ok
}

func validateAcrossEnvelope(approval Approval, source, destination EVMLiveChain, inputUnits *big.Int) error {
	spender := common.HexToAddress(approval.Allowance.Spender)
	to := common.HexToAddress(approval.SwapTransaction.To)
	if spender == (common.Address{}) || to == (common.Address{}) || !acrossAllowed(spender, source.AllowedContracts) || !acrossAllowed(to, source.AllowedContracts) {
		return fmt.Errorf("across response uses a contract outside the configured allowlist")
	}
	if approval.Allowance.Token != "" && (!common.IsHexAddress(approval.Allowance.Token) || common.HexToAddress(approval.Allowance.Token) != source.TokenAddress) {
		return fmt.Errorf("across allowance token does not match the configured source token")
	}
	if len(approval.ApprovalTransactions) != 0 {
		return fmt.Errorf("across requested a dynamic approval during armed execution")
	}
	payload, err := hex.DecodeString(strings.TrimPrefix(approval.SwapTransaction.Data, "0x"))
	if err != nil {
		return fmt.Errorf("across deposit calldata is invalid")
	}
	minimum, ok := new(big.Int).SetString(approval.MinimumOutputAmount, 10)
	if !ok || minimum.Sign() <= 0 {
		return fmt.Errorf("across minimum output is invalid")
	}
	return ValidateDepositCalldata(payload, DepositExpectation{Depositor: source.Owner, Recipient: destination.Owner,
		InputToken: source.TokenAddress, OutputToken: destination.TokenAddress, InputAmount: inputUnits,
		MinimumOutput: minimum, DestinationChainID: destination.ChainID})
}

type DepositExpectation struct {
	Depositor, Recipient    common.Address
	InputToken, OutputToken common.Address
	InputAmount             *big.Int
	MinimumOutput           *big.Int
	DestinationChainID      uint64
}

// ValidateDepositCalldata accepts the current EVM-only Across deposit entry
// points and verifies every economic field before signing. The optional
// five-byte integrator suffix documented by Across is ignored only after the
// ABI payload itself has been decoded.
func ValidateDepositCalldata(payload []byte, expected DepositExpectation) error {
	if len(payload) < 4 || expected.Depositor == (common.Address{}) || expected.Recipient == (common.Address{}) ||
		expected.InputToken == (common.Address{}) || expected.OutputToken == (common.Address{}) ||
		expected.InputAmount == nil || expected.InputAmount.Sign() <= 0 || expected.MinimumOutput == nil ||
		expected.MinimumOutput.Sign() <= 0 || expected.DestinationChainID == 0 {
		return fmt.Errorf("across deposit expectation is incomplete")
	}
	parsed, err := abi.JSON(strings.NewReader(spokePoolDepositABI))
	if err != nil {
		return err
	}
	method, err := parsed.MethodById(payload[:4])
	if err != nil {
		return fmt.Errorf("across calldata is not an allowlisted deposit entry point")
	}
	data := payload[4:]
	if len(data) >= 5 && data[len(data)-5] == 0x1d && data[len(data)-4] == 0xc0 && data[len(data)-3] == 0xde {
		data = data[:len(data)-5]
	}
	values := make(map[string]any)
	if err := method.Inputs.UnpackIntoMap(values, data); err != nil {
		return fmt.Errorf("decode Across %s calldata: %w", method.Name, err)
	}
	addressField := func(name string) (common.Address, bool) {
		switch value := values[name].(type) {
		case common.Address:
			return value, true
		case [32]byte:
			for _, prefix := range value[:12] {
				if prefix != 0 {
					return common.Address{}, false
				}
			}
			return common.BytesToAddress(value[12:]), true
		default:
			return common.Address{}, false
		}
	}
	depositor, depositorOK := addressField("depositor")
	recipient, recipientOK := addressField("recipient")
	inputToken, inputOK := addressField("inputToken")
	outputToken, outputOK := addressField("outputToken")
	inputAmount, inputAmountOK := values["inputAmount"].(*big.Int)
	outputAmount, outputAmountOK := values["outputAmount"].(*big.Int)
	destination, destinationOK := values["destinationChainId"].(*big.Int)
	message, messageOK := values["message"].([]byte)
	if !depositorOK || !recipientOK || !inputOK || !outputOK || !inputAmountOK || !outputAmountOK ||
		!destinationOK || !destination.IsUint64() || !messageOK {
		return fmt.Errorf("across deposit calldata fields are malformed")
	}
	if depositor != expected.Depositor || recipient != expected.Recipient || inputToken != expected.InputToken ||
		outputToken != expected.OutputToken || inputAmount.Cmp(expected.InputAmount) != 0 ||
		outputAmount.Cmp(expected.MinimumOutput) < 0 || destination.Uint64() != expected.DestinationChainID || len(message) != 0 {
		return fmt.Errorf("across deposit calldata does not match the approved bridge request")
	}
	return nil
}

func acrossAllowed(candidate common.Address, values []common.Address) bool {
	for _, value := range values {
		if value == candidate {
			return true
		}
	}
	return false
}

func (s *EVMLiveService) awaitFill(ctx context.Context, hash string) (Status, error) {
	waitCtx, cancel := context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()
	ticker := time.NewTicker(s.config.PollInterval)
	defer ticker.Stop()
	for {
		status, err := s.config.Client.DepositStatus(waitCtx, hash)
		if err == nil {
			switch status.State {
			case DepositFilled:
				return status, nil
			case DepositExpired, DepositRefunded:
				return Status{}, fmt.Errorf("across deposit reached terminal state %s", status.State)
			}
		}
		select {
		case <-waitCtx.Done():
			return Status{}, fmt.Errorf("across delivery remains pending: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func (s *EVMLiveService) awaitReceipt(ctx context.Context, client *ethclient.Client, hash string) (*types.Receipt, error) {
	if !common.IsHexHash(hash) {
		return nil, fmt.Errorf("across transaction hash is invalid")
	}
	waitCtx, cancel := context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()
	ticker := time.NewTicker(s.config.PollInterval)
	defer ticker.Stop()
	for {
		receipt, err := client.TransactionReceipt(waitCtx, common.HexToHash(hash))
		if err == nil && receipt != nil {
			return receipt, nil
		}
		if err != nil && !errors.Is(err, geth.NotFound) {
			return nil, err
		}
		select {
		case <-waitCtx.Done():
			return nil, fmt.Errorf("across receipt outcome unknown: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func evmTokenBalance(ctx context.Context, client *ethclient.Client, token, owner common.Address) (*big.Int, error) {
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

func transferTo(receipt *types.Receipt, token, recipient common.Address) (*big.Int, error) {
	total := new(big.Int)
	for _, log := range receipt.Logs {
		if log == nil || log.Address != token || len(log.Topics) < 3 || log.Topics[0] != erc20TransferTopic ||
			common.BytesToAddress(log.Topics[2].Bytes()[12:]) != recipient || len(log.Data) < 32 {
			continue
		}
		total.Add(total, new(big.Int).SetBytes(log.Data[:32]))
	}
	if total.Sign() <= 0 {
		return nil, fmt.Errorf("across fill receipt contains no attributable token delivery")
	}
	return total, nil
}

func acrossReceiptCost(receipt *types.Receipt, asset market.AssetID, chain market.ChainID) execution.CostComponent {
	units := new(big.Int)
	if receipt != nil && receipt.EffectiveGasPrice != nil {
		units.Mul(new(big.Int).SetUint64(receipt.GasUsed), receipt.EffectiveGasPrice)
	}
	amount, _ := market.NewAssetQuantity(asset, new(big.Rat).SetFrac(units, new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)))
	return execution.CostComponent{Kind: "across_source_gas", Chain: chain, Amount: amount, Evidence: "evm_receipt"}
}

func tokenDecimalScale(decimals uint8) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
}

func acrossSpreadCost(source, destination EVMLiveChain, input, output *big.Int) (execution.CostComponent, error) {
	if input == nil || output == nil || source.Token.Asset == "" || source.Token.Asset != destination.Token.Asset {
		return execution.CostComponent{}, fmt.Errorf("across spread evidence is incomplete")
	}
	sourceScale := tokenDecimalScale(source.Token.Decimals)
	destinationScale := tokenDecimalScale(destination.Token.Decimals)
	spread := new(big.Rat).Sub(new(big.Rat).SetFrac(input, sourceScale), new(big.Rat).SetFrac(output, destinationScale))
	if spread.Sign() < 0 {
		return execution.CostComponent{}, fmt.Errorf("across delivered output exceeds its human input")
	}
	amount, err := market.NewAssetQuantity(source.Token.Asset, spread)
	if err != nil {
		return execution.CostComponent{}, err
	}
	return execution.CostComponent{Kind: "across_bridge_spread", Chain: source.ID, Amount: amount,
		IncludedInOutput: true, Evidence: "confirmed_input_minus_fill_output"}, nil
}

func acrossTransactionByPhase(records []executionport.SequentialTransactionRecord, phase string) (executionport.SequentialTransactionRecord, bool) {
	for _, record := range records {
		if record.Phase == phase {
			return record, true
		}
	}
	return executionport.SequentialTransactionRecord{}, false
}

var _ crosschainport.RecoverableLiveTransferService = (*EVMLiveService)(nil)
