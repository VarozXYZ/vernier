package nttmanualcanary

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strings"
	"time"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	gethcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	solanago "github.com/gagliardetto/solana-go"
	solanarpc "github.com/gagliardetto/solana-go/rpc"

	"github.com/VarozXYZ/vernier/adapters/crosschain/wormholentt"
	domainexecution "github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	chainport "github.com/VarozXYZ/vernier/ports/chain"
	crosschainport "github.com/VarozXYZ/vernier/ports/crosschain"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

const (
	defaultDestinationBalanceVisibilityTimeout = 10 * time.Second
	defaultDestinationBalancePollInterval      = 250 * time.Millisecond
)

type LiveServiceConfig struct {
	ProfilePath              string
	StorePath                string
	SolanaChain              market.ChainID
	EVMChain                 market.ChainID
	SolanaNativeAsset        market.AssetID
	EVMNativeAsset           market.AssetID
	Accounts                 map[market.ChainID]domainexecution.AccountID
	NonceCoordinator         chainport.EVMNonceCoordinator
	TokenDecimals            map[market.ChainID]uint8
	ConfirmationTimeout      time.Duration
	BalanceVisibilityTimeout time.Duration
	BalancePollInterval      time.Duration
	Output                   io.Writer
	SourceTokenBalance       func(market.ChainID) (*big.Int, error)
	NativeBalance            func(market.ChainID) (*big.Int, error)
}

type LiveService struct {
	config        LiveServiceConfig
	profile       profile
	solanaAdapter *wormholentt.SolanaAdapter
	evmAdapter    *wormholentt.EVMAdapter
	solanaPayer   solanago.PublicKey
	evmSender     common.Address
}

// EVMApprovalTarget identifies the ERC-20 allowance required by the Polygon
// side of the configured NTT route. It intentionally exposes no signer data.
type EVMApprovalTarget struct {
	ChainID *big.Int
	Token   common.Address
	Manager common.Address
}

// LoadEVMApprovalTarget resolves the token and manager from the same private
// profile used by the NTT canary and Live runtime.
func LoadEVMApprovalTarget(path string) (EVMApprovalTarget, error) {
	loaded, err := loadProfile(path)
	if err != nil {
		return EVMApprovalTarget{}, err
	}
	chainID, ok := new(big.Int).SetString(strings.TrimSpace(loaded.EVM.ChainID), 10)
	if !ok || chainID.Sign() <= 0 {
		return EVMApprovalTarget{}, fmt.Errorf("invalid EVM NTT chain ID")
	}
	if !common.IsHexAddress(loaded.EVM.Token) ||
		!common.IsHexAddress(loaded.EVM.Manager) {
		return EVMApprovalTarget{}, fmt.Errorf("invalid EVM NTT approval address")
	}
	token := common.HexToAddress(loaded.EVM.Token)
	manager := common.HexToAddress(loaded.EVM.Manager)
	if token == (common.Address{}) || manager == (common.Address{}) {
		return EVMApprovalTarget{}, fmt.Errorf("EVM NTT approval address cannot be zero")
	}
	return EVMApprovalTarget{
		ChainID: chainID,
		Token:   token,
		Manager: manager,
	}, nil
}

func NewLiveService(config LiveServiceConfig) (*LiveService, error) {
	if config.ProfilePath == "" || config.StorePath == "" ||
		config.SolanaChain == "" || config.EVMChain == "" ||
		config.SolanaChain == config.EVMChain || len(config.Accounts) != 2 ||
		config.TokenDecimals[config.SolanaChain] == 0 ||
		config.TokenDecimals[config.EVMChain] == 0 {
		return nil, fmt.Errorf("NTT Live service configuration is incomplete")
	}
	if config.Output == nil {
		config.Output = io.Discard
	}
	if config.SolanaNativeAsset == "" {
		config.SolanaNativeAsset = "sol"
	}
	if config.EVMNativeAsset == "" {
		config.EVMNativeAsset = "evm_native"
	}
	if config.BalanceVisibilityTimeout <= 0 {
		config.BalanceVisibilityTimeout =
			defaultDestinationBalanceVisibilityTimeout
	}
	if config.BalancePollInterval <= 0 {
		config.BalancePollInterval = defaultDestinationBalancePollInterval
	}
	loaded, err := loadProfile(config.ProfilePath)
	if err != nil {
		return nil, err
	}
	if config.ConfirmationTimeout > 0 {
		seconds := int(
			(config.ConfirmationTimeout + time.Second - 1) / time.Second,
		)
		loaded.ConfirmationTimeoutS = seconds
	}
	solanaAdapter, evmAdapter, payer, sender, err := compose(loaded)
	if err != nil {
		return nil, err
	}
	return &LiveService{
		config: config, profile: loaded, solanaAdapter: solanaAdapter,
		evmAdapter: evmAdapter, solanaPayer: payer, evmSender: sender,
	}, nil
}

func (s *LiveService) Transfer(
	ctx context.Context,
	request domainexecution.SequentialStageRequest,
	journal executionport.SequentialJournal,
) (crosschainport.LiveTransferResult, error) {
	return s.transfer(
		ctx,
		request,
		journal,
		"",
		nttLiveOperationID(request, "initial"),
		"",
	)
}

func (s *LiveService) transfer(
	ctx context.Context,
	request domainexecution.SequentialStageRequest,
	journal executionport.SequentialJournal,
	sourceTransaction string,
	operationID string,
	phasePrefix string,
) (crosschainport.LiveTransferResult, error) {
	if err := request.Validate(); err != nil {
		return crosschainport.LiveTransferResult{}, err
	}
	route, err := s.route(request.Stage.SourceChain)
	if err != nil {
		return crosschainport.LiveTransferResult{}, err
	}
	sourceDecimals := s.config.TokenDecimals[request.Stage.SourceChain]
	destinationDecimals := s.config.TokenDecimals[request.Stage.DestinationChain]
	transferUnits, dustUnits, trimmedDecimals, err :=
		wormholentt.TrimTransferAmount(
			request.Input.Units(),
			sourceDecimals,
			destinationDecimals,
		)
	if err != nil {
		return crosschainport.LiveTransferResult{},
			executionport.NewStageError(executionport.DispositionRejected, err)
	}
	transferInput, err := market.NewTokenAmount(
		request.Stage.InputToken,
		transferUnits,
	)
	if err != nil {
		return crosschainport.LiveTransferResult{}, err
	}
	if dustUnits.Sign() > 0 {
		fmt.Fprintf(
			s.config.Output,
			"ntt_amount source=%s requested_units=%s transfer_units=%s retained_dust_units=%s message_decimals=%d\n",
			request.Stage.SourceChain,
			request.Input.Units(),
			transferUnits,
			dustUnits,
			trimmedDecimals,
		)
	}
	before, err := s.destinationBalance(ctx, route)
	if err != nil {
		return crosschainport.LiveTransferResult{},
			executionport.NewStageError(executionport.DispositionRejected, err)
	}
	result := &nttLiveResult{}
	hooks := &nttLiveHooks{
		Request: request, Journal: journal, Accounts: s.config.Accounts,
		OperationID: operationID, PhasePrefix: phasePrefix,
		SolanaChain: s.config.SolanaChain, EVMChain: s.config.EVMChain,
		SolanaNativeAsset:        s.config.SolanaNativeAsset,
		EVMNativeAsset:           s.config.EVMNativeAsset,
		NonceCoordinator:         s.config.NonceCoordinator,
		SourceTokenBalance:       s.config.SourceTokenBalance,
		NativeBalance:            s.config.NativeBalance,
		BalanceVisibilityTimeout: s.config.BalanceVisibilityTimeout,
		BalancePollInterval:      s.config.BalancePollInterval,
		Result:                   result,
	}
	err = executeArmed(
		ctx, s.config.Output, route, s.profile, s.solanaAdapter, s.evmAdapter,
		s.solanaPayer, s.evmSender, transferUnits.String(), sourceTransaction,
		s.config.StorePath, "", hooks,
	)
	if err != nil {
		disposition := executionport.DispositionRejected
		if result.SourceIdentity.Hash != "" {
			disposition = executionport.DispositionPossible
		}
		return crosschainport.LiveTransferResult{},
			executionport.NewStageErrorWithCosts(
				disposition,
				result.Costs,
				err,
			)
	}
	if result.SourceIdentity.Hash == "" {
		return crosschainport.LiveTransferResult{},
			executionport.NewStageError(
				executionport.DispositionPossible,
				fmt.Errorf("NTT settlement is missing the source transaction identity"),
			)
	}
	if result.DestinationIdentity.Hash == "" {
		return crosschainport.LiveTransferResult{},
			executionport.NewStageError(
				executionport.DispositionPossible,
				fmt.Errorf("NTT settlement is missing the destination transaction identity"),
			)
	}
	expectedOutputUnits, err := RebaseTransferUnits(
		transferUnits,
		sourceDecimals,
		destinationDecimals,
	)
	if err != nil {
		return crosschainport.LiveTransferResult{},
			executionport.NewStageError(executionport.DispositionPossible, err)
	}
	after, observedDelta, attempts, err := AwaitDestinationBalanceVisibility(
		ctx,
		before,
		expectedOutputUnits,
		s.config.BalanceVisibilityTimeout,
		s.config.BalancePollInterval,
		func(readCtx context.Context) (*big.Int, error) {
			return s.destinationBalance(readCtx, route)
		},
	)
	evidence := "wormhole_ntt_destination_balance"
	var settlementBefore, settlementAfter *big.Int
	if err != nil {
		if ctx.Err() != nil {
			return crosschainport.LiveTransferResult{},
				executionport.NewStageError(
					executionport.DispositionPossible,
					ctx.Err(),
				)
		}
		// A successful destination redeem is the authoritative NTT
		// settlement. The wallet balance is only corroborating evidence and
		// may move concurrently because prefunded execution can consume or
		// replenish the same inventory while the RPC catches up. Recovery
		// already reconstructs this exact settlement from the two confirmed
		// transaction identities, so do the same on the initial path instead
		// of unnecessarily leaving the parent saga active.
		fmt.Fprintf(
			s.config.Output,
			"ntt_settlement warning=destination_balance_unavailable action=accept_confirmed_destination_transaction source_tx=%s destination_tx=%s detail=%q\n",
			result.SourceIdentity.Hash,
			result.DestinationIdentity.Hash,
			err,
		)
		evidence = "wormhole_ntt_confirmed_destination_transaction"
	} else {
		settlementBefore = new(big.Int).Set(before)
		settlementAfter = new(big.Int).Set(after)
	}
	if err == nil && attempts > 1 {
		fmt.Fprintf(
			s.config.Output,
			"ntt_settlement phase=destination_balance_visible attempts=%d before_units=%s after_units=%s observed_delta_units=%s expected_delta_units=%s\n",
			attempts,
			before,
			after,
			observedDelta,
			expectedOutputUnits,
		)
	}
	output, err := market.NewTokenAmount(
		request.Stage.OutputToken,
		expectedOutputUnits,
	)
	if err != nil {
		return crosschainport.LiveTransferResult{},
			executionport.NewStageError(
				executionport.DispositionPossible,
				fmt.Errorf("construct NTT settlement output: %w", err),
			)
	}
	return crosschainport.LiveTransferResult{
		ActualInput: transferInput, ActualOutput: output,
		Costs:                    append([]domainexecution.CostComponent(nil), result.Costs...),
		SourceIdentity:           result.SourceIdentity,
		DestinationIdentity:      result.DestinationIdentity,
		DestinationBalanceBefore: settlementBefore,
		DestinationBalanceAfter:  settlementAfter,
		ObservedAt:               time.Now().UTC(), Evidence: evidence,
	}, nil
}

func (s *LiveService) RecoverTransfer(
	ctx context.Context,
	request domainexecution.SequentialStageRequest,
	transactions []executionport.SequentialTransactionRecord,
	journal executionport.SequentialJournal,
) (crosschainport.LiveTransferResult, error) {
	if err := request.Validate(); err != nil {
		return crosschainport.LiveTransferResult{}, err
	}
	var source *executionport.SequentialTransactionRecord
	var destination *executionport.SequentialTransactionRecord
	for index := range transactions {
		record := &transactions[index]
		switch {
		case strings.Contains(record.Phase, "source_transfer-"):
			if source == nil || record.PreparedAt.After(source.PreparedAt) {
				source = record
			}
		case strings.Contains(record.Phase, "destination_redeem-"),
			strings.Contains(record.Phase, "ntt_redeem_manual-"):
			if destination == nil ||
				record.PreparedAt.After(destination.PreparedAt) {
				destination = record
			}
		}
	}
	for _, record := range []*executionport.SequentialTransactionRecord{
		source,
		destination,
	} {
		if record == nil || definitiveNTTStatus(record.Status) {
			continue
		}
		changed, err := s.reconcileOuterTransaction(
			ctx,
			request,
			*record,
			journal,
		)
		if err != nil {
			return crosschainport.LiveTransferResult{},
				executionport.NewRecoveryError(
					executionport.RecoveryFailureTemporary,
					err,
				)
		}
		if !changed {
			return crosschainport.LiveTransferResult{},
				executionport.NewRecoveryError(
					executionport.RecoveryFailureUncertain,
					fmt.Errorf(
						"NTT transaction %s remains uncertain",
						record.Identity.Hash,
					),
				)
		}
		return crosschainport.LiveTransferResult{},
			executionport.NewRecoveryError(
				executionport.RecoveryFailureTemporary,
				fmt.Errorf("NTT transaction state was reconciled"),
			)
	}
	if source != nil && source.Status == "confirmed" &&
		destination != nil && destination.Status == "confirmed" {
		return s.reconstructedSettlement(request, *source, *destination)
	}
	if source == nil || source.Status == "rejected" ||
		source.Status == "confirmed_revert" {
		attempt := len(transactions) + 1
		return s.transfer(
			ctx,
			request,
			journal,
			"",
			nttLiveOperationID(request, fmt.Sprintf("source-%d", attempt)),
			fmt.Sprintf("recovery_%d_", attempt),
		)
	}
	if source.Status != "confirmed" {
		return crosschainport.LiveTransferResult{},
			executionport.NewRecoveryError(
				executionport.RecoveryFailureUncertain,
				fmt.Errorf("NTT source transaction has no definitive outcome"),
			)
	}
	attempt := len(transactions) + 1
	return s.transfer(
		ctx,
		request,
		journal,
		source.Identity.Hash,
		nttLiveOperationID(request, fmt.Sprintf("destination-%d", attempt)),
		fmt.Sprintf("recovery_%d_", attempt),
	)
}

func (s *LiveService) reconstructedSettlement(
	request domainexecution.SequentialStageRequest,
	source executionport.SequentialTransactionRecord,
	destination executionport.SequentialTransactionRecord,
) (crosschainport.LiveTransferResult, error) {
	sourceDecimals := s.config.TokenDecimals[request.Stage.SourceChain]
	destinationDecimals := s.config.TokenDecimals[request.Stage.DestinationChain]
	transferUnits, _, _, err := wormholentt.TrimTransferAmount(
		request.Input.Units(),
		sourceDecimals,
		destinationDecimals,
	)
	if err != nil {
		return crosschainport.LiveTransferResult{}, err
	}
	outputUnits, err := RebaseTransferUnits(
		transferUnits,
		sourceDecimals,
		destinationDecimals,
	)
	if err != nil {
		return crosschainport.LiveTransferResult{}, err
	}
	input, err := market.NewTokenAmount(
		request.Stage.InputToken,
		transferUnits,
	)
	if err != nil {
		return crosschainport.LiveTransferResult{}, err
	}
	output, err := market.NewTokenAmount(
		request.Stage.OutputToken,
		outputUnits,
	)
	if err != nil {
		return crosschainport.LiveTransferResult{}, err
	}
	return crosschainport.LiveTransferResult{
		ActualInput: input, ActualOutput: output,
		SourceIdentity:      source.Identity,
		DestinationIdentity: destination.Identity,
		ObservedAt:          time.Now().UTC(),
		Evidence:            "wormhole_ntt_durable_transaction_reconciliation",
	}, nil
}

func (s *LiveService) reconcileOuterTransaction(
	ctx context.Context,
	request domainexecution.SequentialStageRequest,
	record executionport.SequentialTransactionRecord,
	journal executionport.SequentialJournal,
) (bool, error) {
	chain := record.Identity.Chain
	switch chain {
	case s.config.EVMChain:
		rpcURL, err := requiredEnv(s.profile.EVM.RPCURLEnv)
		if err != nil {
			return false, err
		}
		client, err := ethclient.DialContext(ctx, rpcURL)
		if err != nil {
			return false, err
		}
		defer client.Close()
		receipt, err := client.TransactionReceipt(
			ctx,
			common.HexToHash(record.Identity.Hash),
		)
		if errors.Is(err, geth.NotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		status := "confirmed"
		if receipt.Status != types.ReceiptStatusSuccessful {
			status = "confirmed_revert"
		}
		return true, journal.MarkTransaction(
			ctx,
			request.Operation,
			request.Stage.Ordinal,
			record.Phase,
			status,
		)
	case s.config.SolanaChain:
		rpcURL, err := requiredEnv(s.profile.Solana.RPCURLEnv)
		if err != nil {
			return false, err
		}
		signature, err := solanago.SignatureFromBase58(record.Identity.Hash)
		if err != nil {
			return false, err
		}
		statuses, err := solanarpc.New(rpcURL).GetSignatureStatuses(
			ctx,
			true,
			signature,
		)
		if err != nil {
			return false, err
		}
		if statuses == nil || len(statuses.Value) != 1 ||
			statuses.Value[0] == nil {
			return false, nil
		}
		status := "confirmed"
		if statuses.Value[0].Err != nil {
			status = "confirmed_revert"
		}
		return true, journal.MarkTransaction(
			ctx,
			request.Operation,
			request.Stage.Ordinal,
			record.Phase,
			status,
		)
	default:
		return false, fmt.Errorf("NTT recovery chain %q is unknown", chain)
	}
}

func definitiveNTTStatus(status string) bool {
	switch status {
	case "confirmed", "confirmed_revert", "rejected":
		return true
	default:
		return false
	}
}

func nttLiveOperationID(
	request domainexecution.SequentialStageRequest,
	suffix string,
) string {
	return fmt.Sprintf(
		"ntt-%s-%d-%s",
		request.Operation,
		request.Stage.Ordinal,
		suffix,
	)
}

func RebaseTransferUnits(
	units *big.Int,
	sourceDecimals uint8,
	destinationDecimals uint8,
) (*big.Int, error) {
	if units == nil || units.Sign() <= 0 {
		return nil, fmt.Errorf("NTT settlement transfer units must be positive")
	}
	if sourceDecimals == 0 || destinationDecimals == 0 {
		return nil, fmt.Errorf("NTT settlement token decimals must be positive")
	}
	result := new(big.Int).Set(units)
	if sourceDecimals == destinationDecimals {
		return result, nil
	}
	if destinationDecimals > sourceDecimals {
		scale := new(big.Int).Exp(
			big.NewInt(10),
			big.NewInt(int64(destinationDecimals-sourceDecimals)),
			nil,
		)
		return result.Mul(result, scale), nil
	}
	scale := new(big.Int).Exp(
		big.NewInt(10),
		big.NewInt(int64(sourceDecimals-destinationDecimals)),
		nil,
	)
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(result, scale, remainder)
	if remainder.Sign() != 0 {
		return nil, fmt.Errorf(
			"NTT settlement transfer units cannot be represented by the destination token",
		)
	}
	return quotient, nil
}

func AwaitDestinationBalanceVisibility(
	ctx context.Context,
	before *big.Int,
	expectedDelta *big.Int,
	timeout time.Duration,
	pollInterval time.Duration,
	read func(context.Context) (*big.Int, error),
) (after *big.Int, observedDelta *big.Int, attempts int, err error) {
	if before == nil || before.Sign() < 0 {
		return nil, nil, 0, fmt.Errorf(
			"NTT destination balance before transfer is invalid",
		)
	}
	if expectedDelta == nil || expectedDelta.Sign() <= 0 {
		return nil, nil, 0, fmt.Errorf(
			"NTT expected destination balance delta is invalid",
		)
	}
	if read == nil {
		return nil, nil, 0, fmt.Errorf(
			"NTT destination balance reader is unavailable",
		)
	}
	if timeout <= 0 {
		timeout = defaultDestinationBalanceVisibilityTimeout
	}
	if pollInterval <= 0 {
		pollInterval = defaultDestinationBalancePollInterval
	}

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var latest *big.Int
	var latestReadErr error
	for {
		attempts++
		current, readErr := read(waitCtx)
		if readErr == nil && current != nil && current.Sign() >= 0 {
			latest = new(big.Int).Set(current)
			latestReadErr = nil
			delta := new(big.Int).Sub(current, before)
			if delta.Cmp(expectedDelta) >= 0 {
				return latest, delta, attempts, nil
			}
		} else {
			latestReadErr = readErr
		}

		timer := time.NewTimer(pollInterval)
		select {
		case <-waitCtx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			if ctx.Err() != nil {
				return nil, nil, attempts, ctx.Err()
			}
			latestText := "unavailable"
			if latest != nil {
				latestText = latest.String()
			}
			if latestReadErr != nil {
				return nil, nil, attempts, fmt.Errorf(
					"NTT destination balance was not visible after %s: before=%s latest=%s expected_delta=%s last_read_failed=true",
					timeout,
					before,
					latestText,
					expectedDelta,
				)
			}
			return nil, nil, attempts, fmt.Errorf(
				"NTT destination balance was not visible after %s: before=%s latest=%s expected_delta=%s",
				timeout,
				before,
				latestText,
				expectedDelta,
			)
		case <-timer.C:
		}
	}
}

func AwaitEVMSourceBalanceVisibility(
	ctx context.Context,
	required *big.Int,
	timeout time.Duration,
	pollInterval time.Duration,
	read func(context.Context) (*big.Int, uint64, error),
) (balance *big.Int, observedBlock uint64, attempts int, err error) {
	if required == nil || required.Sign() <= 0 {
		return nil, 0, 0, fmt.Errorf(
			"NTT required source balance is invalid",
		)
	}
	if read == nil {
		return nil, 0, 0, fmt.Errorf(
			"NTT source balance reader is unavailable",
		)
	}
	if timeout <= 0 {
		timeout = defaultDestinationBalanceVisibilityTimeout
	}
	if pollInterval <= 0 {
		pollInterval = defaultDestinationBalancePollInterval
	}

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var latest *big.Int
	var latestBlock uint64
	var latestReadErr error
	for {
		attempts++
		current, block, readErr := read(waitCtx)
		if readErr == nil && current != nil && current.Sign() >= 0 {
			latest = new(big.Int).Set(current)
			latestBlock = block
			latestReadErr = nil
			if current.Cmp(required) >= 0 {
				return latest, latestBlock, attempts, nil
			}
		} else {
			latestReadErr = readErr
			if block > latestBlock {
				latestBlock = block
			}
		}

		timer := time.NewTimer(pollInterval)
		select {
		case <-waitCtx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			if ctx.Err() != nil {
				return nil, latestBlock, attempts, ctx.Err()
			}
			latestText := "unavailable"
			if latest != nil {
				latestText = latest.String()
			}
			if latestReadErr != nil {
				return nil, latestBlock, attempts, fmt.Errorf(
					"NTT source balance was not visible after %s: latest=%s required=%s observed_block=%d last_read_failed=true",
					timeout,
					latestText,
					required,
					latestBlock,
				)
			}
			return nil, latestBlock, attempts, fmt.Errorf(
				"NTT source balance was not visible after %s: latest=%s required=%s observed_block=%d",
				timeout,
				latestText,
				required,
				latestBlock,
			)
		case <-timer.C:
		}
	}
}

func (s *LiveService) route(source market.ChainID) (direction, error) {
	switch source {
	case s.config.SolanaChain:
		return solanaToEVM, nil
	case s.config.EVMChain:
		return evmToSolana, nil
	default:
		return "", fmt.Errorf("NTT source chain is not configured")
	}
}

func (s *LiveService) destinationBalance(
	ctx context.Context,
	route direction,
) (*big.Int, error) {
	switch route {
	case evmToSolana:
		keyText, err := requiredEnv(s.profile.Solana.SignerEnv)
		if err != nil {
			return nil, err
		}
		key, err := parseSolanaPrivateKey(keyText)
		if err != nil {
			return nil, err
		}
		ata, _, err := solanago.FindAssociatedTokenAddress(
			key.PublicKey(), s.solanaAdapter.TokenMint(),
		)
		if err != nil {
			return nil, err
		}
		rpcURL, err := requiredEnv(s.profile.Solana.RPCURLEnv)
		if err != nil {
			return nil, err
		}
		balance, err := solanarpc.New(rpcURL).GetTokenAccountBalance(
			ctx, ata, solanarpc.CommitmentConfirmed,
		)
		if err != nil || balance == nil || balance.Value == nil {
			return nil, fmt.Errorf("read NTT destination Solana balance: %w", err)
		}
		value, ok := new(big.Int).SetString(balance.Value.Amount, 10)
		if !ok {
			return nil, fmt.Errorf("decode NTT destination Solana balance")
		}
		return value, nil
	case solanaToEVM:
		keyText, err := requiredEnv(s.profile.EVM.SignerEnv)
		if err != nil {
			return nil, err
		}
		key, err := gethcrypto.HexToECDSA(strings.TrimPrefix(strings.TrimSpace(keyText), "0x"))
		if err != nil {
			return nil, fmt.Errorf("invalid EVM private key")
		}
		rpcURL, err := requiredEnv(s.profile.EVM.RPCURLEnv)
		if err != nil {
			return nil, err
		}
		client, err := ethclient.DialContext(ctx, rpcURL)
		if err != nil {
			return nil, err
		}
		defer client.Close()
		call, err := s.evmAdapter.BuildBalanceCall(
			gethcrypto.PubkeyToAddress(key.PublicKey),
		)
		if err != nil {
			return nil, err
		}
		raw, err := client.CallContract(
			ctx, geth.CallMsg{To: &call.To, Data: call.Data}, nil,
		)
		if err != nil {
			return nil, err
		}
		return wormholentt.DecodeTokenBalance(raw)
	default:
		return nil, fmt.Errorf("unsupported NTT direction")
	}
}

var _ crosschainport.LiveTransferService = (*LiveService)(nil)
var _ crosschainport.RecoverableLiveTransferService = (*LiveService)(nil)
