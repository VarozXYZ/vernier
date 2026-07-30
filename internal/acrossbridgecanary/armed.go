package acrossbridgecanary

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strings"
	"time"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	solanago "github.com/gagliardetto/solana-go"
	solanarpc "github.com/gagliardetto/solana-go/rpc"

	"github.com/VarozXYZ/vernier/adapters/chain/solana"
	"github.com/VarozXYZ/vernier/adapters/crosschain/across"
	sqlitepersistence "github.com/VarozXYZ/vernier/adapters/persistence/sqlite"
	domainexecution "github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	chainport "github.com/VarozXYZ/vernier/ports/chain"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	"github.com/VarozXYZ/vernier/runtime/configuration"
)

type destinationEvidence struct {
	Identity string
	Balance  *big.Int
	Source   string
}

type destinationWatcher interface {
	Await(context.Context, *big.Int) (destinationEvidence, error)
	Close()
}

type liveExecutionResult struct {
	SourceChain         string
	SourceIdentity      string
	DestinationChain    string
	DestinationIdentity string
	BalanceBefore       *big.Int
	BalanceAfter        *big.Int
	Evidence            string
	Costs               []domainexecution.CostComponent
	SourceNonce         *uint64
}

type liveExecutionHooks struct {
	Request          domainexecution.SequentialStageRequest
	Journal          executionport.SequentialJournal
	OperationID      string
	Accounts         map[string]domainexecution.AccountID
	NativeAssets     map[market.ChainID]market.AssetID
	NonceCoordinator chainport.EVMNonceCoordinator
	Result           *liveExecutionResult
}

func (h *liveExecutionHooks) prepared(
	ctx context.Context,
	chain, phase, identity string,
) error {
	if h == nil {
		return nil
	}
	if h.Result != nil && phase == "across_source" {
		h.Result.SourceChain = chain
		h.Result.SourceIdentity = identity
	}
	var nonce *uint64
	if chain == "polygon" && h.Result != nil && h.Result.SourceNonce != nil {
		copy := *h.Result.SourceNonce
		nonce = &copy
	}
	return h.Journal.RecordPreparedTransaction(ctx, executionport.PreparedTransaction{
		Operation: h.Request.Operation, Ordinal: h.Request.Stage.Ordinal,
		Phase: phase,
		Identity: domainexecution.TransactionIdentity{
			Chain:   domainexecutionChain(chain, h.Request),
			Account: h.Accounts[chain], Hash: identity, Nonce: nonce,
		},
		PreparedAt: time.Now().UTC(),
	})
}

func (h *liveExecutionHooks) mark(
	ctx context.Context,
	phase, status string,
) error {
	if h == nil {
		return nil
	}
	return h.Journal.MarkTransaction(
		ctx, h.Request.Operation, h.Request.Stage.Ordinal, phase, status,
	)
}

func (h *liveExecutionHooks) cost(
	chain, kind string,
	asset market.AssetID,
	units *big.Int,
	decimals int64,
	evidence string,
) error {
	if h == nil || h.Result == nil || units == nil || units.Sign() == 0 {
		return nil
	}
	if units.Sign() < 0 {
		return fmt.Errorf("across execution cost cannot be negative")
	}
	amount, err := market.NewAssetQuantity(
		asset,
		new(big.Rat).SetFrac(
			units,
			new(big.Int).Exp(big.NewInt(10), big.NewInt(decimals), nil),
		),
	)
	if err != nil {
		return err
	}
	h.Result.Costs = append(h.Result.Costs, domainexecution.CostComponent{
		Kind: kind, Chain: domainexecutionChain(chain, h.Request),
		Amount: amount, Evidence: evidence,
	})
	return nil
}

func (h *liveExecutionHooks) nativeAsset(
	chain string,
	fallback market.AssetID,
) market.AssetID {
	if h == nil {
		return fallback
	}
	if asset := h.NativeAssets[domainexecutionChain(chain, h.Request)]; asset != "" {
		return asset
	}
	return fallback
}

func domainexecutionChain(
	chain string,
	request domainexecution.SequentialStageRequest,
) market.ChainID {
	if chain == "solana" {
		if request.Stage.SourceChain == "solana" {
			return request.Stage.SourceChain
		}
		return request.Stage.DestinationChain
	}
	if request.Stage.SourceChain != "solana" {
		return request.Stage.SourceChain
	}
	return request.Stage.DestinationChain
}

func resumeArmed(
	ctx context.Context,
	output io.Writer,
	config configuration.ParsedConfig,
	client *across.Client,
	operationID string,
	storePath string,
	timeout time.Duration,
) error {
	if timeout <= 0 {
		return fmt.Errorf("confirmation timeout must be positive")
	}
	store, err := sqlitepersistence.OpenAcrossCanary(storePath)
	if err != nil {
		return err
	}
	defer store.Close()
	operation, err := store.Get(ctx, operationID)
	if err != nil {
		return err
	}
	if operation.Status == "completed" {
		fmt.Fprintf(output, "result=already_completed operation=%s destination_tx=%s\n", operation.ID, emptyAsUnknown(operation.DestinationIdentity))
		return nil
	}
	hasPlaceholderIdentity := operation.SourceChain == "pending" && operation.SourceIdentity == operation.ID
	if operation.SourceIdentity == "" || hasPlaceholderIdentity {
		if operation.Status == "outcome_unknown" || operation.Status == "created" {
			cause := fmt.Errorf("operation stopped before a source transaction was prepared")
			if err := store.Mark(ctx, operation.ID, "failed", cause); err != nil {
				return err
			}
			fmt.Fprintf(output, "result=failed_no_broadcast operation=%s\n", operation.ID)
			return nil
		}
		return fmt.Errorf("persisted operation %s has no source transaction identity", operation.ID)
	}
	if operation.BalanceBefore == "" || operation.ExpectedOutput == "" {
		return fmt.Errorf("persisted operation %s lacks reconciliation data", operation.ID)
	}
	selected := direction(operation.Direction)
	if selected != evmToSolana && selected != solanaToEVM {
		return fmt.Errorf("persisted operation %s has invalid direction %q", operation.ID, operation.Direction)
	}
	before, ok := new(big.Int).SetString(operation.BalanceBefore, 10)
	if !ok || before.Sign() < 0 {
		return fmt.Errorf("persisted operation %s has invalid destination balance", operation.ID)
	}
	minimum, ok := new(big.Int).SetString(operation.ExpectedOutput, 10)
	if !ok || minimum.Sign() <= 0 {
		return fmt.Errorf("persisted operation %s has invalid expected output", operation.ID)
	}
	target := new(big.Int).Add(new(big.Int).Set(before), minimum)
	endpoints, err := config.ResolveEndpoints(requiredEnvLookup)
	if err != nil {
		return err
	}
	watcher, current, destinationChain, err := openDestinationWatcher(ctx, config, selected, endpoints)
	if err != nil {
		return err
	}
	defer watcher.Close()
	fmt.Fprintf(
		output,
		"resume operation=%s source_tx=%s destination=%s balance_before=%s balance_current=%s target=%s\n",
		operation.ID, operation.SourceIdentity, destinationChain, before, current, target,
	)

	status, statusErr := client.DepositStatus(ctx, operation.SourceIdentity)
	if statusErr == nil {
		switch status.State {
		case across.DepositExpired, across.DepositRefunded:
			err = fmt.Errorf("across deposit is %s", status.State)
			_ = store.Mark(ctx, operation.ID, "failed", err)
			return err
		case across.DepositFilled:
			if current.Cmp(target) >= 0 {
				return completeResumedDestination(
					ctx, output, store, operation.ID, destinationChain,
					status.FillTransaction, before, current,
				)
			}
		}
	}

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	evidenceChannel := make(chan destinationEvidence, 2)
	errorChannel := make(chan error, 1)
	statusErrorChannel := make(chan error, 1)
	go func() {
		evidence, waitErr := watcher.Await(waitCtx, target)
		if waitErr != nil {
			errorChannel <- waitErr
			return
		}
		evidenceChannel <- evidence
	}()
	go pollAcrossStatus(waitCtx, client, operation.SourceIdentity, evidenceChannel, statusErrorChannel)

	var balanceEvidence *destinationEvidence
	fillIdentity := ""
	for {
		select {
		case evidence := <-evidenceChannel:
			if evidence.Source == "across_status" {
				fillIdentity = evidence.Identity
			} else if evidence.Balance != nil && evidence.Balance.Cmp(target) >= 0 {
				copy := evidence
				balanceEvidence = &copy
			}
			if balanceEvidence != nil && fillIdentity != "" {
				return completeResumedDestination(
					ctx, output, store, operation.ID, destinationChain,
					fillIdentity, before, balanceEvidence.Balance,
				)
			}
		case waitErr := <-errorChannel:
			_ = store.Mark(ctx, operation.ID, "outcome_unknown", waitErr)
			return fmt.Errorf("await destination: %w", waitErr)
		case statusErr := <-statusErrorChannel:
			fmt.Fprintf(
				output,
				"tracking_warning source=across_status error=%q action=websocket_remains_primary\n",
				statusErr,
			)
		case <-waitCtx.Done():
			_ = store.Mark(ctx, operation.ID, "outcome_unknown", waitCtx.Err())
			return fmt.Errorf("destination outcome is unknown: %w", waitCtx.Err())
		}
	}
}

func completeResumedDestination(
	ctx context.Context,
	output io.Writer,
	store *sqlitepersistence.AcrossCanaryStore,
	operationID, destinationChain, identity string,
	before, after *big.Int,
) error {
	if err := store.Destination(ctx, operationID, identity, after.String()); err != nil {
		return err
	}
	if err := store.Mark(ctx, operationID, "completed", nil); err != nil {
		return err
	}
	fmt.Fprintf(
		output,
		"destination_confirmed chain=%s tx=%s balance_before=%s balance_after=%s evidence=websocket_or_reconciled_balance\n"+
			"result=completed operation=%s\n",
		destinationChain, emptyAsUnknown(identity), before, after, operationID,
	)
	return nil
}

func executeArmed(
	ctx context.Context,
	output io.Writer,
	config configuration.ParsedConfig,
	client *across.Client,
	request across.ApprovalRequest,
	approval across.Approval,
	selected direction,
	storePath string,
	timeout time.Duration,
	hooks *liveExecutionHooks,
) error {
	if timeout <= 0 {
		return fmt.Errorf("confirmation timeout must be positive")
	}
	endpoints, err := config.ResolveEndpoints(func(name string) (string, bool) {
		return requiredEnvLookup(name)
	})
	if err != nil {
		return err
	}
	operationID := ""
	if hooks != nil {
		operationID = strings.TrimSpace(hooks.OperationID)
	}
	if operationID == "" {
		operationID, err = newAcrossOperationID()
		if err != nil {
			return err
		}
	}
	store, err := sqlitepersistence.OpenAcrossCanary(storePath)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.Create(ctx, sqlitepersistence.AcrossCanaryOperation{
		ID: operationID, Direction: string(selected), AmountUnits: request.Amount,
		ExpectedOutput: approval.MinimumOutputAmount, Status: "created", CreatedAt: time.Now().UTC(),
	}); err != nil {
		return err
	}
	fmt.Fprintf(output, "operation=%s mode=armed journal=%s\n", operationID, storePath)

	minimumOutput, _ := new(big.Int).SetString(approval.MinimumOutputAmount, 10)
	watcher, before, destinationChain, err := openDestinationWatcher(ctx, config, selected, endpoints)
	if err != nil {
		_ = store.Mark(ctx, operationID, "failed", err)
		return err
	}
	defer watcher.Close()

	var sourceIdentity, sourceChain string
	if selected == evmToSolana {
		sourceChain, sourceIdentity, err = executeEVMSource(
			ctx, output, config, endpoints, approval.SwapTransaction,
			func(identity string) error {
				if hookErr := hooks.prepared(ctx, "polygon", "across_source", identity); hookErr != nil {
					return hookErr
				}
				return store.Prepared(ctx, operationID, "polygon", identity, destinationChain, before.String())
			},
			func() error {
				if hookErr := hooks.mark(ctx, "across_source", "broadcast"); hookErr != nil {
					return hookErr
				}
				return store.Mark(ctx, operationID, "broadcast", nil)
			},
			hooks,
			timeout,
		)
	} else {
		sourceChain, sourceIdentity, err = executeSolanaSource(
			ctx, output, config, endpoints, approval.SwapTransaction,
			func(identity string) error {
				if hookErr := hooks.prepared(ctx, "solana", "across_source", identity); hookErr != nil {
					return hookErr
				}
				return store.Prepared(ctx, operationID, "solana", identity, destinationChain, before.String())
			},
			func() error {
				if hookErr := hooks.mark(ctx, "across_source", "broadcast"); hookErr != nil {
					return hookErr
				}
				return store.Mark(ctx, operationID, "broadcast", nil)
			},
			hooks,
			timeout,
		)
	}
	if err != nil {
		status := "failed"
		if persisted, getErr := store.Get(ctx, operationID); getErr == nil && persisted.Status != "created" {
			status = "outcome_unknown"
		}
		_ = store.Mark(ctx, operationID, status, err)
		return err
	}
	_ = sourceChain
	if err := store.Mark(ctx, operationID, "source_confirmed", nil); err != nil {
		return err
	}
	if err := hooks.mark(ctx, "across_source", "confirmed"); err != nil {
		return err
	}
	fmt.Fprintf(output, "source_confirmed chain=%s tx=%s\n", sourceChain, sourceIdentity)

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	targetBalance := new(big.Int).Add(new(big.Int).Set(before), minimumOutput)
	evidenceChannel := make(chan destinationEvidence, 1)
	errorChannel := make(chan error, 1)
	statusErrorChannel := make(chan error, 1)
	go func() {
		evidence, waitErr := watcher.Await(waitCtx, targetBalance)
		if waitErr != nil {
			errorChannel <- waitErr
			return
		}
		evidenceChannel <- evidence
	}()
	go pollAcrossStatus(waitCtx, client, sourceIdentity, evidenceChannel, statusErrorChannel)

	var evidence destinationEvidence
	for {
		select {
		case evidence = <-evidenceChannel:
			if evidence.Balance != nil && evidence.Balance.Cmp(new(big.Int).Add(before, minimumOutput)) >= 0 {
				goto confirmed
			}
			if evidence.Source == "across_status" && evidence.Identity != "" {
				continue
			}
		case waitErr := <-errorChannel:
			_ = store.Mark(ctx, operationID, "outcome_unknown", waitErr)
			return fmt.Errorf("await destination: %w", waitErr)
		case statusErr := <-statusErrorChannel:
			fmt.Fprintf(
				output,
				"tracking_warning source=across_status error=%q action=websocket_remains_primary\n",
				statusErr,
			)
		case <-waitCtx.Done():
			_ = store.Mark(ctx, operationID, "outcome_unknown", waitCtx.Err())
			return fmt.Errorf("destination outcome is unknown: %w", waitCtx.Err())
		}
	}

confirmed:
	if evidence.Identity == "" {
		statusCtx, statusCancel := context.WithTimeout(ctx, 10*time.Second)
		evidence.Identity = awaitAcrossIdentity(statusCtx, client, sourceIdentity)
		statusCancel()
	}
	if err := store.Destination(ctx, operationID, evidence.Identity, evidence.Balance.String()); err != nil {
		return err
	}
	if err := store.Mark(ctx, operationID, "completed", nil); err != nil {
		return err
	}
	if hooks != nil && hooks.Result != nil {
		*hooks.Result = liveExecutionResult{
			SourceChain: sourceChain, SourceIdentity: sourceIdentity,
			DestinationChain:    destinationChain,
			DestinationIdentity: evidence.Identity,
			BalanceBefore:       new(big.Int).Set(before),
			BalanceAfter:        new(big.Int).Set(evidence.Balance),
			Evidence:            evidence.Source,
			Costs:               append([]domainexecution.CostComponent(nil), hooks.Result.Costs...),
		}
	}
	fmt.Fprintf(
		output,
		"destination_confirmed chain=%s tx=%s balance_before=%s balance_after=%s evidence=%s\n"+
			"result=completed operation=%s\n",
		destinationChain, emptyAsUnknown(evidence.Identity), before, evidence.Balance, evidence.Source, operationID,
	)
	return nil
}

func executeEVMSource(
	ctx context.Context,
	output io.Writer,
	config configuration.ParsedConfig,
	endpoints map[string]string,
	artifact across.Transaction,
	persist func(string) error,
	markBroadcast func() error,
	hooks *liveExecutionHooks,
	timeout time.Duration,
) (string, string, error) {
	privateText, err := requiredEnv("POLYGON_PRIVATE_KEY")
	if err != nil {
		return "", "", err
	}
	privateKey, err := parseEVMPrivateKey(privateText)
	if err != nil {
		return "", "", err
	}
	client, err := ethclient.DialContext(ctx, endpoints["polygon.http"])
	if err != nil {
		client, err = ethclient.DialContext(ctx, endpoints["polygon"])
	}
	if err != nil {
		return "", "", err
	}
	defer client.Close()
	from := crypto.PubkeyToAddress(privateKey.PublicKey)
	to := common.HexToAddress(artifact.To)
	data, err := hex.DecodeString(strings.TrimPrefix(artifact.Data, "0x"))
	if err != nil {
		return "", "", err
	}
	value := new(big.Int)
	if artifact.Value != "" {
		if parsed, ok := value.SetString(artifact.Value, 10); !ok || parsed.Sign() < 0 {
			return "", "", fmt.Errorf("across EVM value is invalid")
		}
	}
	call := geth.CallMsg{From: from, To: &to, Data: data, Value: value}
	if _, err := client.CallContract(ctx, call, nil); err != nil {
		return "", "", fmt.Errorf("simulate Across EVM source: %w", err)
	}
	gas, err := client.EstimateGas(ctx, call)
	if err != nil {
		return "", "", err
	}
	gas = gas * 120 / 100
	var nonce uint64
	if hooks != nil && hooks.NonceCoordinator != nil {
		nonce, err = hooks.NonceCoordinator.NextNonce()
	} else {
		nonce, err = client.PendingNonceAt(ctx, from)
	}
	if err != nil {
		return "", "", fmt.Errorf("read coordinated Polygon nonce: %w", err)
	}
	chainID, err := client.ChainID(ctx)
	if err != nil {
		return "", "", err
	}
	tip, err := client.SuggestGasTipCap(ctx)
	if err != nil {
		return "", "", err
	}
	header, err := client.HeaderByNumber(ctx, nil)
	if err != nil || header.BaseFee == nil {
		return "", "", fmt.Errorf("read Polygon base fee: %w", err)
	}
	feeCap := new(big.Int).Add(new(big.Int).Mul(header.BaseFee, big.NewInt(2)), tip)
	transaction := types.NewTx(&types.DynamicFeeTx{
		ChainID: chainID, Nonce: nonce, GasTipCap: tip, GasFeeCap: feeCap,
		Gas: gas, To: &to, Value: value, Data: data,
	})
	signed, err := types.SignTx(transaction, types.LatestSignerForChainID(chainID), privateKey)
	if err != nil {
		return "", "", err
	}
	identity := signed.Hash().Hex()
	if hooks != nil && hooks.Result != nil {
		nonceCopy := nonce
		hooks.Result.SourceNonce = &nonceCopy
	}
	if err := persist(identity); err != nil {
		return "", "", err
	}
	fmt.Fprintf(output, "prepared chain=polygon tx=%s nonce=%d gas=%d persistence=wal_full\n", identity, nonce, gas)
	if err := client.SendTransaction(ctx, signed); err != nil {
		if hooks != nil && hooks.NonceCoordinator != nil {
			hooks.NonceCoordinator.MarkNonceUsed(nonce)
		}
		return "polygon", identity, fmt.Errorf("broadcast Polygon returned uncertain result; do not resend automatically: %w", err)
	}
	if hooks != nil && hooks.NonceCoordinator != nil {
		hooks.NonceCoordinator.MarkNonceUsed(nonce)
	}
	if err := markBroadcast(); err != nil {
		return "polygon", identity, err
	}
	fmt.Fprintf(output, "broadcast chain=polygon tx=%s\n", identity)
	confirmCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		receipt, receiptErr := client.TransactionReceipt(confirmCtx, signed.Hash())
		if receiptErr == nil {
			if receipt.Status != types.ReceiptStatusSuccessful {
				return "polygon", identity, fmt.Errorf("polygon source transaction reverted")
			}
			if receipt.EffectiveGasPrice != nil {
				fee := new(big.Int).Mul(
					new(big.Int).SetUint64(receipt.GasUsed),
					receipt.EffectiveGasPrice,
				)
				if err := hooks.cost(
					"polygon", "network_fee", hooks.nativeAsset("polygon", "evm_native"), fee, 18,
					"across_evm_source_receipt",
				); err != nil {
					return "polygon", identity, err
				}
			}
			fmt.Fprintf(output, "confirmed chain=polygon block=%d gas_used=%d\n", receipt.BlockNumber.Uint64(), receipt.GasUsed)
			return "polygon", identity, nil
		}
		select {
		case <-confirmCtx.Done():
			return "polygon", identity, confirmCtx.Err()
		case <-ticker.C:
		}
	}
}

func executeSolanaSource(
	ctx context.Context,
	output io.Writer,
	config configuration.ParsedConfig,
	endpoints map[string]string,
	artifact across.Transaction,
	persist func(string) error,
	markBroadcast func() error,
	hooks *liveExecutionHooks,
	timeout time.Duration,
) (string, string, error) {
	privateText, err := requiredEnv("SOLANA_PRIVATE_KEY")
	if err != nil {
		return "", "", err
	}
	privateKey, err := parseSolanaPrivateKey(privateText)
	if err != nil {
		return "", "", err
	}
	raw, err := base64.StdEncoding.DecodeString(artifact.Data)
	if err != nil {
		return "", "", fmt.Errorf("decode Across Solana transaction: %w", err)
	}
	transaction, err := solanago.TransactionFromBytes(raw)
	if err != nil {
		return "", "", err
	}
	if err := solana.CompletePartiallySignedTransaction(transaction, privateKey); err != nil {
		return "", "", fmt.Errorf("sign Across Solana transaction: %w", err)
	}
	if len(transaction.Signatures) == 0 {
		return "", "", fmt.Errorf("across Solana transaction has no signature")
	}
	identity := transaction.Signatures[0].String()
	rpcURL := endpoints["solana.http"]
	if rpcURL == "" {
		rpcURL = endpoints["solana"]
	}
	client := solanarpc.New(rpcURL)
	simulation, err := client.SimulateTransactionWithOpts(ctx, transaction, &solanarpc.SimulateTransactionOpts{
		SigVerify: true, Commitment: solanarpc.CommitmentConfirmed,
	})
	if err != nil || simulation.Value.Err != nil {
		return "", "", fmt.Errorf("simulate Across Solana source: rpc=%v transaction=%v", err, simulationError(simulation))
	}
	if err := persist(identity); err != nil {
		return "", "", err
	}
	if simulation.Value.UnitsConsumed == nil {
		fmt.Fprintln(output, "simulation=ok chain=solana units=unknown")
	} else {
		fmt.Fprintf(output, "simulation=ok chain=solana units=%d\n", *simulation.Value.UnitsConsumed)
	}
	fmt.Fprintf(output, "prepared chain=solana tx=%s persistence=wal_full\n", identity)
	signature, err := client.SendTransactionWithOpts(
		ctx,
		transaction,
		AcrossSolanaBridgeTransactionOpts(false),
	)
	if err != nil {
		return "solana", identity, fmt.Errorf("broadcast Solana returned uncertain result; do not resend automatically: %w", err)
	}
	if signature.String() != identity {
		return "solana", identity, fmt.Errorf("solana RPC returned an unexpected signature")
	}
	if err := markBroadcast(); err != nil {
		return "solana", identity, err
	}
	fmt.Fprintf(output, "broadcast chain=solana tx=%s\n", identity)
	confirmCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	lastBroadcast := time.Now()
	for {
		statuses, statusErr := client.GetSignatureStatuses(confirmCtx, true, signature)
		if statusErr == nil && statuses != nil && len(statuses.Value) == 1 && statuses.Value[0] != nil {
			status := statuses.Value[0]
			if status.Err != nil {
				return "solana", identity, fmt.Errorf("solana source transaction failed: %v", status.Err)
			}
			if status.ConfirmationStatus == solanarpc.ConfirmationStatusConfirmed ||
				status.ConfirmationStatus == solanarpc.ConfirmationStatusFinalized {
				version := uint64(0)
				transactionResult, transactionErr := client.GetTransaction(
					confirmCtx,
					signature,
					&solanarpc.GetTransactionOpts{
						Encoding:                       solanago.EncodingBase64,
						Commitment:                     solanarpc.CommitmentConfirmed,
						MaxSupportedTransactionVersion: &version,
					},
				)
				if transactionErr != nil || transactionResult == nil ||
					transactionResult.Meta == nil {
					return "solana", identity, fmt.Errorf(
						"read Across Solana source fee metadata: %w",
						transactionErr,
					)
				}
				fee, additional, _ := solana.PayerLamportDebits(transactionResult.Meta)
				if err := hooks.cost(
					"solana", "network_fee", hooks.nativeAsset("solana", "sol"),
					new(big.Int).SetUint64(fee), 9,
					"across_solana_source_meta",
				); err != nil {
					return "solana", identity, err
				}
				if err := hooks.cost(
					"solana", "additional_payer_debit", hooks.nativeAsset("solana", "sol"),
					new(big.Int).SetUint64(additional), 9,
					"across_solana_source_meta",
				); err != nil {
					return "solana", identity, err
				}
				return "solana", identity, nil
			}
		}
		if time.Since(lastBroadcast) >= time.Second {
			retryCtx, cancelRetry := context.WithTimeout(confirmCtx, 5*time.Second)
			_, _ = client.SendTransactionWithOpts(
				retryCtx,
				transaction,
				AcrossSolanaBridgeTransactionOpts(true),
			)
			cancelRetry()
			lastBroadcast = time.Now()
		}
		select {
		case <-confirmCtx.Done():
			return "solana", identity, confirmCtx.Err()
		case <-ticker.C:
		}
	}
}

const AcrossSolanaBridgeMaxRetries = uint(20)

func AcrossSolanaBridgeTransactionOpts(
	skipPreflight bool,
) solanarpc.TransactionOpts {
	maxRetries := AcrossSolanaBridgeMaxRetries
	return solanarpc.TransactionOpts{
		SkipPreflight:       skipPreflight,
		PreflightCommitment: solanarpc.CommitmentConfirmed,
		MaxRetries:          &maxRetries,
	}
}

func openDestinationWatcher(
	ctx context.Context,
	config configuration.ParsedConfig,
	selected direction,
	endpoints map[string]string,
) (destinationWatcher, *big.Int, string, error) {
	if selected == evmToSolana {
		privateText, err := requiredEnv("SOLANA_PRIVATE_KEY")
		if err != nil {
			return nil, nil, "", err
		}
		privateKey, err := parseSolanaPrivateKey(privateText)
		if err != nil {
			return nil, nil, "", err
		}
		var mintText string
		for _, candidate := range config.Markets {
			if config.Chains[candidate.Chain].Kind == "solana" {
				mintText = candidate.Quote.AddressText
			}
		}
		mint, err := solanago.PublicKeyFromBase58(mintText)
		if err != nil {
			return nil, nil, "", err
		}
		ata, _, err := solanago.FindAssociatedTokenAddress(privateKey.PublicKey(), mint)
		if err != nil {
			return nil, nil, "", err
		}
		httpURL, wsURL := endpoints["solana.http"], endpoints["solana.websocket"]
		network, err := solana.DialReadOnlyNetwork(ctx, "solana", "Solana", httpURL, wsURL)
		if err != nil {
			return nil, nil, "", err
		}
		subscription, err := network.SubscribeAccount(ctx, ata.String())
		if err != nil {
			network.Close()
			return nil, nil, "", err
		}
		account, err := network.ReadAccount(ctx, ata.String())
		if err != nil {
			subscription.Unsubscribe()
			network.Close()
			return nil, nil, "", err
		}
		before, err := splTokenAmount(account.Data)
		if err != nil {
			subscription.Unsubscribe()
			network.Close()
			return nil, nil, "", err
		}
		return &solanaDestinationWatcher{network: network, subscription: subscription}, before, "solana", nil
	}
	privateText, err := requiredEnv("POLYGON_PRIVATE_KEY")
	if err != nil {
		return nil, nil, "", err
	}
	privateKey, err := parseEVMPrivateKey(privateText)
	if err != nil {
		return nil, nil, "", err
	}
	var token common.Address
	for _, candidate := range config.Markets {
		if config.Chains[candidate.Chain].Kind == "evm" {
			token = candidate.Quote.Address
		}
	}
	httpClient, err := ethclient.DialContext(ctx, endpoints["polygon.http"])
	if err != nil {
		return nil, nil, "", err
	}
	wsClient, err := ethclient.DialContext(ctx, endpoints["polygon.websocket"])
	if err != nil {
		httpClient.Close()
		return nil, nil, "", err
	}
	recipient := crypto.PubkeyToAddress(privateKey.PublicKey)
	logs := make(chan types.Log, 16)
	query := geth.FilterQuery{
		Addresses: []common.Address{token},
		Topics: [][]common.Hash{
			{crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))},
			nil,
			{common.BytesToHash(common.LeftPadBytes(recipient.Bytes(), 32))},
		},
	}
	subscription, err := wsClient.SubscribeFilterLogs(ctx, query, logs)
	if err != nil {
		httpClient.Close()
		wsClient.Close()
		return nil, nil, "", err
	}
	before, err := erc20Balance(ctx, httpClient, token, recipient)
	if err != nil {
		subscription.Unsubscribe()
		httpClient.Close()
		wsClient.Close()
		return nil, nil, "", err
	}
	return &evmDestinationWatcher{
		http: httpClient, websocket: wsClient, subscription: subscription,
		logs: logs, token: token, recipient: recipient,
	}, before, "polygon", nil
}

type solanaDestinationWatcher struct {
	network      *solana.ReadOnlyNetwork
	subscription solana.AccountSubscription
}

func (w *solanaDestinationWatcher) Await(ctx context.Context, target *big.Int) (destinationEvidence, error) {
	for {
		select {
		case notification := <-w.subscription.Notifications():
			amount, err := splTokenAmount(notification.Value.Data)
			if err == nil && amount.Cmp(target) >= 0 {
				return destinationEvidence{Balance: amount, Source: "solana_account_websocket"}, nil
			}
		case err := <-w.subscription.Err():
			return destinationEvidence{}, err
		case <-ctx.Done():
			return destinationEvidence{}, ctx.Err()
		}
	}
}

func (w *solanaDestinationWatcher) Close() {
	w.subscription.Unsubscribe()
	w.network.Close()
}

type evmDestinationWatcher struct {
	http         *ethclient.Client
	websocket    *ethclient.Client
	subscription geth.Subscription
	logs         chan types.Log
	token        common.Address
	recipient    common.Address
}

func (w *evmDestinationWatcher) Await(ctx context.Context, target *big.Int) (destinationEvidence, error) {
	for {
		select {
		case event := <-w.logs:
			balance, err := erc20Balance(ctx, w.http, w.token, w.recipient)
			if err == nil && balance.Cmp(target) >= 0 {
				return destinationEvidence{Identity: event.TxHash.Hex(), Balance: balance, Source: "evm_transfer_websocket"}, nil
			}
		case err := <-w.subscription.Err():
			return destinationEvidence{}, err
		case <-ctx.Done():
			return destinationEvidence{}, ctx.Err()
		}
	}
}

func (w *evmDestinationWatcher) Close() {
	w.subscription.Unsubscribe()
	w.http.Close()
	w.websocket.Close()
}

func erc20Balance(ctx context.Context, client *ethclient.Client, token, owner common.Address) (*big.Int, error) {
	data := append(crypto.Keccak256([]byte("balanceOf(address)"))[:4], common.LeftPadBytes(owner.Bytes(), 32)...)
	result, err := client.CallContract(ctx, geth.CallMsg{To: &token, Data: data}, nil)
	if err != nil || len(result) != 32 {
		return nil, fmt.Errorf("read destination ERC-20 balance: %w", err)
	}
	return new(big.Int).SetBytes(result), nil
}

func splTokenAmount(data []byte) (*big.Int, error) {
	if len(data) < 72 {
		return nil, fmt.Errorf("SPL token account data is too short")
	}
	return new(big.Int).SetUint64(binary.LittleEndian.Uint64(data[64:72])), nil
}

func pollAcrossStatus(
	ctx context.Context,
	client *across.Client,
	source string,
	output chan<- destinationEvidence,
	errors chan<- error,
) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	lastError := ""
	for {
		status, err := client.DepositStatus(ctx, source)
		if err != nil && !isAcrossDepositAwaitingIndex(err) && err.Error() != lastError {
			lastError = err.Error()
			select {
			case errors <- err:
			default:
			}
		}
		if err == nil && status.State == across.DepositFilled {
			select {
			case output <- destinationEvidence{Identity: status.FillTransaction, Source: "across_status"}:
			case <-ctx.Done():
			}
			return
		}
		if err == nil {
			lastError = ""
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func isAcrossDepositAwaitingIndex(err error) bool {
	var apiError *across.APIError
	return errors.As(err, &apiError) && apiError.HTTPStatus == 404
}

func awaitAcrossIdentity(ctx context.Context, client *across.Client, source string) string {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		status, err := client.DepositStatus(ctx, source)
		if err == nil && status.State == across.DepositFilled && status.FillTransaction != "" {
			return status.FillTransaction
		}
		select {
		case <-ctx.Done():
			return ""
		case <-ticker.C:
		}
	}
}

func newAcrossOperationID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return "across-" + hex.EncodeToString(raw[:]), nil
}

func requiredEnvLookup(name string) (string, bool) {
	value, err := requiredEnv(name)
	return value, err == nil
}

func simulationError(result *solanarpc.SimulateTransactionResponse) any {
	if result == nil {
		return "missing response"
	}
	return result.Value.Err
}
