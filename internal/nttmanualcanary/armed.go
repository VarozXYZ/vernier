package nttmanualcanary

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"strings"
	"time"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	gethcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	solanago "github.com/gagliardetto/solana-go"
	computebudget "github.com/gagliardetto/solana-go/programs/compute-budget"
	solanarpc "github.com/gagliardetto/solana-go/rpc"
	solanajsonrpc "github.com/gagliardetto/solana-go/rpc/jsonrpc"

	"github.com/VarozXYZ/vernier/adapters/crosschain/wormholentt"
	sqlitestore "github.com/VarozXYZ/vernier/adapters/persistence/sqlite"
	domainexecution "github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	"github.com/VarozXYZ/vernier/internal/rpcpolicy"
	"github.com/VarozXYZ/vernier/internal/safeerr"
	chainport "github.com/VarozXYZ/vernier/ports/chain"
	crosschainport "github.com/VarozXYZ/vernier/ports/crosschain"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

type armedRuntime struct {
	config        profile
	solanaAdapter *wormholentt.SolanaAdapter
	evmAdapter    *wormholentt.EVMAdapter
	solanaKey     solanago.PrivateKey
	evmKey        *ecdsa.PrivateKey
	solanaRPC     *solanarpc.Client
	evmRPC        *ethclient.Client
	store         *sqlitestore.NTTCanaryStore
	output        io.Writer
	operationID   string
	direction     direction
	ordinal       int
	measurements  []phaseMeasurement
	liveHooks     *nttLiveHooks
}

type nttLiveResult struct {
	SourceIdentity      domainexecution.TransactionIdentity
	DestinationIdentity domainexecution.TransactionIdentity
	Costs               []domainexecution.CostComponent
}

type nttLiveHooks struct {
	Request                  domainexecution.SequentialStageRequest
	Journal                  executionport.SequentialJournal
	OperationID              string
	PhasePrefix              string
	Accounts                 map[market.ChainID]domainexecution.AccountID
	SolanaChain              market.ChainID
	EVMChain                 market.ChainID
	SolanaNativeAsset        market.AssetID
	EVMNativeAsset           market.AssetID
	NonceCoordinator         chainport.EVMNonceCoordinator
	BalanceVisibilityTimeout time.Duration
	BalancePollInterval      time.Duration
	Result                   *nttLiveResult
	SourceTokenBalance       func(market.ChainID) (*big.Int, error)
	NativeBalance            func(market.ChainID) (*big.Int, error)
}

func (h *nttLiveHooks) prepared(
	ctx context.Context,
	chain market.ChainID,
	phase string,
	identity domainexecution.TransactionIdentity,
) error {
	if h == nil {
		return nil
	}
	persistedPhase := strings.TrimSpace(h.PhasePrefix) + phase
	identity.Account = h.Accounts[chain]
	if err := h.Journal.RecordPreparedTransaction(ctx, executionport.PreparedTransaction{
		Operation: h.Request.Operation, Ordinal: h.Request.Stage.Ordinal,
		Phase: persistedPhase, Identity: identity, PreparedAt: time.Now().UTC(),
	}); err != nil {
		return err
	}
	if h.Result != nil {
		switch {
		case strings.HasPrefix(phase, "source_transfer-"):
			h.Result.SourceIdentity = identity
		case strings.HasPrefix(phase, "destination_redeem-"),
			strings.HasPrefix(phase, "ntt_redeem_manual-"):
			h.Result.DestinationIdentity = identity
		}
	}
	return nil
}

func (h *nttLiveHooks) mark(
	ctx context.Context,
	phase, status string,
) error {
	if h == nil {
		return nil
	}
	return h.Journal.MarkTransaction(
		ctx,
		h.Request.Operation,
		h.Request.Stage.Ordinal,
		strings.TrimSpace(h.PhasePrefix)+phase,
		status,
	)
}

type phaseMeasurement struct {
	Chain    string
	Phase    string
	Identity string
	Metrics  sqlitestore.NTTCanaryTransactionMetrics
}

func (r *armedRuntime) liveChain(kind string) market.ChainID {
	if r.liveHooks == nil {
		return market.ChainID(kind)
	}
	if kind == "solana" {
		return r.liveHooks.SolanaChain
	}
	return r.liveHooks.EVMChain
}

func executeArmed(
	ctx context.Context,
	output io.Writer,
	route direction,
	config profile,
	solanaAdapter *wormholentt.SolanaAdapter,
	evmAdapter *wormholentt.EVMAdapter,
	solanaPayer solanago.PublicKey,
	evmSender common.Address,
	amountText, sourceTransaction, storeOverride, verifiedSignatureTx string,
	hooks *nttLiveHooks,
) error {
	commandStarted := time.Now()
	solanaKeyText, err := requiredEnv(config.Solana.SignerEnv)
	if err != nil {
		return err
	}
	solanaKey, err := parseSolanaPrivateKey(solanaKeyText)
	if err != nil {
		return err
	}
	if !solanaKey.PublicKey().Equals(solanaPayer) {
		return fmt.Errorf("derived Solana signer changed during composition")
	}
	evmKeyText, err := requiredEnv(config.EVM.SignerEnv)
	if err != nil {
		return err
	}
	evmKey, err := gethcrypto.HexToECDSA(
		strings.TrimPrefix(strings.TrimSpace(evmKeyText), "0x"),
	)
	if err != nil {
		return fmt.Errorf("invalid EVM private key")
	}
	if gethcrypto.PubkeyToAddress(evmKey.PublicKey) != evmSender {
		return fmt.Errorf("derived EVM signer changed during composition")
	}
	solanaRPCURL, err := requiredEnv(config.Solana.RPCURLEnv)
	if err != nil {
		return err
	}
	evmRPCURL, err := requiredEnv(config.EVM.RPCURLEnv)
	if err != nil {
		return err
	}
	evmRPC, err := ethclient.DialContext(ctx, evmRPCURL)
	if err != nil {
		return fmt.Errorf("connect EVM RPC: %w", err)
	}
	defer evmRPC.Close()
	storePath := strings.TrimSpace(storeOverride)
	if storePath == "" {
		storePath = config.StorePath
	}
	store, err := sqlitestore.OpenNTTCanary(storePath)
	if err != nil {
		return err
	}
	defer store.Close()
	operationID := ""
	if hooks != nil {
		operationID = strings.TrimSpace(hooks.OperationID)
	}
	if operationID == "" {
		operationID, err = newCanaryOperationID()
		if err != nil {
			return err
		}
	}
	storedAmount := amountText
	if storedAmount == "" {
		storedAmount = "source-transaction-recovery"
	}
	runtime := &armedRuntime{
		config: config, solanaAdapter: solanaAdapter, evmAdapter: evmAdapter,
		solanaKey: solanaKey, evmKey: evmKey,
		solanaRPC: solanarpc.New(solanaRPCURL), evmRPC: evmRPC,
		store: store, output: output, operationID: operationID,
		direction: route,
		liveHooks: hooks,
	}
	fmt.Fprintf(
		output,
		"operation=%s mode=armed direction=%s journal=%s\n",
		operationID,
		route,
		storePath,
	)
	readinessStarted := time.Now()
	if err := runtime.validateNetworks(ctx); err != nil {
		return err
	}
	readinessDuration := time.Since(readinessStarted)
	now := time.Now().UTC()
	if err := store.CreateOrReuseUnbroadcast(
		ctx,
		sqlitestore.NTTCanaryOperation{
			ID: operationID, Direction: string(route), AmountUnits: storedAmount,
			Stage: "created", SourceTx: sourceTransaction,
			CreatedAt: now,
		},
	); err != nil {
		fmt.Fprintf(
			output,
			"ntt_recovery_error phase=journal_prepare operation=%s error=%q\n",
			operationID,
			safeerr.Message(err),
		)
		return err
	}

	bridgeStarted := time.Now()
	sourceStarted := time.Now()
	var message crosschainport.MessageID
	if sourceTransaction == "" {
		amount, ok := new(big.Int).SetString(amountText, 10)
		if !ok || amount.Sign() <= 0 {
			return fmt.Errorf("armed amount must be a positive fixed integer")
		}
		sourceTransaction, message, err = runtime.executeSource(ctx, route, amount)
		if err != nil {
			_ = store.Fail(ctx, operationID, "source_failed", err)
			return err
		}
	} else {
		persisted, found, lookupErr :=
			store.FindMessageBySourceTransaction(ctx, sourceTransaction)
		if lookupErr != nil {
			err = fmt.Errorf("load durable Wormhole message identity: %w", lookupErr)
		} else if found {
			address, decodeErr := hex.DecodeString(persisted.EmitterAddress)
			if decodeErr != nil || len(address) != 32 {
				err = fmt.Errorf("durable Wormhole emitter address is invalid")
			} else {
				message.EmitterChain = persisted.EmitterChain
				copy(message.EmitterAddress[:], address)
				message.Sequence = persisted.Sequence
				fmt.Fprintf(
					output,
					"source_message=reused tx=%s emitter_chain=%d emitter_address=%s sequence=%d\n",
					sourceTransaction,
					message.EmitterChain,
					persisted.EmitterAddress,
					message.Sequence,
				)
			}
		} else {
			message, err = messageFromSourceTransaction(
				ctx,
				route,
				config,
				solanaAdapter,
				evmAdapter,
				sourceTransaction,
			)
		}
		if err != nil {
			fmt.Fprintf(
				output,
				"ntt_recovery_error phase=source_message operation=%s tx=%s error=%q\n",
				operationID,
				sourceTransaction,
				safeerr.Message(err),
			)
			_ = store.Fail(ctx, operationID, "source_recovery_failed", err)
			return err
		}
	}
	sourceDuration := time.Since(sourceStarted)
	emitterAddress := hex.EncodeToString(message.EmitterAddress[:])
	if err := store.UpdateMessage(
		ctx,
		operationID,
		sourceTransaction,
		message.EmitterChain,
		emitterAddress,
		message.Sequence,
		"",
		"source_confirmed",
	); err != nil {
		return err
	}
	fmt.Fprintf(
		output,
		"source_confirmed tx=%s emitter_chain=%d emitter_address=%s sequence=%d\n",
		sourceTransaction,
		message.EmitterChain,
		emitterAddress,
		message.Sequence,
	)

	attestationStarted := time.Now()
	attestation, err := runtime.awaitAttestation(ctx, message)
	if err != nil {
		_ = store.Fail(ctx, operationID, "attestation_failed", err)
		return err
	}
	attestationDuration := time.Since(attestationStarted)
	fingerprint := wormholentt.FingerprintVAA(attestation.Raw)
	if err := store.UpdateMessage(
		ctx,
		operationID,
		sourceTransaction,
		message.EmitterChain,
		emitterAddress,
		message.Sequence,
		fingerprint,
		"attestation_ready",
	); err != nil {
		return err
	}
	fmt.Fprintf(output, "vaa_ready fingerprint=%s\n", fingerprint)
	fmt.Fprintf(
		output,
		"timer phase=attestation duration=%s\n",
		formatMetricDuration(attestationDuration),
	)
	destinationStarted := time.Now()
	if err := runtime.executeDestination(
		ctx,
		route,
		attestation.Raw,
		verifiedSignatureTx,
	); err != nil {
		_ = store.Fail(ctx, operationID, "destination_failed", err)
		return err
	}
	destinationDuration := time.Since(destinationStarted)
	bridgeDuration := time.Since(bridgeStarted)
	commandDuration := time.Since(commandStarted)
	if err := store.UpdateMessage(
		ctx,
		operationID,
		sourceTransaction,
		message.EmitterChain,
		emitterAddress,
		message.Sequence,
		fingerprint,
		"completed",
	); err != nil {
		return err
	}
	runtime.finalizeSolanaTelemetry(ctx)
	runtime.persistMeasurements(ctx)
	mode := "fresh"
	if storedAmount == "source-transaction-recovery" {
		mode = "recovery"
	}
	operationMetrics := runtime.operationMetrics(
		mode,
		readinessDuration,
		sourceDuration,
		attestationDuration,
		destinationDuration,
		bridgeDuration,
		commandDuration,
	)
	if runtime.liveHooks != nil && runtime.liveHooks.Result != nil {
		costs, costErr := runtime.liveCostComponents(operationMetrics)
		if costErr != nil {
			return costErr
		}
		runtime.liveHooks.Result.Costs = costs
	}
	if err := store.RecordOperationMetrics(
		context.WithoutCancel(ctx),
		operationMetrics,
	); err != nil {
		fmt.Fprintf(output, "telemetry_warning stage=persist_operation error=%q\n", err)
	}
	runtime.printMetricsSummary(operationMetrics)
	fmt.Fprintf(output, "completed operation=%s\n", operationID)
	return nil
}

func (r *armedRuntime) validateNetworks(ctx context.Context) error {
	chainID, err := r.evmRPC.ChainID(ctx)
	if err != nil {
		return fmt.Errorf("read EVM chain ID: %w", err)
	}
	if chainID.Cmp(r.evmAdapter.ChainID()) != 0 {
		return fmt.Errorf("configured EVM chain ID does not match RPC")
	}
	var solanaNative *big.Int
	if r.liveHooks != nil && r.liveHooks.NativeBalance != nil {
		solanaNative, err = r.liveHooks.NativeBalance(r.liveHooks.SolanaChain)
		if err != nil {
			return fmt.Errorf("read local Solana payer balance: %w", err)
		}
	} else {
		solanaBalance, balanceErr := r.solanaRPC.GetBalance(
			ctx, r.solanaKey.PublicKey(), solanarpc.CommitmentConfirmed,
		)
		if balanceErr != nil {
			return fmt.Errorf("read Solana payer balance: %w", balanceErr)
		}
		solanaNative = new(big.Int).SetUint64(solanaBalance.Value)
	}
	if solanaNative.Cmp(new(big.Int).SetUint64(r.config.Solana.MinimumBalanceLamports)) < 0 {
		return executionport.NewChainRecoveryError(
			executionport.RecoveryFailureInsufficientNative,
			r.liveChain("solana"),
			fmt.Errorf(
				"solana payer has %s lamports; at least %d are required",
				solanaNative,
				r.config.Solana.MinimumBalanceLamports,
			),
		)
	}
	minimumEVM, ok := new(big.Int).SetString(
		strings.TrimSpace(r.config.EVM.MinimumNativeBalanceWei),
		10,
	)
	if !ok || minimumEVM.Sign() < 0 {
		return fmt.Errorf("invalid minimum EVM native balance")
	}
	evmAddress := gethcrypto.PubkeyToAddress(r.evmKey.PublicKey)
	var evmBalance *big.Int
	if r.liveHooks != nil && r.liveHooks.NativeBalance != nil {
		evmBalance, err = r.liveHooks.NativeBalance(r.liveHooks.EVMChain)
		if err != nil {
			return fmt.Errorf("read local EVM sender balance: %w", err)
		}
	} else {
		evmBalance, err = r.evmRPC.BalanceAt(ctx, evmAddress, nil)
		if err != nil {
			return fmt.Errorf("read EVM sender balance: %w", err)
		}
	}
	if evmBalance.Cmp(minimumEVM) < 0 {
		return executionport.NewChainRecoveryError(
			executionport.RecoveryFailureInsufficientNative,
			r.liveChain("evm"),
			fmt.Errorf(
				"EVM sender has %s wei; at least %s are required",
				evmBalance,
				minimumEVM,
			),
		)
	}
	fmt.Fprintf(
		r.output,
		"readiness=ok solana_payer=%s evm_sender=%s\n",
		r.solanaKey.PublicKey(),
		evmAddress.Hex(),
	)
	return nil
}

func (r *armedRuntime) executeSource(
	ctx context.Context,
	route direction,
	amount *big.Int,
) (string, crosschainport.MessageID, error) {
	switch route {
	case evmToSolana:
		if err := r.checkEVMSource(ctx, amount); err != nil {
			return "", crosschainport.MessageID{}, err
		}
		priceCall, err := r.evmAdapter.BuildDeliveryPriceCall(
			r.config.Solana.WormholeChain,
		)
		if err != nil {
			return "", crosschainport.MessageID{}, err
		}
		rawPrice, err := r.evmRPC.CallContract(
			ctx,
			geth.CallMsg{To: &priceCall.To, Data: priceCall.Data},
			nil,
		)
		if err != nil {
			return "", crosschainport.MessageID{}, fmt.Errorf(
				"query NTT delivery price: %w",
				err,
			)
		}
		price, err := wormholentt.DecodeDeliveryPrice(rawPrice)
		if err != nil {
			return "", crosschainport.MessageID{}, err
		}
		evmSender := gethcrypto.PubkeyToAddress(r.evmKey.PublicKey)
		call, err := r.evmAdapter.BuildTransfer(
			amount,
			r.config.Solana.WormholeChain,
			wormholentt.SolanaUniversalAddress(r.solanaKey.PublicKey()),
			wormholentt.EVMUniversalAddress(evmSender),
			price,
		)
		if err != nil {
			return "", crosschainport.MessageID{}, err
		}
		receipt, err := r.sendEVM(ctx, "source_transfer", call)
		if err != nil {
			return "", crosschainport.MessageID{}, fmt.Errorf(
				"send NTT source transfer amount %s: %w",
				amount,
				err,
			)
		}
		message, err := r.evmAdapter.MessageFromReceipt(receipt)
		return receipt.TxHash.Hex(), message, err
	case solanaToEVM:
		if !amount.IsUint64() {
			return "", crosschainport.MessageID{}, fmt.Errorf("solana amount exceeds uint64")
		}
		if err := r.checkSolanaTokenBalance(ctx, amount.Uint64()); err != nil {
			return "", crosschainport.MessageID{}, err
		}
		evmSender := gethcrypto.PubkeyToAddress(r.evmKey.PublicKey)
		plan, err := r.solanaAdapter.BuildTransferBurn(
			r.solanaKey.PublicKey(),
			amount.Uint64(),
			r.config.EVM.WormholeChain,
			wormholentt.EVMUniversalAddress(evmSender),
		)
		if err != nil {
			return "", crosschainport.MessageID{}, err
		}
		signature, err := r.sendSolana(
			ctx,
			"source_transfer",
			plan.Instructions,
			[]solanago.PrivateKey{plan.Outbox},
		)
		if err != nil {
			return "", crosschainport.MessageID{}, err
		}
		message, err := messageFromSourceTransaction(
			ctx,
			solanaToEVM,
			r.config,
			r.solanaAdapter,
			r.evmAdapter,
			signature,
		)
		return signature, message, err
	default:
		return "", crosschainport.MessageID{}, fmt.Errorf("unsupported direction")
	}
}

func (r *armedRuntime) executeDestination(
	ctx context.Context,
	route direction,
	rawVAA []byte,
	verifiedSignatureTx string,
) error {
	switch route {
	case solanaToEVM:
		call, err := r.evmAdapter.BuildRedeem(rawVAA)
		if err != nil {
			return err
		}
		receipt, err := r.sendEVM(ctx, "destination_redeem", call)
		if err != nil {
			return err
		}
		fmt.Fprintf(r.output, "destination_confirmed chain=evm tx=%s\n", receipt.TxHash.Hex())
		return nil
	case evmToSolana:
		vaa, err := wormholentt.ParseVAA(rawVAA)
		if err != nil {
			return err
		}
		account, err := r.solanaRPC.GetAccountInfoWithOpts(
			ctx,
			r.solanaAdapter.GuardianSetAddress(vaa.GuardianSetIndex),
			&solanarpc.GetAccountInfoOpts{
				Encoding:   solanago.EncodingBase64,
				Commitment: solanarpc.CommitmentConfirmed,
			},
		)
		if err != nil || account == nil || account.Value == nil {
			return fmt.Errorf("read Wormhole GuardianSet: %w", err)
		}
		guardians, err := wormholentt.DecodeGuardianSet(account.Value.Data.GetBinary())
		if err != nil {
			return err
		}
		var plan wormholentt.SolanaRedemptionPlan
		if verifiedSignatureTx == "" {
			plan, err = r.solanaAdapter.BuildRedemption(
				rawVAA,
				guardians.Keys,
				r.solanaKey.PublicKey(),
			)
		} else {
			signatureSet, setErr := r.verifiedSignatureSetFromTransaction(
				ctx,
				verifiedSignatureTx,
				vaa,
				len(guardians.Keys),
			)
			if setErr != nil {
				return setErr
			}
			plan, err = r.solanaAdapter.BuildRedemptionWithVerifiedSignatureSet(
				rawVAA,
				guardians.Keys,
				r.solanaKey.PublicKey(),
				signatureSet,
			)
			if err == nil {
				fmt.Fprintf(
					r.output,
					"guardian_verification=reused signature_set=%s evidence_tx=%s\n",
					signatureSet,
					verifiedSignatureTx,
				)
			}
		}
		if err != nil {
			return err
		}
		recipientATA, _, err := solanago.FindAssociatedTokenAddress(
			solanago.PublicKey(plan.Message.Recipient),
			r.solanaAdapter.TokenMint(),
		)
		if err != nil {
			return err
		}
		recipientAccount, err := r.solanaRPC.GetAccountInfoWithOpts(
			ctx,
			recipientATA,
			&solanarpc.GetAccountInfoOpts{
				Encoding:   solanago.EncodingBase64,
				Commitment: solanarpc.CommitmentConfirmed,
			},
		)
		if err != nil || recipientAccount == nil || recipientAccount.Value == nil {
			return fmt.Errorf(
				"destination Solana token account %s must already exist",
				recipientATA,
			)
		}
		posted, postedErr := r.solanaRPC.GetAccountInfoWithOpts(
			ctx,
			plan.PostedVAA,
			&solanarpc.GetAccountInfoOpts{
				Encoding:   solanago.EncodingBase64,
				Commitment: solanarpc.CommitmentConfirmed,
			},
		)
		alreadyPosted := postedErr == nil && posted != nil && posted.Value != nil
		if !alreadyPosted {
			for _, batch := range plan.Verify {
				if _, err := r.sendSolanaDestinationBatch(
					ctx,
					batch,
					solanago.PublicKey{},
					"",
				); err != nil {
					return err
				}
			}
			if _, err := r.sendSolanaDestinationBatch(
				ctx,
				plan.PostVAA,
				plan.PostedVAA,
				"posted_vaa",
			); err != nil {
				return err
			}
		} else {
			fmt.Fprintf(r.output, "posted_vaa=already_present address=%s\n", plan.PostedVAA)
		}
		signature, err := r.sendSolanaDestinationBatch(
			ctx,
			plan.Redeem,
			plan.InboxItem,
			"ntt_inbox_item",
		)
		if err != nil {
			return err
		}
		fmt.Fprintf(r.output, "destination_confirmed chain=solana tx=%s\n", signature)
		return nil
	default:
		return fmt.Errorf("unsupported direction")
	}
}

const maxSolanaDestinationAttempts = 3

// sendSolanaDestinationBatch may create a new transaction identity only after
// the previous blockhash has definitively expired without a signature. Source
// transfers deliberately do not use this helper.
func (r *armedRuntime) sendSolanaDestinationBatch(
	ctx context.Context,
	batch wormholentt.SolanaInstructionBatch,
	completionAccount solanago.PublicKey,
	completionLabel string,
) (string, error) {
	var completion func(context.Context) (bool, error)
	if !completionAccount.IsZero() {
		completion = func(checkCtx context.Context) (bool, error) {
			return r.solanaAccountExists(checkCtx, completionAccount)
		}
	}
	return RecoverSolanaDestinationBatch(
		ctx,
		batch.Kind,
		completionLabel,
		completionAccount.String(),
		maxSolanaDestinationAttempts,
		r.output,
		func(attemptCtx context.Context) (string, error) {
			return r.sendSolana(
				attemptCtx,
				batch.Kind,
				batch.Instructions,
				batch.Signers,
			)
		},
		completion,
		func(expired *SolanaBlockhashExpiredError) {
			if expired.Ordinal > 0 {
				_ = r.store.MarkTransaction(
					context.WithoutCancel(ctx),
					r.operationID,
					expired.Ordinal,
					"confirmed",
				)
			}
			if expired.OuterPhase != "" {
				_ = r.liveHooks.mark(
					context.WithoutCancel(ctx),
					expired.OuterPhase,
					"confirmed",
				)
			}
		},
	)
}

func RecoverSolanaDestinationBatch(
	ctx context.Context,
	phase string,
	completionLabel string,
	completionAccount string,
	maxAttempts int,
	output io.Writer,
	send func(context.Context) (string, error),
	completion func(context.Context) (bool, error),
	markStateConfirmed func(*SolanaBlockhashExpiredError),
) (string, error) {
	if maxAttempts <= 0 || output == nil || send == nil {
		return "", fmt.Errorf("invalid Solana destination recovery policy")
	}
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		signature, err := send(ctx)
		if err == nil {
			return signature, nil
		}
		var expired *SolanaBlockhashExpiredError
		if !errors.As(err, &expired) {
			return "", err
		}
		if completion != nil {
			exists, stateErr := completion(ctx)
			if stateErr != nil {
				return "", fmt.Errorf(
					"reconcile expired Solana %s transaction: %w",
					phase,
					stateErr,
				)
			}
			if exists {
				if markStateConfirmed != nil {
					markStateConfirmed(expired)
				}
				fmt.Fprintf(
					output,
					"recovered chain=solana phase=%s tx=%s evidence=%s "+
						"account=%s\n",
					phase,
					expired.Signature,
					completionLabel,
					completionAccount,
				)
				return expired.Signature, nil
			}
		}
		if attempt == maxAttempts {
			return "", fmt.Errorf(
				"solana %s did not land after %d blockhash lifetimes: %w",
				phase,
				attempt,
				err,
			)
		}
		fmt.Fprintf(
			output,
			"rebuild chain=solana phase=%s previous_tx=%s "+
				"reason=blockhash_expired attempt=%d/%d\n",
			phase,
			expired.Signature,
			attempt+1,
			maxAttempts,
		)
	}
	return "", fmt.Errorf("solana destination recovery exhausted")
}

func (r *armedRuntime) solanaAccountExists(
	ctx context.Context,
	account solanago.PublicKey,
) (bool, error) {
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	result, err := r.solanaRPC.GetAccountInfoWithOpts(
		callCtx,
		account,
		&solanarpc.GetAccountInfoOpts{
			Encoding:   solanago.EncodingBase64,
			Commitment: solanarpc.CommitmentConfirmed,
		},
	)
	if err != nil {
		return false, err
	}
	return result != nil && result.Value != nil, nil
}

func (r *armedRuntime) verifiedSignatureSetFromTransaction(
	ctx context.Context,
	reference string,
	vaa wormholentt.VAA,
	guardianCount int,
) (solanago.PublicKey, error) {
	return verifiedSignatureSetFromTransaction(
		ctx,
		r.solanaRPC,
		r.solanaAdapter,
		reference,
		vaa,
		guardianCount,
	)
}

func verifiedSignatureSetFromTransaction(
	ctx context.Context,
	client *solanarpc.Client,
	adapter *wormholentt.SolanaAdapter,
	reference string,
	vaa wormholentt.VAA,
	guardianCount int,
) (solanago.PublicKey, error) {
	signature, err := solanago.SignatureFromBase58(strings.TrimSpace(reference))
	if err != nil {
		return solanago.PublicKey{}, fmt.Errorf("invalid guardian-verification transaction")
	}
	version := uint64(0)
	result, err := client.GetTransaction(
		ctx,
		signature,
		&solanarpc.GetTransactionOpts{
			Encoding:                       solanago.EncodingBase64,
			Commitment:                     solanarpc.CommitmentConfirmed,
			MaxSupportedTransactionVersion: &version,
		},
	)
	if err != nil || result == nil || result.Meta == nil || result.Meta.Err != nil ||
		result.Transaction == nil {
		return solanago.PublicKey{}, fmt.Errorf(
			"read successful guardian-verification transaction: %w",
			err,
		)
	}
	transaction, err := result.Transaction.GetTransaction()
	if err != nil {
		return solanago.PublicKey{}, err
	}
	var signatureSet solanago.PublicKey
	for _, instruction := range transaction.Message.Instructions {
		if int(instruction.ProgramIDIndex) >= len(transaction.Message.AccountKeys) ||
			transaction.Message.AccountKeys[instruction.ProgramIDIndex] !=
				adapter.WormholeCore() ||
			len(instruction.Data) == 0 || instruction.Data[0] != 7 ||
			len(instruction.Accounts) < 3 {
			continue
		}
		accountIndex := int(instruction.Accounts[2])
		if accountIndex >= len(transaction.Message.AccountKeys) {
			return solanago.PublicKey{}, fmt.Errorf(
				"guardian-verification transaction has an invalid SignatureSet account",
			)
		}
		signatureSet = transaction.Message.AccountKeys[accountIndex]
		break
	}
	if signatureSet.IsZero() {
		return solanago.PublicKey{}, fmt.Errorf(
			"transaction contains no Wormhole guardian verification",
		)
	}
	account, err := client.GetAccountInfoWithOpts(
		ctx,
		signatureSet,
		&solanarpc.GetAccountInfoOpts{
			Encoding:   solanago.EncodingBase64,
			Commitment: solanarpc.CommitmentConfirmed,
		},
	)
	if err != nil || account == nil || account.Value == nil {
		return solanago.PublicKey{}, fmt.Errorf(
			"read verified Wormhole SignatureSet %s: %w",
			signatureSet,
			err,
		)
	}
	if account.Value.Owner != adapter.WormholeCore() {
		return solanago.PublicKey{}, fmt.Errorf(
			"verified SignatureSet is not owned by Wormhole Core",
		)
	}
	set, err := wormholentt.DecodeSignatureSet(account.Value.Data.GetBinary())
	if err != nil {
		return solanago.PublicKey{}, err
	}
	if set.GuardianSetIndex != vaa.GuardianSetIndex || set.Hash != vaa.Hash ||
		len(set.Signatures) != guardianCount {
		return solanago.PublicKey{}, fmt.Errorf(
			"verified SignatureSet does not belong to this VAA",
		)
	}
	signatures := 0
	for _, present := range set.Signatures {
		if present {
			signatures++
		}
	}
	required := ((guardianCount*10/3)*2)/10 + 1
	if signatures < required {
		return solanago.PublicKey{}, fmt.Errorf(
			"verified SignatureSet has %d signatures; quorum requires %d",
			signatures,
			required,
		)
	}
	return signatureSet, nil
}

func (r *armedRuntime) awaitAttestation(
	ctx context.Context,
	message crosschainport.MessageID,
) (crosschainport.Attestation, error) {
	endpoints, err := guardianEndpoints(r.config)
	if err != nil {
		return crosschainport.Attestation{}, err
	}
	client, err := wormholentt.NewGuardianClient(wormholentt.GuardianClientConfig{
		Endpoints:    endpoints,
		PollInterval: 200 * time.Millisecond,
	})
	if err != nil {
		return crosschainport.Attestation{}, err
	}
	waitCtx, cancel := context.WithTimeout(ctx, r.config.AttestationTimeout)
	defer cancel()
	return client.Await(waitCtx, message)
}

func (r *armedRuntime) checkEVMSource(ctx context.Context, amount *big.Int) error {
	sender := gethcrypto.PubkeyToAddress(r.evmKey.PublicKey)
	balanceCall, err := r.evmAdapter.BuildBalanceCall(sender)
	if err != nil {
		return err
	}
	var balance *big.Int
	if r.liveHooks != nil && r.liveHooks.SourceTokenBalance != nil {
		balance, err = r.liveHooks.SourceTokenBalance(r.liveHooks.EVMChain)
		if err != nil {
			return fmt.Errorf("read local EVM token balance: %w", err)
		}
		if balance.Cmp(amount) < 0 {
			return fmt.Errorf("EVM token balance %s is below amount %s", balance, amount)
		}
	} else if r.liveHooks == nil {
		rawBalance, callErr := r.evmRPC.CallContract(
			ctx,
			geth.CallMsg{To: &balanceCall.To, Data: balanceCall.Data},
			nil,
		)
		if callErr != nil {
			return fmt.Errorf("query source EVM token balance: %w", callErr)
		}
		balance, err = wormholentt.DecodeTokenBalance(rawBalance)
		if err != nil {
			return err
		}
		if balance.Cmp(amount) < 0 {
			return fmt.Errorf(
				"EVM token balance %s is below amount %s",
				balance,
				amount,
			)
		}
	} else {
		var observedBlock uint64
		var attempts int
		balance, observedBlock, attempts, err =
			AwaitEVMSourceBalanceVisibility(
				ctx,
				amount,
				r.liveHooks.BalanceVisibilityTimeout,
				r.liveHooks.BalancePollInterval,
				func(readCtx context.Context) (*big.Int, uint64, error) {
					block, blockErr := r.evmRPC.BlockNumber(readCtx)
					if blockErr != nil {
						return nil, 0, blockErr
					}
					raw, callErr := r.evmRPC.CallContract(
						readCtx,
						geth.CallMsg{
							To:   &balanceCall.To,
							Data: balanceCall.Data,
						},
						new(big.Int).SetUint64(block),
					)
					if callErr != nil {
						return nil, block, callErr
					}
					value, decodeErr :=
						wormholentt.DecodeTokenBalance(raw)
					return value, block, decodeErr
				},
			)
		if err != nil {
			return fmt.Errorf(
				"wait for source EVM token balance visibility: %w",
				err,
			)
		}
		fmt.Fprintf(
			r.output,
			"ntt_readiness phase=source_balance_visible chain=evm attempts=%d observed_block=%d token_balance=%s required_units=%s\n",
			attempts,
			observedBlock,
			balance,
			amount,
		)
	}
	allowanceCall, err := r.evmAdapter.BuildAllowanceCall(sender)
	if err != nil {
		return err
	}
	rawAllowance, err := r.evmRPC.CallContract(
		ctx,
		geth.CallMsg{To: &allowanceCall.To, Data: allowanceCall.Data},
		nil,
	)
	if err != nil {
		return fmt.Errorf("query source EVM token allowance: %w", err)
	}
	allowance, err := wormholentt.DecodeAllowance(rawAllowance)
	if err != nil {
		return err
	}
	if allowance.Cmp(amount) < 0 {
		return fmt.Errorf(
			"EVM token allowance %s is below amount %s; approve manager %s first",
			allowance,
			amount,
			r.evmAdapter.Manager().Hex(),
		)
	}
	fmt.Fprintf(
		r.output,
		"source_readiness=ok chain=evm token_balance=%s allowance=%s\n",
		balance,
		allowance,
	)
	return nil
}

func (r *armedRuntime) checkSolanaTokenBalance(ctx context.Context, amount uint64) error {
	if r.liveHooks != nil && r.liveHooks.SourceTokenBalance != nil {
		balance, err := r.liveHooks.SourceTokenBalance(r.liveHooks.SolanaChain)
		if err != nil {
			return fmt.Errorf("read local Solana token balance: %w", err)
		}
		if !balance.IsUint64() || balance.Uint64() < amount {
			return fmt.Errorf("solana token balance %s is below amount %d", balance, amount)
		}
		fmt.Fprintf(r.output, "source_readiness=ok chain=solana token_balance=%s source=local_balance_manager\n", balance)
		return nil
	}
	ata, _, err := solanago.FindAssociatedTokenAddress(
		r.solanaKey.PublicKey(),
		r.solanaAdapter.TokenMint(),
	)
	if err != nil {
		return err
	}
	balance, err := r.solanaRPC.GetTokenAccountBalance(
		ctx,
		ata,
		solanarpc.CommitmentConfirmed,
	)
	if err != nil || balance == nil || balance.Value == nil {
		return fmt.Errorf("read source Solana token account %s: %w", ata, err)
	}
	units, ok := new(big.Int).SetString(balance.Value.Amount, 10)
	if !ok || !units.IsUint64() || units.Uint64() < amount {
		return fmt.Errorf(
			"solana token balance %s is below amount %d",
			balance.Value.Amount,
			amount,
		)
	}
	fmt.Fprintf(
		r.output,
		"source_readiness=ok chain=solana token_account=%s token_balance=%s\n",
		ata,
		balance.Value.Amount,
	)
	return nil
}

func (r *armedRuntime) sendEVM(
	ctx context.Context,
	phase string,
	call wormholentt.EVMCall,
) (*types.Receipt, error) {
	phaseStarted := time.Now()
	sender := gethcrypto.PubkeyToAddress(r.evmKey.PublicKey)
	var nonce uint64
	var err error
	if r.liveHooks != nil && r.liveHooks.NonceCoordinator != nil {
		nonce, err = r.liveHooks.NonceCoordinator.NextNonce()
	} else {
		nonce, err = r.evmRPC.PendingNonceAt(ctx, sender)
	}
	if err != nil {
		return nil, fmt.Errorf("read coordinated EVM nonce: %w", err)
	}
	tip, err := r.evmRPC.SuggestGasTipCap(ctx)
	if err != nil {
		return nil, fmt.Errorf("read EVM priority fee: %w", err)
	}
	header, err := r.evmRPC.HeaderByNumber(ctx, nil)
	if err != nil || header == nil || header.BaseFee == nil {
		return nil, fmt.Errorf("read EVM base fee: %w", err)
	}
	feeCap := new(big.Int).Add(
		new(big.Int).Mul(header.BaseFee, big.NewInt(2)),
		tip,
	)
	estimatedGas, err := r.evmRPC.EstimateGas(ctx, geth.CallMsg{
		From: sender, To: &call.To, Value: call.Value, Data: call.Data,
		GasFeeCap: feeCap, GasTipCap: tip,
	})
	if err != nil {
		return nil, fmt.Errorf("estimate EVM gas for %s: %w", phase, err)
	}
	gasLimit := estimatedGas * r.config.EVM.GasLimitMultiplierBPS / 10_000
	if gasLimit < estimatedGas || gasLimit == 0 {
		return nil, fmt.Errorf("invalid EVM gas multiplier")
	}
	nativeBalance, err := r.evmRPC.BalanceAt(ctx, sender, nil)
	if err != nil {
		return nil, fmt.Errorf("read EVM balance before %s: %w", phase, err)
	}
	maximumCost := new(big.Int).Mul(new(big.Int).SetUint64(gasLimit), feeCap)
	maximumCost.Add(maximumCost, call.Value)
	if nativeBalance.Cmp(maximumCost) < 0 {
		return nil, fmt.Errorf(
			"EVM native balance %s cannot cover maximum %s cost %s",
			nativeBalance,
			phase,
			maximumCost,
		)
	}
	transaction := types.NewTx(&types.DynamicFeeTx{
		ChainID: r.evmAdapter.ChainID(), Nonce: nonce,
		GasTipCap: tip, GasFeeCap: feeCap, Gas: gasLimit,
		To: &call.To, Value: call.Value, Data: call.Data,
	})
	signed, err := types.SignTx(
		transaction,
		types.LatestSignerForChainID(r.evmAdapter.ChainID()),
		r.evmKey,
	)
	if err != nil {
		return nil, err
	}
	r.ordinal++
	ordinal := r.ordinal
	now := time.Now().UTC()
	if err := r.store.RecordPrepared(ctx, sqlitestore.NTTCanaryTransaction{
		OperationID: r.operationID, Ordinal: ordinal, Phase: phase,
		Chain: "evm", Identity: signed.Hash().Hex(),
		Nonce: strconv.FormatUint(nonce, 10), Status: "prepared",
		CreatedAt: now,
	}); err != nil {
		return nil, fmt.Errorf("persist EVM identity before broadcast: %w", err)
	}
	outerPhase := fmt.Sprintf("%s-%d", phase, ordinal)
	nonceCopy := nonce
	if err := r.liveHooks.prepared(ctx, r.liveChain("evm"), outerPhase, domainexecution.TransactionIdentity{
		Chain: r.liveChain("evm"), Hash: signed.Hash().Hex(), Nonce: &nonceCopy,
	}); err != nil {
		return nil, fmt.Errorf("persist Live EVM identity before broadcast: %w", err)
	}
	fmt.Fprintf(
		r.output,
		"prepared chain=evm phase=%s tx=%s nonce=%d gas=%d timer=%s\n",
		phase,
		signed.Hash().Hex(),
		nonce,
		gasLimit,
		formatMetricDuration(time.Since(phaseStarted)),
	)
	broadcastStarted := time.Now()
	if err := r.evmRPC.SendTransaction(ctx, signed); err != nil {
		if r.liveHooks != nil && r.liveHooks.NonceCoordinator != nil {
			r.liveHooks.NonceCoordinator.MarkNonceUsed(nonce)
		}
		_ = r.store.MarkTransaction(
			context.WithoutCancel(ctx),
			r.operationID,
			ordinal,
			"outcome_unknown",
		)
		_ = r.liveHooks.mark(context.WithoutCancel(ctx), outerPhase, "outcome_unknown")
		return nil, fmt.Errorf(
			"broadcast EVM %s returned an uncertain result; do not resend automatically: %w",
			phase,
			err,
		)
	}
	if r.liveHooks != nil && r.liveHooks.NonceCoordinator != nil {
		r.liveHooks.NonceCoordinator.MarkNonceUsed(nonce)
	}
	broadcastDuration := time.Since(broadcastStarted)
	if err := r.store.MarkTransaction(
		ctx,
		r.operationID,
		ordinal,
		"broadcast",
	); err != nil {
		return nil, err
	}
	if err := r.liveHooks.mark(ctx, outerPhase, "broadcast"); err != nil {
		return nil, err
	}
	fmt.Fprintf(
		r.output,
		"broadcast chain=evm phase=%s tx=%s timer=%s\n",
		phase,
		signed.Hash().Hex(),
		formatMetricDuration(broadcastDuration),
	)
	confirmationStarted := time.Now()
	receipt, err := r.awaitEVMReceipt(ctx, signed.Hash())
	if err != nil {
		_ = r.store.MarkTransaction(
			context.WithoutCancel(ctx),
			r.operationID,
			ordinal,
			"outcome_unknown",
		)
		_ = r.liveHooks.mark(context.WithoutCancel(ctx), outerPhase, "outcome_unknown")
		return nil, err
	}
	status := "confirmed"
	if receipt.Status != types.ReceiptStatusSuccessful {
		status = "failed"
	}
	if err := r.store.MarkTransaction(ctx, r.operationID, ordinal, status); err != nil {
		return nil, err
	}
	outerStatus := "confirmed"
	if status != "confirmed" {
		outerStatus = "outcome_unknown"
	}
	if err := r.liveHooks.mark(ctx, outerPhase, outerStatus); err != nil {
		return nil, err
	}
	if receipt.Status != types.ReceiptStatusSuccessful {
		return nil, fmt.Errorf("EVM %s transaction reverted: %s", phase, signed.Hash())
	}
	confirmationDuration := time.Since(confirmationStarted)
	networkFee := new(big.Int)
	effectiveGasPrice := new(big.Int)
	if receipt.EffectiveGasPrice != nil {
		effectiveGasPrice.Set(receipt.EffectiveGasPrice)
		networkFee.Mul(
			new(big.Int).SetUint64(receipt.GasUsed),
			receipt.EffectiveGasPrice,
		)
	}
	measurement := phaseMeasurement{
		Chain: "evm", Phase: phase, Identity: signed.Hash().Hex(),
		Metrics: sqlitestore.NTTCanaryTransactionMetrics{
			OperationID: r.operationID, Ordinal: ordinal,
			PrepareDuration:      broadcastStarted.Sub(phaseStarted),
			BroadcastDuration:    broadcastDuration,
			ConfirmationDuration: confirmationDuration,
			TotalDuration:        time.Since(phaseStarted),
			NetworkFeeUnits:      networkFee.String(),
			FeeAsset:             "wei",
			AdditionalDebitUnits: signed.Value().String(),
			GasUsed:              receipt.GasUsed,
			EffectiveGasPrice:    effectiveGasPrice.String(),
		},
	}
	r.recordMeasurement(ctx, measurement)
	fmt.Fprintf(
		r.output,
		"confirmed chain=evm phase=%s tx=%s block=%d gas_used=%d "+
			"gas_price_wei=%s network_fee_wei=%s value_wei=%s "+
			"confirm=%s total=%s\n",
		phase,
		signed.Hash().Hex(),
		receipt.BlockNumber.Uint64(),
		receipt.GasUsed,
		effectiveGasPrice,
		networkFee,
		signed.Value(),
		formatMetricDuration(confirmationDuration),
		formatMetricDuration(measurement.Metrics.TotalDuration),
	)
	return receipt, nil
}

func (r *armedRuntime) awaitEVMReceipt(
	ctx context.Context,
	hash common.Hash,
) (*types.Receipt, error) {
	waitCtx, cancel := context.WithTimeout(
		ctx,
		time.Duration(r.config.ConfirmationTimeoutS)*time.Second,
	)
	defer cancel()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		receipt, err := r.evmRPC.TransactionReceipt(waitCtx, hash)
		if err == nil {
			return receipt, nil
		}
		if !errors.Is(err, geth.NotFound) {
			return nil, fmt.Errorf("read EVM receipt %s: %w", hash, err)
		}
		select {
		case <-waitCtx.Done():
			return nil, fmt.Errorf(
				"EVM transaction %s outcome is unknown after confirmation timeout",
				hash,
			)
		case <-ticker.C:
		}
	}
}

func (r *armedRuntime) sendSolana(
	ctx context.Context,
	phase string,
	instructions []solanago.Instruction,
	additionalSigners []solanago.PrivateKey,
) (string, error) {
	phaseStarted := time.Now()
	latest, err := rpcpolicy.Read(
		ctx,
		rpcpolicy.DefaultReadAttempts,
		rpcpolicy.DefaultInitialDelay,
		func(callCtx context.Context) (*solanarpc.GetLatestBlockhashResult, error) {
			return r.solanaRPC.GetLatestBlockhash(
				callCtx,
				solanarpc.CommitmentConfirmed,
			)
		},
	)
	if err != nil || latest == nil || latest.Value == nil {
		return "", fmt.Errorf(
			"read latest Solana blockhash after %d attempts: %s",
			rpcpolicy.DefaultReadAttempts,
			safeerr.Message(err),
		)
	}
	limit, err := computebudget.NewSetComputeUnitLimitInstruction(
		r.config.Solana.ComputeUnitLimit,
	).ValidateAndBuild()
	if err != nil {
		return "", err
	}
	price, err := computebudget.NewSetComputeUnitPriceInstruction(
		r.config.Solana.ComputeUnitPrice,
	).ValidateAndBuild()
	if err != nil {
		return "", err
	}
	allInstructions, err := withSolanaComputeBudget(limit, price, instructions)
	if err != nil {
		return "", err
	}
	transaction, err := solanago.NewTransaction(
		allInstructions,
		latest.Value.Blockhash,
		solanago.TransactionPayer(r.solanaKey.PublicKey()),
	)
	if err != nil {
		return "", fmt.Errorf("compile Solana %s transaction: %w", phase, err)
	}
	keys := make(map[solanago.PublicKey]solanago.PrivateKey, len(additionalSigners)+1)
	keys[r.solanaKey.PublicKey()] = r.solanaKey
	for _, signer := range additionalSigners {
		keys[signer.PublicKey()] = signer
	}
	if _, err := transaction.Sign(func(key solanago.PublicKey) *solanago.PrivateKey {
		value, ok := keys[key]
		if !ok {
			return nil
		}
		return &value
	}); err != nil {
		return "", fmt.Errorf("sign Solana %s transaction: %w", phase, err)
	}
	if len(transaction.Signatures) == 0 {
		return "", fmt.Errorf("signed Solana %s transaction has no identity", phase)
	}
	signature := transaction.Signatures[0]
	r.ordinal++
	ordinal := r.ordinal
	now := time.Now().UTC()
	if err := r.store.RecordPrepared(ctx, sqlitestore.NTTCanaryTransaction{
		OperationID: r.operationID, Ordinal: ordinal, Phase: phase,
		Chain: "solana", Identity: signature.String(),
		Blockhash:            latest.Value.Blockhash.String(),
		LastValidBlockHeight: latest.Value.LastValidBlockHeight,
		Status:               "prepared", CreatedAt: now,
	}); err != nil {
		return "", fmt.Errorf("persist Solana identity before broadcast: %w", err)
	}
	outerPhase := fmt.Sprintf("%s-%d", phase, ordinal)
	if err := r.liveHooks.prepared(ctx, r.liveChain("solana"), outerPhase, domainexecution.TransactionIdentity{
		Chain: r.liveChain("solana"), Hash: signature.String(),
		Blockhash:            latest.Value.Blockhash.String(),
		LastValidBlockHeight: latest.Value.LastValidBlockHeight,
	}); err != nil {
		return "", fmt.Errorf("persist Live Solana identity before broadcast: %w", err)
	}
	fmt.Fprintf(
		r.output,
		"prepared chain=solana phase=%s tx=%s blockhash=%s "+
			"last_valid_height=%d timer=%s\n",
		phase,
		signature,
		latest.Value.Blockhash,
		latest.Value.LastValidBlockHeight,
		formatMetricDuration(time.Since(phaseStarted)),
	)
	broadcastStarted := time.Now()
	broadcastCtx, cancelBroadcast := context.WithTimeout(ctx, 5*time.Second)
	returned, err := r.solanaRPC.SendTransactionWithOpts(
		broadcastCtx,
		transaction,
		SolanaBridgeTransactionOpts(false),
	)
	cancelBroadcast()
	if err != nil {
		var rpcError *solanajsonrpc.RPCError
		if errors.As(err, &rpcError) && rpcError.Code == -32002 {
			_ = r.store.MarkTransaction(
				context.WithoutCancel(ctx),
				r.operationID,
				ordinal,
				"failed",
			)
			_ = r.liveHooks.mark(context.WithoutCancel(ctx), outerPhase, "rejected")
			return "", fmt.Errorf(
				"solana %s preflight rejected the transaction; it was not broadcast: %w",
				phase,
				err,
			)
		}
		fmt.Fprintf(
			r.output,
			"broadcast_warning chain=solana phase=%s tx=%s "+
				"action=confirm_and_rebroadcast\n",
			phase,
			signature,
		)
	} else if returned != signature {
		return "", fmt.Errorf("solana RPC returned an unexpected transaction identity")
	}
	broadcastDuration := time.Since(broadcastStarted)
	if err := r.store.MarkTransaction(
		ctx,
		r.operationID,
		ordinal,
		"broadcast",
	); err != nil {
		return "", err
	}
	if err := r.liveHooks.mark(ctx, outerPhase, "broadcast"); err != nil {
		return "", err
	}
	fmt.Fprintf(
		r.output,
		"broadcast chain=solana phase=%s tx=%s timer=%s\n",
		phase,
		signature,
		formatMetricDuration(broadcastDuration),
	)
	confirmationStarted := time.Now()
	rebroadcasts, err := r.awaitSolanaConfirmation(
		ctx,
		signature,
		latest.Value.LastValidBlockHeight,
		transaction,
	)
	if err != nil {
		var expired *SolanaBlockhashExpiredError
		if errors.As(err, &expired) {
			expired.Ordinal = ordinal
			expired.OuterPhase = outerPhase
		}
		_ = r.store.MarkTransaction(
			context.WithoutCancel(ctx),
			r.operationID,
			ordinal,
			"outcome_unknown",
		)
		_ = r.liveHooks.mark(context.WithoutCancel(ctx), outerPhase, "outcome_unknown")
		return "", err
	}
	confirmationDuration := time.Since(confirmationStarted)
	if err := r.store.MarkTransaction(
		ctx,
		r.operationID,
		ordinal,
		"confirmed",
	); err != nil {
		return "", err
	}
	if err := r.liveHooks.mark(ctx, outerPhase, "confirmed"); err != nil {
		return "", err
	}
	measurement := phaseMeasurement{
		Chain: "solana", Phase: phase, Identity: signature.String(),
		Metrics: sqlitestore.NTTCanaryTransactionMetrics{
			OperationID: r.operationID, Ordinal: ordinal,
			PrepareDuration:      broadcastStarted.Sub(phaseStarted),
			BroadcastDuration:    broadcastDuration,
			ConfirmationDuration: confirmationDuration,
			TotalDuration:        time.Since(phaseStarted),
			NetworkFeeUnits:      "",
			FeeAsset:             "lamports",
			AdditionalDebitUnits: "",
		},
	}
	r.recordMeasurement(ctx, measurement)
	fmt.Fprintf(
		r.output,
		"confirmed chain=solana phase=%s tx=%s rebroadcasts=%d confirm=%s total=%s\n",
		phase,
		signature,
		rebroadcasts,
		formatMetricDuration(confirmationDuration),
		formatMetricDuration(measurement.Metrics.TotalDuration),
	)
	return signature.String(), nil
}

func (r *armedRuntime) recordMeasurement(
	_ context.Context,
	measurement phaseMeasurement,
) {
	r.measurements = append(r.measurements, measurement)
}

func (r *armedRuntime) finalizeSolanaTelemetry(ctx context.Context) {
	telemetryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	for index := range r.measurements {
		measurement := &r.measurements[index]
		if measurement.Chain != "solana" {
			continue
		}
		signature, err := solanago.SignatureFromBase58(measurement.Identity)
		if err != nil {
			fmt.Fprintf(
				r.output,
				"telemetry_warning stage=solana_transaction phase=%s error=%q\n",
				measurement.Phase,
				err,
			)
			continue
		}
		meta, err := r.solanaTransactionMeta(telemetryCtx, signature)
		if err != nil {
			fmt.Fprintf(
				r.output,
				"telemetry_warning stage=solana_transaction phase=%s error=%q\n",
				measurement.Phase,
				err,
			)
			continue
		}
		payerDebit := uint64(0)
		if len(meta.PreBalances) > 0 && len(meta.PostBalances) > 0 &&
			meta.PreBalances[0] >= meta.PostBalances[0] {
			payerDebit = meta.PreBalances[0] - meta.PostBalances[0]
		}
		additionalDebit := uint64(0)
		if payerDebit > meta.Fee {
			additionalDebit = payerDebit - meta.Fee
		}
		computeUnits := uint64(0)
		if meta.ComputeUnitsConsumed != nil {
			computeUnits = *meta.ComputeUnitsConsumed
		}
		measurement.Metrics.NetworkFeeUnits = strconv.FormatUint(meta.Fee, 10)
		measurement.Metrics.AdditionalDebitUnits = strconv.FormatUint(
			additionalDebit,
			10,
		)
		measurement.Metrics.ComputeUnits = computeUnits
		fmt.Fprintf(
			r.output,
			"telemetry chain=solana phase=%s tx=%s network_fee_lamports=%d "+
				"additional_debit_lamports=%d payer_debit_lamports=%d "+
				"compute_units=%d\n",
			measurement.Phase,
			measurement.Identity,
			meta.Fee,
			additionalDebit,
			payerDebit,
			computeUnits,
		)
	}
}

func (r *armedRuntime) persistMeasurements(ctx context.Context) {
	for _, measurement := range r.measurements {
		if err := r.store.RecordTransactionMetrics(
			context.WithoutCancel(ctx),
			measurement.Metrics,
		); err != nil {
			fmt.Fprintf(
				r.output,
				"telemetry_warning stage=persist_transaction phase=%s error=%q\n",
				measurement.Phase,
				err,
			)
		}
	}
}

func (r *armedRuntime) solanaTransactionMeta(
	ctx context.Context,
	signature solanago.Signature,
) (*solanarpc.TransactionMeta, error) {
	version := uint64(0)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		result, err := r.solanaRPC.GetTransaction(
			ctx,
			signature,
			&solanarpc.GetTransactionOpts{
				Encoding:                       solanago.EncodingBase64,
				Commitment:                     solanarpc.CommitmentConfirmed,
				MaxSupportedTransactionVersion: &version,
			},
		)
		if err == nil && result != nil && result.Meta != nil {
			return result.Meta, nil
		}
		select {
		case <-ctx.Done():
			if err != nil {
				return nil, fmt.Errorf("read Solana transaction metadata: %w", err)
			}
			return nil, fmt.Errorf("solana transaction metadata is unavailable")
		case <-ticker.C:
		}
	}
}

func (r *armedRuntime) operationMetrics(
	mode string,
	readiness, source, attestation, destination, bridge, command time.Duration,
) sqlitestore.NTTCanaryOperationMetrics {
	evmFee := new(big.Int)
	evmValue := new(big.Int)
	solanaFee := new(big.Int)
	solanaDebit := new(big.Int)
	for _, measurement := range r.measurements {
		fee, ok := new(big.Int).SetString(measurement.Metrics.NetworkFeeUnits, 10)
		if !ok {
			fee = new(big.Int)
		}
		additional, ok := new(big.Int).SetString(
			measurement.Metrics.AdditionalDebitUnits,
			10,
		)
		if !ok {
			additional = new(big.Int)
		}
		switch measurement.Chain {
		case "evm":
			evmFee.Add(evmFee, fee)
			evmValue.Add(evmValue, additional)
		case "solana":
			solanaFee.Add(solanaFee, fee)
			solanaDebit.Add(solanaDebit, fee)
			solanaDebit.Add(solanaDebit, additional)
		}
	}
	return sqlitestore.NTTCanaryOperationMetrics{
		OperationID: r.operationID, Mode: mode,
		ReadinessDuration: readiness, SourceDuration: source,
		AttestationDuration: attestation, DestinationDuration: destination,
		BridgeDuration: bridge, CommandDuration: command,
		EVMNetworkFeeWei: evmFee.String(), EVMValueWei: evmValue.String(),
		SolanaFeeLamports:   solanaFee.String(),
		SolanaDebitLamports: solanaDebit.String(),
	}
}

func (r *armedRuntime) printMetricsSummary(
	metrics sqlitestore.NTTCanaryOperationMetrics,
) {
	evmTotal := addDecimalUnits(metrics.EVMNetworkFeeWei, metrics.EVMValueWei)
	fmt.Fprintf(
		r.output,
		"metrics operation=%s mode=%s direction=%s\n",
		r.operationID,
		metrics.Mode,
		r.direction,
	)
	fmt.Fprintf(
		r.output,
		"  timers readiness=%s source=%s attestation=%s destination=%s "+
			"bridge=%s command=%s\n",
		formatMetricDuration(metrics.ReadinessDuration),
		formatMetricDuration(metrics.SourceDuration),
		formatMetricDuration(metrics.AttestationDuration),
		formatMetricDuration(metrics.DestinationDuration),
		formatMetricDuration(metrics.BridgeDuration),
		formatMetricDuration(metrics.CommandDuration),
	)
	fmt.Fprintf(
		r.output,
		"  evm network_fee_wei=%s value_wei=%s payer_debit_wei=%s\n",
		metrics.EVMNetworkFeeWei,
		metrics.EVMValueWei,
		evmTotal,
	)
	fmt.Fprintf(
		r.output,
		"  solana network_fee_lamports=%s payer_debit_lamports=%s\n",
		metrics.SolanaFeeLamports,
		metrics.SolanaDebitLamports,
	)
}

func (r *armedRuntime) liveCostComponents(
	metrics sqlitestore.NTTCanaryOperationMetrics,
) ([]domainexecution.CostComponent, error) {
	if r.liveHooks == nil {
		return nil, nil
	}
	result := make([]domainexecution.CostComponent, 0, 4)
	appendUnits := func(
		kind string,
		chain market.ChainID,
		asset market.AssetID,
		units string,
		decimals int64,
		evidence string,
	) error {
		value, ok := new(big.Int).SetString(units, 10)
		if !ok || value.Sign() < 0 {
			return fmt.Errorf("invalid NTT %s cost units", kind)
		}
		if value.Sign() == 0 {
			return nil
		}
		amount, err := market.NewAssetQuantity(
			asset,
			new(big.Rat).SetFrac(
				value,
				new(big.Int).Exp(big.NewInt(10), big.NewInt(decimals), nil),
			),
		)
		if err != nil {
			return err
		}
		result = append(result, domainexecution.CostComponent{
			Kind: kind, Chain: chain, Amount: amount, Evidence: evidence,
		})
		return nil
	}
	if err := appendUnits(
		"network_fee", r.liveHooks.EVMChain, r.liveHooks.EVMNativeAsset,
		metrics.EVMNetworkFeeWei, 18, "ntt_evm_receipts",
	); err != nil {
		return nil, err
	}
	if err := appendUnits(
		"transaction_value", r.liveHooks.EVMChain, r.liveHooks.EVMNativeAsset,
		metrics.EVMValueWei, 18, "ntt_evm_receipts",
	); err != nil {
		return nil, err
	}
	if err := appendUnits(
		"network_fee", r.liveHooks.SolanaChain, r.liveHooks.SolanaNativeAsset,
		metrics.SolanaFeeLamports, 9, "ntt_solana_transaction_meta",
	); err != nil {
		return nil, err
	}
	solanaDebit, debitOK := new(big.Int).SetString(metrics.SolanaDebitLamports, 10)
	solanaFee, feeOK := new(big.Int).SetString(metrics.SolanaFeeLamports, 10)
	if !debitOK || !feeOK || solanaDebit.Sign() < 0 || solanaFee.Sign() < 0 {
		return nil, fmt.Errorf("invalid NTT Solana debit metrics")
	}
	additional := new(big.Int).Sub(solanaDebit, solanaFee)
	if additional.Sign() < 0 {
		return nil, fmt.Errorf("NTT Solana payer debit is below network fee")
	}
	if err := appendUnits(
		"additional_payer_debit", r.liveHooks.SolanaChain, r.liveHooks.SolanaNativeAsset,
		additional.String(), 9, "ntt_solana_payer_balance_delta",
	); err != nil {
		return nil, err
	}
	return result, nil
}

func addDecimalUnits(left, right string) string {
	leftValue, leftOK := new(big.Int).SetString(left, 10)
	rightValue, rightOK := new(big.Int).SetString(right, 10)
	if !leftOK {
		leftValue = new(big.Int)
	}
	if !rightOK {
		rightValue = new(big.Int)
	}
	return leftValue.Add(leftValue, rightValue).String()
}

func formatMetricDuration(value time.Duration) string {
	return value.Round(10 * time.Microsecond).String()
}

func simulateSolanaBatch(
	ctx context.Context,
	output io.Writer,
	config profile,
	phase string,
	instructions []solanago.Instruction,
	additionalSigners []solanago.PrivateKey,
) error {
	privateText, err := requiredEnv(config.Solana.SignerEnv)
	if err != nil {
		return err
	}
	privateKey, err := parseSolanaPrivateKey(privateText)
	if err != nil {
		return err
	}
	rpcURL, err := requiredEnv(config.Solana.RPCURLEnv)
	if err != nil {
		return err
	}
	client := solanarpc.New(rpcURL)
	latest, err := rpcpolicy.Read(
		ctx,
		rpcpolicy.DefaultReadAttempts,
		rpcpolicy.DefaultInitialDelay,
		func(callCtx context.Context) (*solanarpc.GetLatestBlockhashResult, error) {
			return client.GetLatestBlockhash(
				callCtx,
				solanarpc.CommitmentConfirmed,
			)
		},
	)
	if err != nil || latest == nil || latest.Value == nil {
		return fmt.Errorf(
			"read latest solana blockhash for simulation after %d attempts: %s",
			rpcpolicy.DefaultReadAttempts,
			safeerr.Message(err),
		)
	}
	limit, err := computebudget.NewSetComputeUnitLimitInstruction(
		config.Solana.ComputeUnitLimit,
	).ValidateAndBuild()
	if err != nil {
		return err
	}
	price, err := computebudget.NewSetComputeUnitPriceInstruction(
		config.Solana.ComputeUnitPrice,
	).ValidateAndBuild()
	if err != nil {
		return err
	}
	allInstructions, err := withSolanaComputeBudget(limit, price, instructions)
	if err != nil {
		return err
	}
	transaction, err := solanago.NewTransaction(
		allInstructions,
		latest.Value.Blockhash,
		solanago.TransactionPayer(privateKey.PublicKey()),
	)
	if err != nil {
		return fmt.Errorf("compile solana %s simulation: %w", phase, err)
	}
	keys := make(map[solanago.PublicKey]solanago.PrivateKey, len(additionalSigners)+1)
	keys[privateKey.PublicKey()] = privateKey
	for _, signer := range additionalSigners {
		keys[signer.PublicKey()] = signer
	}
	if _, err := transaction.Sign(func(key solanago.PublicKey) *solanago.PrivateKey {
		value, ok := keys[key]
		if !ok {
			return nil
		}
		return &value
	}); err != nil {
		return fmt.Errorf("sign solana %s simulation: %w", phase, err)
	}
	result, err := client.SimulateTransactionWithOpts(
		ctx,
		transaction,
		&solanarpc.SimulateTransactionOpts{
			SigVerify:  true,
			Commitment: solanarpc.CommitmentConfirmed,
		},
	)
	if err != nil {
		return fmt.Errorf("simulate solana %s: %w", phase, err)
	}
	if result == nil || result.Value == nil {
		return fmt.Errorf("simulate solana %s returned no result", phase)
	}
	if result.Value.Err != nil {
		return fmt.Errorf(
			"simulate solana %s failed: %v logs=%v",
			phase,
			result.Value.Err,
			result.Value.Logs,
		)
	}
	units := uint64(0)
	if result.Value.UnitsConsumed != nil {
		units = *result.Value.UnitsConsumed
	}
	fmt.Fprintf(
		output,
		"simulation=ok chain=solana phase=%s units_consumed=%d broadcast=disabled\n",
		phase,
		units,
	)
	return nil
}

func withSolanaComputeBudget(
	limit, price solanago.Instruction,
	instructions []solanago.Instruction,
) ([]solanago.Instruction, error) {
	result := make([]solanago.Instruction, 0, len(instructions)+2)
	result = append(result, limit, price)
	for _, instruction := range instructions {
		if wormholentt.IsSecp256k1Instruction(instruction) {
			if len(result) > 255 {
				return nil, fmt.Errorf("Secp256k1 instruction index exceeds uint8")
			}
			rebased, err := wormholentt.RebaseSecp256k1Instruction(
				instruction,
				uint8(len(result)),
			)
			if err != nil {
				return nil, err
			}
			instruction = rebased
		}
		result = append(result, instruction)
	}
	return result, nil
}

func (r *armedRuntime) awaitSolanaConfirmation(
	ctx context.Context,
	signature solanago.Signature,
	lastValidBlockHeight uint64,
	transaction *solanago.Transaction,
) (int, error) {
	return AwaitSolanaConfirmationWithReader(
		ctx,
		signature.String(),
		lastValidBlockHeight,
		time.Duration(r.config.ConfirmationTimeoutS)*time.Second,
		5*time.Second,
		250*time.Millisecond,
		time.Second,
		func(callCtx context.Context) (bool, error, error) {
			statuses, err := r.solanaRPC.GetSignatureStatuses(
				callCtx,
				true,
				signature,
			)
			if err != nil {
				return false, nil, err
			}
			if len(statuses.Value) != 1 || statuses.Value[0] == nil {
				return false, nil, nil
			}
			status := statuses.Value[0]
			if status.Err != nil {
				return false, fmt.Errorf("%v", status.Err), nil
			}
			confirmed := status.ConfirmationStatus ==
				solanarpc.ConfirmationStatusConfirmed ||
				status.ConfirmationStatus ==
					solanarpc.ConfirmationStatusFinalized
			return confirmed, nil, nil
		},
		func(callCtx context.Context) (uint64, error) {
			return r.solanaRPC.GetBlockHeight(
				callCtx,
				solanarpc.CommitmentConfirmed,
			)
		},
		func(callCtx context.Context) error {
			returned, err := r.solanaRPC.SendTransactionWithOpts(
				callCtx,
				transaction,
				SolanaBridgeTransactionOpts(true),
			)
			if err != nil {
				return err
			}
			if returned != signature {
				return fmt.Errorf("solana RPC returned an unexpected transaction identity")
			}
			return nil
		},
	)
}

const SolanaBridgeMaxRetries = uint(20)

func SolanaBridgeTransactionOpts(skipPreflight bool) solanarpc.TransactionOpts {
	maxRetries := SolanaBridgeMaxRetries
	return solanarpc.TransactionOpts{
		SkipPreflight:       skipPreflight,
		PreflightCommitment: solanarpc.CommitmentConfirmed,
		MaxRetries:          &maxRetries,
	}
}

type SolanaBlockhashExpiredError struct {
	Signature            string
	LastValidBlockHeight uint64
	ObservedBlockHeight  uint64
	Ordinal              int
	OuterPhase           string
}

func (e *SolanaBlockhashExpiredError) Error() string {
	return fmt.Sprintf(
		"solana transaction %s was not found before blockhash expiry "+
			"(last_valid_height=%d observed_height=%d)",
		e.Signature,
		e.LastValidBlockHeight,
		e.ObservedBlockHeight,
	)
}

// AwaitSolanaConfirmationWithReader gives each read a short deadline and keeps
// polling after transport failures until the overall confirmation deadline.
// A read error is not evidence that a broadcast failed, and its raw text is
// deliberately omitted from the terminal error because RPC URLs may contain
// credentials.
func AwaitSolanaConfirmationWithReader(
	ctx context.Context,
	signature string,
	lastValidBlockHeight uint64,
	timeout time.Duration,
	requestTimeout time.Duration,
	pollInterval time.Duration,
	rebroadcastInterval time.Duration,
	read func(context.Context) (confirmed bool, transactionErr error, rpcErr error),
	readBlockHeight func(context.Context) (uint64, error),
	rebroadcast func(context.Context) error,
) (int, error) {
	if lastValidBlockHeight == 0 || timeout <= 0 || requestTimeout <= 0 ||
		pollInterval <= 0 || rebroadcastInterval <= 0 || read == nil ||
		readBlockHeight == nil || rebroadcast == nil {
		return 0, fmt.Errorf("invalid Solana confirmation policy")
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	lastBroadcast := time.Now()
	rebroadcasts := 0
	for {
		callCtx, cancelCall := context.WithTimeout(waitCtx, requestTimeout)
		confirmed, transactionErr, _ := read(callCtx)
		cancelCall()
		if transactionErr != nil {
			return rebroadcasts, fmt.Errorf(
				"solana transaction %s failed: %v",
				signature,
				transactionErr,
			)
		}
		if confirmed {
			return rebroadcasts, nil
		}
		heightCtx, cancelHeight := context.WithTimeout(waitCtx, requestTimeout)
		height, heightErr := readBlockHeight(heightCtx)
		cancelHeight()
		if heightErr == nil && height > lastValidBlockHeight {
			return rebroadcasts, &SolanaBlockhashExpiredError{
				Signature:            signature,
				LastValidBlockHeight: lastValidBlockHeight,
				ObservedBlockHeight:  height,
			}
		}
		if time.Since(lastBroadcast) >= rebroadcastInterval {
			broadcastCtx, cancelBroadcast := context.WithTimeout(
				waitCtx,
				requestTimeout,
			)
			_ = rebroadcast(broadcastCtx)
			cancelBroadcast()
			lastBroadcast = time.Now()
			rebroadcasts++
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
			return rebroadcasts, fmt.Errorf(
				"solana transaction %s outcome is unknown after confirmation timeout",
				signature,
			)
		case <-timer.C:
		}
	}
}

func newCanaryOperationID() (string, error) {
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", fmt.Errorf("generate NTT canary operation ID: %w", err)
	}
	return "ntt-" + hex.EncodeToString(entropy[:]), nil
}
