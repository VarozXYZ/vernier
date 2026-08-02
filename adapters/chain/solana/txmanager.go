package solana

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	solanago "github.com/gagliardetto/solana-go"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	chainport "github.com/VarozXYZ/vernier/ports/chain"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

const maxSerializedTransactionBytes = 1232

type ReconciliationRPC interface {
	ReadSignatureStatus(context.Context, string) (SignatureStatus, error)
	ReadTransaction(context.Context, string) (Transaction, error)
	CurrentBlockHeight(context.Context) (uint64, error)
	IsBlockhashValid(context.Context, string) (bool, error)
}

type SignedTransactionSimulator interface {
	SimulateSignedTransaction(context.Context, []byte) error
}

type SignedTransactionEconomicSimulator interface {
	SimulateSignedTransactionEconomic(
		context.Context,
		[]byte,
		string,
	) (*big.Int, uint64, uint64, error)
}

type MessageFeeEstimator interface {
	FeeForMessage(context.Context, string) (uint64, error)
}

type ArtifactNetworkCostEstimate struct {
	NetworkFeeLamports          uint64
	BaseFeeLamports             uint64
	PriorityFeeLamports         uint64
	SenderTipLamports           uint64
	ComputeUnitLimit            uint32
	ProviderPriceMicroLamports  uint64
	EffectivePriceMicroLamports uint64
	PriorityFeeCapped           bool
	TotalLamports               *big.Int
	CapturedAt                  time.Time
}

type TxManagerConfig struct {
	Chain                  market.ChainID
	Account                execution.AccountID
	PrivateKey             solanago.PrivateKey
	SenderEndpoint         string
	PingEndpoint           string
	SenderTipAccount       solanago.PublicKey
	SenderTipLamports      uint64
	ComputeUnitLimit       uint32
	MaxPriorityFeeLamports uint64
	Client                 *http.Client
	Reconciliation         ReconciliationRPC
	Simulator              SignedTransactionSimulator
	FeeEstimator           MessageFeeEstimator
	Clock                  func() time.Time
	WarmInterval           time.Duration
	SettlementDecoder      TransactionSettlementDecoder
	TokenAccounts          map[market.TokenID]string
}

type TxManager struct {
	chain                  market.ChainID
	account                execution.AccountID
	privateKey             solanago.PrivateKey
	senderEndpoint         string
	pingEndpoint           string
	senderTipAccount       solanago.PublicKey
	senderTipLamports      uint64
	computeLimit           uint32
	maxPriorityFeeLamports uint64
	client                 *http.Client
	reconciliation         ReconciliationRPC
	simulator              SignedTransactionSimulator
	feeEstimator           MessageFeeEstimator
	clock                  func() time.Time
	warmInterval           time.Duration
	settlementDecoder      TransactionSettlementDecoder
	tokenAccounts          map[market.TokenID]string
	requestID              atomic.Uint64
	warmOnce               sync.Once
}

func NewTxManager(config TxManagerConfig) (*TxManager, error) {
	if config.Chain == "" || config.Account == "" || len(config.PrivateKey) == 0 ||
		strings.TrimSpace(config.SenderEndpoint) == "" || config.Reconciliation == nil || config.Clock == nil {
		return nil, fmt.Errorf("solana TxManager requires chain, account, signer, Sender, reconciliation, and clock")
	}
	endpoint, err := url.Parse(config.SenderEndpoint)
	if err != nil || endpoint.Scheme == "" || endpoint.Host == "" {
		return nil, fmt.Errorf("invalid Helius Sender endpoint")
	}
	if config.PingEndpoint == "" {
		ping := *endpoint
		ping.Path = "/ping"
		ping.RawQuery = endpoint.RawQuery
		config.PingEndpoint = ping.String()
	}
	ping, err := url.Parse(config.PingEndpoint)
	if err != nil || ping.Scheme == "" || ping.Host == "" {
		return nil, fmt.Errorf("invalid Helius Sender ping endpoint")
	}
	if config.ComputeUnitLimit == 0 {
		config.ComputeUnitLimit = 1_400_000
	}
	if config.SenderTipAccount.IsZero() {
		config.SenderTipAccount = NextHeliusSenderTipAccount()
	}
	if !IsHeliusSenderTipAccount(config.SenderTipAccount) {
		return nil, fmt.Errorf("invalid Helius Sender tip account")
	}
	if config.SenderTipLamports == 0 {
		config.SenderTipLamports = 1_000_000
	}
	if config.Client == nil {
		config.Client = &http.Client{Timeout: 5 * time.Second}
	}
	if config.WarmInterval == 0 {
		config.WarmInterval = 5 * time.Second
	}
	if config.WarmInterval < time.Second {
		return nil, fmt.Errorf("helius Sender warm interval must be at least one second")
	}
	tokenAccounts := make(map[market.TokenID]string, len(config.TokenAccounts))
	for token, account := range config.TokenAccounts {
		if token != "" && strings.TrimSpace(account) != "" {
			tokenAccounts[token] = strings.TrimSpace(account)
		}
	}
	return &TxManager{
		chain: config.Chain, account: config.Account, privateKey: append(solanago.PrivateKey(nil), config.PrivateKey...),
		senderEndpoint: endpoint.String(), pingEndpoint: ping.String(), computeLimit: config.ComputeUnitLimit,
		senderTipAccount: config.SenderTipAccount, senderTipLamports: config.SenderTipLamports,
		maxPriorityFeeLamports: config.MaxPriorityFeeLamports,
		client:                 config.Client, reconciliation: config.Reconciliation, clock: config.Clock,
		simulator: config.Simulator, feeEstimator: config.FeeEstimator, warmInterval: config.WarmInterval,
		settlementDecoder: config.SettlementDecoder,
		tokenAccounts:     tokenAccounts,
	}, nil
}

func (m *TxManager) Account() execution.AccountID { return m.account }

func (m *TxManager) Warm(ctx context.Context) error {
	if err := m.ping(ctx); err != nil {
		return err
	}
	m.warmOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(m.warmInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					pingCtx, cancel := context.WithTimeout(ctx, time.Second)
					_ = m.ping(pingCtx)
					cancel()
				}
			}
		}()
	})
	return nil
}

func (m *TxManager) ping(ctx context.Context) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, m.pingEndpoint, nil)
	if err != nil {
		return err
	}
	response, err := m.client.Do(request)
	if err != nil {
		return fmt.Errorf("warm Helius Sender connection: %w", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4<<10))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("warm Helius Sender connection: HTTP %s", response.Status)
	}
	return nil
}

func (m *TxManager) Prepare(_ context.Context, artifact executionport.Artifact) (chainport.PreparedTransaction, error) {
	if artifact.Leg.Account != m.account || artifact.Leg.Chain != m.chain ||
		artifact.Metadata["kind"] != "jupiter_build_v2" {
		return chainport.PreparedTransaction{}, fmt.Errorf("solana artifact targets another TxManager or is not Jupiter build v2")
	}
	raw, signature, blockhash, err := AssembleJupiterBuildForSender(
		artifact.Payload,
		m.privateKey,
		m.computeLimit,
		m.senderTipAccount,
		m.senderTipLamports,
		m.maxPriorityFeeLamports,
	)
	if err != nil {
		return chainport.PreparedTransaction{}, err
	}
	if len(raw) > maxSerializedTransactionBytes {
		return chainport.PreparedTransaction{}, &executionport.ArtifactTooLargeError{
			ActualBytes: len(raw), MaximumBytes: maxSerializedTransactionBytes,
		}
	}
	if artifact.Blockhash != "" && artifact.Blockhash != blockhash {
		// BuildSource may retain a hexadecimal representation for byte-array
		// responses. The assembler's base58 value is canonical for RPC.
		artifact.Blockhash = blockhash
	}
	return chainport.PreparedTransaction{
		Leg: artifact.Leg,
		Identity: execution.TransactionIdentity{
			Chain: m.chain, Account: m.account, Hash: signature, Blockhash: blockhash,
			LastValidBlockHeight: artifact.LastValidBlockHeight,
		},
		SignedPayload: raw, PreparedAt: m.clock().UTC(),
	}, nil
}

func (m *TxManager) SimulatePrepared(
	ctx context.Context,
	prepared chainport.PreparedTransaction,
) error {
	if m.simulator == nil {
		return fmt.Errorf("solana transaction simulator is unavailable")
	}
	if prepared.Leg.Account != m.account ||
		prepared.Leg.Chain != m.chain ||
		len(prepared.SignedPayload) == 0 {
		return fmt.Errorf("solana prepared transaction is invalid")
	}
	return m.simulator.SimulateSignedTransaction(ctx, prepared.SignedPayload)
}

func (m *TxManager) SimulatePreparedEconomic(
	ctx context.Context,
	request chainport.EconomicSimulationRequest,
) (chainport.EconomicSimulationResult, error) {
	prepared := request.Prepared
	if prepared.Leg.Account != m.account || prepared.Leg.Chain != m.chain ||
		len(prepared.SignedPayload) == 0 || request.OutputBalanceBefore == nil {
		return chainport.EconomicSimulationResult{},
			fmt.Errorf("solana economic simulation input is invalid")
	}
	simulator, ok := m.simulator.(SignedTransactionEconomicSimulator)
	if !ok {
		return chainport.EconomicSimulationResult{},
			fmt.Errorf("solana economic transaction simulator is unavailable")
	}
	outputToken := prepared.Leg.ExpectedOutput.Token()
	account := m.tokenAccounts[outputToken]
	if account == "" {
		return chainport.EconomicSimulationResult{},
			fmt.Errorf("solana simulation output token account is unavailable")
	}
	post, units, slot, err := simulator.SimulateSignedTransactionEconomic(
		ctx, prepared.SignedPayload, account,
	)
	if err != nil {
		return chainport.EconomicSimulationResult{}, err
	}
	delta := new(big.Int).Sub(post, request.OutputBalanceBefore)
	if delta.Sign() <= 0 {
		return chainport.EconomicSimulationResult{},
			fmt.Errorf("solana simulation returned no positive output delta")
	}
	output, err := market.NewTokenAmount(outputToken, delta)
	if err != nil {
		return chainport.EconomicSimulationResult{}, err
	}
	return chainport.EconomicSimulationResult{
		Input: prepared.Leg.Input, Output: output, UnitsConsumed: units,
		ContextVersion: slot, Evidence: "solana_simulate_post_spl_balance",
	}, nil
}

// EstimateArtifactNetworkCost builds the exact Helius Sender transaction and
// asks the RPC for its current signature/priority fee. The call is intended
// exclusively for the background cost oracle. The Sender tip is an ordinary
// transfer and is added separately.
func (m *TxManager) EstimateArtifactNetworkCost(
	ctx context.Context,
	artifact executionport.Artifact,
) (*big.Int, time.Time, error) {
	estimate, err := m.EstimateArtifactNetworkCostDetails(ctx, artifact)
	if err != nil {
		return nil, time.Time{}, err
	}
	return new(big.Int).Set(estimate.TotalLamports), estimate.CapturedAt, nil
}

func (m *TxManager) EstimateArtifactNetworkCostDetails(
	ctx context.Context,
	artifact executionport.Artifact,
) (ArtifactNetworkCostEstimate, error) {
	if m.feeEstimator == nil {
		return ArtifactNetworkCostEstimate{}, fmt.Errorf("solana message fee estimator is unavailable")
	}
	prepared, err := m.Prepare(ctx, artifact)
	if err != nil {
		return ArtifactNetworkCostEstimate{}, err
	}
	transaction, err := solanago.TransactionFromBytes(prepared.SignedPayload)
	if err != nil {
		return ArtifactNetworkCostEstimate{}, fmt.Errorf("decode Solana transaction for fee estimate: %w", err)
	}
	fee, err := m.feeEstimator.FeeForMessage(ctx, transaction.Message.ToBase64())
	if err != nil {
		return ArtifactNetworkCostEstimate{}, err
	}
	computeLimit, effectivePrice := compiledComputeBudget(transaction)
	priorityFee := priorityFeeLamports(computeLimit, effectivePrice)
	baseFee := uint64(0)
	if fee >= priorityFee {
		baseFee = fee - priorityFee
	}
	providerPrice := providerComputeUnitPrice(artifact.Payload)
	total := new(big.Int).SetUint64(fee)
	total.Add(total, new(big.Int).SetUint64(m.senderTipLamports))
	return ArtifactNetworkCostEstimate{
		NetworkFeeLamports:          fee,
		BaseFeeLamports:             baseFee,
		PriorityFeeLamports:         priorityFee,
		SenderTipLamports:           m.senderTipLamports,
		ComputeUnitLimit:            computeLimit,
		ProviderPriceMicroLamports:  providerPrice,
		EffectivePriceMicroLamports: effectivePrice,
		PriorityFeeCapped: providerPrice > 0 &&
			effectivePrice > 0 &&
			effectivePrice < providerPrice,
		TotalLamports: total,
		CapturedAt:    m.clock().UTC(),
	}, nil
}

func (m *TxManager) Broadcast(ctx context.Context, prepared chainport.PreparedTransaction) (chainport.BroadcastResult, error) {
	if prepared.Leg.Account != m.account || len(prepared.SignedPayload) == 0 {
		return chainport.BroadcastResult{}, fmt.Errorf("solana prepared transaction is invalid")
	}
	payload := struct {
		JSONRPC string `json:"jsonrpc"`
		ID      uint64 `json:"id"`
		Method  string `json:"method"`
		Params  []any  `json:"params"`
	}{
		JSONRPC: "2.0", ID: m.requestID.Add(1), Method: "sendTransaction",
		Params: []any{
			base64.StdEncoding.EncodeToString(prepared.SignedPayload),
			map[string]any{"encoding": "base64", "skipPreflight": true, "maxRetries": 0},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return chainport.BroadcastResult{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, m.senderEndpoint, strings.NewReader(string(body)))
	if err != nil {
		return chainport.BroadcastResult{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := m.client.Do(request)
	if err != nil {
		return chainport.BroadcastResult{
			Identity: prepared.Identity, Disposition: chainport.BroadcastPossible, Attempts: 1,
		}, err
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return chainport.BroadcastResult{
			Identity: prepared.Identity, Disposition: chainport.BroadcastPossible, Attempts: 1,
		}, err
	}
	var envelope senderRPCEnvelope
	envelopeErr := json.Unmarshal(responseBody, &envelope)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		disposition := chainport.BroadcastRejected
		if response.StatusCode >= 500 &&
			!senderInvalidRequest(envelope, envelopeErr) {
			disposition = chainport.BroadcastPossible
		}
		return chainport.BroadcastResult{
			Identity: prepared.Identity, Disposition: disposition, Attempts: 1,
		}, senderHTTPError(response.Status, responseBody, envelope, envelopeErr)
	}
	if envelopeErr != nil {
		return chainport.BroadcastResult{
			Identity: prepared.Identity, Disposition: chainport.BroadcastPossible, Attempts: 1,
		}, envelopeErr
	}
	if envelope.Error != nil {
		return chainport.BroadcastResult{
				Identity: prepared.Identity, Disposition: chainport.BroadcastRejected, Attempts: 1,
			},
			fmt.Errorf("helius Sender RPC %d: %s", envelope.Error.Code, envelope.Error.Message)
	}
	if envelope.Result == "" || envelope.Result != prepared.Identity.Hash {
		return chainport.BroadcastResult{
			Identity: prepared.Identity, Disposition: chainport.BroadcastPossible, Attempts: 1,
		}, fmt.Errorf("helius Sender returned an unexpected signature")
	}
	endpoint, _ := url.Parse(m.senderEndpoint)
	return chainport.BroadcastResult{
		Identity: prepared.Identity, Disposition: chainport.BroadcastAccepted,
		Accepted: true, Endpoint: endpoint.Host,
		Attempts: 1, AcceptedAt: m.clock().UTC(),
	}, nil
}

type senderRPCEnvelope struct {
	Result string `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func senderInvalidRequest(
	envelope senderRPCEnvelope,
	decodeErr error,
) bool {
	if decodeErr != nil || envelope.Error == nil {
		return false
	}
	switch envelope.Error.Code {
	case -32600, -32601, -32602:
		return true
	default:
		return false
	}
}

func senderHTTPError(
	status string,
	body []byte,
	envelope senderRPCEnvelope,
	decodeErr error,
) error {
	if decodeErr == nil && envelope.Error != nil {
		return fmt.Errorf(
			"helius Sender HTTP %s RPC %d: %s",
			status,
			envelope.Error.Code,
			sanitizeSenderMessage(envelope.Error.Message),
		)
	}
	detail := sanitizeSenderMessage(string(body))
	if detail == "" {
		return fmt.Errorf("helius Sender HTTP %s", status)
	}
	return fmt.Errorf("helius Sender HTTP %s: %s", status, detail)
}

func sanitizeSenderMessage(message string) string {
	const maximum = 512
	message = strings.Join(strings.Fields(message), " ")
	if len(message) > maximum {
		message = message[:maximum] + "..."
	}
	return message
}

func (m *TxManager) Reconcile(ctx context.Context, step execution.OperationStep) (execution.Settlement, error) {
	identity := step.Identity
	if identity.Chain != m.chain || identity.Account != m.account || identity.Hash == "" {
		return execution.Settlement{}, fmt.Errorf("solana reconciliation identity is invalid")
	}
	status, err := m.reconciliation.ReadSignatureStatus(ctx, identity.Hash)
	if err != nil {
		return execution.Settlement{}, err
	}
	if status.Found {
		state := execution.StateConfirmedSuccess
		if hasSolanaError(status.Err) {
			state = execution.StateConfirmedRevert
		} else if status.ConfirmationStatus != "confirmed" && status.ConfirmationStatus != "finalized" {
			state = execution.StateOutcomeUnknown
		}
		if state == execution.StateConfirmedSuccess && m.settlementDecoder != nil {
			transaction, transactionErr := m.reconciliation.ReadTransaction(ctx, identity.Hash)
			if transactionErr != nil {
				return execution.Settlement{}, transactionErr
			}
			settlement, decodeErr := m.settlementDecoder.DecodeTransaction(step, transaction)
			if decodeErr != nil {
				return execution.Settlement{}, decodeErr
			}
			settlement.Identity = identity
			if settlement.ObservedAt.IsZero() {
				settlement.ObservedAt = m.clock().UTC()
			}
			return settlement, nil
		}
		return execution.Settlement{
			Identity: identity, Technical: state, Economic: execution.EconomicReserved,
			ObservedAt: m.clock().UTC(), Evidence: "solana_signature_status",
		}, nil
	}
	if identity.Blockhash != "" {
		valid, validErr := m.reconciliation.IsBlockhashValid(ctx, identity.Blockhash)
		if validErr != nil {
			return execution.Settlement{}, validErr
		}
		height, heightErr := m.reconciliation.CurrentBlockHeight(ctx)
		if heightErr != nil {
			return execution.Settlement{}, heightErr
		}
		if !valid && identity.LastValidBlockHeight > 0 && height > identity.LastValidBlockHeight {
			return execution.Settlement{
				Identity: identity, Technical: execution.StateBroadcastRejected,
				Economic: execution.EconomicReserved, ObservedAt: m.clock().UTC(),
				Evidence: "blockhash_expired_without_signature",
			}, nil
		}
	}
	return execution.Settlement{
		Identity: identity, Technical: execution.StateOutcomeUnknown,
		Economic: execution.EconomicReserved, ObservedAt: m.clock().UTC(), Evidence: "signature_not_found",
	}, nil
}

func hasSolanaError(raw json.RawMessage) bool {
	trimmed := strings.TrimSpace(string(raw))
	return trimmed != "" && trimmed != "null"
}

type buildTransactionResponse struct {
	ComputeBudgetInstructions []wireInstruction   `json:"computeBudgetInstructions"`
	SetupInstructions         []wireInstruction   `json:"setupInstructions"`
	SwapInstruction           wireInstruction     `json:"swapInstruction"`
	CleanupInstruction        *wireInstruction    `json:"cleanupInstruction"`
	OtherInstructions         []wireInstruction   `json:"otherInstructions"`
	TipInstruction            *wireInstruction    `json:"tipInstruction"`
	Addresses                 map[string][]string `json:"addressesByLookupTableAddress"`
	Blockhash                 struct {
		Value                []byte `json:"blockhash"`
		LastValidBlockHeight uint64 `json:"lastValidBlockHeight"`
	} `json:"blockhashWithMetadata"`
}

type wireInstruction struct {
	ProgramID string `json:"programId"`
	Accounts  []struct {
		Pubkey     string `json:"pubkey"`
		IsWritable bool   `json:"isWritable"`
		IsSigner   bool   `json:"isSigner"`
	} `json:"accounts"`
	Data string `json:"data"`
}

// AssembleJupiterBuild compiles and signs one Swap V2 /build response without
// broadcasting it. Manual canaries use this to run RPC simulation before any
// armed execution; TxManager uses the same path in the Live hot path.
func AssembleJupiterBuild(payload []byte, privateKey solanago.PrivateKey, computeLimit uint32) ([]byte, string, string, error) {
	return assembleJupiterBuildWithTip(
		payload,
		privateKey,
		computeLimit,
		solanago.PublicKey{},
		0,
		0,
		false,
	)
}

// AssembleJupiterBuildForSender replaces any provider-specific tip with one
// transfer to a Helius-designated tip account. This avoids paying two tips and
// ensures the signed transaction is eligible for Helius Sender routing.
func AssembleJupiterBuildForSender(
	payload []byte,
	privateKey solanago.PrivateKey,
	computeLimit uint32,
	tipAccount solanago.PublicKey,
	tipLamports uint64,
	maxPriorityFeeLamports ...uint64,
) ([]byte, string, string, error) {
	if !IsHeliusSenderTipAccount(tipAccount) {
		return nil, "", "", fmt.Errorf("invalid Helius Sender tip account")
	}
	if tipLamports == 0 {
		return nil, "", "", fmt.Errorf("helius Sender tip must be positive")
	}
	priorityCap := uint64(0)
	if len(maxPriorityFeeLamports) > 0 {
		priorityCap = maxPriorityFeeLamports[0]
	}
	return assembleJupiterBuildWithTip(
		payload,
		privateKey,
		computeLimit,
		tipAccount,
		tipLamports,
		priorityCap,
		false,
	)
}

// AssembleJupiterBuildForSimulation creates a transaction that is signed by
// the configured payer while deliberately leaving any other required signer
// slots empty. It is only valid with RPC simulation using sigVerify=false and
// must never be broadcast.
func AssembleJupiterBuildForSimulation(
	payload []byte,
	payer solanago.PrivateKey,
	computeLimit uint32,
	tipAccount solanago.PublicKey,
	tipLamports uint64,
	maxPriorityFeeLamports ...uint64,
) ([]byte, string, error) {
	if !IsHeliusSenderTipAccount(tipAccount) {
		return nil, "", fmt.Errorf("invalid Helius Sender tip account")
	}
	if tipLamports == 0 {
		return nil, "", fmt.Errorf("helius Sender tip must be positive")
	}
	priorityCap := uint64(0)
	if len(maxPriorityFeeLamports) > 0 {
		priorityCap = maxPriorityFeeLamports[0]
	}
	raw, _, blockhash, err := assembleJupiterBuildWithTip(
		payload,
		payer,
		computeLimit,
		tipAccount,
		tipLamports,
		priorityCap,
		true,
	)
	return raw, blockhash, err
}

func assembleJupiterBuildWithTip(
	payload []byte,
	privateKey solanago.PrivateKey,
	computeLimit uint32,
	senderTipAccount solanago.PublicKey,
	senderTipLamports uint64,
	maxPriorityFeeLamports uint64,
	partial bool,
) ([]byte, string, string, error) {
	var build buildTransactionResponse
	if err := json.Unmarshal(payload, &build); err != nil {
		return nil, "", "", fmt.Errorf("decode Jupiter build artifact: %w", err)
	}
	if len(build.Blockhash.Value) != 32 || build.SwapInstruction.ProgramID == "" {
		return nil, "", "", fmt.Errorf("jupiter build artifact has invalid blockhash or swap instruction")
	}
	instructions := make([]solanago.Instruction, 0,
		2+len(build.ComputeBudgetInstructions)+len(build.SetupInstructions)+len(build.OtherInstructions))
	appendWire := func(candidate wireInstruction) error {
		instruction, err := decodeWireInstruction(candidate)
		if err != nil {
			return err
		}
		instructions = append(instructions, instruction)
		return nil
	}
	selectedComputeLimit := computeLimit
	for _, instruction := range build.ComputeBudgetInstructions {
		if providerLimit, ok := computeUnitLimit(instruction); ok {
			selectedComputeLimit = providerLimit
			if computeLimit > 0 && selectedComputeLimit > computeLimit {
				selectedComputeLimit = computeLimit
			}
			break
		}
	}
	if selectedComputeLimit == 0 {
		return nil, "", "", fmt.Errorf("jupiter build has no usable compute-unit limit")
	}
	hasComputeLimit := false
	hasComputePrice := false
	for _, instruction := range build.ComputeBudgetInstructions {
		if _, ok := computeUnitLimit(instruction); ok {
			if hasComputeLimit {
				continue
			}
			instructions = append(instructions, setComputeUnitLimit(selectedComputeLimit))
			hasComputeLimit = true
			continue
		}
		if providerPrice, ok := computeUnitPrice(instruction); ok {
			if hasComputePrice {
				continue
			}
			selectedPrice, err := capComputeUnitPrice(
				providerPrice,
				selectedComputeLimit,
				maxPriorityFeeLamports,
			)
			if err != nil {
				return nil, "", "", err
			}
			instructions = append(instructions, setComputeUnitPrice(selectedPrice))
			hasComputePrice = true
			continue
		}
		if err := appendWire(instruction); err != nil {
			return nil, "", "", err
		}
	}
	if !hasComputeLimit {
		instructions = append(
			[]solanago.Instruction{setComputeUnitLimit(selectedComputeLimit)},
			instructions...,
		)
	}
	for _, instruction := range build.SetupInstructions {
		if err := appendWire(instruction); err != nil {
			return nil, "", "", err
		}
	}
	if err := appendWire(build.SwapInstruction); err != nil {
		return nil, "", "", err
	}
	if build.CleanupInstruction != nil {
		if err := appendWire(*build.CleanupInstruction); err != nil {
			return nil, "", "", err
		}
	}
	for _, instruction := range build.OtherInstructions {
		if err := appendWire(instruction); err != nil {
			return nil, "", "", err
		}
	}
	if !senderTipAccount.IsZero() {
		tipData := make([]byte, 12)
		binary.LittleEndian.PutUint32(tipData[:4], 2)
		binary.LittleEndian.PutUint64(tipData[4:], senderTipLamports)
		instructions = append(instructions, solanago.NewInstruction(
			solanago.SystemProgramID,
			solanago.AccountMetaSlice{
				solanago.NewAccountMeta(privateKey.PublicKey(), true, true),
				solanago.NewAccountMeta(senderTipAccount, true, false),
			},
			tipData,
		))
	} else {
		if build.TipInstruction == nil {
			return nil, "", "", errors.New("jupiter build artifact has no tip instruction")
		}
		if err := appendWire(*build.TipInstruction); err != nil {
			return nil, "", "", err
		}
	}
	tables := make(map[solanago.PublicKey]solanago.PublicKeySlice, len(build.Addresses))
	for tableText, addressTexts := range build.Addresses {
		table, err := solanago.PublicKeyFromBase58(tableText)
		if err != nil {
			return nil, "", "", err
		}
		addresses := make(solanago.PublicKeySlice, len(addressTexts))
		for index, addressText := range addressTexts {
			addresses[index], err = solanago.PublicKeyFromBase58(addressText)
			if err != nil {
				return nil, "", "", err
			}
		}
		tables[table] = addresses
	}
	hash := solanago.HashFromBytes(build.Blockhash.Value)
	transaction, err := solanago.NewTransaction(
		instructions, hash, solanago.TransactionPayer(privateKey.PublicKey()),
		solanago.TransactionAddressTables(tables),
	)
	if err != nil {
		return nil, "", "", fmt.Errorf("compile Jupiter v0 transaction: %w", err)
	}
	sign := transaction.Sign
	if partial {
		sign = transaction.PartialSign
	}
	if _, err := sign(func(key solanago.PublicKey) *solanago.PrivateKey {
		if key.Equals(privateKey.PublicKey()) {
			return &privateKey
		}
		return nil
	}); err != nil {
		return nil, "", "", err
	}
	if len(transaction.Signatures) == 0 {
		return nil, "", "", fmt.Errorf("signed Solana transaction has no signature")
	}
	raw, err := transaction.MarshalBinary()
	if err != nil {
		return nil, "", "", err
	}
	return raw, transaction.Signatures[0].String(), hash.String(), nil
}

var heliusSenderTipAccounts = [...]solanago.PublicKey{
	solanago.MustPublicKeyFromBase58("4ACfpUFoaSD9bfPdeu6DBt89gB6ENTeHBXCAi87NhDEE"),
	solanago.MustPublicKeyFromBase58("D2L6yPZ2FmmmTKPgzaMKdhu6EWZcTpLy1Vhx8uvZe7NZ"),
	solanago.MustPublicKeyFromBase58("9bnz4RShgq1hAnLnZbP8kbgBg1kEmcJBYQq3gQbmnSta"),
	solanago.MustPublicKeyFromBase58("5VY91ws6B2hMmBFRsXkoAAdsPHBJwRfBht4DXox3xkwn"),
	solanago.MustPublicKeyFromBase58("2nyhqdwKcJZR2vcqCyrYsaPVdAnFoJjiksCXJ7hfEYgD"),
	solanago.MustPublicKeyFromBase58("2q5pghRs6arqVjRvT5gfgWfWcHWmw1ZuCzphgd5KfWGJ"),
	solanago.MustPublicKeyFromBase58("wyvPkWjVZz1M8fHQnMMCDTQDbkManefNNhweYk5WkcF"),
	solanago.MustPublicKeyFromBase58("3KCKozbAaF75qEU33jtzozcJ29yJuaLJTy2jFdzUY8bT"),
	solanago.MustPublicKeyFromBase58("4vieeGHPYPG2MmyPRcYjdiDmmhN3ww7hsFNap8pVN3Ey"),
	solanago.MustPublicKeyFromBase58("4TQLFNWK8AovT1gFvda5jfw2oJeRMKEmw7aH6MGBJ3or"),
}

var heliusSenderTipIndex atomic.Uint64

func NextHeliusSenderTipAccount() solanago.PublicKey {
	index := heliusSenderTipIndex.Add(1) - 1
	return heliusSenderTipAccounts[index%uint64(len(heliusSenderTipAccounts))]
}

func IsHeliusSenderTipAccount(candidate solanago.PublicKey) bool {
	for _, account := range heliusSenderTipAccounts {
		if candidate.Equals(account) {
			return true
		}
	}
	return false
}

func computeUnitLimit(candidate wireInstruction) (uint32, bool) {
	if candidate.ProgramID != "ComputeBudget111111111111111111111111111111" {
		return 0, false
	}
	data, err := base64.StdEncoding.DecodeString(candidate.Data)
	if err != nil || len(data) != 5 || data[0] != 2 {
		return 0, false
	}
	limit := binary.LittleEndian.Uint32(data[1:])
	return limit, limit > 0
}

func setComputeUnitLimit(limit uint32) solanago.Instruction {
	data := make([]byte, 5)
	data[0] = 2
	binary.LittleEndian.PutUint32(data[1:], limit)
	return solanago.NewInstruction(
		solanago.MustPublicKeyFromBase58("ComputeBudget111111111111111111111111111111"),
		nil,
		data,
	)
}

func computeUnitPrice(candidate wireInstruction) (uint64, bool) {
	if candidate.ProgramID != "ComputeBudget111111111111111111111111111111" {
		return 0, false
	}
	data, err := base64.StdEncoding.DecodeString(candidate.Data)
	if err != nil || len(data) != 9 || data[0] != 3 {
		return 0, false
	}
	price := binary.LittleEndian.Uint64(data[1:])
	return price, price > 0
}

func setComputeUnitPrice(microLamports uint64) solanago.Instruction {
	data := make([]byte, 9)
	data[0] = 3
	binary.LittleEndian.PutUint64(data[1:], microLamports)
	return solanago.NewInstruction(
		solanago.MustPublicKeyFromBase58("ComputeBudget111111111111111111111111111111"),
		nil,
		data,
	)
}

func capComputeUnitPrice(
	providerMicroLamports uint64,
	computeUnits uint32,
	maxPriorityFeeLamports uint64,
) (uint64, error) {
	if maxPriorityFeeLamports == 0 {
		return providerMicroLamports, nil
	}
	numerator := new(big.Int).SetUint64(maxPriorityFeeLamports)
	numerator.Mul(numerator, big.NewInt(1_000_000))
	maxPrice := numerator.Div(
		numerator,
		new(big.Int).SetUint64(uint64(computeUnits)),
	)
	if maxPrice.Sign() <= 0 {
		return 0, fmt.Errorf(
			"max priority fee %d lamports is too low for %d compute units",
			maxPriorityFeeLamports,
			computeUnits,
		)
	}
	if !maxPrice.IsUint64() || providerMicroLamports <= maxPrice.Uint64() {
		return providerMicroLamports, nil
	}
	return maxPrice.Uint64(), nil
}

func providerComputeUnitPrice(payload []byte) uint64 {
	var build buildTransactionResponse
	if json.Unmarshal(payload, &build) != nil {
		return 0
	}
	for _, instruction := range build.ComputeBudgetInstructions {
		if price, ok := computeUnitPrice(instruction); ok {
			return price
		}
	}
	return 0
}

func compiledComputeBudget(transaction *solanago.Transaction) (uint32, uint64) {
	if transaction == nil {
		return 0, 0
	}
	var limit uint32
	var price uint64
	for _, instruction := range transaction.Message.Instructions {
		program, err := transaction.Message.ResolveProgramIDIndex(
			instruction.ProgramIDIndex,
		)
		if err != nil ||
			program.String() != "ComputeBudget111111111111111111111111111111" {
			continue
		}
		switch {
		case len(instruction.Data) == 5 && instruction.Data[0] == 2:
			limit = binary.LittleEndian.Uint32(instruction.Data[1:])
		case len(instruction.Data) == 9 && instruction.Data[0] == 3:
			price = binary.LittleEndian.Uint64(instruction.Data[1:])
		}
	}
	return limit, price
}

func priorityFeeLamports(computeUnits uint32, microLamports uint64) uint64 {
	if computeUnits == 0 || microLamports == 0 {
		return 0
	}
	product := new(big.Int).SetUint64(uint64(computeUnits))
	product.Mul(product, new(big.Int).SetUint64(microLamports))
	product.Add(product, big.NewInt(999_999))
	product.Div(product, big.NewInt(1_000_000))
	if !product.IsUint64() {
		return ^uint64(0)
	}
	return product.Uint64()
}

func decodeWireInstruction(candidate wireInstruction) (solanago.Instruction, error) {
	program, err := solanago.PublicKeyFromBase58(candidate.ProgramID)
	if err != nil {
		return nil, err
	}
	data, err := base64.StdEncoding.DecodeString(candidate.Data)
	if err != nil {
		return nil, err
	}
	accounts := make(solanago.AccountMetaSlice, len(candidate.Accounts))
	for index, account := range candidate.Accounts {
		key, keyErr := solanago.PublicKeyFromBase58(account.Pubkey)
		if keyErr != nil {
			return nil, keyErr
		}
		accounts[index] = solanago.NewAccountMeta(key, account.IsWritable, account.IsSigner)
	}
	return solanago.NewInstruction(program, accounts, data), nil
}

var _ chainport.TxManager = (*TxManager)(nil)
var _ chainport.PreparedTransactionSimulator = (*TxManager)(nil)
var _ ReconciliationRPC = (*ReadOnlyNetwork)(nil)
var _ SignedTransactionSimulator = (*ReadOnlyNetwork)(nil)
