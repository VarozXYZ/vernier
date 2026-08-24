package livecanary_test

import (
	"bytes"
	"context"
	"errors"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	chainport "github.com/VarozXYZ/vernier/ports/chain"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	"github.com/VarozXYZ/vernier/runtime/livecanary"
)

type rebuildingValidator struct {
	now   time.Time
	calls int
}

func (v *rebuildingValidator) Validate(
	_ context.Context,
	request executionport.ValidationRequest,
) (executionport.Artifact, error) {
	v.calls++
	output, _ := market.NewTokenAmount(
		request.Leg.ExpectedOutput.Token(), big.NewInt(4_000_000_000),
	)
	quote, _ := market.NewQuote(market.Quote{
		Source: "validator", Market: request.Leg.Market, SnapshotVersion: 1,
		Purpose: market.QuotePurposeLiveValidation,
		Mode:    market.QuoteModeExactInput, Quality: market.QuoteQualityExact,
		AmountIn: request.Leg.Input, AmountOut: output, QuotedAt: v.now,
	})
	builtAt := v.now
	if v.calls == 1 {
		builtAt = v.now.Add(-2 * time.Second)
	}
	return executionport.Artifact{
		Leg: request.Leg, ValidatedQuote: quote,
		Metadata: map[string]string{"kind": "test"}, BuiltAt: builtAt,
	}, nil
}

type durableJournal struct {
	mu        sync.Mutex
	prepared  bool
	phases    []string
	marked    []string
	decisions []executionport.TriggerFirstDecision
}

type recoveryAttemptJournal struct {
	attempts []executionport.SequentialRecoveryAttempt
}

func (*recoveryAttemptJournal) CreateRecoverableSequentialOperation(
	context.Context, execution.SequentialOperation, execution.SequentialPlan,
) error {
	return nil
}
func (*recoveryAttemptJournal) LoadSequentialRecovery(
	context.Context, execution.OperationID,
) (executionport.SequentialRecoverySnapshot, error) {
	return executionport.SequentialRecoverySnapshot{}, nil
}
func (*recoveryAttemptJournal) SetSequentialRecoveryState(
	context.Context, execution.OperationID, execution.SequentialOperationState, error,
) error {
	return nil
}
func (j *recoveryAttemptJournal) RecordSequentialRecoveryAttempt(
	_ context.Context, attempt executionport.SequentialRecoveryAttempt,
) error {
	j.attempts = append(j.attempts, attempt)
	return nil
}

func (j *durableJournal) RecordTriggerFirstDecision(_ context.Context,
	decision executionport.TriggerFirstDecision) error {
	j.decisions = append(j.decisions, decision)
	return nil
}

func (*durableJournal) CreateSequentialOperation(context.Context, execution.SequentialOperation) error {
	return nil
}
func (j *durableJournal) RecordPreparedTransaction(
	_ context.Context,
	transaction executionport.PreparedTransaction,
) error {
	j.prepared = true
	j.phases = append(j.phases, transaction.Phase)
	return nil
}
func (j *durableJournal) RecordPreparedTransactions(
	ctx context.Context,
	transactions []executionport.PreparedTransaction,
) error {
	for _, transaction := range transactions {
		if err := j.RecordPreparedTransaction(ctx, transaction); err != nil {
			return err
		}
	}
	return nil
}
func (j *durableJournal) MarkTransaction(_ context.Context, _ execution.OperationID, _ int, _, status string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.marked = append(j.marked, status)
	return nil
}
func (*durableJournal) RecordStageSettlement(context.Context, execution.SequentialStageSettlement) error {
	return nil
}
func (*durableJournal) FinishSequentialOperation(context.Context, execution.OperationID, execution.SequentialOperationState, error) error {
	return nil
}
func (*durableJournal) ActiveSequentialOperation(context.Context) (execution.SequentialOperation, bool, error) {
	return execution.SequentialOperation{}, false, nil
}

type settledTxManager struct {
	journal         *durableJournal
	broadcasts      int
	prepareCalls    int
	oversizedAt     string
	simulations     int
	simulationErr   error
	simulationErrs  []error
	economicErrs    []error
	actualOutput    market.TokenAmount
	preparedInput   market.TokenAmount
	reconciliation  *execution.Settlement
	reconcileDelay  time.Duration
	reconciliations atomic.Int32
}

type nonceRetryTxManager struct {
	nonce        uint64
	prepareCalls int
	broadcasts   int
	resyncs      int
	actualOutput market.TokenAmount
}

func (*nonceRetryTxManager) Account() execution.AccountID { return "account" }
func (*nonceRetryTxManager) Warm(context.Context) error   { return nil }
func (m *nonceRetryTxManager) NextNonce() (uint64, error) { return m.nonce, nil }
func (m *nonceRetryTxManager) MarkNonceUsed(nonce uint64) {
	if nonce >= m.nonce {
		m.nonce = nonce + 1
	}
}
func (m *nonceRetryTxManager) ResyncNonce(_ context.Context, rejected uint64) (uint64, error) {
	m.resyncs++
	m.nonce = rejected + 2
	return m.nonce, nil
}
func (m *nonceRetryTxManager) Prepare(
	_ context.Context, artifact executionport.Artifact,
) (chainport.PreparedTransaction, error) {
	m.prepareCalls++
	nonce := m.nonce
	return chainport.PreparedTransaction{
		Leg: artifact.Leg,
		Identity: execution.TransactionIdentity{
			Chain: artifact.Leg.Chain, Account: artifact.Leg.Account,
			Hash: "transaction-" + strconv.Itoa(m.prepareCalls), Nonce: &nonce,
		},
		PreparedAt: time.Now(),
	}, nil
}
func (*nonceRetryTxManager) SimulatePrepared(context.Context, chainport.PreparedTransaction) error {
	return nil
}
func (m *nonceRetryTxManager) SimulatePreparedEconomic(
	_ context.Context, request chainport.EconomicSimulationRequest,
) (chainport.EconomicSimulationResult, error) {
	return chainport.EconomicSimulationResult{
		Input: request.Prepared.Leg.Input, Output: m.actualOutput,
		ContextVersion: request.BalanceVersion, Evidence: "nonce-retry-simulation",
	}, nil
}
func (m *nonceRetryTxManager) Broadcast(
	_ context.Context, prepared chainport.PreparedTransaction,
) (chainport.BroadcastResult, error) {
	m.broadcasts++
	if m.broadcasts == 1 {
		return chainport.BroadcastResult{
			Identity: prepared.Identity, Disposition: chainport.BroadcastRejected, Attempts: 2,
		}, &chainport.AllFanoutNonceTooLowError{Nonce: *prepared.Identity.Nonce, Attempts: 2}
	}
	return chainport.BroadcastResult{
		Identity: prepared.Identity, Disposition: chainport.BroadcastAccepted,
		Accepted: true, Endpoint: "fresh-fanout", Attempts: 1,
	}, nil
}
func (m *nonceRetryTxManager) Reconcile(
	_ context.Context, step execution.OperationStep,
) (execution.Settlement, error) {
	return execution.Settlement{
		Identity: step.Identity, Technical: execution.StateConfirmedSuccess,
		Economic: execution.EconomicEffectVerified, ActualIn: step.Leg.Input,
		ActualOut: m.actualOutput, ObservedAt: time.Now(), Evidence: "test-receipt",
	}, nil
}

func (*settledTxManager) Account() execution.AccountID { return "account" }
func (*settledTxManager) Warm(context.Context) error   { return nil }
func (m *settledTxManager) Prepare(
	_ context.Context,
	artifact executionport.Artifact,
) (chainport.PreparedTransaction, error) {
	m.prepareCalls++
	if m.oversizedAt != "" &&
		artifact.Metadata["max_accounts"] == m.oversizedAt {
		return chainport.PreparedTransaction{}, &executionport.ArtifactTooLargeError{
			ActualBytes: 1_300, MaximumBytes: 1_232,
		}
	}
	m.preparedInput = artifact.Leg.Input
	return chainport.PreparedTransaction{
		Leg: artifact.Leg,
		Identity: execution.TransactionIdentity{
			Chain: "chain-a", Account: "account", Hash: "transaction",
		},
		PreparedAt: time.Now(),
	}, nil
}
func (m *settledTxManager) SimulatePrepared(
	context.Context,
	chainport.PreparedTransaction,
) error {
	m.simulations++
	if len(m.simulationErrs) > 0 {
		err := m.simulationErrs[0]
		m.simulationErrs = m.simulationErrs[1:]
		return err
	}
	return m.simulationErr
}

func (m *settledTxManager) SimulatePreparedEconomic(
	_ context.Context,
	request chainport.EconomicSimulationRequest,
) (chainport.EconomicSimulationResult, error) {
	m.simulations++
	if len(m.economicErrs) > 0 {
		err := m.economicErrs[0]
		m.economicErrs = m.economicErrs[1:]
		if err != nil {
			return chainport.EconomicSimulationResult{}, err
		}
	}
	if m.simulationErr != nil {
		return chainport.EconomicSimulationResult{}, m.simulationErr
	}
	return chainport.EconomicSimulationResult{
		Input: request.Prepared.Leg.Input, Output: m.actualOutput,
		ContextVersion: request.BalanceVersion, Evidence: "test-economic-simulation",
	}, nil
}
func (m *settledTxManager) Broadcast(
	_ context.Context,
	prepared chainport.PreparedTransaction,
) (chainport.BroadcastResult, error) {
	if !m.journal.prepared {
		panic("broadcast happened before durable identity persistence")
	}
	m.broadcasts++
	return chainport.BroadcastResult{
		Identity: prepared.Identity, Disposition: chainport.BroadcastAccepted,
		Accepted: true, Endpoint: "test",
	}, nil
}
func (m *settledTxManager) Reconcile(
	ctx context.Context,
	step execution.OperationStep,
) (execution.Settlement, error) {
	m.reconciliations.Add(1)
	if m.reconcileDelay > 0 {
		timer := time.NewTimer(m.reconcileDelay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return execution.Settlement{}, ctx.Err()
		case <-timer.C:
		}
	}
	if m.reconciliation != nil {
		result := *m.reconciliation
		result.Identity = step.Identity
		return result, nil
	}
	return execution.Settlement{
		Identity: step.Identity, Technical: execution.StateConfirmedSuccess,
		Economic: execution.EconomicEffectVerified,
		ActualIn: m.preparedInput, ActualOut: m.actualOutput,
		ObservedAt: time.Now(), Evidence: "test-receipt",
	}, nil
}

type fixedConfirmationSource struct {
	settlement execution.Settlement
	delay      time.Duration
}

func (*fixedConfirmationSource) Warm(context.Context) error { return nil }
func (s *fixedConfirmationSource) Await(
	ctx context.Context,
	step execution.OperationStep,
) (execution.Settlement, error) {
	if s.delay > 0 {
		timer := time.NewTimer(s.delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return execution.Settlement{}, ctx.Err()
		case <-timer.C:
		}
	}
	result := s.settlement
	result.Identity = step.Identity
	return result, nil
}

type fixedCostValuator struct {
	value market.AssetQuantity
}

func (v fixedCostValuator) Value(
	component execution.CostComponent,
) (execution.CostComponent, error) {
	return component.WithQuoteValue(v.value)
}

type compactingValidator struct {
	now          time.Time
	validate     int
	compact      int
	compactLimit string
}

type fixedOutputValidator struct {
	now       time.Time
	output    *big.Int
	calls     int
	inputs    []market.TokenAmount
	slippages []*executionport.SlippageConstraint
}

type fixedFloorOutputValidator struct {
	fixedOutputValidator
	bps uint16
}

func (v *fixedFloorOutputValidator) Validate(ctx context.Context,
	request executionport.ValidationRequest) (executionport.Artifact, error) {
	artifact, err := v.fixedOutputValidator.Validate(ctx, request)
	if err != nil {
		return executionport.Artifact{}, err
	}
	if request.Slippage == nil {
		minimum := ceilTestMulDiv(
			artifact.ValidatedQuote.AmountOut.Units(),
			big.NewInt(int64(10_000-v.bps)),
			big.NewInt(10_000),
		)
		artifact.Metadata["slippage_bps"] = strconv.FormatUint(uint64(v.bps), 10)
		artifact.Metadata["minimum_output_units"] = minimum.String()
	}
	return artifact, nil
}

func ceilTestMulDiv(value, numerator, denominator *big.Int) *big.Int {
	product := new(big.Int).Mul(value, numerator)
	quotient, remainder := new(big.Int).QuoRem(product, denominator, new(big.Int))
	if remainder.Sign() != 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	return quotient
}

type recordingSellPreflight struct {
	validator   *fixedOutputValidator
	simulations int
}

func (p *recordingSellPreflight) ValidateAndSimulate(
	ctx context.Context,
	request execution.SequentialStageRequest,
) (livecanary.SellPreflightResult, error) {
	p.simulations++
	placeholder, err := market.NewTokenAmount(
		request.Stage.OutputToken,
		big.NewInt(1),
	)
	if err != nil {
		return livecanary.SellPreflightResult{}, err
	}
	artifact, err := p.validator.Validate(
		ctx,
		executionport.ValidationRequest{
			Operation: request.Operation,
			Leg: execution.Leg{
				ID:             "sell-preflight",
				Side:           execution.LegSell,
				Chain:          request.Stage.SourceChain,
				Account:        "reference-account",
				Market:         request.Stage.Market,
				Input:          request.Input,
				ExpectedOutput: placeholder,
			},
		},
	)
	return livecanary.SellPreflightResult{
		Artifact: artifact,
		Identity: "reference-account",
	}, err
}

type failingValidator struct{ err error }

func (v failingValidator) Validate(
	context.Context,
	executionport.ValidationRequest,
) (executionport.Artifact, error) {
	return executionport.Artifact{}, v.err
}

type retryingValidator struct {
	now    time.Time
	output *big.Int
	err    error
	calls  int
}

type constraintRejectingValidator struct {
	now              time.Time
	output           *big.Int
	rejectConstraint bool
	calls            int
}

func (v *constraintRejectingValidator) Validate(
	_ context.Context,
	request executionport.ValidationRequest,
) (executionport.Artifact, error) {
	v.calls++
	output, _ := market.NewTokenAmount(
		request.Leg.ExpectedOutput.Token(), v.output,
	)
	if v.rejectConstraint && request.Slippage != nil {
		return executionport.Artifact{}, &executionport.SlippageThresholdError{
			Provider: "synthetic", Actual: output,
			Required: request.Slippage.MinimumOutput,
		}
	}
	quote, _ := market.NewQuote(market.Quote{
		Source: "validator", Market: request.Leg.Market, SnapshotVersion: 1,
		Purpose: market.QuotePurposeLiveValidation,
		Mode:    market.QuoteModeExactInput, Quality: market.QuoteQualityExact,
		AmountIn: request.Leg.Input, AmountOut: output, QuotedAt: v.now,
	})
	return executionport.Artifact{
		Leg: request.Leg, ValidatedQuote: quote,
		Metadata: map[string]string{"kind": "test"}, BuiltAt: v.now,
	}, nil
}

func (v *retryingValidator) Validate(
	_ context.Context,
	request executionport.ValidationRequest,
) (executionport.Artifact, error) {
	v.calls++
	if v.calls == 1 {
		return executionport.Artifact{}, v.err
	}
	output, _ := market.NewTokenAmount(
		request.Leg.ExpectedOutput.Token(),
		v.output,
	)
	quote, _ := market.NewQuote(market.Quote{
		Source: "validator", Market: request.Leg.Market, SnapshotVersion: 1,
		Purpose: market.QuotePurposeLiveValidation,
		Mode:    market.QuoteModeExactInput, Quality: market.QuoteQualityExact,
		AmountIn: request.Leg.Input, AmountOut: output, QuotedAt: v.now,
	})
	return executionport.Artifact{
		Leg: request.Leg, ValidatedQuote: quote,
		Metadata: map[string]string{"kind": "test"}, BuiltAt: v.now,
	}, nil
}

type fixedQuoteEstimator struct {
	output *big.Int
	err    error
	errs   []error
	calls  atomic.Int32
}

func (e *fixedQuoteEstimator) QuoteExactInput(
	_ context.Context,
	_ market.TokenAmount,
	output market.TokenID,
) (market.TokenAmount, error) {
	e.calls.Add(1)
	if len(e.errs) > 0 {
		err := e.errs[0]
		e.errs = e.errs[1:]
		if err != nil {
			return market.TokenAmount{}, err
		}
	}
	if e.err != nil {
		return market.TokenAmount{}, e.err
	}
	return market.NewTokenAmount(output, e.output)
}

type fixedExitCostSource struct {
	costs map[execution.SequentialExitRoute]market.AssetQuantity
}

func (s fixedExitCostSource) ExitCost(
	_ arbitrage.Direction,
	route execution.SequentialExitRoute,
	_ time.Time,
) (market.AssetQuantity, bool) {
	value, ok := s.costs[route]
	return value, ok
}

func (v *fixedOutputValidator) Validate(
	_ context.Context,
	request executionport.ValidationRequest,
) (executionport.Artifact, error) {
	v.calls++
	v.inputs = append(v.inputs, request.Leg.Input)
	v.slippages = append(v.slippages, request.Slippage)
	output, _ := market.NewTokenAmount(
		request.Leg.ExpectedOutput.Token(),
		v.output,
	)
	quote, _ := market.NewQuote(market.Quote{
		Source: "validator", Market: request.Leg.Market,
		SnapshotVersion: 1,
		Purpose:         market.QuotePurposeLiveValidation,
		Mode:            market.QuoteModeExactInput,
		Quality:         market.QuoteQualityExact,
		AmountIn:        request.Leg.Input,
		AmountOut:       output,
		QuotedAt:        v.now,
	})
	allocation := execution.RouteAllocation{Input: request.Leg.Input, ExpectedOutput: output,
		Groups: []execution.RouteGroup{{ID: "test", InputToken: request.Leg.Input.Token(), OutputToken: output.Token(),
			Branches: []execution.RouteBranch{{Market: request.Leg.Market, PlannedInput: request.Leg.Input.Units(), ExpectedOutput: output.Units()}}}}}
	metadata := map[string]string{"kind": "test"}
	if request.Slippage != nil {
		metadata["slippage_bps"] = new(big.Int).SetUint64(uint64(request.Slippage.BPS)).String()
		metadata["minimum_output_units"] = request.Slippage.MinimumOutput.String()
		for key, value := range request.Slippage.Evidence {
			metadata["decision_"+key] = value
		}
	}
	return executionport.Artifact{
		Leg: request.Leg, ValidatedQuote: quote, Allocation: &allocation,
		Metadata: metadata, BuiltAt: v.now,
	}, nil
}

func (v *compactingValidator) artifact(
	request executionport.ValidationRequest,
	maxAccounts string,
) executionport.Artifact {
	output, _ := market.NewTokenAmount(
		request.Leg.ExpectedOutput.Token(), big.NewInt(4_000_000_000),
	)
	quote, _ := market.NewQuote(market.Quote{
		Source: "validator", Market: request.Leg.Market, SnapshotVersion: 1,
		Purpose: market.QuotePurposeLiveValidation,
		Mode:    market.QuoteModeExactInput, Quality: market.QuoteQualityExact,
		AmountIn: request.Leg.Input, AmountOut: output, QuotedAt: v.now,
	})
	return executionport.Artifact{
		Leg: request.Leg, ValidatedQuote: quote,
		Metadata: map[string]string{
			"kind":         "test",
			"max_accounts": maxAccounts,
		},
		BuiltAt: v.now,
	}
}

func (v *compactingValidator) Validate(
	_ context.Context,
	request executionport.ValidationRequest,
) (executionport.Artifact, error) {
	v.validate++
	return v.artifact(request, "64"), nil
}

func (v *compactingValidator) ValidateCompact(
	_ context.Context,
	request executionport.ValidationRequest,
	previous executionport.Artifact,
) (executionport.Artifact, error) {
	v.compact++
	if previous.Metadata["max_accounts"] != "64" {
		return executionport.Artifact{}, errors.New("unexpected previous account limit")
	}
	return v.artifact(request, v.compactLimit), nil
}

func TestSwapDriverRebuildsStaleArtifactBeforeDurableBroadcast(t *testing.T) {
	now := time.Now().UTC()
	input, _ := market.NewTokenAmount("quote-a", big.NewInt(1_000_000))
	output, _ := market.NewTokenAmount("base-a", big.NewInt(4_000_000_000))
	journal := &durableJournal{}
	manager := &settledTxManager{journal: journal, actualOutput: output}
	validator := &rebuildingValidator{now: now}
	driver := &livecanary.SwapDriver{
		Bindings: map[market.MarketID]livecanary.SwapBinding{
			"market-a": {
				Account: "account", Validator: validator, TxManager: manager,
			},
		},
		Clock:          func() time.Time { return now },
		ArtifactMaxAge: time.Second,
	}
	settlement, err := driver.ExecuteStage(
		context.Background(),
		execution.SequentialStageRequest{
			Operation: "operation", Plan: "plan",
			Stage: execution.SequentialStagePlan{
				Ordinal: 1, Stage: execution.StageBuy,
				SourceChain: "chain-a", InputToken: "quote-a",
				OutputToken: "base-a", Market: "market-a",
			},
			Input: input,
		},
		journal,
	)
	if err != nil {
		t.Fatal(err)
	}
	if validator.calls != 2 {
		t.Fatalf("expected one rebuild, got %d validations", validator.calls)
	}
	if manager.broadcasts != 1 || !journal.prepared {
		t.Fatalf("unexpected broadcast state: broadcasts=%d prepared=%t", manager.broadcasts, journal.prepared)
	}
	if settlement.ActualOutput.Units().Cmp(output.Units()) != 0 {
		t.Fatalf("unexpected actual output %s", settlement.ActualOutput)
	}
}

func TestSwapDriverUsesEconomicWebSocketSettlementWithoutWaitingForReceipt(t *testing.T) {
	now := time.Now().UTC()
	input, _ := market.NewTokenAmount("quote-a", big.NewInt(1_000_000))
	output, _ := market.NewTokenAmount("base-a", big.NewInt(4_000_000_000))
	journal := &durableJournal{}
	manager := &settledTxManager{
		journal: journal, actualOutput: output,
		reconcileDelay: time.Second,
	}
	driver := &livecanary.SwapDriver{
		Bindings: map[market.MarketID]livecanary.SwapBinding{
			"market-a": {
				Account: "account",
				Validator: &fixedOutputValidator{
					now: now, output: output.Units(),
				},
				TxManager: manager,
				Confirmation: &fixedConfirmationSource{
					delay: 5 * time.Millisecond,
					settlement: execution.Settlement{
						Technical: execution.StateConfirmedSuccess,
						Economic:  execution.EconomicEffectVerified,
						ActualIn:  input, ActualOut: output,
						ObservedAt: now, Evidence: "websocket-economic",
					},
				},
			},
		},
		Clock:         func() time.Time { return now },
		FallbackAfter: time.Second,
	}
	started := time.Now()
	settlement, err := driver.ExecuteStage(
		context.Background(),
		execution.SequentialStageRequest{
			Operation: "operation", Plan: "plan",
			Stage: execution.SequentialStagePlan{
				Ordinal: 1, Stage: execution.StageBuy,
				SourceChain: "chain-a", InputToken: "quote-a",
				OutputToken: "base-a", Market: "market-a",
			},
			Input: input,
		},
		journal,
	)
	if err != nil {
		t.Fatal(err)
	}
	if settlement.Evidence != "websocket-economic" ||
		time.Since(started) > 250*time.Millisecond {
		t.Fatalf("settlement=%+v elapsed=%s", settlement, time.Since(started))
	}
}

func TestSwapDriverDoesNotTreatWebSocketInclusionAsEconomicSettlement(t *testing.T) {
	now := time.Now().UTC()
	input, _ := market.NewTokenAmount("quote-a", big.NewInt(1_000_000))
	output, _ := market.NewTokenAmount("base-a", big.NewInt(4_000_000_000))
	receiptSettlement := execution.Settlement{
		Technical: execution.StateConfirmedSuccess,
		Economic:  execution.EconomicEffectVerified,
		ActualIn:  input, ActualOut: output,
		ObservedAt: now, Evidence: "receipt-economic",
	}
	journal := &durableJournal{}
	manager := &settledTxManager{
		journal: journal, reconciliation: &receiptSettlement,
		reconcileDelay: 10 * time.Millisecond,
	}
	driver := &livecanary.SwapDriver{
		Bindings: map[market.MarketID]livecanary.SwapBinding{
			"market-a": {
				Account: "account",
				Validator: &fixedOutputValidator{
					now: now, output: output.Units(),
				},
				TxManager: manager,
				Confirmation: &fixedConfirmationSource{
					settlement: execution.Settlement{
						Technical: execution.StateConfirmedSuccess,
						Economic:  execution.EconomicReserved,
						ActualOut: output, ObservedAt: now,
						Evidence: "websocket-inclusion",
					},
				},
			},
		},
		Clock:         func() time.Time { return now },
		FallbackAfter: time.Second,
	}
	settlement, err := driver.ExecuteStage(
		context.Background(),
		execution.SequentialStageRequest{
			Operation: "operation", Plan: "plan",
			Stage: execution.SequentialStagePlan{
				Ordinal: 1, Stage: execution.StageBuy,
				SourceChain: "chain-a", InputToken: "quote-a",
				OutputToken: "base-a", Market: "market-a",
			},
			Input: input,
		},
		journal,
	)
	if err != nil {
		t.Fatal(err)
	}
	if settlement.Evidence != "receipt-economic" ||
		manager.reconciliations.Load() == 0 {
		t.Fatalf(
			"inclusion-only websocket evidence settled swap: %+v",
			settlement,
		)
	}
}

func TestSwapDriverCompactsOversizedArtifactBeforePersistence(t *testing.T) {
	now := time.Now().UTC()
	input, _ := market.NewTokenAmount("quote-a", big.NewInt(1_000_000))
	output, _ := market.NewTokenAmount("base-a", big.NewInt(4_000_000_000))
	journal := &durableJournal{}
	manager := &settledTxManager{
		journal: journal, actualOutput: output, oversizedAt: "64",
	}
	validator := &compactingValidator{now: now, compactLimit: "48"}
	driver := &livecanary.SwapDriver{
		Bindings: map[market.MarketID]livecanary.SwapBinding{
			"market-a": {
				Account: "account", Validator: validator, TxManager: manager,
			},
		},
		Clock: func() time.Time { return now },
	}
	_, err := driver.ExecuteStage(
		context.Background(),
		execution.SequentialStageRequest{
			Operation: "operation", Plan: "plan",
			Stage: execution.SequentialStagePlan{
				Ordinal: 1, Stage: execution.StageBuy,
				SourceChain: "chain-a", InputToken: "quote-a",
				OutputToken: "base-a", Market: "market-a",
			},
			Input: input,
		},
		journal,
	)
	if err != nil {
		t.Fatal(err)
	}
	if validator.validate != 1 || validator.compact != 1 {
		t.Fatalf(
			"validations: initial=%d compact=%d",
			validator.validate,
			validator.compact,
		)
	}
	if manager.prepareCalls != 2 {
		t.Fatalf("prepare calls = %d", manager.prepareCalls)
	}
	if manager.broadcasts != 1 || !journal.prepared {
		t.Fatalf(
			"unexpected durable broadcast state: broadcasts=%d prepared=%t",
			manager.broadcasts,
			journal.prepared,
		)
	}
}

func TestSwapDriverReturnsValuedGasEvidenceForConfirmedRevert(t *testing.T) {
	now := time.Now().UTC()
	input, _ := market.NewTokenAmount("base-b", big.NewInt(4_000_000_000))
	output, _ := market.NewTokenAmount("quote-b", big.NewInt(1_000_000))
	nativeCost, _ := market.NewAssetQuantity("pol", big.NewRat(1, 100))
	quoteCost, _ := market.NewAssetQuantity("usdc", big.NewRat(2, 100))
	journal := &durableJournal{}
	manager := &settledTxManager{
		journal: journal,
		reconciliation: &execution.Settlement{
			Technical: execution.StateConfirmedRevert,
			Economic:  execution.EconomicReserved,
			Costs: []execution.CostComponent{{
				Kind: "network_fee", Chain: "chain-b",
				Amount: nativeCost, Evidence: "evm_receipt_gas",
			}},
			ObservedAt: now,
			Evidence:   "evm_receipt_revert",
		},
	}
	validator := &fixedOutputValidator{
		now: now, output: output.Units(),
	}
	driver := &livecanary.SwapDriver{
		Bindings: map[market.MarketID]livecanary.SwapBinding{
			"market-b": {
				Account: "account", Validator: validator,
				TxManager: manager,
			},
		},
		Clock: func() time.Time { return now },
		Costs: fixedCostValuator{value: quoteCost},
	}
	_, err := driver.ExecuteStage(
		context.Background(),
		execution.SequentialStageRequest{
			Operation: "operation", Plan: "plan",
			Stage: execution.SequentialStagePlan{
				Ordinal: 3, Stage: execution.StageSell,
				SourceChain: "chain-b", InputToken: "base-b",
				OutputToken: "quote-b", Market: "market-b",
			},
			Input: input,
		},
		journal,
	)
	if err == nil ||
		executionport.ErrorDisposition(err) !=
			executionport.DispositionConfirmedFailure {
		t.Fatalf("error=%v", err)
	}
	costs := executionport.ErrorCosts(err)
	if len(costs) != 1 ||
		costs[0].QuoteValue.Rat().Cmp(quoteCost.Rat()) != 0 {
		t.Fatalf("confirmed revert costs=%+v", costs)
	}
	if len(journal.marked) < 2 ||
		journal.marked[len(journal.marked)-1] != "confirmed_revert" {
		t.Fatalf("transaction states=%v", journal.marked)
	}
	manager.reconciliation = nil
	manager.actualOutput = output
	if _, err := driver.ExecuteStage(
		context.Background(),
		execution.SequentialStageRequest{
			Operation: "operation", Plan: "plan",
			Stage: execution.SequentialStagePlan{
				Ordinal: 3, Stage: execution.StageSell,
				SourceChain: "chain-b", InputToken: "base-b",
				OutputToken: "quote-b", Market: "market-b",
			},
			Input: input,
		},
		journal,
	); err != nil {
		t.Fatal(err)
	}
	if len(journal.phases) != 2 ||
		journal.phases[0] != "swap" ||
		journal.phases[1] != "swap_recovery_1" {
		t.Fatalf("transaction phases=%v", journal.phases)
	}
}

func TestSwapDriverPreflightsBothTransactionsAndReusesSimulatedBuy(t *testing.T) {
	now := time.Now().UTC()
	plan := preflightPlan(t, now)
	journal := &durableJournal{}
	buyOutput, _ := market.NewTokenAmount(
		"base-a",
		big.NewInt(4_052_168_781),
	)
	buyManager := &settledTxManager{
		journal: journal, actualOutput: buyOutput,
	}
	sellManager := &settledTxManager{journal: journal}
	buyValidator := &fixedOutputValidator{
		now: now, output: big.NewInt(4_052_168_781),
	}
	sellValidator := &fixedOutputValidator{
		now: now, output: big.NewInt(1_020_000),
	}
	preflightSellValidator := &fixedOutputValidator{
		now: now, output: big.NewInt(1_010_000),
	}
	sellPreflight := &recordingSellPreflight{
		validator: preflightSellValidator,
	}
	driver := &livecanary.SwapDriver{
		Bindings: map[market.MarketID]livecanary.SwapBinding{
			"market-a": {
				Account: "account", Validator: buyValidator,
				TxManager: buyManager,
			},
			"market-b": {
				Account: "account", Validator: sellValidator,
				TxManager: sellManager,
			},
		},
		SellPreflights: map[market.MarketID]livecanary.SellPreflight{
			"market-b": sellPreflight,
		},
		TokenDecimals: map[market.TokenID]uint8{
			"quote-a": 6, "base-a": 9,
			"base-b": 18, "quote-b": 6,
		},
		BridgePrecision: 8,
		Clock:           func() time.Time { return now },
	}
	if err := driver.Preflight(
		context.Background(),
		"operation",
		plan,
	); err != nil {
		t.Fatal(err)
	}
	if buyValidator.calls != 1 || preflightSellValidator.calls != 1 ||
		buyManager.simulations != 1 ||
		sellPreflight.simulations != 1 ||
		sellValidator.calls != 0 || sellManager.simulations != 0 {
		t.Fatalf(
			"preflight calls buy=%d/%d preflight_sell=%d/%d real_sell=%d/%d",
			buyValidator.calls,
			buyManager.simulations,
			preflightSellValidator.calls,
			sellPreflight.simulations,
			sellValidator.calls,
			sellManager.simulations,
		)
	}
	expectedSellInput := "4052168780000000000"
	if len(preflightSellValidator.inputs) != 1 ||
		preflightSellValidator.inputs[0].String() != expectedSellInput {
		t.Fatalf(
			"sell input = %v; want %s",
			preflightSellValidator.inputs,
			expectedSellInput,
		)
	}
	_, err := driver.ExecuteStage(
		context.Background(),
		execution.SequentialStageRequest{
			Operation: "operation", Plan: plan.ID,
			Stage: plan.Stages[0], Input: plan.InitialInput,
		},
		journal,
	)
	if err != nil {
		t.Fatal(err)
	}
	if buyValidator.calls != 1 ||
		buyManager.prepareCalls != 1 ||
		buyManager.simulations != 1 ||
		buyManager.broadcasts != 1 {
		t.Fatalf(
			"simulated buy was rebuilt: validations=%d prepares=%d simulations=%d broadcasts=%d",
			buyValidator.calls,
			buyManager.prepareCalls,
			buyManager.simulations,
			buyManager.broadcasts,
		)
	}
}

func TestSwapDriverAppliesDynamicBuyBudgetWithoutExtraQuote(t *testing.T) {
	now := time.Now().UTC()
	plan := dynamicPreflightPlan(t, now, "752", "1")
	buyValidator := &fixedOutputValidator{
		now: now, output: big.NewInt(3_000_000_000),
	}
	sellValidator := &fixedOutputValidator{
		now: now, output: big.NewInt(752_000_000),
	}
	driver := &livecanary.SwapDriver{
		Bindings: map[market.MarketID]livecanary.SwapBinding{
			"market-a": {
				Account: "account", Validator: buyValidator,
				TxManager: &settledTxManager{},
			},
			"market-b": {
				Account: "account", Validator: sellValidator,
				TxManager: &settledTxManager{},
			},
		},
		TokenDecimals: map[market.TokenID]uint8{
			"quote-a": 6, "base-a": 9,
			"base-b": 18, "quote-b": 6,
		},
		BridgePrecision: 8,
		QuoteAsset:      "quote",
		MinimumNet:      big.NewRat(1, 2),
		DynamicSlippage: livecanary.DynamicSlippagePolicy{
			Enabled: true, MaxBPS: 500,
		},
		Clock: func() time.Time { return now },
	}
	if err := driver.Preflight(
		context.Background(),
		"operation",
		plan,
	); err != nil {
		t.Fatal(err)
	}
	if len(buyValidator.slippages) != 1 ||
		buyValidator.slippages[0] == nil ||
		buyValidator.slippages[0].BPS != 6 ||
		buyValidator.slippages[0].MinimumOutput.String() !=
			"2998005320" {
		t.Fatalf(
			"unexpected buy slippage: %+v",
			buyValidator.slippages,
		)
	}
	if len(sellValidator.slippages) != 1 ||
		sellValidator.slippages[0] != nil {
		t.Fatalf(
			"preflight sell unexpectedly used dynamic slippage: %+v",
			sellValidator.slippages,
		)
	}
}

func TestSwapDriverCapsDynamicBuySlippage(t *testing.T) {
	now := time.Now().UTC()
	plan := dynamicPreflightPlan(t, now, "800", "0")
	buyValidator := &fixedOutputValidator{
		now: now, output: big.NewInt(3_000_000_000),
	}
	driver := &livecanary.SwapDriver{
		Bindings: map[market.MarketID]livecanary.SwapBinding{
			"market-a": {
				Account: "account", Validator: buyValidator,
				TxManager: &settledTxManager{},
			},
			"market-b": {
				Account: "account",
				Validator: &fixedOutputValidator{
					now: now, output: big.NewInt(800_000_000),
				},
				TxManager: &settledTxManager{},
			},
		},
		TokenDecimals: map[market.TokenID]uint8{
			"quote-a": 6, "base-a": 9,
			"base-b": 18, "quote-b": 6,
		},
		BridgePrecision: 8,
		QuoteAsset:      "quote",
		DynamicSlippage: livecanary.DynamicSlippagePolicy{
			Enabled: true, MaxBPS: 500,
		},
		Clock: func() time.Time { return now },
	}
	if err := driver.Preflight(
		context.Background(),
		"operation",
		plan,
	); err != nil {
		t.Fatal(err)
	}
	if buyValidator.slippages[0] == nil ||
		buyValidator.slippages[0].BPS != 500 {
		t.Fatalf(
			"dynamic buy cap was not applied: %+v",
			buyValidator.slippages,
		)
	}
}

func TestSwapDriverUsesFixedValidatorSlippageForSell(t *testing.T) {
	now := time.Now().UTC()
	plan := dynamicPreflightPlan(t, now, "752", "0")
	validator := &fixedOutputValidator{
		now: now, output: big.NewInt(752_000_000),
	}
	zeroCost, _ := market.NewAssetQuantity("quote", new(big.Rat))
	driver := &livecanary.SwapDriver{
		Bindings: map[market.MarketID]livecanary.SwapBinding{
			"market-b": {
				Account: "account", Validator: validator,
				TxManager: &settledTxManager{},
			},
		},
		TokenDecimals: map[market.TokenID]uint8{
			"quote-a": 6, "base-a": 9,
			"base-b": 18, "quote-b": 6,
		},
		BridgePrecision: 8,
		QuoteAsset:      "quote",
		MinimumNet:      big.NewRat(1, 2),
		ExitCosts: fixedExitCostSource{
			costs: map[execution.SequentialExitRoute]market.AssetQuantity{
				execution.ExitSellAtDestination: zeroCost,
			},
		},
		DynamicSlippage: livecanary.DynamicSlippagePolicy{
			Enabled: true, MaxBPS: 500,
		},
		Clock: func() time.Time { return now },
	}
	bridged, _ := market.NewTokenAmount(
		"base-b",
		big.NewInt(3_000_000_000_000_000_000),
	)
	if _, err := driver.SelectExit(
		context.Background(),
		"operation",
		plan,
		bridged,
		nil,
	); err != nil {
		t.Fatal(err)
	}
	if len(validator.slippages) != 1 || validator.slippages[0] != nil {
		t.Fatalf(
			"sell unexpectedly received a dynamic constraint: %+v",
			validator.slippages,
		)
	}

	oneCost, _ := market.NewAssetQuantity("quote", big.NewRat(1, 1))
	driver.ExitCosts = fixedExitCostSource{
		costs: map[execution.SequentialExitRoute]market.AssetQuantity{
			execution.ExitSellAtDestination: oneCost,
		},
	}
	if _, err := driver.SelectExit(
		context.Background(),
		"operation-floor",
		plan,
		bridged,
		nil,
	); err != nil {
		t.Fatal(err)
	}
	if validator.slippages[1] != nil {
		t.Fatalf(
			"sell cost changed the fixed validator slippage: %+v",
			validator.slippages,
		)
	}
}

func TestSwapDriverRejectsDeterioratedRoundTripBeforeFirstBroadcast(t *testing.T) {
	now := time.Now().UTC()
	plan := preflightPlan(t, now)
	journal := &durableJournal{}
	buyManager := &settledTxManager{journal: journal}
	sellManager := &settledTxManager{journal: journal}
	driver := &livecanary.SwapDriver{
		Bindings: map[market.MarketID]livecanary.SwapBinding{
			"market-a": {
				Account: "account",
				Validator: &fixedOutputValidator{
					now: now, output: big.NewInt(4_052_168_781),
				},
				TxManager: buyManager,
			},
			"market-b": {
				Account: "account",
				Validator: &fixedOutputValidator{
					now: now, output: big.NewInt(999_999),
				},
				TxManager: sellManager,
			},
		},
		TokenDecimals: map[market.TokenID]uint8{
			"quote-a": 6, "base-a": 9,
			"base-b": 18, "quote-b": 6,
		},
		BridgePrecision: 8,
		Clock:           func() time.Time { return now },
	}
	err := driver.Preflight(context.Background(), "operation", plan)
	if err == nil ||
		executionport.ErrorDisposition(err) !=
			executionport.DispositionRejected {
		t.Fatalf("preflight error = %v", err)
	}
	if buyManager.broadcasts != 0 || sellManager.broadcasts != 0 ||
		journal.prepared {
		t.Fatalf(
			"deteriorated preflight reached persistence or broadcast",
		)
	}
}

func TestSwapDriverRejectsWhenSecondTransactionSimulationFails(t *testing.T) {
	now := time.Now().UTC()
	plan := preflightPlan(t, now)
	journal := &durableJournal{}
	buyManager := &settledTxManager{journal: journal}
	sellManager := &settledTxManager{
		journal:       journal,
		simulationErr: errors.New("sell transaction reverted"),
	}
	driver := &livecanary.SwapDriver{
		Bindings: map[market.MarketID]livecanary.SwapBinding{
			"market-a": {
				Account: "account",
				Validator: &fixedOutputValidator{
					now: now, output: big.NewInt(4_052_168_781),
				},
				TxManager: buyManager,
			},
			"market-b": {
				Account: "account",
				Validator: &fixedOutputValidator{
					now: now, output: big.NewInt(1_010_000),
				},
				TxManager: sellManager,
			},
		},
		TokenDecimals: map[market.TokenID]uint8{
			"quote-a": 6, "base-a": 9,
			"base-b": 18, "quote-b": 6,
		},
		BridgePrecision: 8,
		Clock:           func() time.Time { return now },
	}
	err := driver.Preflight(context.Background(), "operation", plan)
	if err == nil ||
		executionport.ErrorDisposition(err) !=
			executionport.DispositionRejected {
		t.Fatalf("preflight error = %v", err)
	}
	if buyManager.simulations != 1 ||
		sellManager.simulations != 1 ||
		journal.prepared ||
		buyManager.broadcasts != 0 {
		t.Fatalf(
			"simulation failure state buy_sim=%d sell_sim=%d durable=%t broadcasts=%d",
			buyManager.simulations,
			sellManager.simulations,
			journal.prepared,
			buyManager.broadcasts,
		)
	}
}

func TestPrefundedPreflightDoesNotRetryBeforeFirstSettlement(t *testing.T) {
	now := time.Now().UTC()
	plan := prefundedPreflightPlan(t, now)
	journal := &durableJournal{}
	buyManager := &settledTxManager{journal: journal}
	sellManager := &settledTxManager{
		journal: journal,
		simulationErrs: []error{
			errors.New("Jupiter 6001 slippage exceeded"),
			nil,
		},
	}
	driver := &livecanary.SwapDriver{
		Bindings: map[market.MarketID]livecanary.SwapBinding{
			"market-a": {
				Account: "account",
				Validator: &fixedOutputValidator{
					now: now, output: big.NewInt(4_052_168_781),
				},
				TxManager: buyManager,
			},
			"market-b": {
				Account: "account",
				Validator: &fixedOutputValidator{
					now: now, output: big.NewInt(1_010_000),
				},
				TxManager: sellManager,
				SpendableBalance: livecanary.SpendableBalanceReaderFunc(
					func(
						context.Context,
						market.TokenID,
					) (*big.Int, error) {
						return new(big.Int).Exp(
							big.NewInt(10),
							big.NewInt(30),
							nil,
						), nil
					},
				),
			},
		},
		TokenDecimals: map[market.TokenID]uint8{
			"quote-a": 6, "base-a": 9,
			"base-b": 18, "quote-b": 6,
		},
		BridgePrecision:          8,
		Clock:                    func() time.Time { return now },
		ExitValidationAttempts:   2,
		ExitValidationRetryDelay: time.Nanosecond,
	}
	err := driver.Preflight(
		context.Background(),
		"operation",
		plan,
	)
	if err == nil ||
		executionport.ErrorDisposition(err) !=
			executionport.DispositionRejected {
		t.Fatalf("preflight error = %v", err)
	}
	if sellManager.simulations != 1 ||
		buyManager.simulations != 1 ||
		buyManager.broadcasts != 0 ||
		sellManager.broadcasts != 0 {
		t.Fatalf(
			"simulations buy=%d sell=%d broadcasts=%d/%d",
			buyManager.simulations,
			sellManager.simulations,
			buyManager.broadcasts,
			sellManager.broadcasts,
		)
	}
}

func TestPrefundedPreflightRejectsInsufficientPhysicalDestinationInventory(t *testing.T) {
	now := time.Now().UTC()
	plan := prefundedPreflightPlan(t, now)
	buyValidator := &fixedOutputValidator{
		now: now, output: big.NewInt(4_052_168_781),
	}
	sellValidator := &fixedOutputValidator{
		now: now, output: big.NewInt(1_010_000),
	}
	driver := &livecanary.SwapDriver{
		Bindings: map[market.MarketID]livecanary.SwapBinding{
			"market-a": {
				Account: "account", Validator: buyValidator,
				TxManager: &settledTxManager{},
			},
			"market-b": {
				Account: "account", Validator: sellValidator,
				TxManager: &settledTxManager{},
				SpendableBalance: livecanary.SpendableBalanceReaderFunc(
					func(
						context.Context,
						market.TokenID,
					) (*big.Int, error) {
						return big.NewInt(1), nil
					},
				),
			},
		},
		TokenDecimals: map[market.TokenID]uint8{
			"quote-a": 6, "base-a": 9,
			"base-b": 18, "quote-b": 6,
		},
		BridgePrecision: 8,
		Clock:           func() time.Time { return now },
	}
	err := driver.Preflight(
		context.Background(),
		"operation",
		plan,
	)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			"prefunded destination inventory is insufficient",
		) ||
		sellValidator.calls != 0 {
		t.Fatalf(
			"inventory preflight error=%v sell_validations=%d",
			err,
			sellValidator.calls,
		)
	}
}

func TestForcedPrefundedExitReportsFixedSlippageEvidence(t *testing.T) {
	now := time.Now().UTC()
	plan := prefundedPreflightPlan(t, now)
	plan.Opportunity.Reasons = []string{"forced_canary_direction"}
	cost, _ := market.NewAssetQuantity("quote", big.NewRat(1, 100))
	candidate := plan.Opportunity.Candidates[plan.Opportunity.SelectedIndex]
	candidate.Cost = arbitrage.CostSnapshot{
		ID: "admission-cost", Amount: cost, CapturedAt: now,
	}
	plan.Opportunity.Candidates[plan.Opportunity.SelectedIndex] = candidate
	driver := &livecanary.SwapDriver{
		Bindings: map[market.MarketID]livecanary.SwapBinding{
			"market-b": {
				Account: "account",
				Validator: &fixedOutputValidator{
					now: now, output: big.NewInt(1_004_573),
				},
				TxManager: &settledTxManager{},
			},
		},
		TokenDecimals: map[market.TokenID]uint8{
			"quote-a": 6, "base-a": 9,
			"base-b": 18, "quote-b": 6,
		},
		QuoteAsset: "quote",
		DynamicSlippage: livecanary.DynamicSlippagePolicy{
			Enabled: true, MaxBPS: 500,
		},
		Clock: func() time.Time { return now },
	}
	bought, _ := market.NewTokenAmount(
		"base-a",
		big.NewInt(4_836_579_243),
	)
	decision, err := driver.SelectPrefundedExit(
		context.Background(),
		"operation",
		plan,
		bought,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(decision.Evidence, "fixed_slippage") ||
		strings.Contains(decision.Evidence, "dynamic_slippage") {
		t.Fatalf("misleading exit evidence: %s", decision.Evidence)
	}
}

func TestPrefundedSellDoesNotApplyDynamicProfitThreshold(t *testing.T) {
	now := time.Now().UTC()
	plan := prefundedPreflightPlan(t, now)
	candidate := plan.Opportunity.Candidates[plan.Opportunity.SelectedIndex]
	zeroCost, _ := market.NewAssetQuantity("quote", new(big.Rat))
	candidate.Cost = arbitrage.CostSnapshot{
		ID: "admission-cost", Amount: zeroCost, CapturedAt: now,
	}
	plan.Opportunity.Candidates[plan.Opportunity.SelectedIndex] = candidate
	destination := &constraintRejectingValidator{
		now: now, output: big.NewInt(1_010_000), rejectConstraint: true,
	}
	origin := &fixedOutputValidator{
		now: now, output: big.NewInt(1_000_000),
	}
	driver := &livecanary.SwapDriver{
		Bindings: map[market.MarketID]livecanary.SwapBinding{
			"market-a": {Account: "account", Validator: origin, TxManager: &settledTxManager{}},
			"market-b": {Account: "account", Validator: destination, TxManager: &settledTxManager{}},
		},
		TokenDecimals: map[market.TokenID]uint8{
			"quote-a": 6, "base-a": 9, "base-b": 18, "quote-b": 6,
		},
		BridgePrecision: 8, QuoteAsset: "quote", MinimumNet: big.NewRat(1, 2),
		ExitCosts: fixedExitCostSource{costs: map[execution.SequentialExitRoute]market.AssetQuantity{
			execution.ExitSellAtDestination: zeroCost,
			execution.ExitSellAtOrigin:      zeroCost,
		}},
		DynamicSlippage:        livecanary.DynamicSlippagePolicy{Enabled: true, MaxBPS: 500},
		ExitValidationAttempts: 2, ExitValidationRetryDelay: time.Nanosecond,
		Clock: func() time.Time { return now },
	}
	bought, _ := market.NewTokenAmount("base-a", big.NewInt(4_836_579_243))
	decision, err := driver.SelectPrefundedExit(
		context.Background(), "operation", plan, bought, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Route != execution.ExitSellAtDestination ||
		destination.calls != 1 || origin.calls != 0 ||
		!strings.Contains(decision.Evidence, "fixed_slippage") {
		t.Fatalf("unexpected fixed-slippage decision=%+v calls=%d/%d", decision, destination.calls, origin.calls)
	}
}

func TestPrefundedRecoveryRetriesTransientOriginBuildBeforeComparison(t *testing.T) {
	now := time.Now().UTC()
	plan := prefundedPreflightPlan(t, now)
	origin := &retryingValidator{
		now: now, output: big.NewInt(1_020_000),
		err: errors.New("provider HTTP 400: transient route build failure"),
	}
	destination := &fixedOutputValidator{now: now, output: big.NewInt(1_010_000)}
	zeroCost, _ := market.NewAssetQuantity("quote", new(big.Rat))
	driver := &livecanary.SwapDriver{
		Bindings: map[market.MarketID]livecanary.SwapBinding{
			"market-a": {Account: "account", Validator: origin, TxManager: &settledTxManager{}},
			"market-b": {Account: "account", Validator: destination, TxManager: &settledTxManager{}},
		},
		TokenDecimals: map[market.TokenID]uint8{
			"quote-a": 6, "base-a": 9, "base-b": 18, "quote-b": 6,
		},
		BridgePrecision: 8, QuoteAsset: "quote",
		ExitCosts: fixedExitCostSource{costs: map[execution.SequentialExitRoute]market.AssetQuantity{
			execution.ExitSellAtDestination: zeroCost,
			execution.ExitSellAtOrigin:      zeroCost,
		}},
		ExitValidationAttempts: 3, ExitValidationRetryDelay: time.Nanosecond,
		Clock: func() time.Time { return now },
	}
	bought, _ := market.NewTokenAmount("base-a", big.NewInt(4_836_579_243))
	decision, err := driver.SelectPrefundedRecoveryExit(
		context.Background(), "operation", plan, bought, nil,
		errors.New("safe destination failure"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if origin.calls != 2 || destination.calls != 1 ||
		decision.Route != execution.ExitSellAtOrigin {
		t.Fatalf("fresh retries/comparison failed: origin_calls=%d destination_calls=%d decision=%+v", origin.calls, destination.calls, decision)
	}
}

func TestSwapBroadcastAllNonceTooLowResyncsAndRebuildsFreshArtifactOnce(t *testing.T) {
	now := time.Now().UTC()
	plan := preflightPlan(t, now)
	request := execution.SequentialStageRequest{
		Operation: "operation-nonce-resync", Plan: plan.ID,
		Stage: plan.Stages[0], Input: plan.InitialInput,
	}
	output := mustLiveAmount(t, request.Stage.OutputToken, "4000000000")
	manager := &nonceRetryTxManager{nonce: 7, actualOutput: output}
	initial := &fixedOutputValidator{now: now, output: output.Units()}
	fresh := &fixedOutputValidator{now: now, output: output.Units()}
	estimator := &fixedQuoteEstimator{output: output.Units()}
	journal := &durableJournal{}
	driver := &livecanary.SwapDriver{
		Bindings: map[market.MarketID]livecanary.SwapBinding{
			request.Stage.Market: {
				Account: "account", Validator: initial, RecoveryValidator: fresh,
				Estimator: estimator, TxManager: manager, NonceCoordinator: manager,
				BalanceSnapshot: func(market.TokenID) (*big.Int, uint64, error) {
					return new(big.Int), 1, nil
				},
			},
		},
		Clock: func() time.Time { return now },
	}
	settlement, err := driver.ExecuteStage(context.Background(), request, journal)
	if err != nil {
		t.Fatal(err)
	}
	if settlement.ActualOutput.Units().Cmp(output.Units()) != 0 ||
		manager.broadcasts != 2 || manager.resyncs != 1 || manager.prepareCalls != 2 ||
		initial.calls != 1 || fresh.calls != 1 || estimator.calls.Load() != 1 {
		t.Fatalf("unexpected nonce rebuild settlement=%+v broadcasts=%d resyncs=%d prepares=%d validators=%d/%d quotes=%d",
			settlement, manager.broadcasts, manager.resyncs, manager.prepareCalls,
			initial.calls, fresh.calls, estimator.calls.Load())
	}
	if len(journal.phases) != 2 || journal.phases[0] == journal.phases[1] {
		t.Fatalf("rebuilt identity was not durably versioned: phases=%v", journal.phases)
	}
}

func TestHybridPrefundedRecoveryRetriesEntireQuoteBuildSimulationCycleAndPersistsFailures(t *testing.T) {
	now := time.Now().UTC()
	plan := prefundedPreflightPlan(t, now)
	originEstimator := &fixedQuoteEstimator{
		output: big.NewInt(1_020_000),
		errs:   []error{errors.New("temporary quote failure")},
	}
	originValidator := &fixedOutputValidator{now: now, output: big.NewInt(1_020_000)}
	originManager := &settledTxManager{
		actualOutput: mustLiveAmount(t, "quote-a", "1020000"),
		economicErrs: []error{errors.New("temporary simulation failure")},
	}
	destinationEstimator := &fixedQuoteEstimator{output: big.NewInt(1_010_000)}
	destinationValidator := &fixedOutputValidator{now: now, output: big.NewInt(1_010_000)}
	destinationManager := &settledTxManager{actualOutput: mustLiveAmount(t, "quote-b", "1010000")}
	recoveryJournal := &recoveryAttemptJournal{}
	zeroCost, _ := market.NewAssetQuantity("quote", new(big.Rat))
	driver := &livecanary.SwapDriver{
		Bindings: map[market.MarketID]livecanary.SwapBinding{
			"market-a": {
				Account: "account", Validator: originValidator,
				RecoveryValidator: originValidator, Estimator: originEstimator,
				TxManager: originManager,
				BalanceSnapshot: func(market.TokenID) (*big.Int, uint64, error) {
					return new(big.Int), 1, nil
				},
			},
			"market-b": {
				Account: "account", Validator: destinationValidator,
				RecoveryValidator: destinationValidator, Estimator: destinationEstimator,
				TxManager: destinationManager,
				BalanceSnapshot: func(market.TokenID) (*big.Int, uint64, error) {
					return new(big.Int), 1, nil
				},
			},
		},
		TokenDecimals: map[market.TokenID]uint8{
			"quote-a": 6, "base-a": 9, "base-b": 18, "quote-b": 6,
		},
		BridgePrecision: 8, QuoteAsset: "quote",
		ExitCosts: fixedExitCostSource{costs: map[execution.SequentialExitRoute]market.AssetQuantity{
			execution.ExitSellAtDestination: zeroCost,
			execution.ExitSellAtOrigin:      zeroCost,
		}},
		ExitValidationAttempts: 3, ExitValidationRetryDelay: time.Nanosecond,
		Clock: func() time.Time { return now }, RecoveryJournal: recoveryJournal,
	}
	bought, _ := market.NewTokenAmount("base-a", big.NewInt(4_836_579_243))
	decision, err := driver.SelectPrefundedRecoveryExit(
		context.Background(), "operation", plan, bought, nil,
		errors.New("safe destination failure"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Route != execution.ExitSellAtOrigin {
		t.Fatalf("decision=%+v; want origin", decision)
	}
	if originEstimator.calls.Load() != 3 || originValidator.calls != 2 || originManager.simulations != 2 {
		t.Fatalf("full-cycle calls quote/build/simulation=%d/%d/%d; want 3/2/2",
			originEstimator.calls.Load(), originValidator.calls, originManager.simulations)
	}
	if len(recoveryJournal.attempts) != 2 {
		t.Fatalf("persisted attempts=%d; want 2", len(recoveryJournal.attempts))
	}
	if !strings.HasSuffix(recoveryJournal.attempts[0].Action, "_quote") ||
		!strings.HasSuffix(recoveryJournal.attempts[1].Action, "_simulation") ||
		!strings.Contains(recoveryJournal.attempts[0].Detail, "temporary quote failure") ||
		!strings.Contains(recoveryJournal.attempts[1].Detail, "temporary simulation failure") {
		t.Fatalf("unexpected durable attempts: %+v", recoveryJournal.attempts)
	}
}

func TestSwapDriverKeepsFreshProfitableDestinationExitAndReusesIt(t *testing.T) {
	now := time.Now().UTC()
	plan := preflightPlan(t, now)
	journal := &durableJournal{}
	manager := &settledTxManager{
		journal: journal,
		actualOutput: func() market.TokenAmount {
			value, _ := market.NewTokenAmount(
				"quote-b", big.NewInt(1_080_000),
			)
			return value
		}(),
	}
	cost, _ := market.NewAssetQuantity("quote", big.NewRat(1, 100))
	driver := &livecanary.SwapDriver{
		Bindings: map[market.MarketID]livecanary.SwapBinding{
			"market-b": {
				Account: "account",
				Validator: &fixedOutputValidator{
					now: now, output: big.NewInt(1_080_000),
				},
				TxManager: manager,
			},
		},
		TokenDecimals: map[market.TokenID]uint8{
			"quote-a": 6, "base-a": 9,
			"base-b": 18, "quote-b": 6,
		},
		BridgePrecision: 8, QuoteAsset: "quote",
		MinimumNet:   big.NewRat(5, 100),
		ReturnMargin: big.NewRat(1, 100),
		ExitCosts: fixedExitCostSource{
			costs: map[execution.SequentialExitRoute]market.AssetQuantity{
				execution.ExitSellAtDestination: cost,
			},
		},
		Clock: func() time.Time { return now },
	}
	bridged, _ := market.NewTokenAmount(
		"base-b", big.NewInt(4_000_000_000_000_000_000),
	)
	decision, err := driver.SelectExit(
		context.Background(), "operation", plan, bridged, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Route != execution.ExitSellAtDestination ||
		!decision.DestinationQualified ||
		decision.ReturnOutput.Token() != "" {
		t.Fatalf("unexpected decision: %+v", decision)
	}
	_, err = driver.ExecuteStage(
		context.Background(),
		execution.SequentialStageRequest{
			Operation: "operation", Plan: plan.ID,
			Stage: plan.Stages[2], Input: bridged,
		},
		journal,
	)
	if err != nil {
		t.Fatal(err)
	}
	if manager.prepareCalls != 1 || manager.simulations != 1 ||
		manager.broadcasts != 1 {
		t.Fatalf(
			"destination artifact was not reused: prepare=%d simulate=%d broadcast=%d",
			manager.prepareCalls, manager.simulations, manager.broadcasts,
		)
	}
}

func TestSwapDriverRecoveryComparesReturnEvenWhenDestinationRemainsQualified(t *testing.T) {
	now := time.Now().UTC()
	plan := preflightPlan(t, now)
	destinationManager := &settledTxManager{}
	returnEstimator := &fixedQuoteEstimator{output: big.NewInt(1_100_000)}
	destinationValidator := &fixedOutputValidator{
		now: now, output: big.NewInt(1_080_000),
	}
	cost, _ := market.NewAssetQuantity("quote", big.NewRat(1, 100))
	driver := &livecanary.SwapDriver{
		Bindings: map[market.MarketID]livecanary.SwapBinding{
			"market-a": {
				Account: "account",
				Validator: &fixedOutputValidator{
					now: now, output: big.NewInt(1_100_000),
				},
				Estimator: returnEstimator,
				TxManager: &settledTxManager{},
			},
			"market-b": {
				Account:   "account",
				Validator: destinationValidator,
				TxManager: destinationManager,
			},
		},
		TokenDecimals: map[market.TokenID]uint8{
			"quote-a": 6, "base-a": 9,
			"base-b": 18, "quote-b": 6,
		},
		BridgePrecision: 8, QuoteAsset: "quote",
		MinimumNet:   big.NewRat(5, 100),
		ReturnMargin: big.NewRat(1, 100),
		ExitCosts: fixedExitCostSource{
			costs: map[execution.SequentialExitRoute]market.AssetQuantity{
				execution.ExitSellAtDestination: cost,
				execution.ExitReturnToOrigin:    cost,
			},
		},
		DynamicSlippage: livecanary.DynamicSlippagePolicy{
			Enabled: true, MaxBPS: 500,
		},
		Clock: func() time.Time { return now },
	}
	bridged, _ := market.NewTokenAmount(
		"base-b", big.NewInt(4_000_000_000_000_000_000),
	)
	decision, err := driver.SelectRecoveryExit(
		context.Background(),
		"operation",
		plan,
		bridged,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Route != execution.ExitReturnToOrigin ||
		!decision.DestinationQualified ||
		returnEstimator.calls.Load() != 1 ||
		!strings.Contains(
			decision.Evidence,
			"automatic_recovery_comparison",
		) {
		t.Fatalf("unexpected recovery decision: %+v", decision)
	}
	if len(destinationValidator.slippages) != 1 ||
		destinationValidator.slippages[0] != nil {
		t.Fatalf(
			"recovery unexpectedly used dynamic slippage: %+v",
			destinationValidator.slippages,
		)
	}
}

func TestSwapDriverReturnsToOriginOnlyWhenNetRecoveryClearsMargin(t *testing.T) {
	now := time.Now().UTC()
	plan := preflightPlan(t, now)
	destinationManager := &settledTxManager{}
	returnManager := &settledTxManager{}
	destinationCost, _ := market.NewAssetQuantity(
		"quote", big.NewRat(1, 100),
	)
	returnCost, _ := market.NewAssetQuantity(
		"quote", big.NewRat(1, 100),
	)
	driver := &livecanary.SwapDriver{
		// The origin estimate is deliberately quote-only: the bridged token
		// is not present there yet and cannot be simulated.
		Bindings: map[market.MarketID]livecanary.SwapBinding{
			"market-a": {
				Account: "account",
				Validator: &fixedOutputValidator{
					now: now, output: big.NewInt(980_000),
				},
				Estimator: &fixedQuoteEstimator{
					output: big.NewInt(980_000),
				},
				TxManager: returnManager,
			},
			"market-b": {
				Account: "account",
				Validator: &fixedOutputValidator{
					now: now, output: big.NewInt(950_000),
				},
				TxManager: destinationManager,
			},
		},
		TokenDecimals: map[market.TokenID]uint8{
			"quote-a": 6, "base-a": 9,
			"base-b": 18, "quote-b": 6,
		},
		BridgePrecision: 8, QuoteAsset: "quote",
		MinimumNet:   big.NewRat(5, 100),
		ReturnMargin: big.NewRat(1, 100),
		ExitCosts: fixedExitCostSource{
			costs: map[execution.SequentialExitRoute]market.AssetQuantity{
				execution.ExitSellAtDestination: destinationCost,
				execution.ExitReturnToOrigin:    returnCost,
			},
		},
		Clock: func() time.Time { return now },
	}
	bridged, _ := market.NewTokenAmount(
		"base-b", big.NewInt(4_000_000_000_000_000_000),
	)
	decision, err := driver.SelectExit(
		context.Background(), "operation", plan, bridged, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Route != execution.ExitReturnToOrigin ||
		decision.DestinationQualified ||
		decision.DestinationRecovery.Rat().Cmp(big.NewRat(94, 100)) != 0 ||
		decision.ReturnRecovery.Rat().Cmp(big.NewRat(97, 100)) != 0 {
		t.Fatalf("unexpected decision: %+v", decision)
	}
	if destinationManager.simulations != 1 ||
		returnManager.simulations != 0 ||
		returnManager.prepareCalls != 0 {
		t.Fatalf(
			"destination simulations=%d return simulations=%d prepares=%d",
			destinationManager.simulations,
			returnManager.simulations,
			returnManager.prepareCalls,
		)
	}
}

func TestSwapDriverReturnsToOriginWhenDestinationLiquidationIsUnavailable(t *testing.T) {
	now := time.Now().UTC()
	plan := preflightPlan(t, now)
	returnManager := &settledTxManager{}
	var output bytes.Buffer
	returnCost, _ := market.NewAssetQuantity(
		"quote", big.NewRat(1, 100),
	)
	driver := &livecanary.SwapDriver{
		Bindings: map[market.MarketID]livecanary.SwapBinding{
			"market-a": {
				Account: "account",
				Validator: &fixedOutputValidator{
					now: now, output: big.NewInt(980_000),
				},
				Estimator: &fixedQuoteEstimator{
					output: big.NewInt(980_000),
				},
				TxManager: returnManager,
			},
			"market-b": {
				Account:   "account",
				Validator: failingValidator{err: errors.New("route unavailable")},
				TxManager: &settledTxManager{},
			},
		},
		TokenDecimals: map[market.TokenID]uint8{
			"quote-a": 6, "base-a": 9,
			"base-b": 18, "quote-b": 6,
		},
		BridgePrecision: 8, QuoteAsset: "quote",
		ReturnMargin: big.NewRat(1, 100),
		ExitCosts: fixedExitCostSource{
			costs: map[execution.SequentialExitRoute]market.AssetQuantity{
				execution.ExitReturnToOrigin: returnCost,
			},
		},
		Clock:                    func() time.Time { return now },
		Output:                   &output,
		ExitValidationAttempts:   15,
		ExitValidationRetryDelay: time.Nanosecond,
	}
	bridged, _ := market.NewTokenAmount(
		"base-b", big.NewInt(4_000_000_000_000_000_000),
	)
	decision, err := driver.SelectExit(
		context.Background(), "operation", plan, bridged, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Route != execution.ExitReturnToOrigin ||
		!decision.DestinationOutput.IsZero() ||
		decision.ReturnOutput.IsZero() {
		t.Fatalf("unexpected forced-return decision: %+v", decision)
	}
	logged := output.String()
	if strings.Count(logged, "status=failed") != 15 ||
		!strings.Contains(logged, "quote/build preparation: route unavailable") ||
		!strings.Contains(logged, "status=unavailable attempts=15") {
		t.Fatalf("destination failure detail was not logged:\n%s", logged)
	}
}

func TestSwapDriverRetriesDestinationQuoteBeforeReturningToOrigin(t *testing.T) {
	now := time.Now().UTC()
	plan := preflightPlan(t, now)
	validator := &retryingValidator{
		now: now, output: big.NewInt(1_100_000),
		err: errors.New("temporary quote failure"),
	}
	destinationManager := &settledTxManager{}
	returnEstimator := &fixedQuoteEstimator{output: big.NewInt(980_000)}
	cost, _ := market.NewAssetQuantity("quote", big.NewRat(1, 100))
	var output bytes.Buffer
	driver := &livecanary.SwapDriver{
		Bindings: map[market.MarketID]livecanary.SwapBinding{
			"market-a": {
				Account: "account",
				Validator: &fixedOutputValidator{
					now: now, output: big.NewInt(980_000),
				},
				Estimator: returnEstimator,
				TxManager: &settledTxManager{},
			},
			"market-b": {
				Account: "account", Validator: validator,
				TxManager: destinationManager,
			},
		},
		TokenDecimals: map[market.TokenID]uint8{
			"quote-a": 6, "base-a": 9,
			"base-b": 18, "quote-b": 6,
		},
		BridgePrecision: 8, QuoteAsset: "quote",
		MinimumNet: new(big.Rat), ReturnMargin: new(big.Rat),
		ExitCosts: fixedExitCostSource{
			costs: map[execution.SequentialExitRoute]market.AssetQuantity{
				execution.ExitSellAtDestination: cost,
				execution.ExitReturnToOrigin:    cost,
			},
		},
		Clock:                    func() time.Time { return now },
		Output:                   &output,
		ExitValidationAttempts:   15,
		ExitValidationRetryDelay: time.Nanosecond,
	}
	bridged, _ := market.NewTokenAmount(
		"base-b", big.NewInt(4_000_000_000_000_000_000),
	)
	decision, err := driver.SelectRecoveryExit(
		context.Background(), "operation", plan, bridged, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if validator.calls != 2 ||
		destinationManager.simulations != 1 ||
		decision.Route != execution.ExitSellAtDestination {
		t.Fatalf(
			"calls=%d simulations=%d decision=%+v",
			validator.calls,
			destinationManager.simulations,
			decision,
		)
	}
	logged := output.String()
	if !strings.Contains(
		logged,
		`status=failed attempt=1/15 error="quote/build preparation: temporary quote failure"`,
	) ||
		!strings.Contains(logged, "status=ready attempt=2/15") {
		t.Fatalf("retry detail was not logged:\n%s", logged)
	}
}

func TestForcedCanaryPreflightAcceptsExecutableNegativeRoundTrip(t *testing.T) {
	now := time.Now().UTC()
	plan := forcedPreflightPlan(t, now)
	buyManager := &settledTxManager{journal: &durableJournal{}}
	sellManager := &settledTxManager{journal: &durableJournal{}}
	driver := &livecanary.SwapDriver{
		Bindings: map[market.MarketID]livecanary.SwapBinding{
			"market-a": {
				Account: "account",
				Validator: &fixedOutputValidator{
					now: now, output: big.NewInt(4_052_168_781),
				},
				TxManager: buyManager,
			},
			"market-b": {
				Account: "account",
				Validator: &fixedOutputValidator{
					now: now, output: big.NewInt(999_999),
				},
				TxManager: sellManager,
			},
		},
		TokenDecimals: map[market.TokenID]uint8{
			"quote-a": 6, "base-a": 9,
			"base-b": 18, "quote-b": 6,
		},
		BridgePrecision: 8,
		Clock:           func() time.Time { return now },
	}
	if err := driver.Preflight(
		context.Background(),
		"operation",
		plan,
	); err != nil {
		t.Fatal(err)
	}
	if buyManager.simulations != 1 || sellManager.simulations != 1 ||
		buyManager.broadcasts != 0 || sellManager.broadcasts != 0 {
		t.Fatalf(
			"forced preflight simulations=%d/%d broadcasts=%d/%d",
			buyManager.simulations,
			sellManager.simulations,
			buyManager.broadcasts,
			sellManager.broadcasts,
		)
	}
}

func TestForcedCanaryKeepsDestinationExitDespiteBetterReturn(t *testing.T) {
	now := time.Now().UTC()
	plan := forcedPreflightPlan(t, now)
	destinationManager := &settledTxManager{}
	returnEstimator := &fixedQuoteEstimator{output: big.NewInt(980_000)}
	cost, _ := market.NewAssetQuantity("quote", big.NewRat(1, 100))
	driver := &livecanary.SwapDriver{
		Bindings: map[market.MarketID]livecanary.SwapBinding{
			"market-a": {
				Account:   "account",
				Validator: &fixedOutputValidator{now: now, output: big.NewInt(980_000)},
				Estimator: returnEstimator,
				TxManager: &settledTxManager{},
			},
			"market-b": {
				Account:   "account",
				Validator: &fixedOutputValidator{now: now, output: big.NewInt(950_000)},
				TxManager: destinationManager,
			},
		},
		TokenDecimals: map[market.TokenID]uint8{
			"quote-a": 6, "base-a": 9,
			"base-b": 18, "quote-b": 6,
		},
		BridgePrecision: 8, QuoteAsset: "quote",
		MinimumNet: big.NewRat(1, 2), ReturnMargin: new(big.Rat),
		ExitCosts: fixedExitCostSource{
			costs: map[execution.SequentialExitRoute]market.AssetQuantity{
				execution.ExitSellAtDestination: cost,
				execution.ExitReturnToOrigin:    cost,
			},
		},
		Clock: func() time.Time { return now },
	}
	bridged, _ := market.NewTokenAmount(
		"base-b",
		big.NewInt(4_000_000_000_000_000_000),
	)
	decision, err := driver.SelectExit(
		context.Background(),
		"operation",
		plan,
		bridged,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Route != execution.ExitSellAtDestination ||
		decision.DestinationQualified ||
		decision.Evidence !=
			"fresh_destination_build+simulation+forced_canary_destination" {
		t.Fatalf("forced destination decision=%+v", decision)
	}
	if returnEstimator.calls.Load() != 0 {
		t.Fatalf(
			"forced destination requested %d unnecessary return quotes",
			returnEstimator.calls.Load(),
		)
	}
}

func forcedPreflightPlan(
	t *testing.T,
	now time.Time,
) execution.SequentialPlan {
	t.Helper()
	original := preflightPlan(t, now)
	forced, err := livecanary.ForceOpportunity(
		[]arbitrage.Opportunity{original.Opportunity},
		original.Opportunity.Direction,
	)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := execution.NewSequentialPlan(
		original.ID,
		forced,
		original.InitialInput,
		original.Stages[0].SourceChain,
		original.Stages[2].SourceChain,
		original.CreatedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func TestPrefundedParallelPreflightUsesFixedIndependentInputsAndJointPnL(t *testing.T) {
	now := time.Now().UTC()
	plan := parallelPreflightPlan(t, now)
	buyManager := &settledTxManager{actualOutput: mustLiveAmount(t, "base-a", "2010000000")}
	sellManager := &settledTxManager{actualOutput: mustLiveAmount(t, "quote-b", "501500000")}
	buyValidator := &fixedOutputValidator{now: now, output: big.NewInt(2_010_000_000)}
	sellValidator := &fixedOutputValidator{now: now, output: big.NewInt(501_500_000)}
	driver := &livecanary.SwapDriver{
		Bindings: map[market.MarketID]livecanary.SwapBinding{
			"market-a": parallelBinding(buyValidator, buyManager),
			"market-b": parallelBinding(sellValidator, sellManager),
		},
		TokenDecimals: plan.TokenDecimals,
		BaseAsset:     "base", QuoteAsset: "usdc", MinimumNet: big.NewRat(1, 1),
		Clock: func() time.Time { return now },
	}
	if err := driver.Preflight(context.Background(), "operation-parallel", plan); err != nil {
		t.Fatal(err)
	}
	if len(sellValidator.inputs) != 1 {
		t.Fatalf("sell builds = %d", len(sellValidator.inputs))
	}
	if got, want := sellValidator.inputs[0].Units().String(), "2000000000000000000"; got != want {
		t.Fatalf("fixed sell input = %s, want %s", got, want)
	}
	driver.DiscardPreflight("operation-parallel")
}

func TestPrefundedParallelExpiredSignatureIsRejectedWithoutFalseRevert(t *testing.T) {
	now := time.Now().UTC()
	plan := parallelPreflightPlan(t, now)
	journal := &durableJournal{}
	buyOutput := mustLiveAmount(t, "base-a", "2010000000")
	sellOutput := mustLiveAmount(t, "quote-b", "501500000")
	buyManager := &settledTxManager{
		journal: journal, actualOutput: buyOutput,
	}
	sellManager := &settledTxManager{
		journal: journal, actualOutput: sellOutput,
		reconciliation: &execution.Settlement{
			Technical:  execution.StateBroadcastRejected,
			Economic:   execution.EconomicReserved,
			ObservedAt: now,
			Evidence:   "blockhash_expired_without_signature",
		},
	}
	driver := &livecanary.SwapDriver{
		Bindings: map[market.MarketID]livecanary.SwapBinding{
			"market-a": parallelBinding(
				&fixedOutputValidator{now: now, output: buyOutput.Units()},
				buyManager,
			),
			"market-b": parallelBinding(
				&fixedOutputValidator{now: now, output: sellOutput.Units()},
				sellManager,
			),
		},
		TokenDecimals: plan.TokenDecimals,
		BaseAsset:     "base",
		QuoteAsset:    "usdc",
		MinimumNet:    big.NewRat(1, 1),
		Clock:         func() time.Time { return now },
	}
	const operation = execution.OperationID("operation-expired-sell")
	if err := driver.Preflight(context.Background(), operation, plan); err != nil {
		t.Fatal(err)
	}
	settlements, err := driver.ExecuteParallelSwaps(
		context.Background(), operation, plan, journal,
	)
	if err == nil {
		t.Fatal("expired parallel signature unexpectedly settled")
	}
	if executionport.ErrorDisposition(err) != executionport.DispositionRejected {
		t.Fatalf("disposition=%s error=%v", executionport.ErrorDisposition(err), err)
	}
	if len(settlements) != 1 || settlements[0].Request.Stage.Ordinal != 1 {
		t.Fatalf("settlements=%+v", settlements)
	}
	markedRejected := false
	for _, status := range journal.marked {
		if status == "confirmed_revert" {
			t.Fatalf("expired signature was marked as revert: %v", journal.marked)
		}
		if status == "rejected" {
			markedRejected = true
		}
	}
	if !markedRejected {
		t.Fatalf("expired signature was not marked rejected: %v", journal.marked)
	}
}

func TestLocalTriggeredExecutionConfirmsLocalBeforeFreshRemoteBroadcast(t *testing.T) {
	now := time.Now().UTC()
	plan := parallelPreflightPlan(t, now)
	plan.Opportunity.HasTrigger = true
	plan.Opportunity.Trigger = arbitrage.TriggerMetadata{Market: "market-a/hop/0", At: now}
	candidate := plan.Opportunity.Candidates[0]
	candidate.BuyQuote.Market = "market-a"
	candidate.BuyQuote.AmountIn = plan.InitialInput
	sellInput, err := plan.ParallelSellInput()
	if err != nil {
		t.Fatal(err)
	}
	candidate.SellQuote.Market = "market-b"
	candidate.SellQuote.AmountIn = sellInput
	plan.Opportunity.Candidates[0] = candidate
	journal := &durableJournal{}
	localManager := &settledTxManager{journal: journal, actualOutput: mustLiveAmount(t, "base-a", "2010000000")}
	remoteManager := &settledTxManager{journal: journal, actualOutput: mustLiveAmount(t, "quote-b", "501500000")}
	localValidator := &fixedOutputValidator{now: now, output: big.NewInt(2_010_000_000)}
	retainedRemote := &fixedOutputValidator{now: now, output: big.NewInt(501_500_000)}
	freshRemote := &fixedOutputValidator{now: now, output: big.NewInt(501_500_000)}
	localBinding := parallelBinding(localValidator, localManager)
	localBinding.TrustValidatedQuote = true
	remoteBinding := parallelBinding(retainedRemote, remoteManager)
	remoteBinding.RecoveryValidator = freshRemote
	driver := &livecanary.SwapDriver{Bindings: map[market.MarketID]livecanary.SwapBinding{
		"market-a": localBinding, "market-b": remoteBinding,
	}, TokenDecimals: plan.TokenDecimals, BaseAsset: "base", QuoteAsset: "usdc", Clock: func() time.Time { return now }}
	if err := driver.Preflight(context.Background(), "operation-staged", plan); err != nil {
		t.Fatal(err)
	}
	if retainedRemote.calls != 0 || freshRemote.calls != 0 || localManager.simulations != 0 {
		t.Fatalf("hot preflight touched remote or simulated local: retained=%d fresh=%d local_sim=%d", retainedRemote.calls, freshRemote.calls, localManager.simulations)
	}
	settlements, err := driver.ExecuteTriggeredSwaps(context.Background(), "operation-staged", plan, journal)
	if err != nil {
		t.Fatal(err)
	}
	if len(settlements) != 2 || localManager.broadcasts != 1 || remoteManager.broadcasts != 1 || freshRemote.calls != 1 || retainedRemote.calls != 0 {
		t.Fatalf("settlements=%d broadcasts=%d/%d validators=%d/%d", len(settlements), localManager.broadcasts, remoteManager.broadcasts, freshRemote.calls, retainedRemote.calls)
	}
}

func TestTriggerFirstDynamicFloorReservesExactlyOneQuarterOfExpectedNet(t *testing.T) {
	for _, netText := range []string{"0.10", "1", "5", "20", "100"} {
		t.Run(netText, func(t *testing.T) {
			now := time.Now().UTC()
			plan := parallelPreflightPlan(t, now)
			plan.Policy = execution.PolicyPrefundedTriggerFirst
			plan.Opportunity.HasTrigger = true
			plan.Opportunity.Trigger = arbitrage.TriggerMetadata{Market: "market-a/hop/0", At: now}
			candidate := plan.Opportunity.Candidates[0]
			candidate.BuyQuote.Market, candidate.BuyQuote.AmountIn = "market-a", plan.InitialInput
			candidate.BuyQuote.AmountOut = mustLiveAmount(t, "base-a", "1000000000000")
			sellInput, err := plan.ParallelSellInput()
			if err != nil {
				t.Fatal(err)
			}
			candidate.SellQuote.Market, candidate.SellQuote.AmountIn = "market-b", sellInput
			candidate.NetPnL = mustLiveAsset(t, "usdc", netText)
			valuation, err := arbitrage.NewValuationSnapshot(1, "base", "usdc", big.NewRat(1, 1), 1, now)
			if err != nil {
				t.Fatal(err)
			}
			candidate.Valuation = &valuation
			plan.Opportunity.Candidates[0] = candidate
			localValidator := &fixedOutputValidator{now: now, output: candidate.BuyQuote.AmountOut.Units()}
			localBinding := parallelBinding(localValidator, &settledTxManager{})
			localBinding.TrustValidatedQuote = true
			remoteBinding := parallelBinding(&fixedOutputValidator{now: now, output: candidate.SellQuote.AmountOut.Units()}, &settledTxManager{})
			remoteBinding.TrustValidatedQuote = true
			remoteBinding.RecoveryValidator = &fixedOutputValidator{now: now, output: candidate.SellQuote.AmountOut.Units()}
			driver := &livecanary.SwapDriver{Bindings: map[market.MarketID]livecanary.SwapBinding{
				"market-a": localBinding, "market-b": remoteBinding}, TokenDecimals: plan.TokenDecimals,
				BaseAsset: "base", QuoteAsset: "usdc", DynamicSlippage: livecanary.DynamicSlippagePolicy{
					Enabled: true, MaxBPS: 500, HeadroomBPS: 2_500}, Clock: func() time.Time { return now }}
			operationID := execution.OperationID("trigger-first-" + netText)
			if err := driver.Preflight(context.Background(), operationID, plan); err != nil {
				t.Fatal(err)
			}
			if len(localValidator.slippages) != 1 || localValidator.slippages[0] == nil {
				t.Fatalf("missing trigger-first slippage evidence: %+v", localValidator.slippages)
			}
			constraint := localValidator.slippages[0]
			net, _ := new(big.Rat).SetString(netText)
			reserved := new(big.Rat).Quo(new(big.Rat).Set(net), big.NewRat(4, 1))
			budget := new(big.Rat).Mul(new(big.Rat).Set(net), big.NewRat(3, 4))
			if constraint.Evidence["reserved_headroom"] != reserved.RatString() ||
				constraint.Evidence["consumable_budget"] != budget.RatString() {
				t.Fatalf("unexpected 75/25 evidence: %+v", constraint.Evidence)
			}
			if constraint.BPS > 500 || constraint.Evidence["max_bps"] != "500" {
				t.Fatalf("trigger-first percentage cap was not enforced: %+v", constraint)
			}
			if netText == "100" {
				if constraint.BPS != 500 || constraint.Evidence["limiting_bound"] != "percentage_cap" {
					t.Fatalf("percentage cap did not override the wider economic budget: %+v", constraint)
				}
			} else if constraint.Evidence["limiting_bound"] != "economic_budget" {
				t.Fatalf("economic budget should be the tighter bound: %+v", constraint)
			}
			driver.DiscardPreflight(operationID)
		})
	}
}

func TestForcedTriggerFirstCanaryUsesConfiguredFixedSlippage(t *testing.T) {
	now := time.Now().UTC()
	plan := parallelPreflightPlan(t, now)
	plan.Policy = execution.PolicyPrefundedTriggerFirst
	plan.Opportunity.Reasons = []string{"forced_canary_direction"}
	plan.Opportunity.HasTrigger = true
	plan.Opportunity.Trigger = arbitrage.TriggerMetadata{Market: "market-a/hop/0", At: now}
	candidate := plan.Opportunity.Candidates[0]
	candidate.BuyQuote.Market, candidate.BuyQuote.AmountIn = "market-a", plan.InitialInput
	sellInput, err := plan.ParallelSellInput()
	if err != nil {
		t.Fatal(err)
	}
	candidate.SellQuote.Market, candidate.SellQuote.AmountIn = "market-b", sellInput
	candidate.NetPnL = mustLiveAsset(t, "usdc", "-0.10")
	plan.Opportunity.Candidates[0] = candidate
	localValidator := &fixedFloorOutputValidator{fixedOutputValidator: fixedOutputValidator{
		now: now, output: candidate.BuyQuote.AmountOut.Units(),
	}, bps: 100}
	remoteValidator := &fixedOutputValidator{now: now, output: candidate.SellQuote.AmountOut.Units()}
	journal := &durableJournal{}
	localManager := &settledTxManager{journal: journal, actualOutput: candidate.BuyQuote.AmountOut}
	remoteManager := &settledTxManager{journal: journal, actualOutput: candidate.SellQuote.AmountOut}
	localBinding := parallelBinding(localValidator, localManager)
	localBinding.TrustValidatedQuote = true
	remoteBinding := parallelBinding(remoteValidator, remoteManager)
	remoteBinding.TrustValidatedQuote = true
	remoteBinding.RecoveryValidator = remoteValidator
	driver := &livecanary.SwapDriver{Bindings: map[market.MarketID]livecanary.SwapBinding{
		"market-a": localBinding, "market-b": remoteBinding,
	}, TokenDecimals: plan.TokenDecimals, BaseAsset: "base", QuoteAsset: "usdc",
		DynamicSlippage: livecanary.DynamicSlippagePolicy{
			Enabled: true, MaxBPS: 500, HeadroomBPS: 2_500,
		}, Clock: func() time.Time { return now }}
	if err := driver.Preflight(context.Background(), "forced-trigger-first", plan); err != nil {
		t.Fatal(err)
	}
	if len(localValidator.slippages) != 1 || localValidator.slippages[0] != nil {
		t.Fatalf("forced trigger-first used dynamic slippage: %+v", localValidator.slippages)
	}
	settlements, err := driver.ExecuteTriggeredSwaps(
		context.Background(), "forced-trigger-first", plan, journal,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(settlements) != 2 || localManager.broadcasts != 1 || remoteManager.broadcasts != 1 {
		t.Fatalf("forced trigger-first did not execute both swaps: settlements=%d broadcasts=%d/%d",
			len(settlements), localManager.broadcasts, remoteManager.broadcasts)
	}
	if len(journal.decisions) != 1 ||
		journal.decisions[0].Kind != executionport.TriggerFirstDecisionForcedFixed ||
		journal.decisions[0].ReservedHeadroom != "" ||
		journal.decisions[0].ConsumableBudget != "" {
		t.Fatalf("forced trigger-first decision evidence=%+v", journal.decisions)
	}
}

func TestTriggerFirstUsesBSCStyleTriggerAsFirstConfirmedLeg(t *testing.T) {
	now := time.Now().UTC()
	plan := parallelPreflightPlan(t, now)
	plan.Policy = execution.PolicyPrefundedTriggerFirst
	plan.Opportunity.HasTrigger = true
	plan.Opportunity.Trigger = arbitrage.TriggerMetadata{Market: "market-b/pool/1", At: now}
	candidate := plan.Opportunity.Candidates[0]
	sellInput, err := plan.ParallelSellInput()
	if err != nil {
		t.Fatal(err)
	}
	candidate.BuyQuote.Market, candidate.BuyQuote.AmountIn = "market-a", plan.InitialInput
	candidate.SellQuote.Market, candidate.SellQuote.AmountIn = "market-b", sellInput
	candidate.NetPnL = mustLiveAsset(t, "usdc", "1.5")
	valuation, _ := arbitrage.NewValuationSnapshot(1, "base", "usdc", big.NewRat(1, 1), 1, now)
	candidate.Valuation = &valuation
	plan.Opportunity.Candidates[0] = candidate
	journal := &durableJournal{}
	buyManager := &settledTxManager{journal: journal, actualOutput: candidate.BuyQuote.AmountOut}
	sellManager := &settledTxManager{journal: journal, actualOutput: candidate.SellQuote.AmountOut}
	buyValidator := &fixedOutputValidator{now: now, output: candidate.BuyQuote.AmountOut.Units()}
	freshBuy := &fixedOutputValidator{now: now, output: candidate.BuyQuote.AmountOut.Units()}
	sellValidator := &fixedOutputValidator{now: now, output: candidate.SellQuote.AmountOut.Units()}
	buyBinding := parallelBinding(buyValidator, buyManager)
	buyBinding.TrustValidatedQuote, buyBinding.RecoveryValidator = true, freshBuy
	sellBinding := parallelBinding(sellValidator, sellManager)
	sellBinding.TrustValidatedQuote, sellBinding.RecoveryValidator = true, sellValidator
	driver := &livecanary.SwapDriver{Bindings: map[market.MarketID]livecanary.SwapBinding{
		"market-a": buyBinding, "market-b": sellBinding}, TokenDecimals: plan.TokenDecimals,
		BaseAsset: "base", QuoteAsset: "usdc", DynamicSlippage: livecanary.DynamicSlippagePolicy{
			Enabled: true, HeadroomBPS: 2_500}, Clock: func() time.Time { return now }}
	operation := execution.OperationID("trigger-first-b")
	if err := driver.Preflight(context.Background(), operation, plan); err != nil {
		t.Fatal(err)
	}
	if buyValidator.calls != 0 || sellValidator.calls != 1 || buyManager.simulations != 0 || sellManager.simulations != 0 {
		t.Fatalf("preflight crossed the hot boundary: buy=%d sell=%d simulations=%d/%d",
			buyValidator.calls, sellValidator.calls, buyManager.simulations, sellManager.simulations)
	}
	settlements, err := driver.ExecuteTriggeredSwaps(context.Background(), operation, plan, journal)
	if err != nil {
		t.Fatal(err)
	}
	if len(settlements) != 2 || sellManager.broadcasts != 1 || buyManager.broadcasts != 1 ||
		freshBuy.calls != 1 || len(journal.decisions) != 1 || journal.decisions[0].Ordinal != 2 {
		t.Fatalf("unexpected trigger-first execution: settlements=%d broadcasts=%d/%d fresh=%d decisions=%+v",
			len(settlements), sellManager.broadcasts, buyManager.broadcasts, freshBuy.calls, journal.decisions)
	}
}

func TestLocalTriggeredSellValidatesBothLegsBeforeSimultaneousBroadcast(t *testing.T) {
	now := time.Now().UTC()
	plan := parallelPreflightPlan(t, now)
	plan.Opportunity.HasTrigger = true
	plan.Opportunity.Trigger = arbitrage.TriggerMetadata{Market: "market-b/hop/0", At: now}
	candidate := plan.Opportunity.Candidates[0]
	candidate.BuyQuote.Market = "market-a"
	candidate.BuyQuote.AmountIn = plan.InitialInput
	candidate.BuyQuote.AmountOut = mustLiveAmount(t, "base-a", "508822000000")
	candidate.SellQuote.Market = "market-b"
	candidate.SellQuote.AmountIn = mustLiveAmount(t, "base-b", "508203000000000000000")
	candidate.SellQuote.AmountOut = mustLiveAmount(t, "quote-b", "499713868")
	candidate.GrossPnL = mustLiveAsset(t, "usdc", "0.322")
	candidate.NetPnL = mustLiveAsset(t, "usdc", "0.172")
	valuation, err := arbitrage.NewValuationSnapshot(1, "base", "usdc", big.NewRat(983, 1000), 1, now)
	if err != nil {
		t.Fatal(err)
	}
	candidate.Valuation = &valuation
	candidate.Cost = arbitrage.CostSnapshot{ID: "complete-flow", Amount: mustLiveAsset(t, "usdc", "0.15"), CapturedAt: now}
	plan.Opportunity.Candidates[0] = candidate

	journal := &durableJournal{}
	remoteManager := &settledTxManager{journal: journal, actualOutput: candidate.BuyQuote.AmountOut}
	localManager := &settledTxManager{journal: journal, actualOutput: candidate.SellQuote.AmountOut}
	retainedRemote := &fixedOutputValidator{now: now, output: candidate.BuyQuote.AmountOut.Units()}
	freshRemote := &fixedOutputValidator{now: now, output: candidate.BuyQuote.AmountOut.Units()}
	localValidator := &fixedOutputValidator{now: now, output: candidate.SellQuote.AmountOut.Units()}
	remoteBinding := parallelBinding(retainedRemote, remoteManager)
	remoteBinding.RecoveryValidator = freshRemote
	localBinding := parallelBinding(localValidator, localManager)
	localBinding.TrustValidatedQuote = true
	driver := &livecanary.SwapDriver{
		Bindings:      map[market.MarketID]livecanary.SwapBinding{"market-a": remoteBinding, "market-b": localBinding},
		TokenDecimals: plan.TokenDecimals, BaseAsset: "base", QuoteAsset: "usdc", MinimumNet: big.NewRat(1, 10),
		DynamicSlippage: livecanary.DynamicSlippagePolicy{Enabled: true, MaxBPS: 100},
		Clock:           func() time.Time { return now },
	}
	if err = driver.Preflight(context.Background(), "operation-protected-sell", plan); err != nil {
		t.Fatalf("complete prefunded economics rejected the local SELL path: %v", err)
	}
	if retainedRemote.calls != 0 || freshRemote.calls != 1 || localValidator.calls != 1 ||
		remoteManager.simulations != 1 || localManager.simulations != 1 {
		t.Fatalf("protected preflight did not validate both legs: retained=%d fresh=%d local=%d simulations=%d/%d",
			retainedRemote.calls, freshRemote.calls, localValidator.calls, remoteManager.simulations, localManager.simulations)
	}
	settlements, err := driver.ExecuteParallelSwaps(context.Background(), "operation-protected-sell", plan, journal)
	if err != nil {
		t.Fatal(err)
	}
	if len(settlements) != 2 || localManager.broadcasts != 1 || remoteManager.broadcasts != 1 || len(freshRemote.slippages) != 1 ||
		freshRemote.slippages[0] == nil || freshRemote.slippages[0].Reason != "dynamic_prefunded_buy_budget" {
		t.Fatalf("unexpected staged SELL execution: settlements=%d broadcasts=%d/%d slippage=%+v",
			len(settlements), localManager.broadcasts, remoteManager.broadcasts, freshRemote.slippages)
	}
}

func TestLocalTriggeredSellRejectsFreshRemoteDeteriorationBeforeEitherBroadcast(t *testing.T) {
	now := time.Now().UTC()
	plan := parallelPreflightPlan(t, now)
	plan.Opportunity.HasTrigger = true
	plan.Opportunity.Trigger = arbitrage.TriggerMetadata{Market: "market-b/hop/0", At: now}
	candidate := plan.Opportunity.Candidates[0]
	candidate.BuyQuote.Market = "market-a"
	candidate.BuyQuote.AmountIn = plan.InitialInput
	candidate.BuyQuote.AmountOut = mustLiveAmount(t, "base-a", "485969070000")
	candidate.SellQuote.Market = "market-b"
	candidate.SellQuote.AmountIn = mustLiveAmount(t, "base-b", "486934000000000000000")
	candidate.SellQuote.AmountOut = mustLiveAmount(t, "quote-b", "502758575")
	candidate.GrossPnL = mustLiveAsset(t, "usdc", "2.908575")
	candidate.NetPnL = mustLiveAsset(t, "usdc", "2.758575")
	valuation, err := arbitrage.NewValuationSnapshot(1, "base", "usdc", big.NewRat(1035, 1000), 1, now)
	if err != nil {
		t.Fatal(err)
	}
	candidate.Valuation = &valuation
	candidate.Cost = arbitrage.CostSnapshot{ID: "complete-flow", Amount: mustLiveAsset(t, "usdc", "0.15"), CapturedAt: now}
	plan.Opportunity.Candidates[0] = candidate

	remoteManager := &settledTxManager{actualOutput: mustLiveAmount(t, "base-a", "482718000000")}
	localManager := &settledTxManager{actualOutput: mustLiveAmount(t, "quote-b", "503442000")}
	retainedRemote := &fixedOutputValidator{now: now, output: candidate.BuyQuote.AmountOut.Units()}
	freshRemote := &fixedOutputValidator{now: now, output: big.NewInt(482_718_000_000)}
	remoteBinding := parallelBinding(retainedRemote, remoteManager)
	remoteBinding.RecoveryValidator = freshRemote
	localBinding := parallelBinding(&fixedOutputValidator{now: now, output: big.NewInt(503_442_000)}, localManager)
	localBinding.TrustValidatedQuote = true
	driver := &livecanary.SwapDriver{
		Bindings: map[market.MarketID]livecanary.SwapBinding{
			"market-a": remoteBinding,
			"market-b": localBinding,
		},
		TokenDecimals: plan.TokenDecimals, BaseAsset: "base", QuoteAsset: "usdc",
		MinimumNet: big.NewRat(1, 10), DynamicSlippage: livecanary.DynamicSlippagePolicy{Enabled: true, MaxBPS: 100},
		Clock: func() time.Time { return now },
	}
	err = driver.Preflight(context.Background(), "operation-fresh-route-deteriorated", plan)
	if err == nil {
		t.Fatal("deteriorated fresh remote route unexpectedly qualified")
	}
	if retainedRemote.calls != 0 || freshRemote.calls != 1 || remoteManager.broadcasts != 0 || localManager.broadcasts != 0 {
		t.Fatalf("deteriorated route crossed the safe boundary: retained=%d fresh=%d broadcasts=%d/%d",
			retainedRemote.calls, freshRemote.calls, remoteManager.broadcasts, localManager.broadcasts)
	}
}

func TestLocalTriggeredSellRejectsInvalidRemoteBuyBudgetBeforeLocalPreparation(t *testing.T) {
	now := time.Now().UTC()
	plan := parallelPreflightPlan(t, now)
	plan.Opportunity.HasTrigger = true
	plan.Opportunity.Trigger = arbitrage.TriggerMetadata{Market: "market-b/hop/0", At: now}
	candidate := plan.Opportunity.Candidates[0]
	candidate.BuyQuote.AmountIn = plan.InitialInput
	candidate.NetPnL = mustLiveAsset(t, "usdc", "0.05")
	valuation, err := arbitrage.NewValuationSnapshot(1, "base", "usdc", big.NewRat(1, 1), 1, now)
	if err != nil {
		t.Fatal(err)
	}
	candidate.Valuation = &valuation
	plan.Opportunity.Candidates[0] = candidate
	localValidator := &fixedOutputValidator{now: now, output: big.NewInt(1)}
	localManager := &settledTxManager{}
	localBinding := parallelBinding(localValidator, localManager)
	localBinding.TrustValidatedQuote = true
	remoteBinding := parallelBinding(&fixedOutputValidator{now: now, output: big.NewInt(1)}, &settledTxManager{})
	remoteBinding.RecoveryValidator = &fixedOutputValidator{now: now, output: big.NewInt(1)}
	driver := &livecanary.SwapDriver{
		Bindings: map[market.MarketID]livecanary.SwapBinding{
			"market-a": remoteBinding,
			"market-b": localBinding,
		},
		TokenDecimals: plan.TokenDecimals, BaseAsset: "base", QuoteAsset: "usdc", MinimumNet: big.NewRat(1, 10),
		DynamicSlippage: livecanary.DynamicSlippagePolicy{Enabled: true, MaxBPS: 100}, Clock: func() time.Time { return now },
	}
	err = driver.Preflight(context.Background(), "operation-invalid-budget", plan)
	if err == nil || localManager.broadcasts != 0 {
		t.Fatalf("invalid remote budget crossed the broadcast boundary: error=%v broadcasts=%d", err, localManager.broadcasts)
	}
}

func TestLocalTriggeredFailureNeverBroadcastsRemoteLeg(t *testing.T) {
	now := time.Now().UTC()
	plan := parallelPreflightPlan(t, now)
	plan.Opportunity.HasTrigger = true
	plan.Opportunity.Trigger = arbitrage.TriggerMetadata{Market: "market-a/hop/0", At: now}
	candidate := plan.Opportunity.Candidates[0]
	candidate.BuyQuote.Market, candidate.BuyQuote.AmountIn = "market-a", plan.InitialInput
	sellInput, err := plan.ParallelSellInput()
	if err != nil {
		t.Fatal(err)
	}
	candidate.SellQuote.Market, candidate.SellQuote.AmountIn = "market-b", sellInput
	plan.Opportunity.Candidates[0] = candidate
	journal := &durableJournal{}
	localManager := &settledTxManager{journal: journal, actualOutput: mustLiveAmount(t, "base-a", "2010000000"), reconciliation: &execution.Settlement{
		Technical: execution.StateConfirmedRevert, Economic: execution.EconomicReleased, ObservedAt: now, Evidence: "test-revert",
	}}
	remoteManager := &settledTxManager{journal: journal, actualOutput: mustLiveAmount(t, "quote-b", "501500000")}
	localBinding := parallelBinding(&fixedOutputValidator{now: now, output: big.NewInt(2_010_000_000)}, localManager)
	localBinding.TrustValidatedQuote = true
	remoteBinding := parallelBinding(&fixedOutputValidator{now: now, output: big.NewInt(501_500_000)}, remoteManager)
	remoteBinding.RecoveryValidator = &fixedOutputValidator{now: now, output: big.NewInt(501_500_000)}
	driver := &livecanary.SwapDriver{Bindings: map[market.MarketID]livecanary.SwapBinding{"market-a": localBinding, "market-b": remoteBinding},
		TokenDecimals: plan.TokenDecimals, BaseAsset: "base", QuoteAsset: "usdc", Clock: func() time.Time { return now }}
	if err = driver.Preflight(context.Background(), "operation-local-revert", plan); err != nil {
		t.Fatal(err)
	}
	if _, err = driver.ExecuteTriggeredSwaps(context.Background(), "operation-local-revert", plan, journal); err == nil {
		t.Fatal("local revert unexpectedly completed")
	}
	if localManager.broadcasts != 1 || remoteManager.broadcasts != 0 {
		t.Fatalf("broadcasts local=%d remote=%d", localManager.broadcasts, remoteManager.broadcasts)
	}
}

func TestRemoteTriggeredPreflightSimulatesBothLegs(t *testing.T) {
	now := time.Now().UTC()
	plan := parallelPreflightPlan(t, now)
	plan.Opportunity.HasTrigger = true
	plan.Opportunity.Trigger = arbitrage.TriggerMetadata{Market: "market-b", At: now}
	candidate := plan.Opportunity.Candidates[0]
	candidate.BuyQuote.Market, candidate.BuyQuote.AmountIn = "market-a", plan.InitialInput
	sellInput, err := plan.ParallelSellInput()
	if err != nil {
		t.Fatal(err)
	}
	candidate.SellQuote.Market, candidate.SellQuote.AmountIn = "market-b", sellInput
	plan.Opportunity.Candidates[0] = candidate
	localManager := &settledTxManager{actualOutput: mustLiveAmount(t, "base-a", "2010000000")}
	remoteManager := &settledTxManager{actualOutput: mustLiveAmount(t, "quote-b", "501500000")}
	localBinding := parallelBinding(&fixedOutputValidator{now: now, output: big.NewInt(2_010_000_000)}, localManager)
	localBinding.TrustValidatedQuote = true
	driver := &livecanary.SwapDriver{Bindings: map[market.MarketID]livecanary.SwapBinding{
		"market-a": localBinding,
		"market-b": parallelBinding(&fixedOutputValidator{now: now, output: big.NewInt(501_500_000)}, remoteManager),
	}, TokenDecimals: plan.TokenDecimals, BaseAsset: "base", QuoteAsset: "usdc", Clock: func() time.Time { return now }}
	if err = driver.Preflight(context.Background(), "operation-remote-trigger", plan); err != nil {
		t.Fatal(err)
	}
	if localManager.simulations != 1 || remoteManager.simulations != 1 {
		t.Fatalf("simulations local=%d remote=%d", localManager.simulations, remoteManager.simulations)
	}
	driver.DiscardPreflight("operation-remote-trigger")
}

func TestPrefundedParallelPreflightRejectsCombinedSimulationDeterioration(t *testing.T) {
	now := time.Now().UTC()
	plan := parallelPreflightPlan(t, now)
	buyManager := &settledTxManager{actualOutput: mustLiveAmount(t, "base-a", "1990000000")}
	sellManager := &settledTxManager{actualOutput: mustLiveAmount(t, "quote-b", "499500000")}
	driver := &livecanary.SwapDriver{
		Bindings: map[market.MarketID]livecanary.SwapBinding{
			"market-a": parallelBinding(
				&fixedOutputValidator{now: now, output: big.NewInt(1_990_000_000)}, buyManager,
			),
			"market-b": parallelBinding(
				&fixedOutputValidator{now: now, output: big.NewInt(499_500_000)}, sellManager,
			),
		},
		TokenDecimals: plan.TokenDecimals,
		BaseAsset:     "base", QuoteAsset: "usdc", MinimumNet: big.NewRat(1, 1),
		Clock: func() time.Time { return now },
	}
	err := driver.Preflight(context.Background(), "operation-negative", plan)
	if err == nil || !strings.Contains(err.Error(), "joint simulated PnL is below threshold") {
		t.Fatalf("preflight error = %v", err)
	}
}

func TestForcedPrefundedParallelCannotBypassMaximumCompleteFlowCost(t *testing.T) {
	now := time.Now().UTC()
	plan := parallelPreflightPlan(t, now)
	plan.Opportunity.Reasons = []string{"forced_canary_direction"}
	driver := &livecanary.SwapDriver{
		Bindings: map[market.MarketID]livecanary.SwapBinding{
			"market-a": parallelBinding(&fixedOutputValidator{now: now, output: big.NewInt(2_010_000_000)},
				&settledTxManager{actualOutput: mustLiveAmount(t, "base-a", "2010000000")}),
			"market-b": parallelBinding(&fixedOutputValidator{now: now, output: big.NewInt(501_500_000)},
				&settledTxManager{actualOutput: mustLiveAmount(t, "quote-b", "501500000")}),
		},
		TokenDecimals: plan.TokenDecimals, BaseAsset: "base", QuoteAsset: "usdc",
		MinimumNet: big.NewRat(100, 1), MaximumCost: big.NewRat(1, 2), Clock: func() time.Time { return now },
	}
	err := driver.Preflight(context.Background(), "operation-forced-cost-cap", plan)
	if err == nil || !strings.Contains(err.Error(), "complete-flow cost exceeds maximum") {
		t.Fatalf("preflight error = %v", err)
	}
}

func parallelBinding(
	validator executionport.Validator,
	manager chainport.TxManager,
) livecanary.SwapBinding {
	return livecanary.SwapBinding{
		Account: "account", Validator: validator, TxManager: manager,
		BalanceSnapshot: func(market.TokenID) (*big.Int, uint64, error) {
			return new(big.Int), 1, nil
		},
	}
}

func TestHybridStagedExecutionIsSelectedOnlyForLocalTrigger(t *testing.T) {
	plan := parallelPreflightPlan(t, time.Now().UTC())
	driver := &livecanary.SwapDriver{Bindings: map[market.MarketID]livecanary.SwapBinding{
		"market-a": {TrustValidatedQuote: true},
		"market-b": {},
	}}
	plan.Opportunity.HasTrigger = true
	plan.Opportunity.Trigger = arbitrage.TriggerMetadata{Market: "market-a/hop/0", At: time.Now().UTC()}
	if !driver.StagedFor(plan) {
		t.Fatal("local trigger did not select staged execution")
	}
	plan.Opportunity.Trigger.Market = "market-b"
	if driver.StagedFor(plan) {
		t.Fatal("remote trigger selected staged execution")
	}
	plan.Opportunity.Trigger.Market = "market-b/hop/0"
	driver.Bindings["market-b"] = livecanary.SwapBinding{TrustValidatedQuote: true}
	if driver.StagedFor(plan) {
		t.Fatal("local SELL trigger selected staged execution")
	}
}

func parallelPreflightPlan(t *testing.T, now time.Time) execution.SequentialPlan {
	t.Helper()
	original := preflightPlan(t, now)
	candidate := original.Opportunity.Candidates[0]
	candidate.Input, _ = market.NewAssetQuantity("usdc", big.NewRat(750, 1))
	candidate.Cost = arbitrage.CostSnapshot{
		ID: "complete-flow", Amount: mustLiveAsset(t, "usdc", "1"), CapturedAt: now,
	}
	original.Opportunity.Candidates[0] = candidate
	initial := mustLiveAmount(t, "quote-a", "500000000")
	plan, err := execution.NewPrefundedParallelPlan(
		"parallel-plan", original.Opportunity, initial,
		"chain-a", "chain-b", now,
	)
	if err != nil {
		t.Fatal(err)
	}
	plan.BaseAsset, plan.QuoteAsset = "base", "usdc"
	plan.TokenDecimals = map[market.TokenID]uint8{
		"quote-a": 6, "quote-b": 6, "base-a": 9, "base-b": 18,
	}
	if err := plan.Validate(); err != nil {
		t.Fatal(err)
	}
	return plan
}

func mustLiveAmount(t *testing.T, token market.TokenID, units string) market.TokenAmount {
	t.Helper()
	value, ok := new(big.Int).SetString(units, 10)
	if !ok {
		t.Fatal("invalid token units")
	}
	amount, err := market.NewTokenAmount(token, value)
	if err != nil {
		t.Fatal(err)
	}
	return amount
}

func mustLiveAsset(t *testing.T, asset market.AssetID, value string) market.AssetQuantity {
	t.Helper()
	amount, err := market.ParseAssetQuantity(asset, value)
	if err != nil {
		t.Fatal(err)
	}
	return amount
}

func preflightPlan(
	t *testing.T,
	now time.Time,
) execution.SequentialPlan {
	t.Helper()
	amount := func(token market.TokenID, units *big.Int) market.TokenAmount {
		value, err := market.NewTokenAmount(token, units)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	discoveryInput := amount("quote-a", big.NewInt(750_000_000))
	discoveryBuyOutput := amount("base-a", big.NewInt(3_000_000_000))
	discoverySellInput := amount(
		"base-b",
		big.NewInt(3_000_000_000_000_000_000),
	)
	discoverySellOutput := amount("quote-b", big.NewInt(752_000_000))
	opportunity := arbitrage.Opportunity{
		Evaluation: "evaluation", ConfigHash: "config",
		Classification: arbitrage.ClassificationPolicyQualified,
		Direction: arbitrage.Direction{
			BuyMarket: "market-a", SellMarket: "market-b",
		},
		Candidates: []arbitrage.Candidate{{
			BuyQuote: market.Quote{
				AmountIn:  discoveryInput,
				AmountOut: discoveryBuyOutput,
			},
			SellQuote: market.Quote{
				AmountIn:  discoverySellInput,
				AmountOut: discoverySellOutput,
			},
		}},
		SelectedIndex: 0,
	}
	initial := amount("quote-a", big.NewInt(1_000_000))
	plan, err := execution.NewSequentialPlan(
		"plan",
		opportunity,
		initial,
		"chain-a",
		"chain-b",
		now,
	)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func prefundedPreflightPlan(
	t *testing.T,
	now time.Time,
) execution.SequentialPlan {
	t.Helper()
	transported := preflightPlan(t, now)
	plan, err := execution.NewPrefundedSequentialPlan(
		transported.ID,
		transported.Opportunity,
		transported.InitialInput,
		transported.Stages[0].SourceChain,
		transported.Stages[2].SourceChain,
		transported.CreatedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func dynamicPreflightPlan(
	t *testing.T,
	now time.Time,
	sellOutput string,
	cost string,
) execution.SequentialPlan {
	t.Helper()
	original := preflightPlan(t, now)
	opportunity := original.Opportunity
	opportunity.Candidates = append(
		[]arbitrage.Candidate(nil),
		opportunity.Candidates...,
	)
	candidate := opportunity.Candidates[opportunity.SelectedIndex]
	sellUnits, ok := new(big.Int).SetString(sellOutput, 10)
	if !ok {
		t.Fatalf("invalid sell output %q", sellOutput)
	}
	sellUnits.Mul(sellUnits, big.NewInt(1_000_000))
	candidate.SellQuote.AmountOut, _ = market.NewTokenAmount(
		"quote-b",
		sellUnits,
	)
	costQuantity, err := market.ParseAssetQuantity("quote", cost)
	if err != nil {
		t.Fatal(err)
	}
	candidate.Cost = arbitrage.CostSnapshot{
		ID: "cost", Amount: costQuantity, CapturedAt: now,
	}
	opportunity.Candidates[opportunity.SelectedIndex] = candidate
	plan, err := execution.NewSequentialPlan(
		"dynamic-plan",
		opportunity,
		candidate.BuyQuote.AmountIn,
		"chain-a",
		"chain-b",
		now,
	)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}
