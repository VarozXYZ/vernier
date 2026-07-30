package livecanary_test

import (
	"bytes"
	"context"
	"errors"
	"math/big"
	"strings"
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
	prepared bool
	phases   []string
	marked   []string
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
func (j *durableJournal) MarkTransaction(_ context.Context, _ execution.OperationID, _ int, _, status string) error {
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
	actualOutput    market.TokenAmount
	preparedInput   market.TokenAmount
	reconciliation  *execution.Settlement
	reconcileDelay  time.Duration
	reconciliations atomic.Int32
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
	calls  int
}

func (e *fixedQuoteEstimator) QuoteExactInput(
	_ context.Context,
	_ market.TokenAmount,
	output market.TokenID,
) (market.TokenAmount, error) {
	e.calls++
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
	return executionport.Artifact{
		Leg: request.Leg, ValidatedQuote: quote,
		Metadata: map[string]string{"kind": "test"},
		BuiltAt:  v.now,
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
			Enabled: true, MaxBPS: 500, FixedSellBPS: 10,
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
			Enabled: true, MaxBPS: 500, FixedSellBPS: 10,
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

func TestSwapDriverRecalculatesDynamicSellWithFixedFloor(t *testing.T) {
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
			Enabled: true, MaxBPS: 500, FixedSellBPS: 10,
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
	if len(validator.slippages) != 1 ||
		validator.slippages[0] == nil ||
		validator.slippages[0].BPS != 19 {
		t.Fatalf(
			"unexpected dynamic sell slippage: %+v",
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
	if validator.slippages[1] == nil ||
		validator.slippages[1].BPS != 10 {
		t.Fatalf(
			"fixed sell floor was not applied: %+v",
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

func TestPrefundedPreflightRetriesFreshDestinationAfterSimulationFailure(t *testing.T) {
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
	if err := driver.Preflight(
		context.Background(),
		"operation",
		plan,
	); err != nil {
		t.Fatal(err)
	}
	if sellManager.simulations != 2 ||
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
			Enabled: true, MaxBPS: 500, FixedSellBPS: 10,
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
			Enabled: true, MaxBPS: 500, FixedSellBPS: 10,
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
		returnEstimator.calls != 1 ||
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
	if returnEstimator.calls != 0 {
		t.Fatalf("forced destination requested %d unnecessary return quotes", returnEstimator.calls)
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
