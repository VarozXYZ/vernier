package saga_test

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/core/saga"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	domainexecution "github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

type sequentialJournal struct {
	mu          sync.Mutex
	operation   domainexecution.SequentialOperation
	finished    domainexecution.SequentialOperationState
	prepared    []executionport.PreparedTransaction
	settlements []domainexecution.SequentialStageSettlement
	exit        domainexecution.SequentialExitDecision
}

func (j *sequentialJournal) CreateSequentialOperation(_ context.Context, operation domainexecution.SequentialOperation) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.operation.ID != "" {
		return errors.New("active")
	}
	j.operation = operation
	return nil
}

func (j *sequentialJournal) RecordPreparedTransaction(_ context.Context, transaction executionport.PreparedTransaction) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.prepared = append(j.prepared, transaction)
	return nil
}

func (j *sequentialJournal) MarkTransaction(_ context.Context, _ domainexecution.OperationID, _ int, _, _ string) error {
	return nil
}

func (j *sequentialJournal) RecordStageSettlement(_ context.Context, settlement domainexecution.SequentialStageSettlement) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.settlements = append(j.settlements, settlement)
	j.operation.CurrentStage = settlement.Request.Stage.Ordinal
	j.operation.CurrentAmount = settlement.ActualOutput
	return nil
}

func (j *sequentialJournal) FinishSequentialOperation(_ context.Context, _ domainexecution.OperationID, state domainexecution.SequentialOperationState, _ error) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.finished = state
	return nil
}

func (j *sequentialJournal) ActiveSequentialOperation(context.Context) (domainexecution.SequentialOperation, bool, error) {
	return domainexecution.SequentialOperation{}, false, nil
}

func (j *sequentialJournal) RecordSequentialExitDecision(
	_ context.Context,
	decision domainexecution.SequentialExitDecision,
) error {
	j.exit = decision
	return nil
}

type sequentialExitSelector struct {
	decision domainexecution.SequentialExitDecision
}

func (s sequentialExitSelector) SelectExit(
	_ context.Context,
	operation domainexecution.OperationID,
	_ domainexecution.SequentialPlan,
	_ market.TokenAmount,
	_ []domainexecution.CostComponent,
) (domainexecution.SequentialExitDecision, error) {
	result := s.decision
	result.Operation = operation
	return result, nil
}

type sequentialDriver struct {
	outputs        map[int]market.TokenAmount
	costs          map[int][]domainexecution.CostComponent
	failAt         int
	failure        error
	mu             sync.Mutex
	inputs         []market.TokenAmount
	preflightCalls int
	preflightErr   error
	discarded      bool
}

type sequentialObserver struct {
	mu       sync.Mutex
	started  int
	stages   []string
	finished []domainexecution.SequentialOperationState
}

func (d *sequentialDriver) Preflight(
	context.Context,
	domainexecution.OperationID,
	domainexecution.SequentialPlan,
) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.preflightCalls++
	return d.preflightErr
}

func (d *sequentialDriver) DiscardPreflight(
	domainexecution.OperationID,
) {
	d.mu.Lock()
	d.discarded = true
	d.mu.Unlock()
}

func (o *sequentialObserver) OperationStarted(
	domainexecution.SequentialOperation,
	domainexecution.SequentialPlan,
) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.started++
}

func (o *sequentialObserver) StageStarted(
	request domainexecution.SequentialStageRequest,
) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.stages = append(o.stages, "started/"+string(request.Stage.Stage))
}

func (o *sequentialObserver) StageSettled(
	settlement domainexecution.SequentialStageSettlement,
) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.stages = append(
		o.stages, "settled/"+string(settlement.Request.Stage.Stage),
	)
}

func (o *sequentialObserver) OperationFinished(
	_ domainexecution.SequentialOperation,
	state domainexecution.SequentialOperationState,
	_ executionport.SequentialResult,
	_ error,
) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.finished = append(o.finished, state)
}

func (d *sequentialDriver) ExecuteStage(
	ctx context.Context,
	request domainexecution.SequentialStageRequest,
	journal executionport.SequentialJournal,
) (domainexecution.SequentialStageSettlement, error) {
	d.mu.Lock()
	d.inputs = append(d.inputs, request.Input)
	d.mu.Unlock()
	if request.Stage.Ordinal == d.failAt {
		return domainexecution.SequentialStageSettlement{}, d.failure
	}
	identity := domainexecution.TransactionIdentity{
		Chain: request.Stage.SourceChain, Account: "test",
		Hash: fmt.Sprintf("source-%d", request.Stage.Ordinal),
	}
	if err := journal.RecordPreparedTransaction(ctx, executionport.PreparedTransaction{
		Operation: request.Operation, Ordinal: request.Stage.Ordinal,
		Phase: "source", Identity: identity, PreparedAt: time.Now(),
	}); err != nil {
		return domainexecution.SequentialStageSettlement{}, err
	}
	_ = journal.MarkTransaction(ctx, request.Operation, request.Stage.Ordinal, "source", "broadcast")
	_ = journal.MarkTransaction(ctx, request.Operation, request.Stage.Ordinal, "source", "confirmed")
	var destination *domainexecution.TransactionIdentity
	if request.Stage.DestinationChain != "" {
		value := domainexecution.TransactionIdentity{
			Chain: request.Stage.DestinationChain, Account: "test",
			Hash: fmt.Sprintf("destination-%d", request.Stage.Ordinal),
		}
		destination = &value
	}
	return domainexecution.SequentialStageSettlement{
		Request: request, ActualInput: request.Input,
		ActualOutput: d.outputs[request.Stage.Ordinal],
		Costs: append(
			[]domainexecution.CostComponent(nil),
			d.costs[request.Stage.Ordinal]...,
		),
		SourceIdentity: identity, DestinationIdentity: destination,
		ObservedAt: time.Now(), Evidence: "test-settlement",
	}, nil
}

func sequentialAmount(t *testing.T, token market.TokenID, units int64) market.TokenAmount {
	t.Helper()
	amount, err := market.NewTokenAmount(token, big.NewInt(units))
	if err != nil {
		t.Fatal(err)
	}
	return amount
}

func sequentialPlan(t *testing.T) domainexecution.SequentialPlan {
	t.Helper()
	input := sequentialAmount(t, "quote-a", 1_000_000)
	buyOutput := sequentialAmount(t, "base-a", 4_000_000_000)
	sellInput := sequentialAmount(t, "base-b", 4_000_000_000_000_000_000)
	sellOutput := sequentialAmount(t, "quote-b", 999_900)
	now := time.Now().UTC()
	inputValue, _ := market.NewAssetQuantity("usdc", big.NewRat(1, 1))
	outputValue, _ := market.NewAssetQuantity("usdc", big.NewRat(9999, 10_000))
	opportunity := arbitrage.Opportunity{
		Evaluation: "evaluation-1", ConfigHash: "config",
		Classification: arbitrage.ClassificationPolicyQualified,
		Direction: arbitrage.Direction{
			BuyMarket: "market-a", SellMarket: "market-b",
		},
		Candidates: []arbitrage.Candidate{{
			Input: inputValue, Output: outputValue,
			BuyQuote:  market.Quote{AmountIn: input, AmountOut: buyOutput},
			SellQuote: market.Quote{AmountIn: sellInput, AmountOut: sellOutput},
		}},
		SelectedIndex: 0,
	}
	plan, err := domainexecution.NewSequentialPlan(
		"plan-1", opportunity, input, "chain-a", "chain-b", now,
	)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func TestSequentialExecutorUsesEveryConfirmedOutputAsTheNextInput(t *testing.T) {
	plan := sequentialPlan(t)
	outputs := map[int]market.TokenAmount{
		1: sequentialAmount(t, "base-a", 4_052_168_781),
		2: sequentialAmount(t, "base-b", 4_052_168_781_000_000_000),
		3: sequentialAmount(t, "quote-b", 1_001_234),
		4: sequentialAmount(t, "quote-a", 1_001_134),
	}
	networkAmount, _ := market.NewAssetQuantity("sol", big.NewRat(1, 100_000))
	networkValue, _ := market.NewAssetQuantity("usdc", big.NewRat(4, 10_000))
	spreadAmount, _ := market.NewAssetQuantity("usdc", big.NewRat(1, 10_000))
	driver := &sequentialDriver{
		outputs: outputs,
		costs: map[int][]domainexecution.CostComponent{
			1: {{
				Kind: "network_fee", Chain: "chain-a", Amount: networkAmount,
				QuoteValue: networkValue, Evidence: "receipt",
			}},
			4: {{
				Kind: "bridge_spread", Chain: "chain-b", Amount: spreadAmount,
				QuoteValue: spreadAmount, IncludedInOutput: true, Evidence: "balance",
			}},
		},
	}
	journal := &sequentialJournal{}
	observer := &sequentialObserver{}
	executor, err := saga.NewSequentialExecutorWithObserver(
		journal,
		executionport.DriverSet{
			Buy: driver, BridgeBase: driver, Sell: driver, BridgeQuoteReturn: driver,
		},
		time.Now,
		observer,
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Execute(context.Background(), "operation-1", plan)
	if err != nil {
		t.Fatal(err)
	}
	if journal.finished != domainexecution.SequentialCompleted {
		t.Fatalf("unexpected final state %q", journal.finished)
	}
	if len(driver.inputs) != 4 || len(result.Settlements) != 4 ||
		len(journal.prepared) != 4 {
		t.Fatalf("unexpected stage counts: inputs=%d settlements=%d prepared=%d",
			len(driver.inputs), len(result.Settlements), len(journal.prepared))
	}
	expectedInputs := []market.TokenAmount{
		plan.InitialInput, outputs[1], outputs[2], outputs[3],
	}
	for index := range expectedInputs {
		if driver.inputs[index].Token() != expectedInputs[index].Token() ||
			driver.inputs[index].Units().Cmp(expectedInputs[index].Units()) != 0 {
			t.Fatalf("stage %d did not receive predecessor settlement", index+1)
		}
	}
	if result.FinalAmount.Units().Cmp(outputs[4].Units()) != 0 {
		t.Fatalf("unexpected final amount %s", result.FinalAmount)
	}
	if result.ExecutionCost.Rat().Cmp(big.NewRat(5, 10_000)) != 0 ||
		result.ExternalCost.Rat().Cmp(big.NewRat(4, 10_000)) != 0 ||
		result.RealizedGross.Rat().Cmp(big.NewRat(1134, 1_000_000)) != 0 ||
		result.RealizedNetPnL.Rat().Cmp(big.NewRat(734, 1_000_000)) != 0 {
		t.Fatalf(
			"unexpected realized economics cost=%s external=%s gross=%s net=%s",
			result.ExecutionCost, result.ExternalCost,
			result.RealizedGross, result.RealizedNetPnL,
		)
	}
	if observer.started != 1 || len(observer.stages) != 8 ||
		len(observer.finished) != 1 ||
		observer.finished[0] != domainexecution.SequentialCompleted {
		t.Fatalf("unexpected observer lifecycle: %+v", observer)
	}
}

func TestSequentialExecutorAbortsOnlyWhenFirstBroadcastIsKnownRejected(t *testing.T) {
	plan := sequentialPlan(t)
	driver := &sequentialDriver{
		outputs: map[int]market.TokenAmount{},
		failAt:  1,
		failure: executionport.NewStageError(
			executionport.DispositionRejected, errors.New("simulation rejected"),
		),
	}
	journal := &sequentialJournal{}
	executor, _ := saga.NewSequentialExecutor(
		journal,
		executionport.DriverSet{
			Buy: driver, BridgeBase: driver, Sell: driver, BridgeQuoteReturn: driver,
		},
		time.Now,
	)
	if _, err := executor.Execute(context.Background(), "operation-1", plan); err == nil {
		t.Fatal("expected execution error")
	}
	if journal.finished != domainexecution.SequentialAborted {
		t.Fatalf("expected safe abort, got %q", journal.finished)
	}
}

func TestSequentialExecutorRequiresManualInterventionAfterExposure(t *testing.T) {
	plan := sequentialPlan(t)
	outputs := map[int]market.TokenAmount{
		1: sequentialAmount(t, "base-a", 4_052_168_781),
	}
	driver := &sequentialDriver{
		outputs: outputs, failAt: 2,
		failure: executionport.NewStageError(
			executionport.DispositionPossible, errors.New("bridge outcome unknown"),
		),
	}
	journal := &sequentialJournal{}
	executor, _ := saga.NewSequentialExecutor(
		journal,
		executionport.DriverSet{
			Buy: driver, BridgeBase: driver, Sell: driver, BridgeQuoteReturn: driver,
		},
		time.Now,
	)
	if _, err := executor.Execute(context.Background(), "operation-1", plan); err == nil {
		t.Fatal("expected execution error")
	}
	if journal.finished != domainexecution.SequentialManualIntervention {
		t.Fatalf("expected manual intervention, got %q", journal.finished)
	}
	if len(driver.inputs) != 2 {
		t.Fatalf("unexpected stage count %d", len(driver.inputs))
	}
}

func TestSequentialExecutorCanReturnBaseAndSellAtOriginWithoutAnotherBridge(t *testing.T) {
	plan := sequentialPlan(t)
	outputs := map[int]market.TokenAmount{
		1: sequentialAmount(t, "base-a", 4_052_168_781),
		2: sequentialAmount(t, "base-b", 4_052_168_781_000_000_000),
		3: sequentialAmount(t, "base-a", 4_052_168_780),
		4: sequentialAmount(t, "quote-a", 990_000),
	}
	recovery, _ := market.NewAssetQuantity("usdc", big.NewRat(99, 100))
	margin, _ := market.NewAssetQuantity("usdc", big.NewRat(1, 100))
	driver := &sequentialDriver{outputs: outputs}
	journal := &sequentialJournal{}
	executor, err := saga.NewSequentialExecutor(
		journal,
		executionport.DriverSet{
			Buy: driver, BridgeBase: driver, Sell: driver,
			BridgeQuoteReturn: driver,
			ExitSelector: sequentialExitSelector{
				decision: domainexecution.SequentialExitDecision{
					Route: domainexecution.ExitReturnToOrigin,
					DestinationOutput: sequentialAmount(
						t, "quote-b", 970_000,
					),
					ReturnOutput: sequentialAmount(
						t, "quote-a", 990_000,
					),
					DestinationRecovery: recovery,
					ReturnRecovery:      recovery,
					SafetyMargin:        margin,
					DecidedAt:           time.Now().UTC(),
					Evidence:            "test",
				},
			},
		},
		time.Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Execute(
		context.Background(),
		"operation-return",
		plan,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.ExitDecision == nil ||
		result.ExitDecision.Route != domainexecution.ExitReturnToOrigin ||
		journal.exit.Route != domainexecution.ExitReturnToOrigin {
		t.Fatalf("exit decision was not retained: %+v", result.ExitDecision)
	}
	gotStages := make([]domainexecution.SequentialStage, 0, 4)
	for _, settlement := range result.Settlements {
		gotStages = append(gotStages, settlement.Request.Stage.Stage)
	}
	wantStages := []domainexecution.SequentialStage{
		domainexecution.StageBuy,
		domainexecution.StageBridgeBase,
		domainexecution.StageBridgeBase,
		domainexecution.StageSell,
	}
	if fmt.Sprint(gotStages) != fmt.Sprint(wantStages) {
		t.Fatalf("stages=%v want=%v", gotStages, wantStages)
	}
	if result.FinalAmount.Token() != "quote-a" ||
		result.FinalAmount.Units().Cmp(big.NewInt(990_000)) != 0 {
		t.Fatalf("unexpected terminal amount %s", result.FinalAmount)
	}
}

func TestSequentialExecutorRejectsPreflightBeforeCreatingOperation(t *testing.T) {
	plan := sequentialPlan(t)
	driver := &sequentialDriver{
		preflightErr: executionport.NewStageError(
			executionport.DispositionRejected,
			errors.New("sell simulation reverted"),
		),
	}
	journal := &sequentialJournal{}
	executor, _ := saga.NewSequentialExecutor(
		journal,
		executionport.DriverSet{
			Buy: driver, BridgeBase: driver,
			Sell: driver, BridgeQuoteReturn: driver,
		},
		time.Now,
	)
	result, err := executor.Execute(
		context.Background(),
		"operation-1",
		plan,
	)
	if err == nil ||
		executionport.ErrorDisposition(err) !=
			executionport.DispositionRejected {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	if journal.operation.ID != "" ||
		journal.finished != "" ||
		len(journal.prepared) != 0 {
		t.Fatalf(
			"preflight changed durable state: operation=%+v finished=%q prepared=%d",
			journal.operation,
			journal.finished,
			len(journal.prepared),
		)
	}
	if driver.preflightCalls != 1 || driver.discarded {
		t.Fatalf(
			"preflight lifecycle: calls=%d discarded=%t",
			driver.preflightCalls,
			driver.discarded,
		)
	}
}
