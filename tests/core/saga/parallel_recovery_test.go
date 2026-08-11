package saga_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/core/saga"
	domainexecution "github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

type parallelRecoveryJournal struct {
	mu          sync.Mutex
	operation   domainexecution.SequentialOperation
	snapshot    executionport.SequentialRecoverySnapshot
	settlements []domainexecution.SequentialStageSettlement
	finished    domainexecution.SequentialOperationState
}

func (j *parallelRecoveryJournal) CreateSequentialOperation(
	context.Context, domainexecution.SequentialOperation,
) error {
	return nil
}

func (j *parallelRecoveryJournal) CreateRecoverableSequentialOperation(
	context.Context,
	domainexecution.SequentialOperation,
	domainexecution.SequentialPlan,
) error {
	return nil
}

func (j *parallelRecoveryJournal) RecordPreparedTransaction(
	context.Context, executionport.PreparedTransaction,
) error {
	return nil
}

func (j *parallelRecoveryJournal) MarkTransaction(
	context.Context, domainexecution.OperationID, int, string, string,
) error {
	return nil
}

func (j *parallelRecoveryJournal) RecordStageSettlement(
	_ context.Context,
	settlement domainexecution.SequentialStageSettlement,
) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.settlements = append(j.settlements, settlement)
	return nil
}

func (j *parallelRecoveryJournal) FinishSequentialOperation(
	_ context.Context,
	_ domainexecution.OperationID,
	state domainexecution.SequentialOperationState,
	_ error,
) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.finished = state
	return nil
}

func (j *parallelRecoveryJournal) ActiveSequentialOperation(
	context.Context,
) (domainexecution.SequentialOperation, bool, error) {
	return j.operation, true, nil
}

func (j *parallelRecoveryJournal) LoadSequentialRecovery(
	context.Context,
	domainexecution.OperationID,
) (executionport.SequentialRecoverySnapshot, error) {
	return j.snapshot, nil
}

func (j *parallelRecoveryJournal) SetSequentialRecoveryState(
	_ context.Context,
	_ domainexecution.OperationID,
	state domainexecution.SequentialOperationState,
	_ error,
) error {
	j.operation.State = state
	return nil
}

func (*parallelRecoveryJournal) RecordSequentialRecoveryAttempt(
	context.Context, executionport.SequentialRecoveryAttempt,
) error {
	return nil
}

type parallelRecoveryDriver struct {
	mu      sync.Mutex
	inputs  []market.TokenAmount
	outputs map[int]market.TokenAmount
}

func (*parallelRecoveryDriver) ExecuteStage(
	context.Context,
	domainexecution.SequentialStageRequest,
	executionport.SequentialJournal,
) (domainexecution.SequentialStageSettlement, error) {
	return domainexecution.SequentialStageSettlement{}, errors.New("recovery must not execute a fresh stage directly")
}

func (d *parallelRecoveryDriver) RecoverStage(
	_ context.Context,
	request domainexecution.SequentialStageRequest,
	_ []executionport.SequentialTransactionRecord,
	_ executionport.SequentialJournal,
) (domainexecution.SequentialStageSettlement, error) {
	d.mu.Lock()
	d.inputs = append(d.inputs, request.Input)
	d.mu.Unlock()
	output, ok := d.outputs[request.Stage.Ordinal]
	if !ok {
		return domainexecution.SequentialStageSettlement{}, errors.New("missing recovery output")
	}
	return parallelSettlement(
		request.Operation,
		request.Plan,
		request.Stage,
		request.Input,
		output,
		time.Now().UTC(),
	), nil
}

func TestParallelRecoveryKeepsSellInputIndependentAfterBuySettlement(t *testing.T) {
	base := sequentialPlan(t)
	plan, err := domainexecution.NewPrefundedParallelPlan(
		"parallel-recovery-plan",
		base.Opportunity,
		base.InitialInput,
		"chain-a",
		"chain-b",
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	plan.BaseAsset = "base"
	plan.QuoteAsset = "usdc"
	plan.TokenDecimals = map[market.TokenID]uint8{
		"quote-a": 6,
		"quote-b": 6,
		"base-a":  9,
		"base-b":  18,
	}
	if err := plan.Validate(); err != nil {
		t.Fatal(err)
	}

	actualBuy := sequentialAmount(t, "base-a", 3_950_000_000)
	sellInput, err := plan.ParallelSellInput()
	if err != nil {
		t.Fatal(err)
	}
	operation := domainexecution.SequentialOperation{
		ID:            "parallel-recovery-operation",
		Plan:          plan.ID,
		OpportunityID: string(plan.Opportunity.Evaluation),
		ConfigHash:    plan.Opportunity.ConfigHash,
		State:         domainexecution.SequentialRecovering,
		CurrentStage:  1,
		CurrentAmount: actualBuy,
		StartedAt:     time.Now().UTC().Add(-time.Minute),
		UpdatedAt:     time.Now().UTC(),
	}
	buySettlement := parallelSettlement(
		operation.ID,
		plan.ID,
		plan.Stages[0],
		plan.InitialInput,
		actualBuy,
		operation.UpdatedAt,
	)
	journal := &parallelRecoveryJournal{operation: operation}
	journal.snapshot = executionport.SequentialRecoverySnapshot{
		Operation: operation,
		Plan:      plan,
		Transactions: []executionport.SequentialTransactionRecord{{
			Operation: operation.ID,
			Ordinal:   2,
			Phase:     "swap",
			Identity: domainexecution.TransactionIdentity{
				Chain: "chain-b", Account: "test", Hash: "reverted-sale",
			},
			Status:     "confirmed_revert",
			PreparedAt: operation.UpdatedAt,
			UpdatedAt:  operation.UpdatedAt,
		}},
		Settlements: []domainexecution.SequentialStageSettlement{buySettlement},
	}
	driver := &parallelRecoveryDriver{outputs: map[int]market.TokenAmount{
		2: sequentialAmount(t, "quote-b", 1_001_000),
		3: sequentialAmount(t, "base-b", 3_950_000_000_000_000_000),
		4: sequentialAmount(t, "quote-a", 1_000_900),
	}}
	coordinator, err := saga.NewSequentialRecoveryCoordinator(
		saga.SequentialRecoveryConfig{
			Journal:         journal,
			RecoveryJournal: journal,
			Drivers: executionport.DriverSet{
				Buy:               driver,
				Sell:              driver,
				BridgeBase:        driver,
				BridgeQuoteReturn: driver,
			},
			Clock: time.Now,
			Sleep: func(context.Context, time.Duration) error { return nil },
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	result, found, err := coordinator.RecoverActive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !found || journal.finished != domainexecution.SequentialCompleted {
		t.Fatalf("found=%t finished=%s", found, journal.finished)
	}
	if len(driver.inputs) != 3 {
		t.Fatalf("recovered inputs=%d", len(driver.inputs))
	}
	if driver.inputs[0].Token() != sellInput.Token() ||
		driver.inputs[0].Units().Cmp(sellInput.Units()) != 0 {
		t.Fatalf(
			"parallel sale used buy output %s instead of independent input %s",
			driver.inputs[0], sellInput,
		)
	}
	if len(result.Settlements) != 4 || result.FinalAmount.Token() != "quote-b" {
		t.Fatalf("unexpected recovered result: settlements=%d final=%s token=%q", len(result.Settlements), result.FinalAmount, result.FinalAmount.Token())
	}
	if result.RealizedNetPnL.Asset() != "usdc" {
		t.Fatalf("unexpected recovered economics: %s", result.RealizedNetPnL)
	}
}
