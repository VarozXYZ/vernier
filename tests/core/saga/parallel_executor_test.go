package saga_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/core/saga"
	domainexecution "github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

type parallelSwapExecutorDriver struct {
	buyOutput  market.TokenAmount
	sellOutput market.TokenAmount
}

func (d *parallelSwapExecutorDriver) ExecuteStage(
	context.Context,
	domainexecution.SequentialStageRequest,
	executionport.SequentialJournal,
) (domainexecution.SequentialStageSettlement, error) {
	return domainexecution.SequentialStageSettlement{}, errors.New("single swap execution is forbidden")
}

func (d *parallelSwapExecutorDriver) ExecuteParallelSwaps(
	_ context.Context,
	operation domainexecution.OperationID,
	plan domainexecution.SequentialPlan,
	_ executionport.SequentialJournal,
) ([]domainexecution.SequentialStageSettlement, error) {
	sellInput, err := plan.ParallelSellInput()
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	return []domainexecution.SequentialStageSettlement{
		parallelSettlement(operation, plan.ID, plan.Stages[0], plan.InitialInput, d.buyOutput, now),
		parallelSettlement(operation, plan.ID, plan.Stages[1], sellInput, d.sellOutput, now),
	}, nil
}

type synchronizedBridgeDriver struct {
	started   chan<- domainexecution.SequentialStageRequest
	release   <-chan struct{}
	failStage domainexecution.SequentialStage
}

func (d synchronizedBridgeDriver) ExecuteStage(
	ctx context.Context,
	request domainexecution.SequentialStageRequest,
	_ executionport.SequentialJournal,
) (domainexecution.SequentialStageSettlement, error) {
	select {
	case d.started <- request:
	case <-ctx.Done():
		return domainexecution.SequentialStageSettlement{}, ctx.Err()
	}
	if request.Stage.Stage == d.failStage {
		return domainexecution.SequentialStageSettlement{}, errors.New("bridge preparation failed")
	}
	select {
	case <-d.release:
	case <-ctx.Done():
		return domainexecution.SequentialStageSettlement{}, ctx.Err()
	}
	output, err := market.NewTokenAmount(request.Stage.OutputToken, request.Input.Units())
	if err != nil {
		return domainexecution.SequentialStageSettlement{}, err
	}
	return parallelSettlement(
		request.Operation, request.Plan, request.Stage,
		request.Input, output, time.Now().UTC(),
	), nil
}

func TestPrefundedParallelWaitsForStartedBridgeWhenItsPeerFails(t *testing.T) {
	base := sequentialPlan(t)
	plan, err := domainexecution.NewPrefundedParallelPlan(
		"parallel-wait-plan", base.Opportunity, base.InitialInput,
		"chain-a", "chain-b", time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	plan.BaseAsset = "base"
	plan.QuoteAsset = base.Opportunity.Candidates[0].Input.Asset()
	plan.TokenDecimals = map[market.TokenID]uint8{
		"quote-a": 6, "quote-b": 6, "base-a": 9, "base-b": 18,
	}
	swaps := &parallelSwapExecutorDriver{
		buyOutput:  sequentialAmount(t, "base-a", 4_052_168_781),
		sellOutput: sequentialAmount(t, "quote-b", 1_001_234),
	}
	started := make(chan domainexecution.SequentialStageRequest, 2)
	release := make(chan struct{})
	bridges := synchronizedBridgeDriver{
		started: started, release: release,
		failStage: domainexecution.StageBridgeQuoteReturn,
	}
	executor, err := saga.NewSequentialExecutor(
		&sequentialJournal{},
		executionport.DriverSet{
			Buy: swaps, Sell: swaps,
			BridgeBase: bridges, BridgeQuoteReturn: bridges,
		},
		time.Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, executeErr := executor.Execute(context.Background(), "parallel-wait-operation", plan)
		done <- executeErr
	}()
	for index := 0; index < 2; index++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("both bridges were not started")
		}
	}
	select {
	case err := <-done:
		t.Fatalf("executor returned while a bridge was still running: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "bridge preparation failed") {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("executor did not join both bridge goroutines")
	}
}

func parallelSettlement(
	operation domainexecution.OperationID,
	plan domainexecution.PlanID,
	stage domainexecution.SequentialStagePlan,
	input, output market.TokenAmount,
	observed time.Time,
) domainexecution.SequentialStageSettlement {
	settlement := domainexecution.SequentialStageSettlement{
		Request: domainexecution.SequentialStageRequest{
			Operation: operation, Plan: plan, Stage: stage, Input: input,
		},
		ActualInput: input, ActualOutput: output,
		SourceIdentity: domainexecution.TransactionIdentity{
			Chain: stage.SourceChain, Account: "test",
			Hash: fmt.Sprintf("parallel-%d", stage.Ordinal),
		},
		ObservedAt: observed, Evidence: "parallel-test",
	}
	if stage.DestinationChain != "" {
		destination := domainexecution.TransactionIdentity{
			Chain: stage.DestinationChain, Account: "test",
			Hash: fmt.Sprintf("parallel-destination-%d", stage.Ordinal),
		}
		settlement.DestinationIdentity = &destination
	}
	return settlement
}

func TestPrefundedParallelStartsBothIndependentBridgesBeforeEitherSettles(t *testing.T) {
	base := sequentialPlan(t)
	plan, err := domainexecution.NewPrefundedParallelPlan(
		"parallel-plan", base.Opportunity, base.InitialInput,
		"chain-a", "chain-b", time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	plan.BaseAsset = "base"
	plan.QuoteAsset = base.Opportunity.Candidates[0].Input.Asset()
	plan.TokenDecimals = map[market.TokenID]uint8{
		"quote-a": 6, "quote-b": 6, "base-a": 9, "base-b": 18,
	}
	if err := plan.Validate(); err != nil {
		t.Fatal(err)
	}
	swaps := &parallelSwapExecutorDriver{
		buyOutput:  sequentialAmount(t, "base-a", 4_052_168_781),
		sellOutput: sequentialAmount(t, "quote-b", 1_001_234),
	}
	started := make(chan domainexecution.SequentialStageRequest, 2)
	release := make(chan struct{})
	bridges := synchronizedBridgeDriver{started: started, release: release}
	journal := &sequentialJournal{}
	executor, err := saga.NewSequentialExecutor(
		journal,
		executionport.DriverSet{
			Buy: swaps, Sell: swaps,
			BridgeBase: bridges, BridgeQuoteReturn: bridges,
		},
		time.Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		result executionport.SequentialResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, executeErr := executor.Execute(
			context.Background(), "parallel-operation", plan,
		)
		done <- outcome{result: result, err: executeErr}
	}()
	seen := map[domainexecution.SequentialStage]bool{}
	for len(seen) < 2 {
		select {
		case request := <-started:
			seen[request.Stage.Stage] = true
		case <-time.After(time.Second):
			t.Fatalf("bridges did not start independently: %v", seen)
		}
	}
	close(release)
	select {
	case outcome := <-done:
		if outcome.err != nil {
			t.Fatal(outcome.err)
		}
		if len(outcome.result.Settlements) != 4 {
			t.Fatalf("settlements=%d", len(outcome.result.Settlements))
		}
	case <-time.After(time.Second):
		t.Fatal("parallel execution did not complete")
	}
}
