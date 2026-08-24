package livecanary_test

import (
	"context"
	"errors"
	"io"
	"math/big"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sqlitestore "github.com/VarozXYZ/vernier/adapters/persistence/sqlite"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	notificationport "github.com/VarozXYZ/vernier/ports/notification"
	"github.com/VarozXYZ/vernier/runtime/livecanary"
	"github.com/VarozXYZ/vernier/runtime/livecompare"
)

type continuingStream struct {
	started    chan struct{}
	reevaluate chan struct{}
}

func (s *continuingStream) RunStream(
	ctx context.Context,
	options livecompare.StreamOptions,
) error {
	close(s.started)
	select {
	case <-ctx.Done():
		return nil
	case <-options.ReevaluationRequests:
		close(s.reevaluate)
	}
	<-ctx.Done()
	return nil
}

type successfulExecutor struct{}

func (successfulExecutor) Execute(
	_ context.Context,
	operation execution.OperationID,
	plan execution.SequentialPlan,
) (executionport.SequentialResult, error) {
	return executionport.SequentialResult{
		Operation: operation, FinalAmount: plan.InitialInput,
	}, nil
}

type capturingExecutor struct {
	plans chan execution.SequentialPlan
}

func (e capturingExecutor) Execute(
	_ context.Context,
	operation execution.OperationID,
	plan execution.SequentialPlan,
) (executionport.SequentialResult, error) {
	e.plans <- plan
	return executionport.SequentialResult{
		Operation: operation, FinalAmount: plan.InitialInput,
	}, nil
}

type immediateReportStream struct {
	report livecompare.Report
}

func (s immediateReportStream) RunStream(
	ctx context.Context,
	options livecompare.StreamOptions,
) error {
	if err := options.OnReport(s.report); err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}

type failedAfterSettlementExecutor struct{}

func (failedAfterSettlementExecutor) Execute(
	_ context.Context,
	operation execution.OperationID,
	plan execution.SequentialPlan,
) (executionport.SequentialResult, error) {
	return executionport.SequentialResult{
		Operation: operation, FinalAmount: plan.InitialInput,
		Settlements: []execution.SequentialStageSettlement{{}},
	}, errors.New("synthetic recoverable failure")
}

type successfulRecovery struct {
	calls      int
	foundAfter int
}

type shutdownRecovery struct {
	calls   int
	started chan struct{}
}

func (r *shutdownRecovery) RecoverActive(
	ctx context.Context,
) (executionport.SequentialResult, bool, error) {
	r.calls++
	if r.calls == 1 {
		return executionport.SequentialResult{}, false, nil
	}
	close(r.started)
	<-ctx.Done()
	return executionport.SequentialResult{}, true, ctx.Err()
}

func (r *successfulRecovery) RecoverActive(
	context.Context,
) (executionport.SequentialResult, bool, error) {
	r.calls++
	if r.calls < r.foundAfter {
		return executionport.SequentialResult{}, false, nil
	}
	return executionport.SequentialResult{
		Operation: "recovered-operation",
	}, true, nil
}

type failingStream struct{ err error }

func (s failingStream) RunStream(
	context.Context,
	livecompare.StreamOptions,
) error {
	return s.err
}

type countingRestorationResumer struct{ calls int }

func (r *countingRestorationResumer) ResumePending(context.Context) error {
	r.calls++
	return nil
}

func TestRecoverOnlyResumesAsynchronousRestorations(t *testing.T) {
	ctx := context.Background()
	manager, err := livecanary.NewManagerWithLimit(
		ctx,
		livecanary.Planner{MarketChains: map[market.MarketID]market.ChainID{
			"market-a": "chain-a", "market-b": "chain-b",
		}, ExecutionUnits: big.NewInt(1_000_000)},
		successfulExecutor{}, 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	store, err := sqlitestore.Open(filepath.Join(t.TempDir(), "opportunities.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	runtime, err := livecanary.NewRuntime(
		failingStream{err: errors.New("stream must not start")}, manager, store,
		nil, io.Discard, nil, nil, true, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	runtime.SetRecovery(&successfulRecovery{foundAfter: 2})
	resumer := &countingRestorationResumer{}
	runtime.SetRestorationResumer(resumer)
	if err := runtime.RecoverOnly(ctx); err != nil {
		t.Fatal(err)
	}
	if resumer.calls != 1 {
		t.Fatalf("restoration resume calls = %d, want 1", resumer.calls)
	}
}

func TestRuntimeReportsStartupAndFailureShutdown(t *testing.T) {
	ctx := context.Background()
	manager, err := livecanary.NewManagerWithLimit(
		ctx,
		livecanary.Planner{
			MarketChains: map[market.MarketID]market.ChainID{
				"market-a": "chain-a", "market-b": "chain-b",
			},
			ExecutionUnits: big.NewInt(1_000_000),
		},
		successfulExecutor{},
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	store, err := sqlitestore.Open(
		filepath.Join(t.TempDir(), "opportunities.sqlite"),
	)
	if err != nil {
		t.Fatal(err)
	}
	sender := &liveNotificationSender{}
	notifier, err := livecanary.NewLiveNotifier(sender, nil)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := livecanary.NewRuntime(
		failingStream{err: errors.New("synthetic stream failure")},
		manager,
		store,
		nil,
		io.Discard,
		notifier,
		nil,
		true,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	runErr := runtime.Run(ctx)
	if runErr == nil || !strings.Contains(runErr.Error(), "synthetic stream failure") {
		t.Fatalf("run error=%v", runErr)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}

	sender.mu.Lock()
	defer sender.mu.Unlock()
	if len(sender.lifecycle) != 2 {
		t.Fatalf("lifecycle events=%d, want 2", len(sender.lifecycle))
	}
	if sender.lifecycle[0].Kind != notificationport.LiveRuntimeStarted ||
		sender.lifecycle[0].Mode != "live" {
		t.Fatalf("unexpected startup event: %+v", sender.lifecycle[0])
	}
	stopped := sender.lifecycle[1]
	if stopped.Kind != notificationport.LiveRuntimeStopped ||
		!strings.Contains(stopped.Reason, "runtime error: synthetic stream failure") ||
		stopped.Uptime < 0 {
		t.Fatalf("unexpected shutdown event: %+v", stopped)
	}
}

func TestProductionRuntimeContinuesAndReevaluatesAfterCompletion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager, err := livecanary.NewManagerWithLimit(
		ctx,
		livecanary.Planner{
			MarketChains: map[market.MarketID]market.ChainID{
				"market-a": "chain-a", "market-b": "chain-b",
			},
			ExecutionUnits: big.NewInt(1_000_000),
		},
		successfulExecutor{},
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	store, err := sqlitestore.Open(
		filepath.Join(t.TempDir(), "opportunities.sqlite"),
	)
	if err != nil {
		t.Fatal(err)
	}
	stream := &continuingStream{
		started: make(chan struct{}), reevaluate: make(chan struct{}),
	}
	reevaluate := make(chan time.Time, 1)
	runtime, err := livecanary.NewRuntime(
		stream,
		manager,
		store,
		nil,
		io.Discard,
		nil,
		reevaluate,
		true,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	runResult := make(chan error, 1)
	go func() { runResult <- runtime.Run(ctx) }()
	select {
	case <-stream.started:
	case <-time.After(time.Second):
		t.Fatal("research stream did not start")
	}
	accepted, err := manager.Offer(runtimeOpportunity(t))
	if err != nil || !accepted {
		t.Fatalf("offer: accepted=%t err=%v", accepted, err)
	}
	select {
	case <-stream.reevaluate:
	case err := <-runResult:
		t.Fatalf("runtime exited after successful operation: %v", err)
	case <-time.After(time.Second):
		t.Fatal("runtime did not request a post-settlement reevaluation")
	}
	select {
	case err := <-runResult:
		t.Fatalf("runtime exited after successful operation: %v", err)
	default:
	}
	cancel()
	select {
	case err := <-runResult:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("runtime cancellation error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime did not stop after cancellation")
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOneOperationRuntimeExitsWithoutPostFlowReevaluation(t *testing.T) {
	ctx := context.Background()
	manager, err := livecanary.NewManagerWithLimit(
		ctx,
		livecanary.Planner{
			MarketChains: map[market.MarketID]market.ChainID{
				"market-a": "chain-a", "market-b": "chain-b",
			},
			ExecutionUnits: big.NewInt(1_000_000),
		},
		successfulExecutor{},
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	store, err := sqlitestore.Open(filepath.Join(t.TempDir(), "opportunities.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	stream := &continuingStream{started: make(chan struct{}), reevaluate: make(chan struct{})}
	runtime, err := livecanary.NewRuntime(
		stream, manager, store, nil, io.Discard, nil,
		make(chan time.Time, 1), true, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	runtime.SetExitAfterOperation(true)
	runResult := make(chan error, 1)
	go func() { runResult <- runtime.Run(ctx) }()
	<-stream.started
	accepted, err := manager.Offer(runtimeOpportunity(t))
	if err != nil || !accepted {
		t.Fatalf("offer: accepted=%t err=%v", accepted, err)
	}
	select {
	case err := <-runResult:
		if err != nil {
			t.Fatalf("one-operation run error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("one-operation runtime did not exit")
	}
	select {
	case <-stream.reevaluate:
		t.Fatal("one-operation runtime requested a post-flow reevaluation")
	default:
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOneOperationRuntimeExitsAfterCompletedRecovery(t *testing.T) {
	ctx := context.Background()
	manager, err := livecanary.NewManagerWithLimit(
		ctx,
		livecanary.Planner{
			MarketChains: map[market.MarketID]market.ChainID{
				"market-a": "chain-a", "market-b": "chain-b",
			},
			ExecutionUnits: big.NewInt(1_000_000),
		},
		failedAfterSettlementExecutor{},
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	store, err := sqlitestore.Open(filepath.Join(t.TempDir(), "opportunities.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	stream := &continuingStream{started: make(chan struct{}), reevaluate: make(chan struct{})}
	recovery := &successfulRecovery{foundAfter: 2}
	runtime, err := livecanary.NewRuntime(
		stream, manager, store, nil, io.Discard, nil,
		make(chan time.Time, 1), true, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	runtime.SetRecovery(recovery)
	runtime.SetExitAfterOperation(true)
	runResult := make(chan error, 1)
	go func() { runResult <- runtime.Run(ctx) }()
	<-stream.started
	accepted, err := manager.Offer(runtimeOpportunity(t))
	if err != nil || !accepted {
		t.Fatalf("offer: accepted=%t err=%v", accepted, err)
	}
	select {
	case err := <-runResult:
		if err != nil {
			t.Fatalf("one-operation recovery run error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("one-operation runtime did not exit after recovery")
	}
	if recovery.calls != 2 {
		// One startup reconciliation plus one recovery for the admitted operation.
		t.Fatalf("recovery calls=%d", recovery.calls)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeShutdownDoesNotReportRecoveryBlocked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	manager, err := livecanary.NewManagerWithLimit(
		ctx,
		livecanary.Planner{
			MarketChains: map[market.MarketID]market.ChainID{
				"market-a": "chain-a", "market-b": "chain-b",
			},
			ExecutionUnits: big.NewInt(1_000_000),
		},
		failedAfterSettlementExecutor{},
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	store, err := sqlitestore.Open(filepath.Join(t.TempDir(), "opportunities.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	stream := &continuingStream{
		started: make(chan struct{}), reevaluate: make(chan struct{}),
	}
	recovery := &shutdownRecovery{started: make(chan struct{})}
	runtime, err := livecanary.NewRuntime(
		stream, manager, store, nil, io.Discard, nil,
		make(chan time.Time, 1), true, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	runtime.SetRecovery(recovery)
	runResult := make(chan error, 1)
	go func() { runResult <- runtime.Run(ctx) }()
	<-stream.started
	accepted, err := manager.Offer(runtimeOpportunity(t))
	if err != nil || !accepted {
		t.Fatalf("offer: accepted=%t err=%v", accepted, err)
	}
	select {
	case <-recovery.started:
	case <-time.After(time.Second):
		t.Fatal("runtime did not enter recovery")
	}
	cancel()
	select {
	case runErr := <-runResult:
		if !errors.Is(runErr, context.Canceled) {
			t.Fatalf("shutdown error=%v", runErr)
		}
		if strings.Contains(runErr.Error(), "recovery blocked") {
			t.Fatalf("shutdown falsely blocked recovery: %v", runErr)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime did not stop after cancellation")
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOneOperationRuntimeCountsStartupRecoveryAsItsSingleOperation(t *testing.T) {
	ctx := context.Background()
	manager, err := livecanary.NewManagerWithLimit(
		ctx,
		livecanary.Planner{
			MarketChains: map[market.MarketID]market.ChainID{
				"market-a": "chain-a", "market-b": "chain-b",
			},
			ExecutionUnits: big.NewInt(1_000_000),
		},
		successfulExecutor{}, 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	store, err := sqlitestore.Open(filepath.Join(t.TempDir(), "opportunities.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	stream := &continuingStream{started: make(chan struct{}), reevaluate: make(chan struct{})}
	recovery := &successfulRecovery{foundAfter: 1}
	runtime, err := livecanary.NewRuntime(
		stream, manager, store, nil, io.Discard, nil,
		make(chan time.Time, 1), true, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	runtime.SetRecovery(recovery)
	runtime.SetExitAfterOperation(true)
	if err := runtime.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if recovery.calls != 1 {
		t.Fatalf("recovery calls=%d", recovery.calls)
	}
	select {
	case <-stream.started:
		t.Fatal("one-operation runtime started discovery after recovering an active operation")
	default:
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestForcedCanaryAcceptsImmediateLocalExecutableCandidate(t *testing.T) {
	ctx := context.Background()
	plans := make(chan execution.SequentialPlan, 1)
	manager, err := livecanary.NewManagerWithLimit(
		ctx,
		livecanary.Planner{
			MarketChains: map[market.MarketID]market.ChainID{
				"market-a": "chain-a", "market-b": "chain-b",
			},
			ExecutionUnits: big.NewInt(1_000_000),
		},
		capturingExecutor{plans: plans}, 1,
	)
	if err != nil {
		t.Fatal(err)
	}
	store, err := sqlitestore.Open(filepath.Join(t.TempDir(), "opportunities.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	opportunity := runtimeOpportunity(t)
	opportunity.Classification = arbitrage.ClassificationNoSpread
	opportunity.SelectedIndex = -1
	executable := opportunity.Candidates[0]
	executable.BuyQuote.Market = opportunity.Direction.BuyMarket
	executable.SellQuote.Market = opportunity.Direction.SellMarket
	report := livecompare.Report{ImmediateCandidate: &executable, ImmediateDirection: opportunity.Direction}
	report.Research.Opportunities = []arbitrage.Opportunity{opportunity}
	direction := opportunity.Direction
	runtime, err := livecanary.NewRuntime(
		immediateReportStream{report: report}, manager, store, nil,
		io.Discard, nil, nil, true, &direction,
	)
	if err != nil {
		t.Fatal(err)
	}
	runtime.SetExitAfterOperation(true)
	if err := runtime.Run(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case plan := <-plans:
		if len(plan.Opportunity.Reasons) != 1 ||
			plan.Opportunity.Reasons[0] != "forced_canary_direction" {
			t.Fatalf("forced plan reasons=%v", plan.Opportunity.Reasons)
		}
		selected := plan.Opportunity.Candidates[plan.Opportunity.SelectedIndex]
		if selected.BuyQuote.Market != direction.BuyMarket ||
			selected.SellQuote.Market != direction.SellMarket {
			t.Fatalf("forced plan did not preserve immediate candidate: %+v", selected)
		}
	default:
		t.Fatal("forced immediate candidate was not executed")
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

func runtimeOpportunity(t *testing.T) arbitrage.Opportunity {
	t.Helper()
	amount := func(token market.TokenID, units int64) market.TokenAmount {
		value, err := market.NewTokenAmount(token, big.NewInt(units))
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	return arbitrage.Opportunity{
		Evaluation:     "evaluation",
		ConfigHash:     "config",
		Classification: arbitrage.ClassificationPolicyQualified,
		Direction: arbitrage.Direction{
			BuyMarket: "market-a", SellMarket: "market-b",
		},
		Candidates: []arbitrage.Candidate{{
			BuyQuote: market.Quote{
				AmountIn:  amount("quote-a", 750_000_000),
				AmountOut: amount("base-a", 3_000_000_000),
			},
			SellQuote: market.Quote{
				AmountIn:  amount("base-b", 3_000_000_000_000_000_000),
				AmountOut: amount("quote-b", 752_000_000),
			},
		}},
		SelectedIndex: 0,
	}
}
