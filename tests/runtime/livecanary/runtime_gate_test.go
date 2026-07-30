package livecanary_test

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	"github.com/VarozXYZ/vernier/runtime/configuration"
	"github.com/VarozXYZ/vernier/runtime/livecanary"
)

func TestRuntimeGateSerializesOperationalStates(t *testing.T) {
	gate := livecanary.NewRuntimeGate()
	if gate.EvaluationAllowed() {
		t.Fatal("starting gate must reject evaluations")
	}
	if err := gate.Transition(
		livecanary.RuntimeGateStarting,
		livecanary.RuntimeGateIdle,
	); err != nil {
		t.Fatal(err)
	}
	if !gate.EvaluationAllowed() {
		t.Fatal("idle gate must admit evaluations")
	}
	if err := gate.Transition(
		livecanary.RuntimeGateIdle,
		livecanary.RuntimeGateExecuting,
	); err != nil {
		t.Fatal(err)
	}
	if gate.EvaluationAllowed() {
		t.Fatal("executing gate admitted an evaluation")
	}
	if err := gate.Transition(
		livecanary.RuntimeGateIdle,
		livecanary.RuntimeGateRefueling,
	); err == nil {
		t.Fatal("stale lease transition unexpectedly succeeded")
	}
}

func TestRuntimeGateAllowsStartupRefuelReconciliation(t *testing.T) {
	gate := livecanary.NewRuntimeGate()
	if err := gate.Transition(
		livecanary.RuntimeGateStarting,
		livecanary.RuntimeGateRefueling,
	); err != nil {
		t.Fatalf("starting refuel transition: %v", err)
	}
	if gate.EvaluationAllowed() {
		t.Fatal("startup refuel gate admitted an evaluation")
	}
	if err := gate.Transition(
		livecanary.RuntimeGateRefueling,
		livecanary.RuntimeGateRecovering,
	); err != nil {
		t.Fatalf("refuel to recovery transition: %v", err)
	}
	if err := gate.Transition(
		livecanary.RuntimeGateRecovering,
		livecanary.RuntimeGateIdle,
	); err != nil {
		t.Fatalf("recovery to idle transition: %v", err)
	}
	if !gate.EvaluationAllowed() {
		t.Fatal("idle gate must admit evaluations after startup reconciliation")
	}
}

func TestRuntimeGateAllowsPostExecutionRefuelWithoutOpeningIdleWindow(t *testing.T) {
	gate := livecanary.NewRuntimeGate()
	if err := gate.Transition(
		livecanary.RuntimeGateStarting,
		livecanary.RuntimeGateIdle,
	); err != nil {
		t.Fatalf("starting to idle transition: %v", err)
	}
	if err := gate.Transition(
		livecanary.RuntimeGateIdle,
		livecanary.RuntimeGateExecuting,
	); err != nil {
		t.Fatalf("execution lease transition: %v", err)
	}
	if err := gate.Transition(
		livecanary.RuntimeGateExecuting,
		livecanary.RuntimeGateRefueling,
	); err != nil {
		t.Fatalf("execution to refuel transition: %v", err)
	}
	if gate.EvaluationAllowed() {
		t.Fatal("refueling gate admitted an evaluation")
	}
	if err := gate.Transition(
		livecanary.RuntimeGateRefueling,
		livecanary.RuntimeGateExecuting,
	); err != nil {
		t.Fatalf("refuel to execution transition: %v", err)
	}
	if gate.EvaluationAllowed() {
		t.Fatal("execution gate admitted an evaluation after refueling")
	}
	if err := gate.Transition(
		livecanary.RuntimeGateExecuting,
		livecanary.RuntimeGateIdle,
	); err != nil {
		t.Fatalf("execution to idle transition: %v", err)
	}
	if !gate.EvaluationAllowed() {
		t.Fatal("idle gate must admit evaluations after post-operation refueling")
	}
}

func TestRuntimeGateConcurrentLeaseHasSingleWinner(t *testing.T) {
	gate := livecanary.NewRuntimeGate()
	if err := gate.Transition(
		livecanary.RuntimeGateStarting,
		livecanary.RuntimeGateIdle,
	); err != nil {
		t.Fatal(err)
	}
	var winners int
	var mu sync.Mutex
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if gate.Transition(
				livecanary.RuntimeGateIdle,
				livecanary.RuntimeGateExecuting,
			) == nil {
				mu.Lock()
				winners++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if winners != 1 {
		t.Fatalf("exclusive gate winners = %d", winners)
	}
}

type fakeRefuelExecutor struct {
	chain        market.ChainID
	balance      executionport.RefuelBalance
	previewCalls int
	executeCalls int
	executeErr   error
}

func (e *fakeRefuelExecutor) Chain() market.ChainID { return e.chain }
func (e *fakeRefuelExecutor) Balance(context.Context) (executionport.RefuelBalance, error) {
	return e.balance, nil
}
func (e *fakeRefuelExecutor) Preview(
	_ context.Context,
	input market.AssetQuantity,
) (executionport.RefuelRecord, error) {
	e.previewCalls++
	amount, _ := market.NewTokenAmount("quote", big.NewInt(13_000_000))
	return executionport.RefuelRecord{
		ID: "preview", Chain: e.chain, State: executionport.RefuelPrepared,
		Input: amount,
	}, nil
}
func (e *fakeRefuelExecutor) Execute(
	_ context.Context,
	input market.AssetQuantity,
	_ executionport.RefuelJournal,
) (executionport.RefuelRecord, error) {
	e.executeCalls++
	amount, _ := market.NewTokenAmount("quote", big.NewInt(13_000_000))
	return executionport.RefuelRecord{
		ID: "armed", Chain: e.chain, State: executionport.RefuelCompleted,
		Input: amount,
	}, e.executeErr
}
func (e *fakeRefuelExecutor) Reconcile(
	_ context.Context,
	record executionport.RefuelRecord,
	_ executionport.RefuelJournal,
) (executionport.RefuelRecord, error) {
	return record, nil
}

type fakeRefuelJournal struct{}

func (fakeRefuelJournal) CreateRefuel(context.Context, executionport.RefuelRecord) error {
	return nil
}
func (fakeRefuelJournal) MarkRefuelBroadcast(
	context.Context,
	string,
	execution.TransactionIdentity,
) error {
	return nil
}
func (fakeRefuelJournal) FinishRefuel(context.Context, executionport.RefuelRecord) error {
	return nil
}
func (fakeRefuelJournal) ActiveRefuel(context.Context) (executionport.RefuelRecord, bool, error) {
	return executionport.RefuelRecord{}, false, nil
}
func (fakeRefuelJournal) LastCompletedRefuel(
	context.Context,
	market.ChainID,
) (executionport.RefuelRecord, bool, error) {
	return executionport.RefuelRecord{}, false, nil
}

func TestRefuelOnceIsDryRunUnlessArmed(t *testing.T) {
	gate := livecanary.NewRuntimeGate()
	if err := gate.Transition(
		livecanary.RuntimeGateStarting,
		livecanary.RuntimeGateIdle,
	); err != nil {
		t.Fatal(err)
	}
	executors := []*fakeRefuelExecutor{
		newFakeRefuelExecutor(t, "solana"),
		newFakeRefuelExecutor(t, "polygon"),
	}
	service := newRefuelService(t, gate, executors)
	if _, err := service.RefuelOnce(
		context.Background(),
		"solana",
		false,
	); err != nil {
		t.Fatal(err)
	}
	if executors[0].previewCalls != 1 ||
		executors[0].executeCalls != 0 {
		t.Fatalf(
			"dry-run preview=%d execute=%d",
			executors[0].previewCalls,
			executors[0].executeCalls,
		)
	}
	if _, err := service.RefuelOnce(
		context.Background(),
		"solana",
		true,
	); err != nil {
		t.Fatal(err)
	}
	if executors[0].executeCalls != 1 {
		t.Fatalf("armed executions = %d", executors[0].executeCalls)
	}
}

func TestUncertainRefuelBlocksGate(t *testing.T) {
	gate := livecanary.NewRuntimeGate()
	_ = gate.Transition(
		livecanary.RuntimeGateStarting,
		livecanary.RuntimeGateIdle,
	)
	executors := []*fakeRefuelExecutor{
		newFakeRefuelExecutor(t, "solana"),
		newFakeRefuelExecutor(t, "polygon"),
	}
	executors[0].executeErr = executionport.NewRecoveryError(
		executionport.RecoveryFailureUncertain,
		errors.New("broadcast outcome unknown"),
	)
	service := newRefuelService(t, gate, executors)
	if _, err := service.RefuelOnce(
		context.Background(),
		"solana",
		true,
	); err == nil {
		t.Fatal("expected uncertain refuel")
	}
	if gate.State() != livecanary.RuntimeGateRecoveryBlocked {
		t.Fatalf("gate state = %s", gate.State())
	}
}

func newFakeRefuelExecutor(
	t *testing.T,
	chain market.ChainID,
) *fakeRefuelExecutor {
	t.Helper()
	native, _ := market.NewAssetQuantity(
		market.AssetID(chain+"-native"),
		big.NewRat(1, 1),
	)
	value, _ := market.NewAssetQuantity("usd", big.NewRat(2, 1))
	return &fakeRefuelExecutor{
		chain: chain,
		balance: executionport.RefuelBalance{
			Chain: chain, Native: native, QuoteValue: value,
		},
	}
}

func newRefuelService(
	t *testing.T,
	gate *livecanary.RuntimeGate,
	executors []*fakeRefuelExecutor,
) *livecanary.RefuelService {
	t.Helper()
	service, err := livecanary.NewRefuelService(
		configuration.ResolvedGasRefuel{
			Enabled:      true,
			ThresholdUSD: big.NewRat(5, 1),
			TargetUSD:    big.NewRat(15, 1),
			MaxUSDC:      big.NewRat(20, 1),
			PollInterval: time.Minute,
			Cooldown:     time.Minute,
			SlippageBPS:  20,
		},
		gate,
		fakeRefuelJournal{},
		[]executionport.RefuelExecutor{executors[0], executors[1]},
		nil,
		time.Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	return service
}
