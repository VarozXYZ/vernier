package saga_test

import (
	"context"
	"errors"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/core/saga"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/inventory"
	"github.com/VarozXYZ/vernier/domain/market"
)

type settlementStore struct {
	settlements atomic.Int32
	manual      atomic.Int32
}

func (*settlementStore) CommitPrepared(context.Context, execution.Operation, inventory.Reservation) error {
	return nil
}
func (*settlementStore) RecordBroadcast(context.Context, execution.OperationID, execution.StepID, execution.TechnicalState, string) error {
	return nil
}
func (s *settlementStore) RecordSettlement(context.Context, execution.Settlement) error {
	s.settlements.Add(1)
	return nil
}
func (*settlementStore) MarkSettled(context.Context, execution.OperationID) error { return nil }
func (s *settlementStore) MarkManualIntervention(context.Context, execution.OperationID, string) error {
	s.manual.Add(1)
	return nil
}
func (*settlementStore) MarkNoExecution(context.Context, execution.OperationID, string) error {
	return nil
}
func (*settlementStore) History(context.Context, execution.OperationID) ([]execution.OperationalEvent, error) {
	return nil, nil
}
func (*settlementStore) Pending(context.Context) ([]execution.Operation, error) { return nil, nil }
func (*settlementStore) Close() error                                           { return nil }

func TestSettlementAppliesObservedEffectsOnlyAfterBothLegs(t *testing.T) {
	now := time.Date(2026, 7, 27, 16, 0, 0, 0, time.UTC)
	plan := executablePlan(t, now)
	operation := preparedOperation(plan, now)
	owner, err := inventory.New(map[inventory.Key]market.TokenAmount{
		{Chain: "chain-a", Account: "account-a", Token: "quote-a"}: tokenAmount(t, "quote-a", 1_000),
		{Chain: "chain-a", Account: "account-a", Token: "base-a"}:  tokenAmount(t, "base-a", 1_000),
		{Chain: "chain-b", Account: "account-b", Token: "base-b"}:  tokenAmount(t, "base-b", 1_000),
		{Chain: "chain-b", Account: "account-b", Token: "quote-b"}: tokenAmount(t, "quote-b", 1_000),
	})
	if err != nil {
		t.Fatal(err)
	}
	requirements := make([]inventory.Requirement, 0, 2)
	for _, leg := range plan.Legs() {
		requirements = append(requirements, inventory.Requirement{
			Key:    inventory.Key{Chain: leg.Chain, Account: leg.Account, Token: leg.Input.Token()},
			Amount: leg.Input,
		})
	}
	if _, err := owner.Reserve("operation-1", "operation-1", requirements); err != nil {
		t.Fatal(err)
	}
	store := &settlementStore{}
	gate := saga.NewOperationGate()
	if !gate.Restore(operation.ID) {
		t.Fatal("could not restore operation gate")
	}
	coordinator, err := saga.NewSettlementCoordinator(store, owner, gate)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Register(operation); err != nil {
		t.Fatal(err)
	}
	buy := operation.Steps[0]
	sell := operation.Steps[1]
	if err := coordinator.Observe(context.Background(), verifiedSettlement(t, operation.ID, buy, 100, 150, now)); err != nil {
		t.Fatal(err)
	}
	if available(t, owner, inventory.Key{Chain: "chain-a", Account: "account-a", Token: "quote-a"}).Cmp(big.NewInt(900)) != 0 {
		t.Fatal("first settlement released a shared reservation")
	}
	if err := coordinator.Observe(context.Background(), verifiedSettlement(t, operation.ID, sell, 145, 101, now)); err != nil {
		t.Fatal(err)
	}
	assertAvailable(t, owner, inventory.Key{Chain: "chain-a", Account: "account-a", Token: "quote-a"}, 900)
	assertAvailable(t, owner, inventory.Key{Chain: "chain-a", Account: "account-a", Token: "base-a"}, 1_150)
	assertAvailable(t, owner, inventory.Key{Chain: "chain-b", Account: "account-b", Token: "base-b"}, 855)
	assertAvailable(t, owner, inventory.Key{Chain: "chain-b", Account: "account-b", Token: "quote-b"}, 1_101)
	if store.settlements.Load() != 2 || store.manual.Load() != 0 {
		t.Fatalf("settlements=%d manual=%d", store.settlements.Load(), store.manual.Load())
	}
	if _, active := gate.Active(); active {
		t.Fatal("verified two-leg settlement did not release operation gate")
	}
}

func TestSettlementMismatchKeepsReservationAndOpensManualBreaker(t *testing.T) {
	now := time.Date(2026, 7, 27, 16, 0, 0, 0, time.UTC)
	plan := executablePlan(t, now)
	operation := preparedOperation(plan, now)
	owner, _ := inventory.New(map[inventory.Key]market.TokenAmount{
		{Chain: "chain-a", Account: "account-a", Token: "quote-a"}: tokenAmount(t, "quote-a", 1_000),
		{Chain: "chain-a", Account: "account-a", Token: "base-a"}:  tokenAmount(t, "base-a", 1_000),
		{Chain: "chain-b", Account: "account-b", Token: "base-b"}:  tokenAmount(t, "base-b", 1_000),
		{Chain: "chain-b", Account: "account-b", Token: "quote-b"}: tokenAmount(t, "quote-b", 1_000),
	})
	var requirements []inventory.Requirement
	for _, leg := range plan.Legs() {
		requirements = append(requirements, inventory.Requirement{
			Key:    inventory.Key{Chain: leg.Chain, Account: leg.Account, Token: leg.Input.Token()},
			Amount: leg.Input,
		})
	}
	if _, err := owner.Reserve("operation-1", "operation-1", requirements); err != nil {
		t.Fatal(err)
	}
	store := &settlementStore{}
	gate := saga.NewOperationGate()
	if !gate.Restore(operation.ID) {
		t.Fatal("could not restore operation gate")
	}
	coordinator, _ := saga.NewSettlementCoordinator(store, owner, gate)
	if err := coordinator.Register(operation); err != nil {
		t.Fatal(err)
	}
	first := verifiedSettlement(t, operation.ID, operation.Steps[0], 100, 150, now)
	second := verifiedSettlement(t, operation.ID, operation.Steps[1], 145, 101, now)
	second.Economic = execution.EconomicEffectMismatch
	if err := coordinator.Observe(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Observe(context.Background(), second); !errors.Is(
		err, saga.ErrManualInterventionRequired,
	) {
		t.Fatalf("manual intervention error = %v", err)
	}
	assertAvailable(t, owner, inventory.Key{Chain: "chain-a", Account: "account-a", Token: "quote-a"}, 900)
	assertAvailable(t, owner, inventory.Key{Chain: "chain-b", Account: "account-b", Token: "base-b"}, 855)
	if store.manual.Load() != 1 {
		t.Fatalf("manual interventions = %d", store.manual.Load())
	}
	if active, ok := gate.Active(); !ok || active != operation.ID {
		t.Fatalf("manual circuit breaker released operation gate: active=%q ok=%v", active, ok)
	}
}

func preparedOperation(plan execution.SagaPlan, now time.Time) execution.Operation {
	steps := make([]execution.OperationStep, 0, 2)
	for _, leg := range plan.Legs() {
		steps = append(steps, execution.OperationStep{
			Leg: leg,
			Identity: execution.TransactionIdentity{
				Chain: leg.Chain, Account: leg.Account, Hash: string(leg.ID) + "-hash",
			},
			Technical: execution.StatePrepared, Economic: execution.EconomicReserved,
		})
	}
	return execution.Operation{
		ID: "operation-1", Plan: plan.ID(), OpportunityID: plan.Opportunity().ID,
		ConfigHash: "synthetic-config", Steps: steps,
		Economics: execution.EconomicsFromOpportunity(plan.Opportunity()),
		Technical: execution.StateCommitted, Economic: execution.EconomicReserved,
		CreatedAt: now, CommittedAt: now,
	}
}

func verifiedSettlement(t *testing.T, operation execution.OperationID, step execution.OperationStep, actualIn, actualOut int64, now time.Time) execution.Settlement {
	t.Helper()
	return execution.Settlement{
		Operation: operation, Step: step.Leg.ID, Identity: step.Identity,
		Technical: execution.StateConfirmedSuccess, Economic: execution.EconomicEffectVerified,
		ActualIn:   tokenAmount(t, step.Leg.Input.Token(), actualIn),
		ActualOut:  tokenAmount(t, step.Leg.ExpectedOutput.Token(), actualOut),
		ObservedAt: now, Evidence: "synthetic-event",
	}
}

func available(t *testing.T, owner *inventory.Inventory, key inventory.Key) *big.Int {
	t.Helper()
	value, ok := owner.Available(key)
	if !ok {
		t.Fatalf("missing inventory key %+v", key)
	}
	return value
}

func assertAvailable(t *testing.T, owner *inventory.Inventory, key inventory.Key, want int64) {
	t.Helper()
	if got := available(t, owner, key); got.Cmp(big.NewInt(want)) != 0 {
		t.Fatalf("available %+v = %s, want %d", key, got, want)
	}
}
