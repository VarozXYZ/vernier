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

type recoveryStore struct {
	pending     []execution.Operation
	reservation inventory.Reservation
}

func (*recoveryStore) CommitPrepared(context.Context, execution.Operation, inventory.Reservation) error {
	return nil
}
func (*recoveryStore) RecordBroadcast(context.Context, execution.OperationID, execution.StepID, execution.TechnicalState, string) error {
	return nil
}
func (*recoveryStore) RecordSettlement(context.Context, execution.Settlement) error { return nil }
func (*recoveryStore) MarkSettled(context.Context, execution.OperationID) error     { return nil }
func (*recoveryStore) MarkManualIntervention(context.Context, execution.OperationID, string) error {
	return nil
}
func (*recoveryStore) MarkNoExecution(context.Context, execution.OperationID, string) error {
	return nil
}
func (*recoveryStore) History(context.Context, execution.OperationID) ([]execution.OperationalEvent, error) {
	return nil, nil
}
func (s *recoveryStore) Pending(context.Context) ([]execution.Operation, error) {
	return append([]execution.Operation(nil), s.pending...), nil
}
func (s *recoveryStore) Reservation(context.Context, execution.OperationID) (inventory.Reservation, error) {
	return s.reservation, nil
}
func (*recoveryStore) Close() error { return nil }

type oneShotConfirmer struct {
	calls atomic.Int32
	err   error
}

func (c *oneShotConfirmer) Confirm(context.Context, execution.Operation) error {
	c.calls.Add(1)
	return c.err
}

func TestRecoveryPerformsOneAttemptAndKeepsUnknownOperationBlocked(t *testing.T) {
	now := time.Date(2026, 7, 27, 18, 0, 0, 0, time.UTC)
	plan := executablePlan(t, now)
	operation := preparedOperation(plan, now)
	var requirements []inventory.Requirement
	balances := make(map[inventory.Key]market.TokenAmount)
	for _, leg := range plan.Legs() {
		key := inventory.Key{Chain: leg.Chain, Account: leg.Account, Token: leg.Input.Token()}
		requirements = append(requirements, inventory.Requirement{Key: key, Amount: leg.Input})
		balances[key] = tokenAmount(t, leg.Input.Token(), 1_000)
	}
	reservation, err := inventory.NewReservation(
		inventory.ReservationID(operation.ID), operation.ID, requirements,
	)
	if err != nil {
		t.Fatal(err)
	}
	store := &recoveryStore{pending: []execution.Operation{operation}, reservation: reservation}
	owner, err := inventory.New(balances)
	if err != nil {
		t.Fatal(err)
	}
	gate := saga.NewOperationGate()
	confirmer := &oneShotConfirmer{}
	recovery, err := saga.NewRecovery(saga.RecoveryConfig{
		Store: store, Inventory: owner, Confirmer: confirmer, Gate: gate,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = recovery.ReconcilePending(context.Background())
	if !errors.Is(err, saga.ErrRecoveryRequired) {
		t.Fatalf("recovery error = %v", err)
	}
	if confirmer.calls.Load() != 1 {
		t.Fatalf("confirmation attempts = %d", confirmer.calls.Load())
	}
	if active, ok := gate.Active(); !ok || active != operation.ID {
		t.Fatalf("recovery gate active=%q ok=%t", active, ok)
	}
	for _, requirement := range requirements {
		available, ok := owner.Available(requirement.Key)
		if !ok || available.Cmp(
			new(big.Int).Sub(big.NewInt(1_000), requirement.Amount.Units()),
		) != 0 {
			t.Fatalf("reservation was not restored for %+v", requirement.Key)
		}
	}
}
