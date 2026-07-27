package saga

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/inventory"
	persistenceport "github.com/VarozXYZ/vernier/ports/persistence"
)

var ErrManualInterventionRequired = errors.New("manual intervention is required")

// SettlementCoordinator joins the two independently observed economic
// effects. A single failed, mismatched, or unknown leg retains reservations
// and opens the manual circuit breaker.
type SettlementCoordinator struct {
	mu         sync.Mutex
	store      persistenceport.OperationalStore
	inventory  *inventory.Inventory
	gate       *OperationGate
	operations map[execution.OperationID]execution.Operation
	observed   map[execution.OperationID]map[execution.StepID]execution.Settlement
}

func NewSettlementCoordinator(store persistenceport.OperationalStore, owner *inventory.Inventory, gates ...*OperationGate) (*SettlementCoordinator, error) {
	if store == nil || owner == nil {
		return nil, fmt.Errorf("settlement coordinator requires store and inventory")
	}
	gate := NewOperationGate()
	if len(gates) > 0 {
		if len(gates) != 1 || gates[0] == nil {
			return nil, fmt.Errorf("settlement coordinator accepts one non-nil operation gate")
		}
		gate = gates[0]
	}
	return &SettlementCoordinator{
		store: store, inventory: owner, gate: gate,
		operations: make(map[execution.OperationID]execution.Operation),
		observed:   make(map[execution.OperationID]map[execution.StepID]execution.Settlement),
	}, nil
}

func (c *SettlementCoordinator) Register(operation execution.Operation) error {
	if err := operation.ValidatePrepared(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.operations[operation.ID] = operation
	return nil
}

func (c *SettlementCoordinator) Observe(ctx context.Context, settlement execution.Settlement) error {
	if settlement.Operation == "" || settlement.Step == "" || settlement.ObservedAt.IsZero() {
		return fmt.Errorf("settlement observation is incomplete")
	}
	if err := c.store.RecordSettlement(ctx, settlement); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	operation, ok := c.operations[settlement.Operation]
	if !ok {
		return fmt.Errorf("settlement operation %q is not registered", settlement.Operation)
	}
	if c.observed[settlement.Operation] == nil {
		c.observed[settlement.Operation] = make(map[execution.StepID]execution.Settlement)
	}
	c.observed[settlement.Operation][settlement.Step] = settlement
	if len(c.observed[settlement.Operation]) < len(operation.Steps) {
		return nil
	}
	var effects []inventory.Effect
	for _, step := range operation.Steps {
		observed, exists := c.observed[operation.ID][step.Leg.ID]
		if !exists || observed.Technical != execution.StateConfirmedSuccess ||
			observed.Economic != execution.EconomicEffectVerified ||
			observed.ActualIn.Token() != step.Leg.Input.Token() ||
			observed.ActualOut.Token() != step.Leg.ExpectedOutput.Token() {
			reason := "settlement mismatch or failed leg"
			if err := c.store.MarkManualIntervention(ctx, operation.ID, reason); err != nil {
				return err
			}
			return fmt.Errorf("%w: %s", ErrManualInterventionRequired, reason)
		}
		effects = append(effects,
			inventory.Effect{
				Key:   inventory.Key{Chain: step.Leg.Chain, Account: step.Leg.Account, Token: observed.ActualIn.Token()},
				Delta: new(big.Int).Neg(observed.ActualIn.Units()),
			},
			inventory.Effect{
				Key:   inventory.Key{Chain: step.Leg.Chain, Account: step.Leg.Account, Token: observed.ActualOut.Token()},
				Delta: observed.ActualOut.Units(),
			},
		)
	}
	if err := c.inventory.Settle(inventory.ReservationID(operation.ID), effects); err != nil {
		_ = c.store.MarkManualIntervention(ctx, operation.ID, "inventory settlement failed")
		return errors.Join(ErrManualInterventionRequired, err)
	}
	if err := c.store.MarkSettled(ctx, operation.ID); err != nil {
		_ = c.store.MarkManualIntervention(ctx, operation.ID, "durable inventory settlement failed")
		return errors.Join(ErrManualInterventionRequired, err)
	}
	delete(c.operations, operation.ID)
	delete(c.observed, operation.ID)
	c.gate.Complete(operation.ID)
	return nil
}
