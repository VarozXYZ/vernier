package saga

import (
	"context"
	"errors"
	"fmt"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/inventory"
	persistenceport "github.com/VarozXYZ/vernier/ports/persistence"
)

var ErrRecoveryRequired = errors.New("live startup remains blocked by an unresolved operation")

type RecoveryConfig struct {
	Store     persistenceport.RecoveryStore
	Inventory *inventory.Inventory
	Confirmer OperationConfirmer
	Gate      *OperationGate
}

type OperationConfirmer interface {
	Confirm(context.Context, execution.Operation) error
}

// Recovery restores durable reservations and reconciles pending transaction
// identities. It never prepares, signs, or broadcasts a transaction.
type Recovery struct {
	config RecoveryConfig
}

func NewRecovery(config RecoveryConfig) (*Recovery, error) {
	if config.Store == nil || config.Inventory == nil || config.Confirmer == nil || config.Gate == nil {
		return nil, fmt.Errorf("recovery requires store, inventory, confirmer, and operation gate")
	}
	return &Recovery{config: config}, nil
}

func (r *Recovery) ReconcilePending(ctx context.Context) error {
	pending, err := r.config.Store.Pending(ctx)
	if err != nil {
		return err
	}
	if len(pending) == 0 {
		return nil
	}
	if len(pending) > 1 {
		return fmt.Errorf("%w: found %d operations under one-active-operation policy", ErrRecoveryRequired, len(pending))
	}
	operation := pending[0]
	reservation, err := r.config.Store.Reservation(ctx, operation.ID)
	if err != nil {
		return err
	}
	if _, err := r.config.Inventory.Reserve(
		reservation.ID, operation.ID, reservation.Requirements(),
	); err != nil {
		return err
	}
	if active, exists := r.config.Gate.Active(); !exists {
		if !r.config.Gate.Restore(operation.ID) {
			return ErrRecoveryRequired
		}
	} else if active != operation.ID {
		return fmt.Errorf("%w: operation gate belongs to %q", ErrRecoveryRequired, active)
	}
	if err := r.config.Confirmer.Confirm(ctx, operation); err != nil {
		return err
	}
	remaining, err := r.config.Store.Pending(ctx)
	if err != nil {
		return err
	}
	if len(remaining) > 0 {
		return ErrRecoveryRequired
	}
	return nil
}
