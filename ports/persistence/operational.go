package persistence

import (
	"context"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/inventory"
)

// OperationalStore is separate from the Research opportunity store. Commit
// Prepared must durably commit the operation and reservations before it
// returns; callers broadcast immediately after that return.
type OperationalStore interface {
	CommitPrepared(context.Context, execution.Operation, inventory.Reservation) error
	RecordBroadcast(context.Context, execution.OperationID, execution.StepID, execution.TechnicalState, string) error
	RecordSettlement(context.Context, execution.Settlement) error
	MarkSettled(context.Context, execution.OperationID) error
	MarkManualIntervention(context.Context, execution.OperationID, string) error
	MarkNoExecution(context.Context, execution.OperationID, string) error
	History(context.Context, execution.OperationID) ([]execution.OperationalEvent, error)
	Pending(context.Context) ([]execution.Operation, error)
	Close() error
}

// RecoveryStore exposes persisted reservations only to the startup
// reconciliation path.
type RecoveryStore interface {
	OperationalStore
	Reservation(context.Context, execution.OperationID) (inventory.Reservation, error)
}
