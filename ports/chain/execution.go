// Package chain defines transaction-manager capabilities owned per
// chain/account.
package chain

import (
	"context"
	"time"

	"github.com/VarozXYZ/vernier/domain/execution"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

// PreparedTransaction is intentionally operational rather than economic.
// SignedPayload exists only in memory and is never handed to persistence.
type PreparedTransaction struct {
	Leg           execution.Leg
	Identity      execution.TransactionIdentity
	SignedPayload []byte
	PreparedAt    time.Time
}

type BroadcastDisposition string

const (
	BroadcastAccepted BroadcastDisposition = "accepted"
	BroadcastRejected BroadcastDisposition = "rejected"
	BroadcastPossible BroadcastDisposition = "possible"
)

type BroadcastResult struct {
	Identity    execution.TransactionIdentity
	Disposition BroadcastDisposition
	Accepted    bool
	Endpoint    string
	Attempts    int
	AcceptedAt  time.Time
}

type TxManager interface {
	Account() execution.AccountID
	Warm(context.Context) error
	Prepare(context.Context, executionport.Artifact) (PreparedTransaction, error)
	Broadcast(context.Context, PreparedTransaction) (BroadcastResult, error)
	Reconcile(context.Context, execution.OperationStep) (execution.Settlement, error)
}

// ConfirmationSource is the primary WebSocket evidence path. Implementations
// must bind an observation to the supplied durable step and include actual
// economic amounts when reporting success.
type ConfirmationSource interface {
	// Warm establishes the persistent subscription before any operation may
	// broadcast, avoiding a post-broadcast subscription race.
	Warm(context.Context) error
	Await(context.Context, execution.OperationStep) (execution.Settlement, error)
}
