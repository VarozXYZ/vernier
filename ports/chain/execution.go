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

// PreparedTransactionSimulator executes the exact signed payload against the
// current chain state without broadcasting it. Live uses this before durable
// persistence so a transaction that is already stale or would revert cannot
// enter the emission path.
type PreparedTransactionSimulator interface {
	SimulatePrepared(context.Context, PreparedTransaction) error
}

// EVMNonceCoordinator is the single in-process authority for the next nonce of
// one EVM account. Every component that broadcasts with that account must use
// the same coordinator, including swaps and bridge source/destination calls.
type EVMNonceCoordinator interface {
	NextNonce() (uint64, error)
	MarkNonceUsed(uint64)
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
