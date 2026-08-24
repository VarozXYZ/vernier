// Package chain defines transaction-manager capabilities owned per
// chain/account.
package chain

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
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

// PrimaryBroadcaster is used for operational transactions whose propagation
// is not latency-sensitive: approvals, bridges, redeems, and gas maintenance.
// Swap execution keeps using TxManager.Broadcast so its configured fanout is
// explicit and cannot leak into unrelated transaction classes.
type PrimaryBroadcaster interface {
	BroadcastPrimary(context.Context, PreparedTransaction) (BroadcastResult, error)
}

// ReceiptStatusReconciler proves only EVM inclusion and receipt status. It is
// intentionally separate from economic settlement decoding and is suitable
// for approvals and other transactions with no token input/output deltas.
type ReceiptStatusReconciler interface {
	ReconcileReceiptStatus(
		context.Context,
		execution.TransactionIdentity,
	) (execution.TechnicalState, error)
}

// BroadcastPrimary selects the primary-only capability when the concrete
// manager exposes it. The fallback preserves compatibility with chain
// managers whose Broadcast implementation already has a single transport.
func BroadcastPrimary(
	ctx context.Context,
	manager TxManager,
	prepared PreparedTransaction,
) (BroadcastResult, error) {
	if broadcaster, ok := manager.(PrimaryBroadcaster); ok {
		return broadcaster.BroadcastPrimary(ctx, prepared)
	}
	return manager.Broadcast(ctx, prepared)
}

// PreparedTransactionSimulator executes the exact signed payload against the
// current chain state without broadcasting it. Live uses this before durable
// persistence so a transaction that is already stale or would revert cannot
// enter the emission path.
type PreparedTransactionSimulator interface {
	SimulatePrepared(context.Context, PreparedTransaction) error
}

// EconomicSimulationRequest carries the local, versioned output balance used
// to derive an exact token delta without adding an account read to the hot
// path. EVM simulators may ignore OutputBalanceBefore when the router returns
// the amount directly.
type EconomicSimulationRequest struct {
	Prepared            PreparedTransaction
	OutputBalanceBefore *big.Int
	BalanceVersion      uint64
}

// EconomicSimulationResult is evidence from executing the exact signed
// payload against the node's current state. It is never an execution promise;
// it is the final pre-commit economic guard.
type EconomicSimulationResult struct {
	Input          market.TokenAmount
	Output         market.TokenAmount
	UnitsConsumed  uint64
	ContextVersion uint64
	Evidence       string
}

// EconomicOutputError is returned only after the exact payload simulated
// successfully but its economic output could not be decoded or attributed.
// Transport failures and on-chain reverts must use ordinary errors instead.
type EconomicOutputError struct {
	Err error
}

func (e *EconomicOutputError) Error() string {
	if e == nil || e.Err == nil {
		return "successful simulation has no attributable economic output"
	}
	return e.Err.Error()
}

func (e *EconomicOutputError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// EconomicPreparedTransactionSimulator is mandatory for prefunded_parallel.
// A nil/zero output after a technically successful simulation is an invariant
// failure and must block Live rather than fall back to provider estimates.
type EconomicPreparedTransactionSimulator interface {
	SimulatePreparedEconomic(
		context.Context,
		EconomicSimulationRequest,
	) (EconomicSimulationResult, error)
}

// EVMNonceCoordinator is the single in-process authority for the next nonce of
// one EVM account. Every component that broadcasts with that account must use
// the same coordinator, including swaps and bridge source/destination calls.
type EVMNonceCoordinator interface {
	NextNonce() (uint64, error)
	MarkNonceUsed(uint64)
}

// EVMNonceResynchronizer is an optional recovery capability. It is used only
// after every broadcast endpoint has definitively rejected one identity with
// nonce_too_low. Implementations must refresh from chain state without ever
// moving the local nonce backwards.
type EVMNonceResynchronizer interface {
	ResyncNonce(context.Context, uint64) (uint64, error)
}

// AllFanoutNonceTooLowError proves that no fanout endpoint accepted the
// transaction and every endpoint classified the same deterministic rejection.
// Callers may safely discard the rejected identity and rebuild with a fresh
// nonce; mixed or uncertain fanout outcomes must never use this path.
type AllFanoutNonceTooLowError struct {
	Nonce    uint64
	Attempts int
	Err      error
}

func (e *AllFanoutNonceTooLowError) Error() string {
	if e == nil {
		return "all EVM fanout endpoints rejected nonce as too low"
	}
	return fmt.Sprintf(
		"all %d EVM fanout endpoints rejected nonce %d as too low",
		e.Attempts,
		e.Nonce,
	)
}

func (e *AllFanoutNonceTooLowError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
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
