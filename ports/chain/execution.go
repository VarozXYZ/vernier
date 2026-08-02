// Package chain defines transaction-manager capabilities owned per
// chain/account.
package chain

import (
	"context"
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

// ConfirmationSource is the primary WebSocket evidence path. Implementations
// must bind an observation to the supplied durable step and include actual
// economic amounts when reporting success.
type ConfirmationSource interface {
	// Warm establishes the persistent subscription before any operation may
	// broadcast, avoiding a post-broadcast subscription race.
	Warm(context.Context) error
	Await(context.Context, execution.OperationStep) (execution.Settlement, error)
}
