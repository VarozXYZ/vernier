package saga

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/inventory"
	chainport "github.com/VarozXYZ/vernier/ports/chain"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	persistenceport "github.com/VarozXYZ/vernier/ports/persistence"
)

var (
	ErrOperationInFlight      = errors.New("a Live operation is already in flight")
	ErrArtifactTooOld         = errors.New("validated artifact exceeded the pre-commit operational timeout")
	ErrNoExecution            = errors.New("broadcast was conclusively rejected on every leg")
	ErrReconciliationRequired = errors.New("broadcast outcome requires reconciliation")
)

type ExecutorConfig struct {
	ConfigHash     string
	Inventory      *inventory.Inventory
	Store          persistenceport.OperationalStore
	Managers       map[execution.AccountID]chainport.TxManager
	ArtifactMaxAge time.Duration
	Clock          func() time.Time
	Gate           *OperationGate
}

type ExecutionTiming struct {
	Reservation       time.Duration
	Preparation       time.Duration
	Persistence       time.Duration
	CommitToBroadcast time.Duration
	Broadcast         time.Duration
}

type ExecutionResult struct {
	Operation              execution.Operation
	Broadcasts             []chainport.BroadcastResult
	Timing                 ExecutionTiming
	ReconciliationRequired bool
}

type Executor struct {
	config ExecutorConfig
	gate   *OperationGate
}

func NewExecutor(config ExecutorConfig) (*Executor, error) {
	if config.ConfigHash == "" || config.Inventory == nil || config.Store == nil ||
		len(config.Managers) < 2 || config.ArtifactMaxAge <= 0 || config.Clock == nil {
		return nil, fmt.Errorf("saga executor requires config hash, inventory, store, managers, timeout, and clock")
	}
	for account, manager := range config.Managers {
		if account == "" || manager == nil || manager.Account() != account {
			return nil, fmt.Errorf("saga executor manager binding is invalid")
		}
	}
	if config.Gate == nil {
		config.Gate = NewOperationGate()
	}
	return &Executor{config: config, gate: config.Gate}, nil
}

func (e *Executor) Gate() *OperationGate { return e.gate }

func (e *Executor) Warm(ctx context.Context) error {
	var group sync.WaitGroup
	errorsByAccount := make(chan error, len(e.config.Managers))
	for _, manager := range e.config.Managers {
		manager := manager
		group.Add(1)
		go func() {
			defer group.Done()
			if err := manager.Warm(ctx); err != nil {
				errorsByAccount <- err
			}
		}()
	}
	group.Wait()
	close(errorsByAccount)
	for err := range errorsByAccount {
		if err != nil {
			return err
		}
	}
	return nil
}

// Execute has no decision guard after CommitPrepared. The first statements
// after a successful commit capture the release timestamp and start both
// broadcasts.
func (e *Executor) Execute(ctx context.Context, operationID execution.OperationID, plan execution.SagaPlan, artifacts map[execution.StepID]executionport.Artifact) (ExecutionResult, error) {
	if !e.gate.TryBegin(operationID) {
		return ExecutionResult{}, ErrOperationInFlight
	}
	committed := false
	defer func() {
		if !committed {
			e.gate.Complete(operationID)
		}
	}()
	if operationID == "" || len(artifacts) != 2 {
		return ExecutionResult{}, fmt.Errorf("execution requires operation identity and two artifacts")
	}
	legs := plan.Legs()
	requirements := make([]inventory.Requirement, 0, len(legs))
	for _, leg := range legs {
		artifact, ok := artifacts[leg.ID]
		if !ok || artifact.Leg.ID != leg.ID {
			return ExecutionResult{}, fmt.Errorf("execution artifact for leg %q is missing", leg.ID)
		}
		requirements = append(requirements, inventory.Requirement{
			Key:    inventory.Key{Chain: leg.Chain, Account: leg.Account, Token: leg.Input.Token()},
			Amount: leg.Input,
		})
	}
	result := ExecutionResult{}
	reserveStarted := e.config.Clock()
	reservationID := inventory.ReservationID(operationID)
	reservation, err := e.config.Inventory.Reserve(reservationID, operationID, requirements)
	result.Timing.Reservation = elapsed(e.config.Clock, reserveStarted)
	if err != nil {
		return result, err
	}
	releaseReservation := true
	defer func() {
		if releaseReservation {
			_ = e.config.Inventory.Release(reservationID)
		}
	}()

	prepareStarted := e.config.Clock()
	prepared := make([]chainport.PreparedTransaction, len(legs))
	prepareErrors := make([]error, len(legs))
	var prepareGroup sync.WaitGroup
	for index, leg := range legs {
		index, leg := index, leg
		prepareGroup.Add(1)
		go func() {
			defer prepareGroup.Done()
			manager := e.config.Managers[leg.Account]
			prepared[index], prepareErrors[index] = manager.Prepare(ctx, artifacts[leg.ID])
		}()
	}
	prepareGroup.Wait()
	result.Timing.Preparation = elapsed(e.config.Clock, prepareStarted)
	for _, prepareErr := range prepareErrors {
		if prepareErr != nil {
			return result, prepareErr
		}
	}
	now := e.config.Clock().UTC()
	for _, artifact := range artifacts {
		age := now.Sub(artifact.BuiltAt)
		if artifact.BuiltAt.IsZero() || age < 0 || age > e.config.ArtifactMaxAge {
			return result, ErrArtifactTooOld
		}
	}

	steps := make([]execution.OperationStep, len(legs))
	for index, leg := range legs {
		var allocation *execution.RouteAllocation
		if artifacts[leg.ID].Allocation != nil {
			cloned := artifacts[leg.ID].Allocation.Clone()
			allocation = &cloned
		}
		steps[index] = execution.OperationStep{
			Operation: operationID, Leg: leg, Identity: prepared[index].Identity,
			Allocation: allocation,
			Technical:  execution.StatePrepared, Economic: execution.EconomicReserved,
		}
	}
	operation := execution.Operation{
		ID: operationID, Plan: plan.ID(), OpportunityID: plan.Opportunity().ID,
		ConfigHash: e.config.ConfigHash, Steps: steps,
		Economics: execution.EconomicsFromOpportunity(plan.Opportunity()),
		Technical: execution.StateCommitted, Economic: execution.EconomicReserved,
		CreatedAt: plan.CreatedAt(), CommittedAt: e.config.Clock().UTC(),
	}
	if err := operation.ValidatePrepared(); err != nil {
		return result, err
	}
	persistStarted := e.config.Clock()
	if err := e.config.Store.CommitPrepared(ctx, operation, reservation); err != nil {
		return result, err
	}
	committed = true
	result.Timing.Persistence = elapsed(e.config.Clock, persistStarted)

	// Deliberately no checks between durable commit and concurrent release.
	commitReturnedAt := e.config.Clock()
	broadcastStarted := e.config.Clock()
	broadcasts := make([]chainport.BroadcastResult, len(prepared))
	broadcastErrors := make([]error, len(prepared))
	var broadcastGroup sync.WaitGroup
	for index, transaction := range prepared {
		index, transaction := index, transaction
		broadcastGroup.Add(1)
		go func() {
			defer broadcastGroup.Done()
			manager := e.config.Managers[transaction.Leg.Account]
			broadcasts[index], broadcastErrors[index] = manager.Broadcast(ctx, transaction)
		}()
	}
	result.Timing.CommitToBroadcast = elapsedAt(broadcastStarted, commitReturnedAt)
	broadcastGroup.Wait()
	result.Timing.Broadcast = elapsed(e.config.Clock, broadcastStarted)
	result.Operation = operation
	result.Broadcasts = broadcasts
	result.ReconciliationRequired = true
	releaseReservation = false

	var accepted, rejected, unknown int
	for index, broadcastErr := range broadcastErrors {
		disposition := broadcasts[index].Disposition
		if disposition == "" {
			if broadcastErr == nil && broadcasts[index].Accepted {
				disposition = chainport.BroadcastAccepted
			} else if broadcastErr != nil {
				disposition = chainport.BroadcastPossible
			}
		}
		state := execution.StateBroadcastPossible
		detail := ""
		switch disposition {
		case chainport.BroadcastAccepted:
			accepted++
			detail = broadcasts[index].Endpoint
		case chainport.BroadcastRejected:
			state = execution.StateBroadcastRejected
			rejected++
			if broadcastErr != nil {
				detail = broadcastErr.Error()
			}
		default:
			state = execution.StateOutcomeUnknown
			unknown++
			if broadcastErr != nil {
				detail = broadcastErr.Error()
			}
		}
		if err := e.config.Store.RecordBroadcast(ctx, operationID, legs[index].ID, state, detail); err != nil {
			return result, err
		}
	}
	if rejected == len(legs) {
		result.ReconciliationRequired = false
		reason := "both broadcasts were conclusively rejected"
		if err := e.config.Store.MarkNoExecution(ctx, operationID, reason); err != nil {
			return result, err
		}
		if err := e.config.Inventory.Release(reservationID); err != nil {
			return result, err
		}
		e.gate.Complete(operationID)
		return result, fmt.Errorf("%w: %s", ErrNoExecution, reason)
	}
	if rejected > 0 || unknown > 0 {
		reason := fmt.Sprintf(
			"broadcast requires reconciliation: accepted=%d rejected=%d unknown=%d",
			accepted, rejected, unknown,
		)
		if err := e.config.Store.MarkManualIntervention(ctx, operationID, reason); err != nil {
			return result, err
		}
		return result, fmt.Errorf("%w: %s", ErrReconciliationRequired, reason)
	}
	return result, nil
}

func elapsed(clock func() time.Time, started time.Time) time.Duration {
	return elapsedAt(clock(), started)
}

func elapsedAt(finished, started time.Time) time.Duration {
	duration := finished.Sub(started)
	if duration < 0 {
		return 0
	}
	return duration
}
