package livecanary

import (
	"fmt"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/domain/execution"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	notificationport "github.com/VarozXYZ/vernier/ports/notification"
)

// RecoveryObserver translates durable recovery transitions into the same
// ordered, off-hot-path notification queue as normal Live execution.
type RecoveryObserver struct {
	notifier *LiveNotifier
	progress *ProgressObserver
	clock    func() time.Time
	interval time.Duration

	mu        sync.Mutex
	lastSent  map[execution.OperationID]time.Time
	snapshots map[execution.OperationID]executionport.SequentialRecoverySnapshot
	balances  *BalanceManager
}

func (o *RecoveryObserver) SetBalanceManager(balances *BalanceManager) {
	if o != nil {
		o.balances = balances
	}
}

func NewRecoveryObserver(
	notifier *LiveNotifier,
	progress *ProgressObserver,
	clock func() time.Time,
) *RecoveryObserver {
	if clock == nil {
		clock = time.Now
	}
	return &RecoveryObserver{
		notifier: notifier, progress: progress,
		clock: clock, interval: 5 * time.Minute,
		lastSent:  make(map[execution.OperationID]time.Time),
		snapshots: make(map[execution.OperationID]executionport.SequentialRecoverySnapshot),
	}
}

func (o *RecoveryObserver) RecoveryStarted(
	snapshot executionport.SequentialRecoverySnapshot,
) {
	if o == nil {
		return
	}
	now := o.clock().UTC()
	o.mu.Lock()
	o.lastSent[snapshot.Operation.ID] = now
	o.snapshots[snapshot.Operation.ID] = snapshot
	o.mu.Unlock()
	if o.balances != nil {
		// Startup bootstrap already observed the physical effects of durable
		// settlements. Mark them as known without applying them twice.
		o.balances.MarkSettlementsObserved(snapshot.Settlements)
	}
	if o.notifier == nil {
		return
	}
	if o.progress != nil {
		// Rebuild the notification projection from durable state. This makes
		// recovery idempotent across process restarts and clears stale pending
		// stages once their settlements already exist in SQLite.
		o.progress.OperationStarted(snapshot.Operation, snapshot.Plan)
		for _, settlement := range snapshot.Settlements {
			o.progress.StageSettled(settlement)
		}
		if snapshot.ExitDecision != nil {
			o.progress.ExitSelected(*snapshot.ExitDecision)
		}
	}
	o.notifier.Notify(notificationport.LiveExecutionEvent{
		Kind:      notificationport.LiveExecutionRecoveryStarted,
		Operation: string(snapshot.Operation.ID),
		State:     string(snapshot.Operation.State),
		Detail: fmt.Sprintf(
			"stage %d/4 · %s",
			snapshot.Operation.CurrentStage+1,
			snapshot.Operation.LastError,
		),
		OccurredAt: now,
	})
}

func (o *RecoveryObserver) RecoveryAttempt(
	attempt executionport.SequentialRecoveryAttempt,
) {
	if o == nil || o.notifier == nil {
		return
	}
	now := o.clock().UTC()
	o.mu.Lock()
	last := o.lastSent[attempt.Operation]
	if !last.IsZero() && now.Sub(last) < o.interval {
		o.mu.Unlock()
		return
	}
	o.lastSent[attempt.Operation] = now
	o.mu.Unlock()
	o.notifier.Notify(notificationport.LiveExecutionEvent{
		Kind:      notificationport.LiveExecutionRecoveryProgress,
		Operation: string(attempt.Operation),
		Ordinal:   attempt.Ordinal,
		State:     attempt.Reason,
		Detail: fmt.Sprintf(
			"%s · attempt %d · next %s",
			attempt.Action,
			attempt.Attempt,
			attempt.RetryAt.UTC().Format(time.RFC3339),
		),
		OccurredAt: now,
	})
}

func (o *RecoveryObserver) RecoveryCompleted(
	result executionport.SequentialResult,
) {
	if o == nil {
		return
	}
	o.mu.Lock()
	snapshot, found := o.snapshots[result.Operation]
	o.mu.Unlock()
	if o.balances != nil {
		for _, settlement := range result.Settlements {
			if err := o.balances.ObserveSettlement(settlement); err != nil &&
				o.balances.config.Logger != nil {
				o.balances.config.Logger.Error(
					"apply recovered local balance settlement",
					"error", err,
				)
			}
		}
	}
	if o.notifier == nil {
		o.forget(result.Operation)
		return
	}
	if o.progress != nil && found {
		for _, settlement := range result.Settlements {
			o.progress.StageSettled(settlement)
		}
		if result.ExitDecision != nil {
			o.progress.ExitSelected(*result.ExitDecision)
		}
	}
	o.notifier.Notify(notificationport.LiveExecutionEvent{
		Kind:       notificationport.LiveExecutionRecoveryCompleted,
		Operation:  string(result.Operation),
		State:      string(execution.SequentialCompleted),
		Output:     result.FinalAmount.String(),
		OccurredAt: o.clock().UTC(),
	})
	if o.progress != nil && found {
		o.progress.OperationFinished(
			snapshot.Operation,
			execution.SequentialCompleted,
			result,
			nil,
		)
	}
	o.forget(result.Operation)
}

func (o *RecoveryObserver) RecoveryAborted(
	operation execution.SequentialOperation,
	result executionport.SequentialResult,
	cause error,
) {
	if o == nil {
		return
	}
	o.mu.Lock()
	snapshot, found := o.snapshots[result.Operation]
	o.mu.Unlock()
	if o.progress != nil && found {
		o.progress.OperationFinished(
			snapshot.Operation,
			execution.SequentialAborted,
			result,
			cause,
		)
	}
	if o.notifier != nil {
		detail := "recovery proved that no economic effect occurred"
		if cause != nil {
			detail = cause.Error()
		}
		o.notifier.Notify(notificationport.LiveExecutionEvent{
			Kind:       notificationport.LiveExecutionFailed,
			Operation:  string(operation.ID),
			State:      string(execution.SequentialAborted),
			Detail:     detail,
			OccurredAt: o.clock().UTC(),
		})
	}
	o.forget(result.Operation)
}

func (o *RecoveryObserver) RecoveryBlocked(
	operation execution.SequentialOperation,
	cause error,
) {
	if o == nil || o.notifier == nil {
		return
	}
	o.forget(operation.ID)
	detail := "manual reconciliation required"
	if cause != nil {
		detail = cause.Error()
	}
	o.notifier.Notify(notificationport.LiveExecutionEvent{
		Kind:       notificationport.LiveExecutionRecoveryBlocked,
		Operation:  string(operation.ID),
		State:      string(execution.SequentialRecoveryBlocked),
		Detail:     detail,
		OccurredAt: o.clock().UTC(),
	})
}

func (o *RecoveryObserver) forget(operation execution.OperationID) {
	o.mu.Lock()
	delete(o.lastSent, operation)
	delete(o.snapshots, operation)
	o.mu.Unlock()
}
