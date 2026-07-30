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
	clock    func() time.Time
	interval time.Duration

	mu       sync.Mutex
	lastSent map[execution.OperationID]time.Time
}

func NewRecoveryObserver(
	notifier *LiveNotifier,
	clock func() time.Time,
) *RecoveryObserver {
	if clock == nil {
		clock = time.Now
	}
	return &RecoveryObserver{
		notifier: notifier, clock: clock, interval: 5 * time.Minute,
		lastSent: make(map[execution.OperationID]time.Time),
	}
}

func (o *RecoveryObserver) RecoveryStarted(
	snapshot executionport.SequentialRecoverySnapshot,
) {
	if o == nil || o.notifier == nil {
		return
	}
	now := o.clock().UTC()
	o.mu.Lock()
	o.lastSent[snapshot.Operation.ID] = now
	o.mu.Unlock()
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
	if o == nil || o.notifier == nil {
		return
	}
	o.forget(result.Operation)
	o.notifier.Notify(notificationport.LiveExecutionEvent{
		Kind:       notificationport.LiveExecutionRecoveryCompleted,
		Operation:  string(result.Operation),
		State:      string(execution.SequentialCompleted),
		Output:     result.FinalAmount.String(),
		OccurredAt: o.clock().UTC(),
	})
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
	o.mu.Unlock()
}
