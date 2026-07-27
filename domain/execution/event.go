package execution

import "time"

type EventKind string

const (
	EventOperationCommitted EventKind = "operation_committed"
	EventBroadcastObserved  EventKind = "broadcast_observed"
	EventSettlementObserved EventKind = "settlement_observed"
	EventOperationCompleted EventKind = "operation_completed"
	EventNoExecution        EventKind = "no_execution_proven"
	EventManualIntervention EventKind = "manual_intervention_required"
)

// OperationalEvent is immutable journal evidence. Sequence is assigned by the
// persistence adapter and orders events inside one OperationalStore.
type OperationalEvent struct {
	Sequence   uint64
	Operation  OperationID
	Step       StepID
	Kind       EventKind
	Technical  TechnicalState
	Economic   EconomicState
	Detail     string
	Evidence   string
	OccurredAt time.Time
}
