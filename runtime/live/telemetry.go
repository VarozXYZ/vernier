package live

import (
	"time"

	corelive "github.com/VarozXYZ/vernier/core/live"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
)

type EventKind string

const (
	EventEvaluationCompleted EventKind = "evaluation_completed"
	EventConfirmationStarted EventKind = "confirmation_started"
	EventConfirmationEnded   EventKind = "confirmation_ended"
	EventCircuitOpened       EventKind = "circuit_opened"
	EventTriggerSkipped      EventKind = "trigger_skipped"
)

// Event is provider-neutral operational telemetry. It deliberately carries no
// signer material, transaction payload, addresses, or private route details.
type Event struct {
	Kind      EventKind
	At        time.Time
	Trigger   market.MarketID
	Operation execution.OperationID
	Result    *corelive.BatchResult
	Reason    string
}

type Observer interface {
	Observe(Event)
}

type ObserverFunc func(Event)

func (f ObserverFunc) Observe(event Event) {
	if f != nil {
		f(event)
	}
}

type noopObserver struct{}

func (noopObserver) Observe(Event) {}
