// Package persistence defines durable Research records without exposing a
// database or serialization format to the domain and core.
package persistence

import (
	"context"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
)

type OpportunityStore interface {
	OpenWindow(context.Context, arbitrage.WindowOpening) error
	RecordImprovement(context.Context, arbitrage.WindowObservation) error
	CloseWindow(context.Context, arbitrage.WindowClosing) error
	FailWindow(context.Context, arbitrage.WindowFailure) error
	FinalizeDangling(context.Context, time.Time) error
	ListWindows(context.Context, arbitrage.WindowQuery) ([]arbitrage.WindowRecord, error)
	Close() error
}

// TrackingStore is an optional extension used by fixed-candidate Research.
// The point pointer allows the durable adapter to record the actual end of
// its own write and return that timestamp to reporting/notification code.
type TrackingStore interface {
	OpenTrackingWindow(context.Context, *arbitrage.TrackingWindow) error
	RecordTrackingPoint(context.Context, *arbitrage.TrackingPoint) error
	MarkTrackingNotificationEnqueued(context.Context, arbitrage.WindowID, uint64, time.Time) error
	CloseTrackingWindow(context.Context, arbitrage.TrackingWindowClosing) error
	SetTrackingMessage(context.Context, arbitrage.WindowID, int64) error
	TrackingMessage(context.Context, arbitrage.WindowID) (int64, bool, error)
	FinalizeDanglingTracking(context.Context, time.Time) ([]arbitrage.DanglingTrackingWindow, error)
}

// SimulationStore keeps the outcome of a read-only transaction simulation
// separate from the economic classification. A rejected or unavailable
// simulation is evidence about execution readiness, not a reason to erase a
// locally qualified market window.
type SimulationStore interface {
	RecordSimulationRound(context.Context, *arbitrage.SimulationRound) error
}
