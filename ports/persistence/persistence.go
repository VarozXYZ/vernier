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
