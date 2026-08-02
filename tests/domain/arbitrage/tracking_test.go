package arbitrage_test

import (
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
)

func TestTrackingDurationsUseWallClockBoundariesWithoutDoubleCounting(t *testing.T) {
	start := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	timestamps := arbitrage.TrackingTimestamps{
		TriggerReceivedAt: start, SnapshotCapturedAt: start.Add(time.Millisecond), QueuedAt: start.Add(time.Millisecond),
		EvaluationStartedAt: start.Add(3 * time.Millisecond), BuyStartedAt: start.Add(3 * time.Millisecond),
		BuyFinishedAt: start.Add(5 * time.Millisecond), ConversionStartedAt: start.Add(5 * time.Millisecond),
		ConversionFinishedAt: start.Add(6 * time.Millisecond), SellStartedAt: start.Add(6 * time.Millisecond),
		SellFinishedAt: start.Add(9 * time.Millisecond), PnLStartedAt: start.Add(9 * time.Millisecond),
		PnLFinishedAt: start.Add(10 * time.Millisecond), PersistenceStartedAt: start.Add(10 * time.Millisecond),
		PersistenceFinishedAt: start.Add(12 * time.Millisecond), NotificationEnqueuedAt: start.Add(13 * time.Millisecond),
		EvaluationFinishedAt: start.Add(13 * time.Millisecond),
	}
	durations := timestamps.Durations()
	if durations.SnapshotCapture != time.Millisecond || durations.Queue != 2*time.Millisecond ||
		durations.BuyQuote != 2*time.Millisecond || durations.DecimalConversion != time.Millisecond ||
		durations.SellQuote != 3*time.Millisecond || durations.PnLCalculation != time.Millisecond ||
		durations.LocalCalculation != 7*time.Millisecond || durations.Persistence != 2*time.Millisecond ||
		durations.EventToEvaluation != 13*time.Millisecond || durations.EventToPersistedPoint != 12*time.Millisecond ||
		durations.NotificationEnqueue != time.Millisecond {
		t.Fatalf("unexpected tracking durations: %+v", durations)
	}
	// The principal duration is the direct event-to-finish wall clock. It is
	// intentionally not the sum of overlapping component timers.
	if durations.EventToEvaluation == durations.Queue+durations.LocalCalculation+durations.Persistence+durations.NotificationEnqueue {
		t.Fatal("principal duration was reconstructed by summing components")
	}
}
