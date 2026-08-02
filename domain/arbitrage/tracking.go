package arbitrage

import (
	"fmt"
	"time"

	"github.com/VarozXYZ/vernier/domain/market"
)

// TrackingTimestamps are the durable clock boundaries for one fixed-candidate
// observation. All values are UTC wall-clock instants; derived durations are
// calculated from them so overlapping work is never double counted.
type TrackingTimestamps struct {
	TriggerReceivedAt      time.Time
	SnapshotCapturedAt     time.Time
	QueuedAt               time.Time
	EvaluationStartedAt    time.Time
	BuyStartedAt           time.Time
	BuyFinishedAt          time.Time
	ConversionStartedAt    time.Time
	ConversionFinishedAt   time.Time
	SellStartedAt          time.Time
	SellFinishedAt         time.Time
	PnLStartedAt           time.Time
	PnLFinishedAt          time.Time
	PersistenceStartedAt   time.Time
	PersistenceFinishedAt  time.Time
	NotificationEnqueuedAt time.Time
	EvaluationFinishedAt   time.Time
}

type TrackingDurations struct {
	SnapshotCapture       time.Duration
	Queue                 time.Duration
	BuyQuote              time.Duration
	DecimalConversion     time.Duration
	SellQuote             time.Duration
	PnLCalculation        time.Duration
	LocalCalculation      time.Duration
	Persistence           time.Duration
	EventToEvaluation     time.Duration
	EventToPersistedPoint time.Duration
	NotificationEnqueue   time.Duration
}

func (t TrackingTimestamps) Durations() TrackingDurations {
	return TrackingDurations{
		SnapshotCapture:       elapsed(t.TriggerReceivedAt, t.SnapshotCapturedAt),
		Queue:                 elapsed(t.QueuedAt, t.EvaluationStartedAt),
		BuyQuote:              elapsed(t.BuyStartedAt, t.BuyFinishedAt),
		DecimalConversion:     elapsed(t.ConversionStartedAt, t.ConversionFinishedAt),
		SellQuote:             elapsed(t.SellStartedAt, t.SellFinishedAt),
		PnLCalculation:        elapsed(t.PnLStartedAt, t.PnLFinishedAt),
		LocalCalculation:      elapsed(t.EvaluationStartedAt, t.PnLFinishedAt),
		Persistence:           elapsed(t.PersistenceStartedAt, t.PersistenceFinishedAt),
		EventToEvaluation:     elapsed(t.TriggerReceivedAt, t.EvaluationFinishedAt),
		EventToPersistedPoint: elapsed(t.TriggerReceivedAt, t.PersistenceFinishedAt),
		NotificationEnqueue:   elapsed(t.PersistenceFinishedAt, t.NotificationEnqueuedAt),
	}
}

func elapsed(start, finish time.Time) time.Duration {
	if start.IsZero() || finish.IsZero() || finish.Before(start) {
		return 0
	}
	return finish.Sub(start)
}

type TrackingSnapshot struct {
	Market    market.MarketID
	Version   uint64
	StateHash [32]byte
}

type TrackingPoint struct {
	ID                   string
	WindowID             WindowID
	Sequence             uint64
	Evaluation           EvaluationID
	Trigger              TriggerMetadata
	Timestamps           TrackingTimestamps
	Snapshots            []TrackingSnapshot
	Input                market.AssetQuantity
	BuyOutput            market.AssetQuantity
	SellOutput           market.AssetQuantity
	GrossPnL             market.AssetQuantity
	NetPnL               market.AssetQuantity
	FixedThreshold       market.AssetQuantity
	PercentageThreshold  market.AssetQuantity
	EffectiveThreshold   market.AssetQuantity
	DeltaFromOpening     market.AssetQuantity
	DeltaFromPrevious    market.AssetQuantity
	Classification       Classification
	Reason               string
	EconomicChange       bool
	IntervalFromPrevious time.Duration
	SinceOpening         time.Duration
}

func (p TrackingPoint) Validate() error {
	if p.ID == "" || p.WindowID == "" || p.Sequence == 0 || p.Evaluation == "" {
		return fmt.Errorf("tracking point identity is required")
	}
	if err := p.Trigger.Validate(); err != nil {
		return err
	}
	if p.Timestamps.TriggerReceivedAt.IsZero() || p.Timestamps.SnapshotCapturedAt.IsZero() ||
		p.Timestamps.EvaluationStartedAt.IsZero() || p.Timestamps.PersistenceStartedAt.IsZero() {
		return fmt.Errorf("tracking point clock boundaries are incomplete")
	}
	asset := p.Input.Asset()
	for _, quantity := range []market.AssetQuantity{
		p.SellOutput, p.GrossPnL, p.NetPnL, p.FixedThreshold,
		p.PercentageThreshold, p.EffectiveThreshold, p.DeltaFromOpening,
		p.DeltaFromPrevious,
	} {
		if quantity.Asset() != asset {
			return fmt.Errorf("tracking point quote quantities must use asset %q", asset)
		}
	}
	if p.BuyOutput.Asset() == "" || len(p.Snapshots) != 2 {
		return fmt.Errorf("tracking point requires base output and two snapshots")
	}
	return nil
}

type TrackingWindow struct {
	ID                  WindowID
	Run                 ResearchRunID
	Strategy            StrategyID
	ConfigHash          string
	Direction           Direction
	Input               market.AssetQuantity
	FixedThreshold      market.AssetQuantity
	PercentageThreshold market.AssetQuantity
	EffectiveThreshold  market.AssetQuantity
	Cost                market.AssetQuantity
	OpeningTrigger      TriggerMetadata
	OpenedAt            time.Time
	DiscoveryStartedAt  time.Time
	DiscoveryFinishedAt time.Time
	OpeningPersistedAt  time.Time
	Opening             Candidate
	OpeningBuyOutput    market.AssetQuantity
	OpeningSnapshots    []TrackingSnapshot
	DiscoveryTrace      []TrackingDiscoveryDirection
}

type DanglingTrackingWindow struct {
	WindowID              WindowID
	MessageID             int64
	Direction             Direction
	Input                 market.AssetQuantity
	BuyOutput             market.AssetQuantity
	SellOutput            market.AssetQuantity
	NetPnL                market.AssetQuantity
	Threshold             market.AssetQuantity
	BestPnL               market.AssetQuantity
	WorstPnL              market.AssetQuantity
	OpenedAt              time.Time
	ClosedAt              time.Time
	LastTriggerAt         time.Time
	Points                uint64
	EconomicChanges       uint64
	CumulativeCalculation time.Duration
	CumulativeQueue       time.Duration
	MaximumQueue          time.Duration
}

type TrackingDiscoveryQuote struct {
	Leg      string
	Input    market.AssetQuantity
	Output   market.AssetQuantity
	Duration time.Duration
	Cached   bool
	Error    string
}

type TrackingDiscoveryDirection struct {
	Direction Direction
	Duration  time.Duration
	Quotes    []TrackingDiscoveryQuote
}

type TrackingWindowClosing struct {
	WindowID              WindowID
	Status                WindowStatus
	Reason                string
	ClosedAt              time.Time
	ClosingTriggerAt      time.Time
	EconomicDuration      time.Duration
	ObservedDuration      time.Duration
	CumulativeCalculation time.Duration
	CumulativeQueue       time.Duration
	MaximumQueue          time.Duration
	Events                uint64
	EconomicChanges       uint64
	InitialPnL            market.AssetQuantity
	FinalPnL              market.AssetQuantity
	BestPnL               market.AssetQuantity
	WorstPnL              market.AssetQuantity
	LatencyMinimum        time.Duration
	LatencyMean           time.Duration
	LatencyP50            time.Duration
	LatencyP95            time.Duration
	LatencyP99            time.Duration
	LatencyMaximum        time.Duration
}
