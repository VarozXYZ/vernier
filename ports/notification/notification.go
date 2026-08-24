// Package notification defines provider-neutral Research alerts.
package notification

import (
	"context"
	"time"
)

type OpportunityOpening struct {
	Direction    string
	BuyProvider  string
	SellProvider string
	Input        string
	BaseBought   string
	SellOutput   string
	GrossPnL     string
	Cost         string
	NetPnL       string
	Threshold    string
	BuyLatency   time.Duration
	SellLatency  time.Duration
	Trigger      string
	TriggerURL   string
	OpenedAt     time.Time
}

type OpeningSender interface {
	SendOpening(context.Context, OpportunityOpening) error
}

type TrackingHistoryPoint struct {
	SinceOpening time.Duration
	SellOutput   string
	NetPnL       string
	Delta        string
	Calculation  time.Duration
	Total        time.Duration
}

// TrackingWindowUpdate is a provider-neutral projection of one durable
// fixed-candidate window. The adapter may trim visible history, but the
// current state and aggregates must always remain present.
type TrackingWindowUpdate struct {
	WindowID              string
	State                 string
	Direction             string
	Input                 string
	BuyOutput             string
	SellOutput            string
	NetPnL                string
	DeltaOpening          string
	DeltaPrevious         string
	Threshold             string
	BestPnL               string
	WorstPnL              string
	Reason                string
	Trigger               string
	TriggerURL            string
	OpenedAt              time.Time
	Points                uint64
	Changes               uint64
	DiscoveryDuration     time.Duration
	TriggerToOpen         time.Duration
	SinceOpening          time.Duration
	EconomicDuration      time.Duration
	ObservedDuration      time.Duration
	QueueDuration         time.Duration
	BuyDuration           time.Duration
	ConversionDuration    time.Duration
	SellDuration          time.Duration
	PnLDuration           time.Duration
	PersistenceDuration   time.Duration
	CalculationDuration   time.Duration
	TriggerToResult       time.Duration
	CumulativeCalculation time.Duration
	CumulativeQueue       time.Duration
	History               []TrackingHistoryPoint
	SimulationStatus      string
	SimulationFailure     string
	SimulationError       string
	SimulationBuyStatus   string
	SimulationSellStatus  string
}

type TrackingWindowSender interface {
	SendTrackingWindow(context.Context, TrackingWindowUpdate) (int64, error)
	EditTrackingWindow(context.Context, int64, TrackingWindowUpdate) error
}

type RetryAfterError interface {
	error
	RetryAfter() time.Duration
}

type ConfigurationWarning struct {
	Code       string
	Provider   string
	Market     string
	Expected   string
	Observed   string
	Details    map[string]string
	ObservedAt time.Time
}

type ConfigurationWarningSender interface {
	SendConfigurationWarning(context.Context, ConfigurationWarning) error
}

type LiveExecutionEventKind string

const (
	LiveExecutionStarted           LiveExecutionEventKind = "started"
	LiveExecutionStageStarted      LiveExecutionEventKind = "stage_started"
	LiveExecutionStageCompleted    LiveExecutionEventKind = "stage_completed"
	LiveExecutionExitSelected      LiveExecutionEventKind = "exit_selected"
	LiveExecutionCompleted         LiveExecutionEventKind = "completed"
	LiveExecutionFailed            LiveExecutionEventKind = "failed"
	LiveExecutionRecoveryStarted   LiveExecutionEventKind = "recovery_started"
	LiveExecutionRecoveryProgress  LiveExecutionEventKind = "recovery_progress"
	LiveExecutionRecoveryCompleted LiveExecutionEventKind = "recovery_completed"
	LiveExecutionRecoveryBlocked   LiveExecutionEventKind = "recovery_blocked"
	LiveExecutionRefuelCompleted   LiveExecutionEventKind = "refuel_completed"
	LiveExecutionRefuelFailed      LiveExecutionEventKind = "refuel_failed"
	LiveExecutionRefuelUncertain   LiveExecutionEventKind = "refuel_uncertain"
)

// LiveExecutionEvent contains only concise operational evidence suitable for
// external alerts. It never carries signed payloads, calldata, credentials, or
// private route configuration.
type LiveExecutionEvent struct {
	Kind              LiveExecutionEventKind
	Operation         string
	State             string
	Direction         string
	Stage             string
	Ordinal           int
	TotalStages       int
	SourceChain       string
	DestinationChain  string
	Input             string
	Output            string
	BuyProvider       string
	SellProvider      string
	ExpectedBase      string
	ExpectedOutput    string
	ExpectedNetPnL    string
	Trigger           string
	TriggerURL        string
	AlternativeOutput string
	DestinationValue  string
	ReturnValue       string
	SafetyMargin      string
	ExecutionCost     string
	QuoteDelta        string
	BaseDelta         string
	BaseValue         string
	NetPnL            string
	SourceTransaction string
	SourceURL         string
	DestinationTx     string
	DestinationURL    string
	Evidence          string
	Detail            string
	Duration          time.Duration
	OccurredAt        time.Time
}

type LiveExecutionSender interface {
	SendLiveExecution(context.Context, LiveExecutionEvent) error
}

type LiveRuntimeEventKind string

const (
	LiveRuntimeStarted             LiveRuntimeEventKind = "started"
	LiveRuntimeStopped             LiveRuntimeEventKind = "stopped"
	LiveRuntimeBalanceInsufficient LiveRuntimeEventKind = "balance_insufficient"
	LiveRuntimeBalanceRecovered    LiveRuntimeEventKind = "balance_recovered"
	LiveRuntimeValidationBlocked   LiveRuntimeEventKind = "validation_blocked"
	LiveRuntimeCostCacheStale      LiveRuntimeEventKind = "cost_cache_stale"
	LiveRuntimeCostCacheRecovered  LiveRuntimeEventKind = "cost_cache_recovered"
	LiveRuntimeQuoteFXStale        LiveRuntimeEventKind = "quote_fx_stale"
	LiveRuntimeQuoteFXRecovered    LiveRuntimeEventKind = "quote_fx_recovered"
)

// LiveRuntimeEvent reports process lifecycle and admission-health state. It
// deliberately excludes setup identifiers, account addresses, endpoints, and
// credentials.
type LiveRuntimeEvent struct {
	Kind           LiveRuntimeEventKind
	Mode           string
	Reason         string
	StartedAt      time.Time
	OccurredAt     time.Time
	Uptime         time.Duration
	Chain          string
	Token          string
	AvailableUnits string
	RequiredUnits  string
}

type LiveRuntimeSender interface {
	SendLiveRuntime(context.Context, LiveRuntimeEvent) error
}
