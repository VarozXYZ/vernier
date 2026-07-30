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
	LiveRuntimeStarted LiveRuntimeEventKind = "started"
	LiveRuntimeStopped LiveRuntimeEventKind = "stopped"
)

// LiveRuntimeEvent reports only process lifecycle state. It deliberately
// excludes setup identifiers, account addresses, endpoints, and credentials.
type LiveRuntimeEvent struct {
	Kind       LiveRuntimeEventKind
	Mode       string
	Reason     string
	StartedAt  time.Time
	OccurredAt time.Time
	Uptime     time.Duration
}

type LiveRuntimeSender interface {
	SendLiveRuntime(context.Context, LiveRuntimeEvent) error
}
