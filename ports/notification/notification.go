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
