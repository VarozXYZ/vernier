package arbitrage

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/VarozXYZ/vernier/domain/market"
)

// TriggerMetadata identifies the normalized feed observation that caused an
// evaluation. It is intentionally compact: the event payload itself is not
// part of the durable Research record.
type TriggerMetadata struct {
	Market    market.MarketID
	Source    market.SourceID
	Position  market.SourcePosition
	Reference market.SourceReference
	At        time.Time
}

func (t TriggerMetadata) Validate() error {
	if t.Market == "" || t.Source == "" {
		return fmt.Errorf("trigger market and source are required")
	}
	if err := t.Position.Validate(); err != nil {
		return err
	}
	if err := t.Reference.Validate(); err != nil {
		return err
	}
	if t.At.IsZero() {
		return fmt.Errorf("trigger timestamp is required")
	}
	return nil
}

type WindowID string

type WindowStatus string

const (
	WindowStatusOpen   WindowStatus = "open"
	WindowStatusClosed WindowStatus = "closed"
	WindowStatusFailed WindowStatus = "failed"
)

// WindowKey identifies one direction of one strategy. A new continuity (for
// example after a process restart) creates a new WindowID for the same key.
type WindowKey struct {
	Run        ResearchRunID
	Strategy   StrategyID
	ConfigHash string
	Direction  Direction
}

type WindowCandidate struct {
	Size     market.AssetQuantity
	GrossPnL market.AssetQuantity
	NetPnL   market.AssetQuantity
	Cost     market.AssetQuantity
}

func (c WindowCandidate) Validate() error {
	if c.Size.Asset() == "" || c.GrossPnL.Asset() == "" || c.NetPnL.Asset() == "" || c.Cost.Asset() == "" {
		return fmt.Errorf("window candidate quantities are required")
	}
	if c.Size.Asset() != c.GrossPnL.Asset() || c.Size.Asset() != c.NetPnL.Asset() || c.Size.Asset() != c.Cost.Asset() {
		return fmt.Errorf("window candidate quantities must use one asset")
	}
	if c.Size.Sign() < 0 || c.Cost.Sign() < 0 {
		return fmt.Errorf("window candidate size and cost cannot be negative")
	}
	return nil
}

// OpportunityWindow is the minimal durable record for a profitable period.
// It deliberately contains no MarketEvent, MarketSnapshot payload, quote
// curve, or recovery/session history.
type OpportunityWindow struct {
	ID                WindowID
	Run               ResearchRunID
	Strategy          StrategyID
	ConfigHash        string
	Direction         Direction
	Trigger           TriggerMetadata
	HasTrigger        bool
	OpenedAt          time.Time
	FirstProfitableAt time.Time
	LastProfitableAt  time.Time
	ClosedAt          time.Time
	Best              WindowCandidate
	HasBest           bool
	Classification    Classification
	Status            WindowStatus
	CloseReason       string
	Degraded          bool
}

func (w OpportunityWindow) Key() WindowKey {
	return WindowKey{Run: w.Run, Strategy: w.Strategy, ConfigHash: w.ConfigHash, Direction: w.Direction}
}

func (w OpportunityWindow) Duration() time.Duration {
	end := w.ClosedAt
	if end.IsZero() {
		end = w.LastProfitableAt
	}
	if w.OpenedAt.IsZero() || end.Before(w.OpenedAt) {
		return 0
	}
	return end.Sub(w.OpenedAt)
}

func (w OpportunityWindow) Validate() error {
	if w.ID == "" || w.Run == "" || w.Strategy == "" || w.ConfigHash == "" {
		return fmt.Errorf("window identity is required")
	}
	if w.Direction.BuyMarket == "" || w.Direction.SellMarket == "" || w.Direction.BuyMarket == w.Direction.SellMarket {
		return fmt.Errorf("window direction is invalid")
	}
	if w.OpenedAt.IsZero() || w.FirstProfitableAt.IsZero() || w.LastProfitableAt.IsZero() {
		return fmt.Errorf("window timestamps are required")
	}
	if w.FirstProfitableAt.Before(w.OpenedAt) || w.LastProfitableAt.Before(w.FirstProfitableAt) {
		return fmt.Errorf("window profitable timestamps are out of order")
	}
	if w.HasTrigger {
		if err := w.Trigger.Validate(); err != nil {
			return err
		}
	}
	if w.HasBest {
		if err := w.Best.Validate(); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("window best candidate is required")
	}
	if w.Classification != ClassificationEconomic && w.Classification != ClassificationPolicyQualified {
		return fmt.Errorf("window classification must be economic or policy_qualified")
	}
	switch w.Status {
	case WindowStatusOpen:
		if !w.ClosedAt.IsZero() || w.CloseReason != "" || w.Degraded {
			return fmt.Errorf("open window cannot have close state")
		}
	case WindowStatusClosed, WindowStatusFailed:
		if w.ClosedAt.IsZero() || w.CloseReason == "" {
			return fmt.Errorf("closed window requires close state")
		}
		if w.ClosedAt.Before(w.LastProfitableAt) {
			return fmt.Errorf("window closes before last profitable observation")
		}
	default:
		return fmt.Errorf("invalid window status %q", w.Status)
	}
	return nil
}

type WindowOpening struct {
	Window OpportunityWindow
}

type WindowObservation struct {
	ID             string
	WindowID       WindowID
	Evaluation     EvaluationID
	ObservedAt     time.Time
	Classification Classification
	Candidate      WindowCandidate
	HasCandidate   bool
	Best           bool
}

func (o WindowObservation) Validate() error {
	if o.ID == "" || o.WindowID == "" || o.Evaluation == "" || o.ObservedAt.IsZero() {
		return fmt.Errorf("window observation identity and timestamp are required")
	}
	if o.Classification != ClassificationEconomic && o.Classification != ClassificationPolicyQualified {
		return fmt.Errorf("window observation classification must be economic or policy_qualified")
	}
	if o.HasCandidate {
		if err := o.Candidate.Validate(); err != nil {
			return err
		}
	}
	if o.Best && !o.HasCandidate {
		return fmt.Errorf("best observation requires a candidate")
	}
	return nil
}

type WindowClosing struct {
	WindowID         WindowID
	ClosedAt         time.Time
	LastProfitableAt time.Time
	Classification   Classification
	Reason           string
	Degraded         bool
}

type WindowFailure struct {
	WindowID         WindowID
	ClosedAt         time.Time
	LastProfitableAt time.Time
	Reason           string
}

type WindowQuery struct {
	Run      ResearchRunID
	Strategy StrategyID
	Status   WindowStatus
	Limit    int
}

type WindowRecord struct {
	Window       OpportunityWindow
	Observations []WindowObservation
}

// WindowFingerprint is useful to SQLite adapters when checking idempotent
// replays without serializing any event payload.
func WindowFingerprint(w OpportunityWindow) string {
	parts := []string{
		string(w.ID), string(w.Run), string(w.Strategy), w.ConfigHash,
		string(w.Direction.BuyMarket), string(w.Direction.SellMarket),
		strconv.FormatBool(w.HasTrigger), string(w.Trigger.Market), string(w.Trigger.Source),
		string(w.Trigger.Position.Kind), strconv.FormatUint(w.Trigger.Position.Value, 10),
		string(w.Trigger.Reference.Kind), w.Trigger.Reference.Value, w.Trigger.At.UTC().Format(time.RFC3339Nano),
		w.OpenedAt.UTC().Format(time.RFC3339Nano), w.FirstProfitableAt.UTC().Format(time.RFC3339Nano),
		w.LastProfitableAt.UTC().Format(time.RFC3339Nano), string(w.Classification), string(w.Status), w.CloseReason,
		strconv.FormatBool(w.Degraded), strconv.FormatBool(w.HasBest), string(w.Best.Size.Asset()), w.Best.Size.String(),
		string(w.Best.GrossPnL.Asset()), w.Best.GrossPnL.String(), string(w.Best.NetPnL.Asset()), w.Best.NetPnL.String(),
		string(w.Best.Cost.Asset()), w.Best.Cost.String(),
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}
