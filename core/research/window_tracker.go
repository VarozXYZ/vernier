package research

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
	persistence "github.com/VarozXYZ/vernier/ports/persistence"
)

// WindowTracker turns the ephemeral stream of Opportunities into the small
// durable lifecycle requested by Research. It is intentionally serial: the
// live comparison loop already orders evaluations, so it needs no event log or
// replay queue of its own.
type WindowTracker struct {
	store   persistence.OpportunityStore
	clock   func() time.Time
	session string
	active  map[arbitrage.WindowKey]arbitrage.OpportunityWindow
}

func NewWindowTracker(store persistence.OpportunityStore, clock func() time.Time) (*WindowTracker, error) {
	if store == nil || clock == nil {
		return nil, fmt.Errorf("opportunity store and clock are required")
	}
	return NewWindowTrackerWithSession(store, clock, newSessionID(clock()))
}

func NewWindowTrackerWithSession(store persistence.OpportunityStore, clock func() time.Time, session string) (*WindowTracker, error) {
	if store == nil || clock == nil || strings.TrimSpace(session) == "" {
		return nil, fmt.Errorf("opportunity store, clock, and session are required")
	}
	return &WindowTracker{store: store, clock: clock, session: session, active: make(map[arbitrage.WindowKey]arbitrage.OpportunityWindow)}, nil
}

// Start closes windows left open by a previous process. This is lifecycle
// cleanup, not recovery persistence: no reconnect/session record is written.
func (t *WindowTracker) Start(ctx context.Context) error {
	return t.store.FinalizeDangling(ctx, t.clock().UTC())
}

// Observe applies one completed strategy opportunity. Only economic and
// policy-qualified classifications create or extend windows. All other
// classifications close an active window, if one exists.
func (t *WindowTracker) Observe(ctx context.Context, opportunity arbitrage.Opportunity) error {
	key := arbitrage.WindowKey{Run: opportunity.Run, Strategy: opportunity.Strategy, ConfigHash: opportunity.ConfigHash, Direction: opportunity.Direction}
	if key.Run == "" || key.Strategy == "" || key.ConfigHash == "" {
		return fmt.Errorf("opportunity identity is required")
	}
	active, exists := t.active[key]
	if isFavorable(opportunity.Classification) {
		if !exists {
			window, err := t.opening(opportunity)
			if err != nil {
				return err
			}
			if err := t.store.OpenWindow(ctx, arbitrage.WindowOpening{Window: window}); err != nil {
				return err
			}
			t.active[key] = window
			return nil
		}
		return t.improve(ctx, key, active, opportunity)
	}
	if !exists {
		return nil
	}
	return t.close(ctx, key, active, opportunity)
}

// FailMarket closes active windows that depend on a market after an explicit
// feed failure. The normal WebSocket path reaches the same behavior through a
// degraded evaluation signal; this method also covers a terminal feed error.
func (t *WindowTracker) FailMarket(ctx context.Context, marketID market.MarketID, reason string, at time.Time) error {
	if strings.TrimSpace(reason) == "" {
		return fmt.Errorf("failure reason is required")
	}
	if at.IsZero() {
		at = t.clock().UTC()
	}
	for key, window := range t.active {
		if marketID != "" && key.Direction.BuyMarket != marketID && key.Direction.SellMarket != marketID {
			continue
		}
		closedAt := at.UTC()
		if closedAt.Before(window.LastProfitableAt) {
			closedAt = window.LastProfitableAt
		}
		if err := t.store.FailWindow(ctx, arbitrage.WindowFailure{
			WindowID: window.ID, ClosedAt: closedAt, LastProfitableAt: window.LastProfitableAt, Reason: reason,
		}); err != nil {
			return err
		}
		delete(t.active, key)
	}
	return nil
}

func (t *WindowTracker) ActiveWindows() []arbitrage.OpportunityWindow {
	result := make([]arbitrage.OpportunityWindow, 0, len(t.active))
	for _, window := range t.active {
		result = append(result, window)
	}
	return result
}

func (t *WindowTracker) opening(opportunity arbitrage.Opportunity) (arbitrage.OpportunityWindow, error) {
	observedAt := opportunityTime(opportunity, t.clock().UTC())
	openedAt := opportunity.TriggeredAt.UTC()
	if openedAt.IsZero() {
		openedAt = observedAt
	}
	if observedAt.Before(openedAt) {
		observedAt = openedAt
	}
	candidate, ok := selectedCandidate(opportunity)
	if !ok {
		return arbitrage.OpportunityWindow{}, fmt.Errorf("favorable opportunity has no selected candidate")
	}
	window := arbitrage.OpportunityWindow{
		ID: arbitrage.WindowID(t.windowID(opportunity)), Run: opportunity.Run, Strategy: opportunity.Strategy,
		ConfigHash: opportunity.ConfigHash, Direction: opportunity.Direction,
		Trigger: opportunity.Trigger, HasTrigger: opportunity.HasTrigger,
		OpenedAt: openedAt, FirstProfitableAt: observedAt, LastProfitableAt: observedAt,
		Best: candidate, HasBest: true, Classification: opportunity.Classification, Status: arbitrage.WindowStatusOpen,
	}
	if window.HasTrigger {
		if window.Trigger.At.IsZero() {
			window.Trigger.At = openedAt
		}
		window.Trigger.At = window.Trigger.At.UTC()
	}
	if err := window.Validate(); err != nil {
		return arbitrage.OpportunityWindow{}, err
	}
	return window, nil
}

func (t *WindowTracker) improve(ctx context.Context, key arbitrage.WindowKey, current arbitrage.OpportunityWindow, opportunity arbitrage.Opportunity) error {
	at := opportunityTime(opportunity, t.clock().UTC())
	if at.Before(current.LastProfitableAt) {
		at = current.LastProfitableAt
	}
	next := current
	next.LastProfitableAt = at
	next.Classification = opportunity.Classification
	candidate, hasCandidate := selectedCandidate(opportunity)
	improved := hasCandidate && (!current.HasBest || greater(candidate.NetPnL, current.Best.NetPnL))
	nextBest := current.Best
	if improved {
		nextBest = candidate
		next.Best = candidate
		next.HasBest = true
	}
	observation := arbitrage.WindowObservation{
		ID: observationID(current.ID, opportunity.Evaluation), WindowID: current.ID, Evaluation: opportunity.Evaluation,
		ObservedAt: at, Classification: opportunity.Classification, Candidate: nextBest, HasCandidate: next.HasBest, Best: improved,
	}
	if err := t.store.RecordImprovement(ctx, observation); err != nil {
		return err
	}
	t.active[key] = next
	return nil
}

func (t *WindowTracker) close(ctx context.Context, key arbitrage.WindowKey, current arbitrage.OpportunityWindow, opportunity arbitrage.Opportunity) error {
	closedAt := opportunity.TriggeredAt.UTC()
	if closedAt.IsZero() {
		closedAt = opportunityTime(opportunity, t.clock().UTC())
	}
	if closedAt.Before(current.LastProfitableAt) {
		closedAt = current.LastProfitableAt
	}
	reason := closeReason(opportunity)
	if isFailure(opportunity) {
		if err := t.store.FailWindow(ctx, arbitrage.WindowFailure{
			WindowID: current.ID, ClosedAt: closedAt, LastProfitableAt: current.LastProfitableAt, Reason: reason,
		}); err != nil {
			return err
		}
	} else if err := t.store.CloseWindow(ctx, arbitrage.WindowClosing{
		WindowID: current.ID, ClosedAt: closedAt, LastProfitableAt: current.LastProfitableAt,
		Classification: opportunity.Classification, Reason: reason, Degraded: false,
	}); err != nil {
		return err
	}
	delete(t.active, key)
	return nil
}

func (t *WindowTracker) windowID(opportunity arbitrage.Opportunity) string {
	value := fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%s|%s|%s",
		t.session, opportunity.Run, opportunity.Strategy, opportunity.ConfigHash,
		opportunity.Direction.BuyMarket, opportunity.Direction.SellMarket, opportunity.Evaluation,
		opportunity.TriggeredAt.UTC().Format(time.RFC3339Nano), opportunity.Trigger.Reference.Kind,
		opportunity.Trigger.Reference.Value)
	hash := arbitrageWindowHash(value)
	return "window-" + hex.EncodeToString(hash[:16])
}

func observationID(window arbitrage.WindowID, evaluation arbitrage.EvaluationID) string {
	hash := arbitrageWindowHash(string(window) + "|" + string(evaluation))
	return "observation-" + hex.EncodeToString(hash[:16])
}

func arbitrageWindowHash(value string) [32]byte {
	return sha256.Sum256([]byte(value))
}

func selectedCandidate(opportunity arbitrage.Opportunity) (arbitrage.WindowCandidate, bool) {
	if opportunity.SelectedIndex < 0 || opportunity.SelectedIndex >= len(opportunity.Candidates) {
		return arbitrage.WindowCandidate{}, false
	}
	candidate := opportunity.Candidates[opportunity.SelectedIndex]
	return arbitrage.WindowCandidate{Size: candidate.Size, GrossPnL: candidate.GrossPnL, NetPnL: candidate.NetPnL, Cost: candidate.Cost.Amount}, true
}

func opportunityTime(opportunity arbitrage.Opportunity, fallback time.Time) time.Time {
	if !opportunity.FinishedAt.IsZero() {
		return opportunity.FinishedAt.UTC()
	}
	if !opportunity.StartedAt.IsZero() {
		return opportunity.StartedAt.UTC()
	}
	return fallback.UTC()
}

func isFavorable(classification arbitrage.Classification) bool {
	return classification == arbitrage.ClassificationEconomic || classification == arbitrage.ClassificationPolicyQualified
}

func isFailure(opportunity arbitrage.Opportunity) bool {
	if opportunity.Classification != arbitrage.ClassificationUnclassifiable {
		return false
	}
	for _, reason := range opportunity.Reasons {
		if strings.Contains(reason, "degraded_market_snapshot") || strings.Contains(reason, "websocket_disconnected") {
			return true
		}
	}
	return false
}

func closeReason(opportunity arbitrage.Opportunity) string {
	if isFailure(opportunity) {
		return "websocket_disconnected"
	}
	if opportunity.Classification == arbitrage.ClassificationUnclassifiable {
		return "unclassifiable"
	}
	return "profitability_lost"
}

func greater(left, right market.AssetQuantity) bool {
	comparison, err := left.Cmp(right)
	return err == nil && comparison > 0
}

func newSessionID(now time.Time) string {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err == nil {
		return hex.EncodeToString(bytes[:])
	}
	return fmt.Sprintf("%d", now.UTC().UnixNano())
}
