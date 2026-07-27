package research

import (
	"strings"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
)

// ScheduledTrigger is the provider-neutral scheduling input used by remote
// Research streams.
type ScheduledTrigger struct {
	Chain       string
	Market      market.MarketID
	TriggeredAt time.Time
	Metadata    arbitrage.TriggerMetadata
	HasMetadata bool
}

// TriggerGate deduplicates transaction-level triggers and retains only the
// newest event accumulated while an evaluation is in flight.
type TriggerGate struct {
	mu      sync.Mutex
	limit   int
	seen    map[string]struct{}
	pending *ScheduledTrigger
}

func NewTriggerGate(limit int) *TriggerGate {
	if limit <= 0 {
		limit = 10_000
	}
	return &TriggerGate{limit: limit, seen: make(map[string]struct{})}
}

// Accept reports whether a trigger should start an evaluation. Bootstrap,
// health, and synthetic triggers are intentionally never deduplicated.
func (g *TriggerGate) Accept(trigger ScheduledTrigger) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.accept(trigger)
}

// OfferPending accepts a trigger and, when it is not a duplicate, replaces
// any prior pending event with this newer one.
func (g *TriggerGate) OfferPending(trigger ScheduledTrigger) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.accept(trigger) {
		return false
	}
	copy := trigger
	g.pending = &copy
	return true
}

func (g *TriggerGate) TakePending() (ScheduledTrigger, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.pending == nil {
		return ScheduledTrigger{}, false
	}
	result := *g.pending
	g.pending = nil
	return result, true
}

func (g *TriggerGate) accept(trigger ScheduledTrigger) bool {
	if !trigger.HasMetadata || strings.TrimSpace(trigger.Metadata.Reference.Value) == "" ||
		trigger.Metadata.Reference.Value == "bootstrap" {
		return true
	}
	switch trigger.Metadata.Reference.Kind {
	case "evm_transaction_hash", "solana_signature":
	default:
		return true
	}
	key := trigger.Chain + "|" + string(trigger.Metadata.Reference.Kind) + "|" + trigger.Metadata.Reference.Value
	if _, duplicate := g.seen[key]; duplicate {
		return false
	}
	if len(g.seen) >= g.limit {
		clear(g.seen)
	}
	g.seen[key] = struct{}{}
	return true
}
