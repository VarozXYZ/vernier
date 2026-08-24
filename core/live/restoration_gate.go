package live

import (
	"fmt"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
)

const MaximumPendingQuoteReturns = 2

// RestorationGate owns admission capacity after prefunded parallel swaps.
// Base restoration is the critical single-flight gate; quote restoration is
// asynchronous and bounded. The coalesced trigger is metadata only and can
// request a fresh evaluation, never execution of a stale candidate.
type RestorationGate struct {
	mu           sync.Mutex
	basePending  bool
	quotePending map[string]struct{}
	coalesced    *arbitrage.TriggerMetadata
}

type RestorationSnapshot struct {
	BasePending bool
	QuoteJobs   []string
}

func NewRestorationGate(snapshot RestorationSnapshot) (*RestorationGate, error) {
	if len(snapshot.QuoteJobs) > MaximumPendingQuoteReturns {
		return nil, fmt.Errorf("pending quote restoration exceeds capacity")
	}
	gate := &RestorationGate{basePending: snapshot.BasePending, quotePending: make(map[string]struct{}, len(snapshot.QuoteJobs))}
	for _, id := range snapshot.QuoteJobs {
		if id == "" {
			return nil, fmt.Errorf("pending quote restoration identity is empty")
		}
		if _, duplicate := gate.quotePending[id]; duplicate {
			return nil, fmt.Errorf("pending quote restoration identity is duplicated")
		}
		gate.quotePending[id] = struct{}{}
	}
	return gate, nil
}

func (g *RestorationGate) CanEvaluate(quoteInventoryAvailable bool) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return !g.basePending && quoteInventoryAvailable && len(g.quotePending) < MaximumPendingQuoteReturns
}

func (g *RestorationGate) BeginOperation() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.basePending {
		return fmt.Errorf("base-token restoration is still pending")
	}
	if len(g.quotePending) >= MaximumPendingQuoteReturns {
		return fmt.Errorf("quote-token restoration capacity is exhausted")
	}
	g.basePending = true
	return nil
}

func (g *RestorationGate) StartQuoteReturn(id string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if id == "" {
		return fmt.Errorf("quote restoration identity is required")
	}
	if _, exists := g.quotePending[id]; exists {
		return nil
	}
	if len(g.quotePending) >= MaximumPendingQuoteReturns {
		return fmt.Errorf("quote-token restoration capacity is exhausted")
	}
	g.quotePending[id] = struct{}{}
	return nil
}

func (g *RestorationGate) CompleteBaseReturn() { g.mu.Lock(); g.basePending = false; g.mu.Unlock() }
func (g *RestorationGate) CompleteQuoteReturn(id string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, exists := g.quotePending[id]; !exists {
		return false
	}
	delete(g.quotePending, id)
	return true
}

func (g *RestorationGate) Coalesce(trigger arbitrage.TriggerMetadata) error {
	if err := trigger.Validate(); err != nil {
		return err
	}
	copy := trigger
	g.mu.Lock()
	g.coalesced = &copy
	g.mu.Unlock()
	return nil
}

func (g *RestorationGate) TakeFreshEvaluationRequest(quoteInventoryAvailable bool, now time.Time) (arbitrage.TriggerMetadata, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.coalesced == nil || g.basePending || !quoteInventoryAvailable ||
		len(g.quotePending) >= MaximumPendingQuoteReturns || now.IsZero() {
		return arbitrage.TriggerMetadata{}, false
	}
	trigger := *g.coalesced
	g.coalesced = nil
	return trigger, true
}

func (g *RestorationGate) Snapshot() RestorationSnapshot {
	g.mu.Lock()
	defer g.mu.Unlock()
	result := RestorationSnapshot{BasePending: g.basePending, QuoteJobs: make([]string, 0, len(g.quotePending))}
	for id := range g.quotePending {
		result.QuoteJobs = append(result.QuoteJobs, id)
	}
	return result
}
