package marketstate

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/domain/market"
	feedport "github.com/VarozXYZ/vernier/ports/feed"
)

// RouteMirror composes independent pool mirrors into the immutable snapshot
// consumed by a multi-hop strategy. Child mirrors remain the owners of their
// mutable protocol state and can be reset independently after reconnect.
type RouteMirror struct {
	mu       sync.RWMutex
	route    market.MarketID
	source   market.SourceID
	clock    Clock
	children map[market.MarketID]feedport.Mirror
	order    []market.MarketID
	version  uint64
	current  market.MarketSnapshot
	hasState bool
	health   market.Health
}

func NewRouteMirror(route market.MarketID, source market.SourceID, children []feedport.Mirror, clock Clock) (*RouteMirror, error) {
	if route == "" || source == "" || len(children) == 0 || clock == nil {
		return nil, fmt.Errorf("route, source, children, and clock are required")
	}
	byID := make(map[market.MarketID]feedport.Mirror, len(children))
	order := make([]market.MarketID, 0, len(children))
	for _, child := range children {
		if child == nil || child.MarketID() == "" {
			return nil, fmt.Errorf("route child mirror is required")
		}
		if _, exists := byID[child.MarketID()]; exists {
			return nil, fmt.Errorf("duplicate route child %q", child.MarketID())
		}
		byID[child.MarketID()] = child
		order = append(order, child.MarketID())
	}
	return &RouteMirror{route: route, source: source, clock: clock, children: byID, order: order, health: market.HealthHealthy}, nil
}

func (r *RouteMirror) MarketID() market.MarketID { return r.route }

func (r *RouteMirror) Child(id market.MarketID) (feedport.Mirror, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	child, ok := r.children[id]
	return child, ok
}

func (r *RouteMirror) Apply(ctx context.Context, event market.MarketEvent) (feedport.ApplyResult, error) {
	return r.apply(ctx, event, false)
}

func (r *RouteMirror) Reset(ctx context.Context, event market.MarketEvent) (feedport.ApplyResult, error) {
	return r.apply(ctx, event, true)
}

func (r *RouteMirror) apply(ctx context.Context, event market.MarketEvent, reset bool) (feedport.ApplyResult, error) {
	child, ok := r.Child(event.Market)
	if !ok {
		return feedport.ApplyResult{}, fmt.Errorf("event market %q is not a route child", event.Market)
	}
	var result feedport.ApplyResult
	var err error
	if reset {
		result, err = child.Reset(ctx, event)
	} else {
		result, err = child.Apply(ctx, event)
	}
	if err != nil || result.Disposition == feedport.ApplyDispositionIgnoredStale {
		return result, err
	}
	if err := r.rebuild(event, result.Snapshot); err != nil {
		return feedport.ApplyResult{}, err
	}
	return result, nil
}

// SetChildHealth is used by a feed to report liveness without changing pool
// state. A route is degraded if any child is degraded.
func (r *RouteMirror) SetChildHealth(ctx context.Context, childID market.MarketID, update feedport.HealthUpdate) error {
	child, ok := r.Child(childID)
	if !ok {
		return fmt.Errorf("unknown route child %q", childID)
	}
	if err := child.SetHealth(ctx, update); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.hasState {
		return nil
	}
	return r.rebuildLocked(update.ObservedAt.UTC(), nil)
}

// SetHealth applies a route-wide health update. Most feeds should prefer
// SetChildHealth so one disconnected hop does not overwrite another hop's
// liveness state.
func (r *RouteMirror) SetHealth(ctx context.Context, update feedport.HealthUpdate) error {
	if err := update.Validate(); err != nil {
		return err
	}
	for id, child := range r.children {
		if err := child.SetHealth(ctx, update); err != nil {
			return fmt.Errorf("set health for %s: %w", id, err)
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.hasState {
		return nil
	}
	return r.rebuildLocked(update.ObservedAt.UTC(), nil)
}

func (r *RouteMirror) Current() (market.MarketSnapshot, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.current, r.hasState
}

func (r *RouteMirror) Health() market.Health {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.health
}

func (r *RouteMirror) rebuild(event market.MarketEvent, trigger market.MarketSnapshot) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rebuildLocked(r.clock().UTC(), &trigger)
}

func (r *RouteMirror) rebuildLocked(now time.Time, trigger *market.MarketSnapshot) error {
	snapshots := make([]market.MarketSnapshot, 0, len(r.order))
	for _, id := range r.order {
		snapshot, ok := r.children[id].Current()
		if !ok {
			r.hasState = false
			return nil
		}
		snapshots = append(snapshots, snapshot)
	}
	bundle, err := market.NewSnapshotBundle(r.route, snapshots)
	if err != nil {
		return err
	}
	health := market.HealthHealthy
	reason := ""
	for _, id := range r.order {
		if childHealth := r.children[id].Health(); childHealth == market.HealthDegraded {
			health = market.HealthDegraded
			reason = "route_child_degraded"
			break
		}
	}
	if r.health != health {
		r.health = health
	}
	r.version++
	metadata := snapshots[len(snapshots)-1].Metadata()
	if trigger != nil {
		metadata = trigger.Metadata()
	}
	metadata.Market = r.route
	metadata.Source = r.source
	metadata.Version = r.version
	metadata.AppliedAt = now
	metadata.Health = health
	metadata.HealthReason = reason
	metadata.HealthChangedAt = now
	metadata.StateHash = bundle.Hash()
	snapshot, err := market.NewMarketSnapshot(metadata, bundle)
	if err != nil {
		return err
	}
	r.current = snapshot
	r.hasState = true
	return nil
}

var _ feedport.Mirror = (*RouteMirror)(nil)
