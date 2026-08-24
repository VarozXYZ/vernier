package livecanary

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"regexp"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
)

// FlowCostComponent is one independently refreshed part of the complete
// sequential flow. Amount is always valued in the setup quote asset.
type FlowCostComponent struct {
	Kind       string
	Amount     market.AssetQuantity
	Evidence   string
	CapturedAt time.Time
}

// FlowCostEstimate is the result of one out-of-band refresh for a direction.
// It includes buy, base bridge, sell and quote-return bridge costs.
type FlowCostEstimate struct {
	Direction  arbitrage.Direction
	Input      market.AssetQuantity
	Components []FlowCostComponent
}

type FlowCostRefresh func(context.Context, []arbitrage.Opportunity) ([]FlowCostEstimate, error)

type cachedFlowCost struct {
	snapshot   arbitrage.CostSnapshot
	components []FlowCostComponent
}

type flowCostKey struct {
	direction arbitrage.Direction
	input     string
}

const staleFallbackCostKind = "stale_fixed_fallback"

var providerURLPattern = regexp.MustCompile(`https?://[^\s"']+`)

// FlowCostOracle owns a direction-aware complete-flow cache. Refresh performs
// I/O in a background goroutine; Snapshot is a read lock plus immutable copies.
type FlowCostOracle struct {
	directions      []arbitrage.Direction
	quoteAsset      market.AssetID
	refresh         FlowCostRefresh
	interval        time.Duration
	ttl             time.Duration
	clock           func() time.Time
	logger          *slog.Logger
	onReady         func()
	onStale         func(error)
	onRecovered     func()
	gate            interface{ EvaluationAllowed() bool }
	staleAlertAfter time.Duration
	staleFallback   market.AssetQuantity

	mu           sync.RWMutex
	cache        map[flowCostKey]cachedFlowCost
	latest       []arbitrage.Opportunity
	lastAttempt  time.Time
	lastErr      error
	blockedSince time.Time
	staleAlerted bool
}

type FlowCostOracleConfig struct {
	Directions      []arbitrage.Direction
	QuoteAsset      market.AssetID
	Refresh         FlowCostRefresh
	RefreshInterval time.Duration
	TTL             time.Duration
	Clock           func() time.Time
	Logger          *slog.Logger
	OnReady         func()
	OnStale         func(error)
	OnRecovered     func()
	StaleAlertAfter time.Duration
	StaleFallback   market.AssetQuantity
	Gate            interface{ EvaluationAllowed() bool }
}

func NewFlowCostOracle(config FlowCostOracleConfig) (*FlowCostOracle, error) {
	if len(config.Directions) == 0 || config.QuoteAsset == "" ||
		config.Refresh == nil || config.RefreshInterval <= 0 ||
		config.TTL < config.RefreshInterval {
		return nil, fmt.Errorf("complete-flow cost oracle configuration is invalid")
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	if config.StaleFallback.Asset() != "" &&
		(config.StaleFallback.Asset() != config.QuoteAsset || config.StaleFallback.Sign() <= 0) {
		return nil, fmt.Errorf("complete-flow stale cost fallback is invalid")
	}
	directions := append([]arbitrage.Direction(nil), config.Directions...)
	seen := make(map[arbitrage.Direction]struct{}, len(directions))
	for _, direction := range directions {
		if direction.BuyMarket == "" || direction.SellMarket == "" ||
			direction.BuyMarket == direction.SellMarket {
			return nil, fmt.Errorf("complete-flow cost direction is invalid")
		}
		if _, duplicate := seen[direction]; duplicate {
			return nil, fmt.Errorf("complete-flow cost direction is repeated")
		}
		seen[direction] = struct{}{}
	}
	return &FlowCostOracle{
		directions: directions, quoteAsset: config.QuoteAsset,
		refresh: config.Refresh, interval: config.RefreshInterval,
		ttl: config.TTL, clock: config.Clock, logger: config.Logger,
		onReady: config.OnReady,
		onStale: config.OnStale, onRecovered: config.OnRecovered,
		staleAlertAfter: config.StaleAlertAfter,
		staleFallback:   config.StaleFallback,
		gate:            config.Gate,
		cache:           make(map[flowCostKey]cachedFlowCost, len(directions)),
	}, nil
}

func (o *FlowCostOracle) Warm(ctx context.Context) error {
	return o.refreshOnce(ctx)
}

func (o *FlowCostOracle) Run(ctx context.Context) {
	ticker := time.NewTicker(o.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.runRefresh(ctx)
		}
		o.checkStaleAlert()
	}
}

func (o *FlowCostOracle) runRefresh(ctx context.Context) {
	if o.gate != nil && !o.gate.EvaluationAllowed() {
		return
	}
	refreshCtx, cancel := context.WithTimeout(ctx, o.interval)
	err := o.refreshOnce(refreshCtx)
	cancel()
	if err != nil {
		o.logger.Warn(
			"complete-flow cost cache refresh failed",
			"error", safeFlowCostError(err),
		)
	}
}

// Observe stores the latest complete route quotes for the next periodic cost
// refresh. It never schedules a refresh or performs provider I/O.
func (o *FlowCostOracle) Observe(opportunities []arbitrage.Opportunity) {
	o.mu.Lock()
	byDirection := make(map[arbitrage.Direction]arbitrage.Opportunity, len(o.latest)+len(opportunities))
	for _, opportunity := range o.latest {
		byDirection[opportunity.Direction] = opportunity
	}
	for _, opportunity := range opportunities {
		index := opportunity.SelectedIndex
		if index < 0 || index >= len(opportunity.Candidates) {
			index = bestCostCalibrationCandidate(opportunity.Candidates)
		}
		if index < 0 {
			continue
		}
		if opportunity.Candidates[index].SellQuote.AmountOut.IsZero() {
			continue
		}
		byDirection[opportunity.Direction] = opportunity
	}
	o.latest = o.latest[:0]
	for _, direction := range o.directions {
		if opportunity, ok := byDirection[direction]; ok {
			o.latest = append(o.latest, cloneOpportunities([]arbitrage.Opportunity{opportunity})[0])
		}
	}
	o.mu.Unlock()
}

func bestCostCalibrationCandidate(candidates []arbitrage.Candidate) int {
	best := -1
	for index, candidate := range candidates {
		if candidate.SellQuote.AmountOut.IsZero() {
			continue
		}
		if best < 0 {
			best = index
			continue
		}
		comparison, err := candidate.NetPnL.Cmp(candidates[best].NetPnL)
		if err == nil && comparison > 0 {
			best = index
		}
	}
	return best
}

func (o *FlowCostOracle) checkStaleAlert() {
	if o.staleAlertAfter <= 0 || o.onStale == nil {
		return
	}
	now := o.clock().UTC()
	o.mu.Lock()
	healthy := o.cacheHealthyLocked(now)
	if healthy {
		o.blockedSince = time.Time{}
		o.mu.Unlock()
		return
	}
	if o.blockedSince.IsZero() || o.staleAlerted ||
		now.Sub(o.blockedSince) < o.staleAlertAfter {
		o.mu.Unlock()
		return
	}
	o.staleAlerted = true
	err := o.lastErr
	o.mu.Unlock()
	if err == nil {
		err = fmt.Errorf("complete-flow cost cache has no fresh snapshot")
	}
	o.onStale(err)
}

// Snapshot implements livecompare.DirectionalCostSource. It never performs
// I/O. Profiles without an explicit fallback continue to reject stale data.
func (o *FlowCostOracle) Snapshot(
	direction arbitrage.Direction,
	at time.Time,
) (arbitrage.CostSnapshot, bool) {
	if cached, ok := o.freshCached(direction, at); ok {
		return cached.snapshot, true
	}
	if o.supportsDirection(direction) && o.staleFallback.Asset() != "" {
		return arbitrage.CostSnapshot{
			ID: fmt.Sprintf(
				"complete-flow/fixed-stale-fallback/%s/%s",
				direction.BuyMarket,
				direction.SellMarket,
			),
			Amount: o.staleFallback, CapturedAt: at.UTC(),
		}, true
	}
	o.markAdmissionBlocked(at)
	return arbitrage.CostSnapshot{}, false
}

// SnapshotFor returns the exact amount-specific complete-flow observation.
// It is a memory-only lookup used by discrete sizing.
func (o *FlowCostOracle) SnapshotFor(direction arbitrage.Direction, input market.AssetQuantity,
	at time.Time) (arbitrage.CostSnapshot, bool) {
	if cached, ok := o.freshCachedFor(direction, input, at); ok {
		return cached.snapshot, true
	}
	if o.supportsDirection(direction) && o.staleFallback.Asset() != "" {
		return arbitrage.CostSnapshot{ID: fmt.Sprintf("complete-flow/fixed-stale-fallback/%s/%s/%s",
			direction.BuyMarket, direction.SellMarket, input.Rat().RatString()), Amount: o.staleFallback, CapturedAt: at.UTC()}, true
	}
	o.markAdmissionBlocked(at)
	return arbitrage.CostSnapshot{}, false
}

func (o *FlowCostOracle) freshCached(
	direction arbitrage.Direction,
	at time.Time,
) (cachedFlowCost, bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	var best cachedFlowCost
	found := false
	for key, cached := range o.cache {
		if key.direction != direction || cached.snapshot.CapturedAt.IsZero() || at.UTC().Sub(cached.snapshot.CapturedAt) > o.ttl {
			continue
		}
		if !found || cached.snapshot.Amount.Rat().Cmp(best.snapshot.Amount.Rat()) > 0 {
			best, found = cached, true
		}
	}
	return best, found
}

func (o *FlowCostOracle) freshCachedFor(direction arbitrage.Direction, input market.AssetQuantity,
	at time.Time) (cachedFlowCost, bool) {
	o.mu.RLock()
	cached, ok := o.cache[flowCostKey{direction: direction, input: input.Rat().RatString()}]
	if !ok {
		cached, ok = o.cache[flowCostKey{direction: direction}]
	}
	o.mu.RUnlock()
	if !ok || cached.snapshot.CapturedAt.IsZero() || at.UTC().Sub(cached.snapshot.CapturedAt) > o.ttl {
		return cachedFlowCost{}, false
	}
	return cached, true
}

func (o *FlowCostOracle) cacheHealthyLocked(at time.Time) bool {
	for _, direction := range o.directions {
		found := false
		for key, cached := range o.cache {
			if key.direction == direction && !cached.snapshot.CapturedAt.IsZero() && at.Sub(cached.snapshot.CapturedAt) <= o.ttl {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (o *FlowCostOracle) supportsDirection(candidate arbitrage.Direction) bool {
	for _, direction := range o.directions {
		if direction == candidate {
			return true
		}
	}
	return false
}

// markAdmissionBlocked records the first time a consumer actually needs a
// fresh cost snapshot and cannot obtain one. Cache age alone is not alertable:
// the operational consequence must have blocked an evaluation first.
func (o *FlowCostOracle) markAdmissionBlocked(at time.Time) {
	o.mu.Lock()
	if o.blockedSince.IsZero() {
		o.blockedSince = at.UTC()
	}
	o.mu.Unlock()
}

func (o *FlowCostOracle) Components(
	direction arbitrage.Direction,
	at time.Time,
) ([]FlowCostComponent, bool) {
	if cached, ok := o.freshCached(direction, at); ok {
		return cloneFlowCostComponents(cached.components), true
	}
	if o.supportsDirection(direction) && o.staleFallback.Asset() != "" {
		return []FlowCostComponent{{
			Kind: staleFallbackCostKind, Amount: o.staleFallback,
			Evidence: "configured_stale_cost_fallback", CapturedAt: at.UTC(),
		}}, true
	}
	o.markAdmissionBlocked(at)
	return nil, false
}

// ExitCost returns only the still-pending cost components for a post-bridge
// liquidation. Costs already incurred by the purchase and first NTT transfer
// are deliberately excluded because they are common to both choices.
func (o *FlowCostOracle) ExitCost(
	direction arbitrage.Direction,
	route execution.SequentialExitRoute,
	at time.Time,
) (market.AssetQuantity, bool) {
	costDirection := direction
	kinds := map[string]bool{
		"swap_sell":           true,
		"quote_bridge_spread": true,
		"quote_bridge_source": true,
	}
	if route == execution.ExitReturnToOrigin {
		costDirection = arbitrage.Direction{
			BuyMarket:  direction.SellMarket,
			SellMarket: direction.BuyMarket,
		}
		kinds = map[string]bool{
			"base_bridge_evm":         true,
			"base_bridge_solana":      true,
			"base_bridge_source":      true,
			"base_bridge_redeem":      true,
			"base_bridge_message_fee": true,
			"swap_sell":               true,
		}
	} else if route == execution.ExitSellAtOrigin {
		costDirection = arbitrage.Direction{
			BuyMarket:  direction.SellMarket,
			SellMarket: direction.BuyMarket,
		}
		kinds = map[string]bool{"swap_sell": true}
	} else if route != execution.ExitSellAtDestination {
		return market.AssetQuantity{}, false
	}
	components, ok := o.Components(costDirection, at)
	if !ok {
		return market.AssetQuantity{}, false
	}
	total, _ := market.NewAssetQuantity(o.quoteAsset, new(big.Rat))
	matched := 0
	for _, component := range components {
		if component.Kind == staleFallbackCostKind {
			return component.Amount, true
		}
		if !kinds[component.Kind] {
			continue
		}
		var err error
		total, err = total.Add(component.Amount)
		if err != nil {
			return market.AssetQuantity{}, false
		}
		matched++
	}
	return total, matched > 0
}

// PrefundedExitCost values all effects that remain after the buy settles.
// Destination still owes its sale and both inventory-restoration transfers;
// origin owes only the local recovery sale.
func (o *FlowCostOracle) PrefundedExitCost(
	direction arbitrage.Direction,
	route execution.SequentialExitRoute,
	at time.Time,
) (market.AssetQuantity, bool) {
	costDirection := direction
	var kinds map[string]bool
	switch route {
	case execution.ExitSellAtDestination:
		kinds = map[string]bool{
			"base_bridge_evm":         true,
			"base_bridge_solana":      true,
			"base_bridge_source":      true,
			"base_bridge_redeem":      true,
			"base_bridge_message_fee": true,
			"swap_sell":               true,
			"quote_bridge_spread":     true,
			"quote_bridge_source":     true,
		}
	case execution.ExitSellAtOrigin:
		costDirection = arbitrage.Direction{
			BuyMarket: direction.SellMarket, SellMarket: direction.BuyMarket,
		}
		kinds = map[string]bool{"swap_sell": true}
	default:
		return market.AssetQuantity{}, false
	}
	components, ok := o.Components(costDirection, at)
	if !ok {
		return market.AssetQuantity{}, false
	}
	total, _ := market.NewAssetQuantity(o.quoteAsset, new(big.Rat))
	matched := 0
	for _, component := range components {
		if component.Kind == staleFallbackCostKind {
			return component.Amount, true
		}
		if !kinds[component.Kind] {
			continue
		}
		var err error
		total, err = total.Add(component.Amount)
		if err != nil {
			return market.AssetQuantity{}, false
		}
		matched++
	}
	return total, matched > 0
}

func (o *FlowCostOracle) LastError() error {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.lastErr
}

func (o *FlowCostOracle) refreshOnce(ctx context.Context) error {
	o.mu.Lock()
	latest := cloneOpportunities(o.latest)
	o.lastAttempt = o.clock().UTC()
	o.mu.Unlock()
	estimates, err := o.refresh(ctx, latest)
	if err != nil {
		o.setRefreshError(err)
		return err
	}
	byKey := make(map[flowCostKey]FlowCostEstimate, len(estimates))
	for _, estimate := range estimates {
		inputKey := ""
		if estimate.Input.Asset() != "" {
			inputKey = estimate.Input.Rat().RatString()
		}
		key := flowCostKey{direction: estimate.Direction, input: inputKey}
		if _, duplicate := byKey[key]; duplicate {
			err = fmt.Errorf("complete-flow refresh repeated a direction and input")
			o.setRefreshError(err)
			return err
		}
		byKey[key] = estimate
	}
	now := o.clock().UTC()
	next := make(map[flowCostKey]cachedFlowCost, len(byKey))
	for _, direction := range o.directions {
		found := false
		for key, estimate := range byKey {
			if key.direction != direction {
				continue
			}
			found = true
			if len(estimate.Components) == 0 {
				err = fmt.Errorf("complete-flow refresh returned empty components")
				o.setRefreshError(err)
				return err
			}
			total := new(big.Rat)
			capturedAt := now
			for _, component := range estimate.Components {
				if component.Kind == "" || component.Evidence == "" || component.Amount.Asset() != o.quoteAsset ||
					component.Amount.Sign() < 0 || component.CapturedAt.IsZero() {
					err = fmt.Errorf("complete-flow cost component is invalid")
					o.setRefreshError(err)
					return err
				}
				total.Add(total, component.Amount.Rat())
				if component.CapturedAt.Before(capturedAt) {
					capturedAt = component.CapturedAt.UTC()
				}
			}
			amount, amountErr := market.NewAssetQuantity(o.quoteAsset, total)
			if amountErr != nil {
				o.setRefreshError(amountErr)
				return amountErr
			}
			next[key] = cachedFlowCost{snapshot: arbitrage.CostSnapshot{ID: fmt.Sprintf(
				"complete-flow/%s/%s/%s/%d", direction.BuyMarket, direction.SellMarket, key.input, now.UnixNano()),
				Amount: amount, CapturedAt: capturedAt}, components: cloneFlowCostComponents(estimate.Components)}
		}
		if !found {
			err = fmt.Errorf(
				"complete-flow refresh omitted %s -> %s",
				direction.BuyMarket, direction.SellMarket,
			)
			o.setRefreshError(err)
			return err
		}
	}
	o.mu.Lock()
	wasReady := o.cacheHealthyLocked(now)
	wasAlerted := o.staleAlerted
	o.cache, o.lastErr = next, nil
	o.blockedSince = time.Time{}
	o.staleAlerted = false
	o.mu.Unlock()
	for key, cached := range next {
		breakdown := make(map[string]string, len(cached.components))
		evidence := make(map[string]string, len(cached.components))
		for _, component := range cached.components {
			breakdown[component.Kind] = component.Amount.Decimal(8)
			if component.Evidence != "" {
				evidence[component.Kind] = component.Evidence
			}
		}
		o.logger.Info(
			"complete-flow cost cache refreshed",
			"buy_market", key.direction.BuyMarket,
			"sell_market", key.direction.SellMarket,
			"input", key.input,
			"cost", cached.snapshot.Amount.Decimal(8),
			"asset", cached.snapshot.Amount.Asset(),
			"components", len(cached.components),
			"breakdown", breakdown,
			"evidence", evidence,
			"captured_at", cached.snapshot.CapturedAt,
		)
	}
	if !wasReady && o.onReady != nil {
		o.onReady()
	}
	if wasAlerted && o.onRecovered != nil {
		o.onRecovered()
	}
	return nil
}

func (o *FlowCostOracle) setRefreshError(err error) {
	o.mu.Lock()
	o.lastErr = safeFlowCostError(err)
	o.mu.Unlock()
}

func safeFlowCostError(err error) error {
	if err == nil {
		return nil
	}
	return errors.New(providerURLPattern.ReplaceAllString(err.Error(), "[provider-url]"))
}

func cloneFlowCostComponents(source []FlowCostComponent) []FlowCostComponent {
	return append([]FlowCostComponent(nil), source...)
}

func cloneOpportunities(source []arbitrage.Opportunity) []arbitrage.Opportunity {
	if len(source) == 0 {
		return nil
	}
	result := make([]arbitrage.Opportunity, len(source))
	for index, opportunity := range source {
		result[index] = opportunity
		result[index].Candidates = append([]arbitrage.Candidate(nil), opportunity.Candidates...)
		result[index].Snapshots = append([]market.SnapshotMetadata(nil), opportunity.Snapshots...)
		result[index].Reasons = append([]string(nil), opportunity.Reasons...)
	}
	return result
}
