package livesequential

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
)

// FlowCostComponent is one independently refreshed part of a complete
// sequential flow. Amount is valued in the setup quote asset. Stage expresses
// economic semantics without naming a chain or transfer provider.
type FlowCostComponent struct {
	Stage      execution.SequentialStage
	Kind       string
	Amount     market.AssetQuantity
	Evidence   string
	CapturedAt time.Time
}

type FlowCostEstimate struct {
	Direction  arbitrage.Direction
	Components []FlowCostComponent
}

type FlowCostRefresh func(
	context.Context,
	[]arbitrage.Opportunity,
) ([]FlowCostEstimate, error)

type cachedFlowCost struct {
	snapshot   arbitrage.CostSnapshot
	components []FlowCostComponent
}

// FlowCostOracle owns a direction-aware complete-flow cache. Refresh performs
// I/O outside discovery; Snapshot is a read lock plus immutable copies.
type FlowCostOracle struct {
	directions []arbitrage.Direction
	quoteAsset market.AssetID
	refresh    FlowCostRefresh
	interval   time.Duration
	ttl        time.Duration
	clock      func() time.Time
	logger     *slog.Logger
	onReady    func()

	mu         sync.RWMutex
	cache      map[arbitrage.Direction]cachedFlowCost
	latest     []arbitrage.Opportunity
	lastErr    error
	refreshNow chan struct{}
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
}

func NewFlowCostOracle(
	config FlowCostOracleConfig,
) (*FlowCostOracle, error) {
	if len(config.Directions) == 0 ||
		config.QuoteAsset == "" ||
		config.Refresh == nil ||
		config.RefreshInterval <= 0 ||
		config.TTL < config.RefreshInterval {
		return nil, fmt.Errorf(
			"complete-flow cost oracle configuration is invalid",
		)
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	directions := append(
		[]arbitrage.Direction(nil),
		config.Directions...,
	)
	seen := make(map[arbitrage.Direction]struct{}, len(directions))
	for _, direction := range directions {
		if direction.BuyMarket == "" ||
			direction.SellMarket == "" ||
			direction.BuyMarket == direction.SellMarket {
			return nil, fmt.Errorf(
				"complete-flow cost direction is invalid",
			)
		}
		if _, duplicate := seen[direction]; duplicate {
			return nil, fmt.Errorf(
				"complete-flow cost direction is repeated",
			)
		}
		seen[direction] = struct{}{}
	}
	return &FlowCostOracle{
		directions: directions,
		quoteAsset: config.QuoteAsset,
		refresh:    config.Refresh,
		interval:   config.RefreshInterval,
		ttl:        config.TTL,
		clock:      config.Clock,
		logger:     config.Logger,
		onReady:    config.OnReady,
		cache: make(
			map[arbitrage.Direction]cachedFlowCost,
			len(directions),
		),
		refreshNow: make(chan struct{}, 1),
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
		case <-o.refreshNow:
			o.runRefresh(ctx)
		case <-ticker.C:
			o.runRefresh(ctx)
		}
	}
}

func (o *FlowCostOracle) runRefresh(ctx context.Context) {
	refreshCtx, cancel := context.WithTimeout(ctx, o.interval)
	err := o.refreshOnce(refreshCtx)
	cancel()
	if err != nil {
		o.logger.Warn(
			"complete-flow cost cache refresh failed",
			"error",
			err,
		)
	}
}

// Observe stores the latest complete route quotes and coalesces an immediate
// out-of-band refresh request. It never performs provider I/O itself.
func (o *FlowCostOracle) Observe(
	opportunities []arbitrage.Opportunity,
) {
	o.mu.Lock()
	o.latest = cloneOpportunities(opportunities)
	o.mu.Unlock()
	select {
	case o.refreshNow <- struct{}{}:
	default:
	}
}

func (o *FlowCostOracle) Snapshot(
	direction arbitrage.Direction,
	at time.Time,
) (arbitrage.CostSnapshot, bool) {
	o.mu.RLock()
	cached, ok := o.cache[direction]
	o.mu.RUnlock()
	if !ok ||
		cached.snapshot.CapturedAt.IsZero() ||
		at.UTC().Sub(cached.snapshot.CapturedAt) > o.ttl {
		return arbitrage.CostSnapshot{}, false
	}
	return cached.snapshot, true
}

func (o *FlowCostOracle) Components(
	direction arbitrage.Direction,
	at time.Time,
) ([]FlowCostComponent, bool) {
	if _, ok := o.Snapshot(direction, at); !ok {
		return nil, false
	}
	o.mu.RLock()
	components := cloneFlowCostComponents(
		o.cache[direction].components,
	)
	o.mu.RUnlock()
	return components, true
}

// ExitCost returns only costs that have not yet been incurred when selecting
// a post-transfer liquidation route.
func (o *FlowCostOracle) ExitCost(
	direction arbitrage.Direction,
	route execution.SequentialExitRoute,
	at time.Time,
) (market.AssetQuantity, bool) {
	costDirection := direction
	stages := map[execution.SequentialStage]bool{
		execution.StageSell:              true,
		execution.StageBridgeQuoteReturn: true,
	}
	if route == execution.ExitReturnToOrigin {
		costDirection = arbitrage.Direction{
			BuyMarket:  direction.SellMarket,
			SellMarket: direction.BuyMarket,
		}
		stages = map[execution.SequentialStage]bool{
			execution.StageBridgeBase: true,
			execution.StageSell:       true,
		}
	} else if route != execution.ExitSellAtDestination {
		return market.AssetQuantity{}, false
	}
	components, ok := o.Components(costDirection, at)
	if !ok {
		return market.AssetQuantity{}, false
	}
	total, _ := market.NewAssetQuantity(
		o.quoteAsset,
		new(big.Rat),
	)
	matched := 0
	for _, component := range components {
		if !stages[component.Stage] {
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
	o.mu.Unlock()

	estimates, err := o.refresh(ctx, latest)
	if err != nil {
		o.setRefreshError(err)
		return err
	}
	byDirection := make(
		map[arbitrage.Direction]FlowCostEstimate,
		len(estimates),
	)
	for _, estimate := range estimates {
		if _, duplicate := byDirection[estimate.Direction]; duplicate {
			err = fmt.Errorf(
				"complete-flow refresh repeated a direction",
			)
			o.setRefreshError(err)
			return err
		}
		byDirection[estimate.Direction] = estimate
	}
	now := o.clock().UTC()
	next := make(
		map[arbitrage.Direction]cachedFlowCost,
		len(o.directions),
	)
	for _, direction := range o.directions {
		estimate, ok := byDirection[direction]
		if !ok || len(estimate.Components) == 0 {
			err = fmt.Errorf(
				"complete-flow refresh omitted %s -> %s",
				direction.BuyMarket,
				direction.SellMarket,
			)
			o.setRefreshError(err)
			return err
		}
		total := new(big.Rat)
		capturedAt := now
		for _, component := range estimate.Components {
			if err = o.validateComponent(component); err != nil {
				o.setRefreshError(err)
				return err
			}
			total.Add(total, component.Amount.Rat())
			if component.CapturedAt.Before(capturedAt) {
				capturedAt = component.CapturedAt.UTC()
			}
		}
		amount, amountErr := market.NewAssetQuantity(
			o.quoteAsset,
			total,
		)
		if amountErr != nil {
			o.setRefreshError(amountErr)
			return amountErr
		}
		next[direction] = cachedFlowCost{
			snapshot: arbitrage.CostSnapshot{
				ID: fmt.Sprintf(
					"complete-flow/%s/%s/%d",
					direction.BuyMarket,
					direction.SellMarket,
					now.UnixNano(),
				),
				Amount:     amount,
				CapturedAt: capturedAt,
			},
			components: cloneFlowCostComponents(
				estimate.Components,
			),
		}
	}

	o.mu.Lock()
	wasReady := len(o.cache) == len(o.directions)
	if wasReady {
		for _, cached := range o.cache {
			if now.Sub(cached.snapshot.CapturedAt) > o.ttl {
				wasReady = false
				break
			}
		}
	}
	o.cache = next
	o.lastErr = nil
	o.mu.Unlock()

	for direction, cached := range next {
		breakdown := make(
			map[string]string,
			len(cached.components),
		)
		for _, component := range cached.components {
			key := string(component.Stage) + "/" + component.Kind
			breakdown[key] = component.Amount.Decimal(8)
		}
		o.logger.Info(
			"complete-flow cost cache refreshed",
			"buy_market",
			direction.BuyMarket,
			"sell_market",
			direction.SellMarket,
			"cost",
			cached.snapshot.Amount.Decimal(8),
			"asset",
			cached.snapshot.Amount.Asset(),
			"components",
			len(cached.components),
			"breakdown",
			breakdown,
			"captured_at",
			cached.snapshot.CapturedAt,
		)
	}
	if !wasReady && o.onReady != nil {
		o.onReady()
	}
	return nil
}

func (o *FlowCostOracle) validateComponent(
	component FlowCostComponent,
) error {
	switch component.Stage {
	case execution.StageBuy,
		execution.StageBridgeBase,
		execution.StageSell,
		execution.StageBridgeQuoteReturn:
	default:
		return fmt.Errorf(
			"complete-flow cost component has invalid stage",
		)
	}
	if component.Kind == "" ||
		component.Evidence == "" ||
		component.Amount.Asset() != o.quoteAsset ||
		component.Amount.Sign() < 0 ||
		component.CapturedAt.IsZero() {
		return fmt.Errorf(
			"complete-flow cost component is invalid",
		)
	}
	return nil
}

func (o *FlowCostOracle) setRefreshError(err error) {
	o.mu.Lock()
	o.lastErr = err
	o.mu.Unlock()
}

func cloneFlowCostComponents(
	source []FlowCostComponent,
) []FlowCostComponent {
	return append([]FlowCostComponent(nil), source...)
}

func cloneOpportunities(
	source []arbitrage.Opportunity,
) []arbitrage.Opportunity {
	if len(source) == 0 {
		return nil
	}
	result := make([]arbitrage.Opportunity, len(source))
	for index, opportunity := range source {
		result[index] = opportunity
		result[index].Candidates = append(
			[]arbitrage.Candidate(nil),
			opportunity.Candidates...,
		)
		result[index].Snapshots = append(
			[]market.SnapshotMetadata(nil),
			opportunity.Snapshots...,
		)
		result[index].Reasons = append(
			[]string(nil),
			opportunity.Reasons...,
		)
	}
	return result
}
