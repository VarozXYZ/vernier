package local

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

var ErrCostCacheStale = errors.New("live execution cost cache is stale")

type CachedCostConfig struct {
	Initial    market.AssetQuantity
	CapturedAt time.Time
	MaxAge     time.Duration
	Clock      func() time.Time
}

// CachedCostEstimator is updated outside the hot path by fee and native-price
// observers. Estimate is a local read and never performs I/O.
type CachedCostEstimator struct {
	mu         sync.RWMutex
	amount     market.AssetQuantity
	capturedAt time.Time
	maxAge     time.Duration
	clock      func() time.Time
}

func NewCachedCostEstimator(config CachedCostConfig) (*CachedCostEstimator, error) {
	if config.Initial.Asset() == "" || config.Initial.Sign() <= 0 ||
		config.CapturedAt.IsZero() || config.MaxAge <= 0 || config.Clock == nil {
		return nil, fmt.Errorf("cached Live execution cost must be positive")
	}
	return &CachedCostEstimator{
		amount: config.Initial, capturedAt: config.CapturedAt.UTC(),
		maxAge: config.MaxAge, clock: config.Clock,
	}, nil
}

func (e *CachedCostEstimator) Update(amount market.AssetQuantity, capturedAt time.Time) error {
	if amount.Asset() == "" || amount.Sign() <= 0 || capturedAt.IsZero() {
		return fmt.Errorf("cached Live execution cost must be positive")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.amount.Asset() != amount.Asset() {
		return fmt.Errorf("cached Live execution cost asset cannot change")
	}
	e.amount = amount
	e.capturedAt = capturedAt.UTC()
	return nil
}

func (e *CachedCostEstimator) Estimate(ctx context.Context, _ executionport.CostRequest) (market.AssetQuantity, error) {
	return e.Current(ctx)
}

func (e *CachedCostEstimator) Current(context.Context) (market.AssetQuantity, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	age := e.clock().UTC().Sub(e.capturedAt)
	if age < 0 || age > e.maxAge {
		return market.AssetQuantity{}, fmt.Errorf(
			"%w: age=%s maximum=%s", ErrCostCacheStale, age, e.maxAge,
		)
	}
	return market.NewAssetQuantity(e.amount.Asset(), e.amount.Rat())
}

var (
	_ executionport.CostEstimator      = (*CachedCostEstimator)(nil)
	_ executionport.CostSnapshotSource = (*CachedCostEstimator)(nil)
)
