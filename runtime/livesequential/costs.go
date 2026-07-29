package livesequential

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
)

type CostAssetPrice struct {
	Value      *big.Rat
	CapturedAt time.Time
	Source     string
}

type CostPriceRefresh func(
	context.Context,
) (map[market.AssetID]CostAssetPrice, error)

// CostValuator keeps native/quote prices warm outside the execution path.
// Value is lock-protected arithmetic and never performs I/O.
type CostValuator struct {
	quoteAsset market.AssetID
	refresh    CostPriceRefresh

	mu     sync.RWMutex
	prices map[market.AssetID]CostAssetPrice
}

func NewCostValuator(
	quoteAsset market.AssetID,
	refresh CostPriceRefresh,
) (*CostValuator, error) {
	if quoteAsset == "" || refresh == nil {
		return nil, fmt.Errorf(
			"sequential cost valuator configuration is incomplete",
		)
	}
	return &CostValuator{
		quoteAsset: quoteAsset,
		refresh:    refresh,
		prices:     make(map[market.AssetID]CostAssetPrice),
	}, nil
}

func (v *CostValuator) Warm(ctx context.Context) error {
	prices, err := v.refresh(ctx)
	if err != nil {
		return fmt.Errorf("warm execution-cost prices: %w", err)
	}
	if err := validatePrices(prices); err != nil {
		return err
	}
	v.mu.Lock()
	v.prices = clonePrices(prices)
	v.mu.Unlock()
	return nil
}

func (v *CostValuator) Run(ctx context.Context, interval time.Duration) {
	if interval < time.Second {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refreshCtx, cancel := context.WithTimeout(ctx, interval)
			prices, err := v.refresh(refreshCtx)
			cancel()
			if err != nil || validatePrices(prices) != nil {
				continue
			}
			v.mu.Lock()
			v.prices = clonePrices(prices)
			v.mu.Unlock()
		}
	}
}

func (v *CostValuator) Value(
	component execution.CostComponent,
) (execution.CostComponent, error) {
	if err := component.Validate(); err != nil {
		return execution.CostComponent{}, err
	}
	if component.Amount.Asset() == v.quoteAsset {
		return component.WithQuoteValue(component.Amount)
	}
	v.mu.RLock()
	price, ok := v.prices[component.Amount.Asset()]
	v.mu.RUnlock()
	if !ok || price.Value == nil || price.Value.Sign() <= 0 {
		return execution.CostComponent{}, fmt.Errorf(
			"no cached %s/%s price for execution cost",
			component.Amount.Asset(), v.quoteAsset,
		)
	}
	value, err := market.NewAssetQuantity(
		v.quoteAsset,
		new(big.Rat).Mul(component.Amount.Rat(), price.Value),
	)
	if err != nil {
		return execution.CostComponent{}, err
	}
	component.Evidence += "+price:" + price.Source
	return component.WithQuoteValue(value)
}

func (v *CostValuator) Price(
	asset market.AssetID,
) (CostAssetPrice, bool) {
	v.mu.RLock()
	price, ok := v.prices[asset]
	v.mu.RUnlock()
	if !ok || price.Value == nil {
		return CostAssetPrice{}, false
	}
	return CostAssetPrice{
		Value:      new(big.Rat).Set(price.Value),
		CapturedAt: price.CapturedAt,
		Source:     price.Source,
	}, true
}

func valueCosts(
	valuator interface {
		Value(execution.CostComponent) (execution.CostComponent, error)
	},
	costs []execution.CostComponent,
) ([]execution.CostComponent, error) {
	if len(costs) == 0 {
		return nil, nil
	}
	if valuator == nil {
		return nil, fmt.Errorf("execution cost valuator is unavailable")
	}
	result := make([]execution.CostComponent, 0, len(costs))
	for _, cost := range costs {
		valued, err := valuator.Value(cost)
		if err != nil {
			return nil, err
		}
		result = append(result, valued)
	}
	return result, nil
}

func validatePrices(prices map[market.AssetID]CostAssetPrice) error {
	if len(prices) == 0 {
		return fmt.Errorf("execution-cost price refresh returned no prices")
	}
	for asset, price := range prices {
		if asset == "" || price.Value == nil || price.Value.Sign() <= 0 ||
			price.CapturedAt.IsZero() || price.Source == "" {
			return fmt.Errorf("execution-cost price refresh is invalid")
		}
	}
	return nil
}

func clonePrices(
	source map[market.AssetID]CostAssetPrice,
) map[market.AssetID]CostAssetPrice {
	result := make(map[market.AssetID]CostAssetPrice, len(source))
	for asset, price := range source {
		result[asset] = CostAssetPrice{
			Value:      new(big.Rat).Set(price.Value),
			CapturedAt: price.CapturedAt.UTC(),
			Source:     price.Source,
		}
	}
	return result
}
