package strategy

import (
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
)

type valuationObservation struct {
	price *big.Rat
	at    time.Time
}

// BaseValuationCache owns the latest valid implied base price observations.
// It deliberately has no wall-clock expiry; feed health and structural
// rebootstrap are separate runtime gates.
type BaseValuationCache struct {
	mu           sync.RWMutex
	base         market.AssetID
	quote        market.AssetID
	version      uint64
	observations map[string]valuationObservation
}

func NewBaseValuationCache(base, quote market.AssetID) (*BaseValuationCache, error) {
	if base == "" || quote == "" || base == quote {
		return nil, fmt.Errorf("valuation cache requires distinct base and quote assets")
	}
	return &BaseValuationCache{base: base, quote: quote, observations: make(map[string]valuationObservation)}, nil
}

// Observe records an exact quote as one latest source/direction observation.
func (c *BaseValuationCache) Observe(key string, quote market.Quote, tokenIn, tokenOut market.Token, observedAt time.Time) error {
	if c == nil || key == "" || observedAt.IsZero() || quote.Quality != market.QuoteQualityExact {
		return fmt.Errorf("valuation observation requires cache, key, exact quote, and timestamp")
	}
	if tokenIn.ID != quote.AmountIn.Token() || tokenOut.ID != quote.AmountOut.Token() ||
		quote.AmountIn.IsZero() || quote.AmountOut.IsZero() {
		return fmt.Errorf("valuation quote does not match its token definitions")
	}
	input, err := quote.AmountIn.ToAssetQuantity(tokenIn)
	if err != nil {
		return err
	}
	output, err := quote.AmountOut.ToAssetQuantity(tokenOut)
	if err != nil {
		return err
	}
	var price *big.Rat
	switch {
	case tokenIn.Asset == c.quote && tokenOut.Asset == c.base:
		price = new(big.Rat).Quo(input.Rat(), output.Rat())
	case tokenIn.Asset == c.base && tokenOut.Asset == c.quote:
		price = new(big.Rat).Quo(output.Rat(), input.Rat())
	default:
		return fmt.Errorf("valuation quote must exchange configured base and quote assets")
	}
	if price.Sign() <= 0 {
		return fmt.Errorf("valuation observation produced a non-positive price")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.version++
	c.observations[key] = valuationObservation{price: price, at: observedAt.UTC()}
	return nil
}

func (c *BaseValuationCache) Snapshot(capturedAt time.Time) (arbitrage.ValuationSnapshot, error) {
	if c == nil || capturedAt.IsZero() {
		return arbitrage.ValuationSnapshot{}, fmt.Errorf("valuation cache and capture time are required")
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.observations) == 0 {
		return arbitrage.ValuationSnapshot{}, fmt.Errorf("valuation cache is not initialized")
	}
	total := new(big.Rat)
	for _, observation := range c.observations {
		total.Add(total, observation.price)
	}
	mean := new(big.Rat).Quo(total, new(big.Rat).SetInt64(int64(len(c.observations))))
	return arbitrage.NewValuationSnapshot(c.version, c.base, c.quote, mean, len(c.observations), capturedAt)
}

func (c *BaseValuationCache) Reset() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.version++
	c.observations = make(map[string]valuationObservation)
}
