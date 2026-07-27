package local_test

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	localexecution "github.com/VarozXYZ/vernier/adapters/execution/local"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

func TestCachedCostBlocksExecutionWhenFeeDataIsStale(t *testing.T) {
	now := time.Date(2026, 7, 27, 19, 0, 0, 0, time.UTC)
	cost, err := market.NewAssetQuantity("quote", big.NewRat(1, 10))
	if err != nil {
		t.Fatal(err)
	}
	clock := now
	cache, err := localexecution.NewCachedCostEstimator(localexecution.CachedCostConfig{
		Initial: cost, CapturedAt: now, MaxAge: time.Second,
		Clock: func() time.Time { return clock },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Estimate(context.Background(), executionport.CostRequest{}); err != nil {
		t.Fatal(err)
	}
	clock = now.Add(2 * time.Second)
	if _, err := cache.Estimate(context.Background(), executionport.CostRequest{}); !errors.Is(
		err, localexecution.ErrCostCacheStale,
	) {
		t.Fatalf("stale estimate error = %v", err)
	}
	if err := cache.Update(cost, clock); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Estimate(context.Background(), executionport.CostRequest{}); err != nil {
		t.Fatal(err)
	}
}
