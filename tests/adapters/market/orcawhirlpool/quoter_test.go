package orcawhirlpool_test

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/adapters/market/orcawhirlpool"
	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

func TestWhirlpoolVirtualReserveQuoteIsIntegerAndImmutable(t *testing.T) {
	price := new(big.Int).Lsh(big.NewInt(1), 64)
	update, err := orcawhirlpool.NewStateUpdate(price, 0, big.NewInt(1000), 30, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	data, hash, err := (orcawhirlpool.Reducer{}).Reduce(context.Background(), nil, update)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := testSnapshot(t, "pool", data, hash)
	quoter, err := orcawhirlpool.NewQuoter("orca", market.Market{ID: "pool", BaseToken: "a", QuoteToken: "b"}, "a", "b")
	if err != nil {
		t.Fatal(err)
	}
	amount, _ := market.NewTokenAmount("a", big.NewInt(100))
	quote, err := quoter.Quote(context.Background(), quoteport.Input{Snapshot: snapshot, TokenIn: "a", TokenOut: "b", AmountIn: amount, Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if quote.AmountOut.Units().Cmp(big.NewInt(99)) != 0 {
		t.Fatalf("amount out = %s", quote.AmountOut.Units())
	}
	state := snapshot.Data().(orcawhirlpool.Snapshot)
	price.SetInt64(1)
	if state.SqrtPriceX64().Cmp(new(big.Int).Lsh(big.NewInt(1), 64)) != 0 {
		t.Fatal("snapshot price was aliased")
	}
}

func testSnapshot(t *testing.T, id market.MarketID, data market.SnapshotData, hash [32]byte) market.MarketSnapshot {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	snapshot, err := market.NewMarketSnapshot(market.SnapshotMetadata{Market: id, Source: "feed", Version: 1, ReceivedAt: now, AppliedAt: now, Health: market.HealthHealthy, HealthChangedAt: now, StateHash: hash}, data)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
