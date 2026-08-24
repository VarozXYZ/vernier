package costing_test

import (
	"context"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/core/costing"
	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

type conversionProvider struct {
	output market.TokenAmount
}

type observedConversionProvider struct {
	output market.TokenAmount
	called chan time.Time
	once   sync.Once
}

func (p *observedConversionProvider) ID() market.SourceID { return "observed-conversion" }

func (p *observedConversionProvider) QuoteConversion(context.Context,
	quoteport.ConversionRequest) (market.TokenAmount, error) {
	p.once.Do(func() { p.called <- time.Now() })
	return p.output, nil
}

func (conversionProvider) ID() market.SourceID { return "conversion" }

func (p conversionProvider) QuoteConversion(context.Context,
	quoteport.ConversionRequest) (market.TokenAmount, error) {
	return p.output, nil
}

func TestQuoteConversionBookProjectsCrossChainTokenIdentityAndDecimals(t *testing.T) {
	now := time.Date(2026, 8, 16, 0, 0, 0, 0, time.UTC)
	operational := market.Token{ID: "quote_operational", Asset: "usd", Chain: "remote", Decimals: 18}
	bridge := market.Token{ID: "quote_bridge", Asset: "usd", Chain: "remote", Decimals: 18}
	peer := market.Token{ID: "quote_peer", Asset: "usd", Chain: "local", Decimals: 6}
	input, _ := market.NewTokenAmount(operational.ID, new(big.Int).Mul(big.NewInt(500), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)))
	output, _ := market.NewTokenAmount(bridge.ID, new(big.Int).Mul(big.NewInt(499), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)))
	worker, err := costing.NewQuoteConversionWorker(costing.QuoteConversionWorkerConfig{
		Provider: conversionProvider{output: output}, Input: input, OutputToken: bridge.ID,
		RefreshInterval: time.Second, TTL: 3 * time.Second, Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	book, err := costing.NewQuoteConversionBookWithAliases([]*costing.QuoteConversionWorker{worker},
		[]costing.QuoteConversionAlias{{Input: operational, Output: peer,
			CanonicalInput: operational, CanonicalOutput: bridge}})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, ok := book.Snapshot(operational.ID, peer.ID, now.Add(time.Second))
	if !ok {
		t.Fatal("projected conversion snapshot is unavailable")
	}
	if got, want := snapshot.Input.Units().String(), "500000000000000000000"; got != want {
		t.Fatalf("projected input = %s, want %s", got, want)
	}
	if got, want := snapshot.Output.Units().String(), "499000000"; got != want {
		t.Fatalf("projected output = %s, want %s", got, want)
	}
}

func TestQuoteConversionWorkerHonorsInitialStagger(t *testing.T) {
	input, _ := market.NewTokenAmount("quote_a", big.NewInt(500_000_000))
	output, _ := market.NewTokenAmount("quote_b", big.NewInt(499_000_000))
	provider := &observedConversionProvider{output: output, called: make(chan time.Time, 1)}
	worker, err := costing.NewQuoteConversionWorker(costing.QuoteConversionWorkerConfig{
		Provider: provider, Input: input, OutputToken: output.Token(), InitialDelay: 30 * time.Millisecond,
		RefreshInterval: time.Second, TTL: 3 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := time.Now()
	go func() { _ = worker.Run(ctx) }()
	select {
	case <-provider.called:
		t.Fatal("worker refreshed before its initial stagger elapsed")
	case <-time.After(10 * time.Millisecond):
	}
	select {
	case called := <-provider.called:
		if called.Sub(started) < 25*time.Millisecond {
			t.Fatalf("worker refreshed after only %s", called.Sub(started))
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("worker did not refresh after its initial stagger")
	}
}
