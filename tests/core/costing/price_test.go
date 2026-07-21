package costing_test

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/core/costing"
	"github.com/VarozXYZ/vernier/domain/market"
	priceport "github.com/VarozXYZ/vernier/ports/price"
)

type priceSource struct {
	id          market.SourceID
	observation market.PriceObservation
	err         error
	calls       int
}

func (s *priceSource) ID() market.SourceID { return s.id }
func (s *priceSource) Observe(context.Context, priceport.Request) (market.PriceObservation, error) {
	s.calls++
	return s.observation, s.err
}

func TestFallbackPriceSourceUsesFallbackOnlyAfterPrimaryFailure(t *testing.T) {
	observation, _ := market.NewPriceObservation("chainlink", "weth", "usd", big.NewRat(2000, 1), "round", time.Unix(1, 0), time.Unix(2, 0))
	primary := &priceSource{id: "coingecko", err: errors.New("unavailable")}
	fallback := &priceSource{id: "chainlink", observation: observation}
	source, err := costing.NewFallbackPriceSource("weth-usd", primary, fallback)
	if err != nil {
		t.Fatal(err)
	}
	got, err := source.Observe(context.Background(), priceport.Request{Base: "weth", Quote: "usd"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Source() != "chainlink" || primary.calls != 1 || fallback.calls != 1 {
		t.Fatalf("source=%s calls=%d/%d", got.Source(), primary.calls, fallback.calls)
	}
}

func TestFallbackPriceSourceDoesNotCallFallbackAfterSuccess(t *testing.T) {
	observation, _ := market.NewPriceObservation("coingecko", "weth", "usd", big.NewRat(2000, 1), "coin", time.Unix(1, 0), time.Unix(2, 0))
	primary := &priceSource{id: "coingecko", observation: observation}
	fallback := &priceSource{id: "chainlink", err: errors.New("must not run")}
	source, _ := costing.NewFallbackPriceSource("weth-usd", primary, fallback)
	if _, err := source.Observe(context.Background(), priceport.Request{Base: "weth", Quote: "usd"}); err != nil {
		t.Fatal(err)
	}
	if primary.calls != 1 || fallback.calls != 0 {
		t.Fatalf("calls=%d/%d", primary.calls, fallback.calls)
	}
}
