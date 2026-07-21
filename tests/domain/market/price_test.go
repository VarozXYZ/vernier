package market_test

import (
	"math/big"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/domain/market"
)

func TestPriceObservationIsImmutable(t *testing.T) {
	value := big.NewRat(123, 10)
	observation, err := market.NewPriceObservation("source", "weth", "usd", value, "reference", time.Unix(1, 0), time.Unix(2, 0))
	if err != nil {
		t.Fatal(err)
	}
	value.SetInt64(0)
	returned := observation.Value()
	returned.SetInt64(0)
	if observation.Value().RatString() != "123/10" {
		t.Fatal("price value aliases mutable input or output")
	}
}
