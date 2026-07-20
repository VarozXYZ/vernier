package market_test

import (
	"math/big"
	"testing"

	"github.com/VarozXYZ/vernier/domain/market"
)

func TestTokenAmountIsImmutable(t *testing.T) {
	units := big.NewInt(123)
	amount, err := market.NewTokenAmount("token", units)
	if err != nil {
		t.Fatal(err)
	}

	units.SetInt64(999)
	copy := amount.Units()
	copy.SetInt64(456)
	if got := amount.String(); got != "123" {
		t.Fatalf("amount changed through an alias: got %s", got)
	}
}

func TestAssetQuantityConversionUsesExactDecimalsAndRoundsDown(t *testing.T) {
	quantity, err := market.ParseAssetQuantity("usd", "1.23456789")
	if err != nil {
		t.Fatal(err)
	}
	token := market.Token{ID: "usd-6", Asset: "usd", Decimals: 6}

	amount, err := quantity.ToTokenAmount(token)
	if err != nil {
		t.Fatal(err)
	}
	if got := amount.String(); got != "1234567" {
		t.Fatalf("unexpected rounded units: got %s", got)
	}

	roundTrip, err := amount.ToAssetQuantity(token)
	if err != nil {
		t.Fatal(err)
	}
	if got := roundTrip.Decimal(6); got != "1.234567" {
		t.Fatalf("unexpected round trip: got %s", got)
	}
}

func TestAssetQuantityRejectsCrossAssetArithmetic(t *testing.T) {
	left, _ := market.ParseAssetQuantity("asset-a", "1")
	right, _ := market.ParseAssetQuantity("asset-b", "1")
	if _, err := left.Add(right); err == nil {
		t.Fatal("expected cross-asset addition to fail")
	}
}

func TestNegativeQuantityCannotBecomeTokenAmount(t *testing.T) {
	quantity, _ := market.ParseAssetQuantity("usd", "-1")
	if _, err := quantity.ToTokenAmount(market.Token{ID: "usd", Asset: "usd", Decimals: 6}); err == nil {
		t.Fatal("expected negative conversion to fail")
	}
}
