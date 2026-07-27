package inventory_test

import (
	"math/big"
	"testing"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/inventory"
	"github.com/VarozXYZ/vernier/domain/market"
)

func TestSettlementIsIdempotentOnlyForIdenticalEffects(t *testing.T) {
	key := inventory.Key{Chain: "chain", Account: "account", Token: "token"}
	owner, err := inventory.New(map[inventory.Key]market.TokenAmount{
		key: amount(t, "token", 100),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Reserve("reservation", "operation", []inventory.Requirement{
		{Key: key, Amount: amount(t, "token", 10)},
	}); err != nil {
		t.Fatal(err)
	}
	effects := []inventory.Effect{{Key: key, Delta: big.NewInt(-10)}}
	if err := owner.Settle("reservation", effects); err != nil {
		t.Fatal(err)
	}
	if err := owner.Settle("reservation", effects); err != nil {
		t.Fatalf("identical repeated settlement failed: %v", err)
	}
	if err := owner.Settle(
		"reservation", []inventory.Effect{{Key: key, Delta: big.NewInt(-9)}},
	); err == nil {
		t.Fatal("different repeated settlement was accepted")
	}
	if _, err := owner.Reserve(
		"reservation", execution.OperationID("operation"), []inventory.Requirement{
			{Key: key, Amount: amount(t, "token", 1)},
		},
	); err == nil {
		t.Fatal("settled reservation identity was reused")
	}
}

func amount(t *testing.T, token market.TokenID, units int64) market.TokenAmount {
	t.Helper()
	result, err := market.NewTokenAmount(token, big.NewInt(units))
	if err != nil {
		t.Fatal(err)
	}
	return result
}
