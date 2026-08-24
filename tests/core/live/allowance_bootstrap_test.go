package live_test

import (
	"context"
	"errors"
	"math/big"
	"testing"

	corelive "github.com/VarozXYZ/vernier/core/live"
)

type allowanceHarness struct {
	value       *big.Int
	writes      []*big.Int
	revertFirst bool
}

func (h *allowanceHarness) Allowance(context.Context, corelive.AllowanceRequirement) (*big.Int, error) {
	return new(big.Int).Set(h.value), nil
}
func (h *allowanceHarness) Approve(_ context.Context, _ corelive.AllowanceRequirement, amount *big.Int) error {
	h.writes = append(h.writes, new(big.Int).Set(amount))
	if h.revertFirst && len(h.writes) == 1 {
		return &corelive.ApprovalError{ConfirmedRevert: true, Err: errors.New("reset required")}
	}
	h.value.Set(amount)
	return nil
}

func TestAllowanceBootstrapApprovesMaximumAndVerifies(t *testing.T) {
	h := &allowanceHarness{value: big.NewInt(10)}
	bootstrap := corelive.AllowanceBootstrap{Reader: h, Writer: h, Requirements: []corelive.AllowanceRequirement{{
		Chain: "chain", Token: "token", Spender: "spender", Purpose: "swap",
	}}}
	result, err := bootstrap.Ensure(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 || !result[0].Changed || len(h.writes) != 1 ||
		h.writes[0].Cmp(corelive.MaximumAllowance) != 0 {
		t.Fatalf("unexpected result=%+v writes=%v", result, h.writes)
	}
}

func TestAllowanceBootstrapUsesZeroResetOnlyAfterConfirmedRevert(t *testing.T) {
	h := &allowanceHarness{value: big.NewInt(10), revertFirst: true}
	bootstrap := corelive.AllowanceBootstrap{Reader: h, Writer: h, Requirements: []corelive.AllowanceRequirement{{
		Chain: "chain", Token: "token", Spender: "spender", Purpose: "bridge",
	}}}
	result, err := bootstrap.Ensure(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 || !result[0].Reset || len(h.writes) != 3 ||
		h.writes[1].Sign() != 0 || h.writes[2].Cmp(corelive.MaximumAllowance) != 0 {
		t.Fatalf("unexpected reset result=%+v writes=%v", result, h.writes)
	}
}

func TestAllowanceBootstrapDryRunNeverWrites(t *testing.T) {
	h := &allowanceHarness{value: big.NewInt(1)}
	bootstrap := corelive.AllowanceBootstrap{Reader: h, Requirements: []corelive.AllowanceRequirement{{
		Chain: "chain", Token: "token", Spender: "spender", Purpose: "swap",
	}}}
	result, err := bootstrap.Ensure(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(h.writes) != 0 || result[0].Changed {
		t.Fatal("dry run wrote an approval")
	}
}

func TestAllowanceBootstrapRevokesObsoleteSpender(t *testing.T) {
	h := &allowanceHarness{value: new(big.Int).Set(corelive.MaximumAllowance)}
	bootstrap := corelive.AllowanceBootstrap{Reader: h, Writer: h, Revocations: []corelive.AllowanceRequirement{{
		Chain: "chain", Token: "token", Spender: "obsolete", Purpose: "obsolete router",
	}}}
	result, err := bootstrap.Ensure(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 || !result[0].Changed || result[0].After.Sign() != 0 ||
		len(h.writes) != 1 || h.writes[0].Sign() != 0 {
		t.Fatalf("obsolete allowance was not revoked: result=%+v writes=%v", result, h.writes)
	}
}
