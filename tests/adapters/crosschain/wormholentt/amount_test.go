package wormholentt_test

import (
	"math/big"
	"testing"

	"github.com/VarozXYZ/vernier/adapters/crosschain/wormholentt"
)

func TestTrimTransferAmountEVMToSolana(t *testing.T) {
	amount := mustAmount(t, "4473904820592314590")
	transferable, dust, decimals, err := wormholentt.TrimTransferAmount(
		amount,
		18,
		9,
	)
	if err != nil {
		t.Fatal(err)
	}
	if transferable.String() != "4473904820000000000" {
		t.Fatalf("transferable = %s", transferable)
	}
	if dust.String() != "592314590" {
		t.Fatalf("dust = %s", dust)
	}
	if decimals != 8 {
		t.Fatalf("trimmed decimals = %d", decimals)
	}
}

func TestTrimTransferAmountSolanaToEVM(t *testing.T) {
	amount := mustAmount(t, "4195867264")
	transferable, dust, decimals, err := wormholentt.TrimTransferAmount(
		amount,
		9,
		18,
	)
	if err != nil {
		t.Fatal(err)
	}
	if transferable.String() != "4195867260" {
		t.Fatalf("transferable = %s", transferable)
	}
	if dust.String() != "4" {
		t.Fatalf("dust = %s", dust)
	}
	if decimals != 8 {
		t.Fatalf("trimmed decimals = %d", decimals)
	}
}

func mustAmount(t *testing.T, text string) *big.Int {
	t.Helper()
	value, ok := new(big.Int).SetString(text, 10)
	if !ok {
		t.Fatalf("invalid test amount %q", text)
	}
	return value
}
