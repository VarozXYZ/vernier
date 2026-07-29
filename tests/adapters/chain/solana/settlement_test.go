package solana_test

import (
	"testing"

	solanaadapter "github.com/VarozXYZ/vernier/adapters/chain/solana"
	solanarpc "github.com/gagliardetto/solana-go/rpc"
)

func TestPayerLamportDebitsSeparatesNetworkFeeAndTip(t *testing.T) {
	fee, additional, total := solanaadapter.PayerLamportDebits(
		&solanarpc.TransactionMeta{
			Fee:          450_524,
			PreBalances:  []uint64{10_000_000},
			PostBalances: []uint64{8_549_476},
		},
	)
	if fee != 450_524 || additional != 1_000_000 || total != 1_450_524 {
		t.Fatalf(
			"unexpected payer debits: fee=%d additional=%d total=%d",
			fee,
			additional,
			total,
		)
	}
}

func TestPayerLamportDebitsHandlesUnavailableBalances(t *testing.T) {
	fee, additional, total := solanaadapter.PayerLamportDebits(
		&solanarpc.TransactionMeta{Fee: 5_000},
	)
	if fee != 5_000 || additional != 0 || total != 0 {
		t.Fatalf(
			"unexpected missing-balance debits: fee=%d additional=%d total=%d",
			fee,
			additional,
			total,
		)
	}
}
