package solana

import solanarpc "github.com/gagliardetto/solana-go/rpc"

// PayerLamportDebits separates the protocol-level transaction fee from other
// observed payer debits, such as a Sender tip or account-creation rent.
func PayerLamportDebits(
	meta *solanarpc.TransactionMeta,
) (networkFee, additionalDebit, totalDebit uint64) {
	if meta == nil {
		return 0, 0, 0
	}
	networkFee = meta.Fee
	if len(meta.PreBalances) == 0 || len(meta.PostBalances) == 0 ||
		meta.PreBalances[0] < meta.PostBalances[0] {
		return networkFee, 0, 0
	}
	totalDebit = meta.PreBalances[0] - meta.PostBalances[0]
	if totalDebit > networkFee {
		additionalDebit = totalDebit - networkFee
	}
	return networkFee, additionalDebit, totalDebit
}
