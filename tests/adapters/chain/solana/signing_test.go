package solana_test

import (
	"strings"
	"testing"

	solanago "github.com/gagliardetto/solana-go"

	chainadapter "github.com/VarozXYZ/vernier/adapters/chain/solana"
)

func TestCompletePartiallySignedTransactionPreservesProviderSignature(t *testing.T) {
	local := solanago.NewWallet().PrivateKey
	provider := solanago.NewWallet().PrivateKey
	transaction := signedFixture(t, local, provider)

	if _, err := transaction.PartialSign(func(key solanago.PublicKey) *solanago.PrivateKey {
		if key.Equals(provider.PublicKey()) {
			return &provider
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	providerSignature := transaction.Signatures[1]

	if err := chainadapter.CompletePartiallySignedTransaction(transaction, local); err != nil {
		t.Fatal(err)
	}
	if transaction.Signatures[1] != providerSignature {
		t.Fatal("provider signature was overwritten")
	}
	if err := transaction.VerifySignatures(); err != nil {
		t.Fatalf("completed signatures are invalid: %v", err)
	}
}

func TestCompletePartiallySignedTransactionRejectsMissingProviderSignature(t *testing.T) {
	local := solanago.NewWallet().PrivateKey
	provider := solanago.NewWallet().PrivateKey
	transaction := signedFixture(t, local, provider)

	err := chainadapter.CompletePartiallySignedTransaction(transaction, local)
	if err == nil || !strings.Contains(err.Error(), "provider signature") {
		t.Fatalf("expected missing provider signature error, got %v", err)
	}
}

func signedFixture(
	t *testing.T,
	local solanago.PrivateKey,
	provider solanago.PrivateKey,
) *solanago.Transaction {
	t.Helper()
	instruction := solanago.NewInstruction(
		solanago.SystemProgramID,
		solanago.AccountMetaSlice{
			{PublicKey: local.PublicKey(), IsSigner: true, IsWritable: true},
			{PublicKey: provider.PublicKey(), IsSigner: true, IsWritable: false},
		},
		[]byte{1},
	)
	transaction, err := solanago.NewTransaction(
		[]solanago.Instruction{instruction},
		solanago.Hash{1},
		solanago.TransactionPayer(local.PublicKey()),
	)
	if err != nil {
		t.Fatal(err)
	}
	return transaction
}
