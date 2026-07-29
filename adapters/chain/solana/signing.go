package solana

import (
	"fmt"

	solanago "github.com/gagliardetto/solana-go"
)

// CompletePartiallySignedTransaction adds the local signature without
// overwriting signatures already embedded by an artifact provider.
func CompletePartiallySignedTransaction(
	transaction *solanago.Transaction,
	privateKey solanago.PrivateKey,
) error {
	if transaction == nil {
		return fmt.Errorf("solana transaction is required")
	}
	if len(privateKey) == 0 {
		return fmt.Errorf("solana private key is required")
	}
	publicKey := privateKey.PublicKey()
	if !transaction.IsSigner(publicKey) {
		return fmt.Errorf("local Solana key %s is not a required transaction signer", publicKey)
	}
	if _, err := transaction.PartialSign(func(key solanago.PublicKey) *solanago.PrivateKey {
		if key.Equals(publicKey) {
			return &privateKey
		}
		return nil
	}); err != nil {
		return fmt.Errorf("partially sign Solana transaction: %w", err)
	}
	if err := transaction.VerifySignatures(); err != nil {
		return fmt.Errorf("solana artifact is missing or has an invalid provider signature: %w", err)
	}
	return nil
}
