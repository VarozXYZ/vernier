package execution_test

import (
	"errors"
	"testing"

	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

func TestRecoveryErrorCarriesDeficientChain(t *testing.T) {
	err := executionport.NewChainRecoveryError(
		executionport.RecoveryFailureInsufficientNative,
		market.ChainID("destination-chain"),
		errors.New("native balance is below minimum"),
	)
	if executionport.RecoveryKind(err) != executionport.RecoveryFailureInsufficientNative {
		t.Fatalf("kind=%s", executionport.RecoveryKind(err))
	}
	chain, ok := executionport.RecoveryChain(err)
	if !ok || chain != "destination-chain" {
		t.Fatalf("chain=%s found=%t", chain, ok)
	}
}
