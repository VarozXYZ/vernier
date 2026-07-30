package acrossbridgecanary_test

import (
	"testing"

	"github.com/VarozXYZ/vernier/internal/acrossbridgecanary"
)

func TestAcrossSolanaBridgeTransactionOptsAllowRPCRebroadcasts(t *testing.T) {
	t.Parallel()

	initial := acrossbridgecanary.AcrossSolanaBridgeTransactionOpts(false)
	retry := acrossbridgecanary.AcrossSolanaBridgeTransactionOpts(true)
	if initial.MaxRetries == nil ||
		*initial.MaxRetries != acrossbridgecanary.AcrossSolanaBridgeMaxRetries {
		t.Fatalf("initial max retries = %v", initial.MaxRetries)
	}
	if retry.MaxRetries == nil ||
		*retry.MaxRetries != acrossbridgecanary.AcrossSolanaBridgeMaxRetries {
		t.Fatalf("retry max retries = %v", retry.MaxRetries)
	}
	if initial.SkipPreflight || !retry.SkipPreflight {
		t.Fatalf(
			"unexpected preflight policy: initial=%t retry=%t",
			initial.SkipPreflight,
			retry.SkipPreflight,
		)
	}
}
