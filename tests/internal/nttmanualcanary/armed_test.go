package nttmanualcanary_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/internal/nttmanualcanary"
)

func TestAwaitSolanaConfirmationRetriesRPCFailures(t *testing.T) {
	t.Parallel()

	attempts := 0
	rebroadcasts, err := nttmanualcanary.AwaitSolanaConfirmationWithReader(
		context.Background(),
		"signature",
		100,
		time.Second,
		50*time.Millisecond,
		time.Millisecond,
		50*time.Millisecond,
		func(context.Context) (bool, error, error) {
			attempts++
			if attempts < 3 {
				return false, nil, errors.New("temporary RPC failure")
			}
			return true, nil, nil
		},
		func(context.Context) (uint64, error) { return 50, nil },
		func(context.Context) error { return nil },
	)
	if err != nil {
		t.Fatalf("await confirmation: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
	if rebroadcasts != 0 {
		t.Fatalf("rebroadcasts = %d, want 0", rebroadcasts)
	}
}

func TestSolanaBridgeTransactionOptsAllowRPCRebroadcasts(t *testing.T) {
	t.Parallel()

	initial := nttmanualcanary.SolanaBridgeTransactionOpts(false)
	retry := nttmanualcanary.SolanaBridgeTransactionOpts(true)
	if initial.MaxRetries == nil || *initial.MaxRetries != nttmanualcanary.SolanaBridgeMaxRetries {
		t.Fatalf("initial max retries = %v", initial.MaxRetries)
	}
	if retry.MaxRetries == nil || *retry.MaxRetries != nttmanualcanary.SolanaBridgeMaxRetries {
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

func TestAwaitSolanaConfirmationReturnsTransactionFailure(t *testing.T) {
	t.Parallel()

	_, err := nttmanualcanary.AwaitSolanaConfirmationWithReader(
		context.Background(),
		"signature",
		100,
		time.Second,
		50*time.Millisecond,
		time.Millisecond,
		50*time.Millisecond,
		func(context.Context) (bool, error, error) {
			return false, errors.New("instruction error"), nil
		},
		func(context.Context) (uint64, error) { return 50, nil },
		func(context.Context) error { return nil },
	)
	if err == nil || !strings.Contains(err.Error(), "instruction error") {
		t.Fatalf("error = %v, want transaction failure", err)
	}
}

func TestAwaitSolanaConfirmationTimeoutDoesNotExposeRPCError(t *testing.T) {
	t.Parallel()

	const secret = "api-key=must-not-leak"
	_, err := nttmanualcanary.AwaitSolanaConfirmationWithReader(
		context.Background(),
		"signature",
		100,
		15*time.Millisecond,
		2*time.Millisecond,
		time.Millisecond,
		2*time.Millisecond,
		func(ctx context.Context) (bool, error, error) {
			<-ctx.Done()
			return false, nil, errors.New("https://rpc.invalid/?" + secret)
		},
		func(context.Context) (uint64, error) { return 50, nil },
		func(context.Context) error { return errors.New("temporary") },
	)
	if err == nil || !strings.Contains(err.Error(), "outcome is unknown") {
		t.Fatalf("error = %v, want unknown outcome", err)
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("error exposed RPC credentials: %v", err)
	}
}

func TestAwaitSolanaConfirmationRebroadcastsSameTransaction(t *testing.T) {
	t.Parallel()

	statusReads := 0
	rebroadcasts, err := nttmanualcanary.AwaitSolanaConfirmationWithReader(
		context.Background(),
		"signature",
		100,
		time.Second,
		50*time.Millisecond,
		time.Millisecond,
		time.Millisecond,
		func(context.Context) (bool, error, error) {
			statusReads++
			return statusReads >= 3, nil, nil
		},
		func(context.Context) (uint64, error) { return 50, nil },
		func(context.Context) error { return errors.New("RPC did not acknowledge retry") },
	)
	if err != nil {
		t.Fatalf("await confirmation: %v", err)
	}
	if rebroadcasts == 0 {
		t.Fatal("expected at least one rebroadcast")
	}
}

func TestAwaitSolanaConfirmationReportsDefinitiveExpiry(t *testing.T) {
	t.Parallel()

	_, err := nttmanualcanary.AwaitSolanaConfirmationWithReader(
		context.Background(),
		"signature",
		100,
		time.Second,
		50*time.Millisecond,
		time.Millisecond,
		time.Millisecond,
		func(context.Context) (bool, error, error) {
			return false, nil, nil
		},
		func(context.Context) (uint64, error) { return 101, nil },
		func(context.Context) error { return nil },
	)
	var expired *nttmanualcanary.SolanaBlockhashExpiredError
	if !errors.As(err, &expired) {
		t.Fatalf("error = %v, want blockhash expiry", err)
	}
	if expired.LastValidBlockHeight != 100 || expired.ObservedBlockHeight != 101 {
		t.Fatalf("unexpected expiry: %+v", expired)
	}
}

func TestRecoverSolanaDestinationBatchRebuildsOnlyAfterExpiry(t *testing.T) {
	t.Parallel()

	attempts := 0
	var output bytes.Buffer
	signature, err := nttmanualcanary.RecoverSolanaDestinationBatch(
		context.Background(),
		"wormhole_post_vaa",
		"posted_vaa",
		"posted-account",
		3,
		&output,
		func(context.Context) (string, error) {
			attempts++
			if attempts == 1 {
				return "", &nttmanualcanary.SolanaBlockhashExpiredError{
					Signature:            "expired-signature",
					LastValidBlockHeight: 100,
					ObservedBlockHeight:  101,
				}
			}
			return "replacement-signature", nil
		},
		func(context.Context) (bool, error) { return false, nil },
		nil,
	)
	if err != nil {
		t.Fatalf("recover destination: %v", err)
	}
	if signature != "replacement-signature" || attempts != 2 {
		t.Fatalf("signature=%q attempts=%d", signature, attempts)
	}
	if !strings.Contains(output.String(), "reason=blockhash_expired") {
		t.Fatalf("missing rebuild telemetry: %s", output.String())
	}
}

func TestRecoverSolanaDestinationBatchAcceptsCompletionState(t *testing.T) {
	t.Parallel()

	attempts := 0
	marked := 0
	var output bytes.Buffer
	signature, err := nttmanualcanary.RecoverSolanaDestinationBatch(
		context.Background(),
		"ntt_redeem_manual",
		"ntt_inbox_item",
		"inbox-account",
		3,
		&output,
		func(context.Context) (string, error) {
			attempts++
			return "", &nttmanualcanary.SolanaBlockhashExpiredError{
				Signature:            "expired-signature",
				LastValidBlockHeight: 100,
				ObservedBlockHeight:  101,
			}
		},
		func(context.Context) (bool, error) { return true, nil },
		func(*nttmanualcanary.SolanaBlockhashExpiredError) { marked++ },
	)
	if err != nil {
		t.Fatalf("recover destination: %v", err)
	}
	if signature != "expired-signature" || attempts != 1 || marked != 1 {
		t.Fatalf(
			"signature=%q attempts=%d marked=%d",
			signature,
			attempts,
			marked,
		)
	}
	if !strings.Contains(output.String(), "evidence=ntt_inbox_item") {
		t.Fatalf("missing state recovery telemetry: %s", output.String())
	}
}

func TestRecoverSolanaDestinationBatchDoesNotRetryUnknownOutcome(t *testing.T) {
	t.Parallel()

	attempts := 0
	_, err := nttmanualcanary.RecoverSolanaDestinationBatch(
		context.Background(),
		"wormhole_post_vaa",
		"",
		"",
		3,
		&bytes.Buffer{},
		func(context.Context) (string, error) {
			attempts++
			return "", errors.New("confirmation unavailable")
		},
		nil,
		nil,
	)
	if err == nil || attempts != 1 {
		t.Fatalf("error=%v attempts=%d, want one attempt", err, attempts)
	}
}
