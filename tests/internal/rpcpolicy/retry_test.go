package rpcpolicy_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/internal/rpcpolicy"
)

func TestRetryReadRetriesRateLimit(t *testing.T) {
	attempts := 0
	err := rpcpolicy.RetryRead(
		context.Background(), 3, time.Millisecond,
		func(context.Context) error {
			attempts++
			if attempts < 3 {
				return errors.New("HTTP status 429 Too Many Requests")
			}
			return nil
		},
	)
	if err != nil || attempts != 3 {
		t.Fatalf("err=%v attempts=%d", err, attempts)
	}
}

func TestRetryReadDoesNotRetryPermanentFailure(t *testing.T) {
	attempts := 0
	err := rpcpolicy.RetryRead(
		context.Background(), 3, time.Millisecond,
		func(context.Context) error {
			attempts++
			return errors.New("invalid account")
		},
	)
	if err == nil || attempts != 1 {
		t.Fatalf("err=%v attempts=%d", err, attempts)
	}
}
