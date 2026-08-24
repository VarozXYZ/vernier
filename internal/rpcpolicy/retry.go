// Package rpcpolicy contains bounded retry policy for read-only RPC calls.
// It must never be used to retry broadcasts.
package rpcpolicy

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"
)

const (
	DefaultReadAttempts = 4
	DefaultInitialDelay = 100 * time.Millisecond
)

// RetryRead retries only failures that are safe for read-only requests. The
// supplied operation must not mutate chain state or broadcast a transaction.
func RetryRead(
	ctx context.Context,
	attempts int,
	initialDelay time.Duration,
	operation func(context.Context) error,
) error {
	if attempts < 1 || initialDelay <= 0 || operation == nil {
		return fmt.Errorf("invalid RPC read retry policy")
	}
	var last error
	delay := initialDelay
	for attempt := 1; attempt <= attempts; attempt++ {
		last = operation(ctx)
		if last == nil {
			return nil
		}
		if !Temporary(last) || attempt == attempts {
			return last
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if delay < time.Second {
			delay *= 2
		}
	}
	return last
}

// Read is the result-bearing form of RetryRead.
func Read[T any](
	ctx context.Context,
	attempts int,
	initialDelay time.Duration,
	operation func(context.Context) (T, error),
) (T, error) {
	var result T
	if operation == nil {
		return result, fmt.Errorf("invalid RPC read retry policy")
	}
	err := RetryRead(ctx, attempts, initialDelay, func(callCtx context.Context) error {
		var callErr error
		result, callErr = operation(callCtx)
		return callErr
	})
	return result, err
}

func Temporary(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var networkError net.Error
	if errors.As(err, &networkError) {
		return networkError.Timeout()
	}
	message := strings.ToLower(err.Error())
	for _, marker := range []string{
		"too many requests", "status code: 429", "status code 429",
		"http status 429", "http 429", "http status 408", "http 408",
		"http status 425", "http 425", "http status 500", "http 500",
		"http status 502", "http 502", "http status 503", "http 503",
		"http status 504", "http 504", "connection reset", "unexpected eof",
		"temporarily unavailable",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}
