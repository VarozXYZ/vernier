package wttbridgecanary_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/VarozXYZ/vernier/internal/wttbridgecanary"
)

func TestArmedAmountMustBeRepeatedExactly(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := wttbridgecanary.Run(context.Background(), []string{
		"-config", "unused.yaml", "-env-file", "unused.env",
		"-source", "chain_alpha", "-destination", "chain_beta",
		"-amount", "10", "-confirm-amount", "10.0", "-arm",
	}, &stdout, &stderr)
	if code != 2 || !strings.Contains(stderr.String(), "must exactly match") {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
}

func TestResumeRequiresArmAndNoTransferFields(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := wttbridgecanary.Run(context.Background(), []string{
		"-config", "unused.yaml", "-env-file", "unused.env",
		"-resume-operation", "manual-wtt-synthetic",
	}, &stdout, &stderr)
	if code != 2 || !strings.Contains(stderr.String(), "requires --arm") {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
}

func TestAmountMustBePositiveDecimal(t *testing.T) {
	for _, amount := range []string{"0.0", "-1", "1/2"} {
		t.Run(amount, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := wttbridgecanary.Run(context.Background(), []string{
				"-config", "unused.yaml", "-env-file", "unused.env",
				"-source", "chain_alpha", "-destination", "chain_beta",
				"-amount", amount,
			}, &stdout, &stderr)
			if code != 2 || !strings.Contains(stderr.String(), "positive exact decimal") {
				t.Fatalf("amount=%q code=%d stderr=%q", amount, code, stderr.String())
			}
		})
	}
}
