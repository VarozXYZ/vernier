package observability_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/VarozXYZ/vernier/runtime/observability"
)

func TestNewLoggerWritesStructuredDiagnosticsAtConfiguredLevel(t *testing.T) {
	var output bytes.Buffer
	logger, err := observability.NewLogger(&output, "debug")
	if err != nil {
		t.Fatal(err)
	}
	logger.Debug("phase", "market", "virtual_base", "block", uint64(42))
	if !strings.Contains(output.String(), "level=DEBUG") || !strings.Contains(output.String(), "market=virtual_base") {
		t.Fatalf("unexpected diagnostic output: %s", output.String())
	}
}

func TestNewLoggerRejectsUnknownLevel(t *testing.T) {
	if _, err := observability.NewLogger(&bytes.Buffer{}, "trace"); err == nil {
		t.Fatal("unknown log level was accepted")
	}
}
