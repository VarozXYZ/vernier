package observability_test

import (
	"bytes"
	"regexp"
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
	line := output.String()
	if !regexp.MustCompile(`^\d{4}-\d{2}-\d{2}/\d{2}:\d{2}:\d{2}/\d{3} level=DEBUG`).MatchString(line) {
		t.Fatalf("unexpected timestamp format: %s", line)
	}
	if strings.Contains(line, "time=") || strings.Contains(line, "+02:") || !strings.Contains(line, "market=virtual_base") {
		t.Fatalf("unexpected diagnostic output: %s", output.String())
	}
}

func TestNewLoggerRejectsUnknownLevel(t *testing.T) {
	if _, err := observability.NewLogger(&bytes.Buffer{}, "trace"); err == nil {
		t.Fatal("unknown log level was accepted")
	}
}
