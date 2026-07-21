// Package observability provides the small runtime logging boundary used by
// commands and adapters. Logs are operational diagnostics; reports remain the
// deterministic research output.
package observability

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
)

// NewLogger creates a structured text logger. It intentionally writes only to
// the supplied diagnostic stream, keeping stdout available for reports.
func NewLogger(writer io.Writer, level string) (*slog.Logger, error) {
	if writer == nil {
		return nil, fmt.Errorf("log writer is required")
	}
	var threshold slog.Level
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		threshold = slog.LevelDebug
	case "info", "":
		threshold = slog.LevelInfo
	case "warn", "warning":
		threshold = slog.LevelWarn
	case "error":
		threshold = slog.LevelError
	default:
		return nil, fmt.Errorf("unsupported log level %q", level)
	}
	return slog.New(slog.NewTextHandler(writer, &slog.HandlerOptions{Level: threshold})), nil
}

func DiscardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
