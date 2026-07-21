// Package observability provides the small runtime logging boundary used by
// commands and adapters. Logs are operational diagnostics; reports remain the
// deterministic research output.
package observability

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"
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
	output := &timestampWriter{writer: writer}
	handler := slog.NewTextHandler(output, &slog.HandlerOptions{
		Level: threshold,
		ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
			if attr.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return attr
		},
	})
	return slog.New(&timestampHandler{next: handler, output: output, mu: &sync.Mutex{}}), nil
}

func DiscardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type timestampHandler struct {
	next   slog.Handler
	output *timestampWriter
	mu     *sync.Mutex
}

func (h *timestampHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *timestampHandler) Handle(ctx context.Context, record slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.output.prefix = formatTimestamp(record.Time)
	return h.next.Handle(ctx, record)
}

func (h *timestampHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &timestampHandler{next: h.next.WithAttrs(attrs), output: h.output, mu: h.mu}
}

func (h *timestampHandler) WithGroup(name string) slog.Handler {
	return &timestampHandler{next: h.next.WithGroup(name), output: h.output, mu: h.mu}
}

type timestampWriter struct {
	writer io.Writer
	prefix string
}

func (w *timestampWriter) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	line := make([]byte, 0, len(w.prefix)+1+len(data))
	line = append(line, w.prefix...)
	line = append(line, ' ')
	line = append(line, data...)
	if _, err := w.writer.Write(line); err != nil {
		return 0, err
	}
	return len(data), nil
}

func formatTimestamp(value time.Time) string {
	return value.Local().Format("2006-01-02/15:04:05/000")
}
