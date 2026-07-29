package buildinfo_test

import (
	"strings"
	"testing"

	"github.com/VarozXYZ/vernier/internal/buildinfo"
)

func TestSummaryIncludesInjectedReleaseIdentity(t *testing.T) {
	originalVersion := buildinfo.Version
	originalCommit := buildinfo.Commit
	originalBuiltAt := buildinfo.BuiltAt
	t.Cleanup(func() {
		buildinfo.Version = originalVersion
		buildinfo.Commit = originalCommit
		buildinfo.BuiltAt = originalBuiltAt
	})

	buildinfo.Version = "v0.1.0-alpha.1"
	buildinfo.Commit = "0123456789abcdef"
	buildinfo.BuiltAt = "2026-01-02T03:04:05Z"

	summary := buildinfo.Summary()
	for _, expected := range []string{
		"vernier v0.1.0-alpha.1",
		"commit 0123456789abcdef",
		"built 2026-01-02T03:04:05Z",
	} {
		if !strings.Contains(summary, expected) {
			t.Fatalf("summary %q does not contain %q", summary, expected)
		}
	}
}

func TestSummaryUsesDevelopmentDefault(t *testing.T) {
	originalVersion := buildinfo.Version
	originalCommit := buildinfo.Commit
	originalBuiltAt := buildinfo.BuiltAt
	t.Cleanup(func() {
		buildinfo.Version = originalVersion
		buildinfo.Commit = originalCommit
		buildinfo.BuiltAt = originalBuiltAt
	})

	buildinfo.Version = ""
	buildinfo.Commit = ""
	buildinfo.BuiltAt = ""

	if got := buildinfo.Summary(); got != "vernier dev" {
		t.Fatalf("summary=%q", got)
	}
}
