// Package buildinfo exposes immutable release identity injected by the build.
package buildinfo

import (
	"fmt"
	"strings"
)

var (
	Version = "dev"
	Commit  = "unknown"
	BuiltAt = "unknown"
)

func Summary() string {
	version := normalized(Version, "dev")
	commit := normalized(Commit, "unknown")
	builtAt := normalized(BuiltAt, "unknown")
	if commit == "unknown" && builtAt == "unknown" {
		return "vernier " + version
	}
	return fmt.Sprintf(
		"vernier %s (commit %s, built %s)",
		version,
		commit,
		builtAt,
	)
}

func normalized(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}
