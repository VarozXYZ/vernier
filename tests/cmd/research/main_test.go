package research_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	runtimeresearch "github.com/VarozXYZ/vernier/runtime/research"
)

var (
	repositoryRoot string
	researchBinary string
)

func TestMain(m *testing.M) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve repository root: %v\n", err)
		os.Exit(1)
	}
	buildDirectory, err := os.MkdirTemp("", "vernier-research-test-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "create build directory: %v\n", err)
		os.Exit(1)
	}
	extension := ""
	if runtime.GOOS == "windows" {
		extension = ".exe"
	}
	binary := filepath.Join(buildDirectory, "research"+extension)
	build := exec.Command("go", "build", "-o", binary, "./cmd/research")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build research command: %v\n%s", err, output)
		_ = os.RemoveAll(buildDirectory)
		os.Exit(1)
	}
	repositoryRoot = root
	researchBinary = binary
	code := m.Run()
	_ = os.RemoveAll(buildDirectory)
	os.Exit(code)
}

func TestRunTextMatchesGolden(t *testing.T) {
	code, stdout, stderr := runCLI(t,
		"--fixture", filepath.Join(repositoryRoot, "examples", "synthetic", "two-market.json"),
		"--format", "text",
	)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	want, err := os.ReadFile(filepath.Join("testdata", "research.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal([]byte(stdout), want) {
		t.Fatalf("text output differs from golden\n--- got ---\n%s\n--- want ---\n%s", stdout, want)
	}
}

func TestRunExitCodes(t *testing.T) {
	fixture := filepath.Join(repositoryRoot, "examples", "synthetic", "two-market.json")
	for name, args := range map[string][]string{
		"missing fixture":    {"--fixture", filepath.Join(t.TempDir(), "missing.json")},
		"bad format":         {"--fixture", fixture, "--format", "yaml"},
		"bad observe format": {"observe-v3", "--format", "json"},
	} {
		t.Run(name, func(t *testing.T) {
			code, _, _ := runCLI(t, args...)
			if code != 2 {
				t.Fatalf("got exit %d, want 2", code)
			}
		})
	}
}

func TestRunReturnsSuccessForExplicitDegradation(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(repositoryRoot, "examples", "synthetic", "two-market.json"))
	if err != nil {
		t.Fatal(err)
	}
	configured, _, err := runtimeresearch.ParseFixture(data)
	if err != nil {
		t.Fatal(err)
	}
	configured.Feeds[1].Disconnect = &runtimeresearch.DisconnectFixture{
		Reason: "websocket_disconnected", ObservedAt: "2026-01-01T00:00:03.100Z",
		EvaluationStartedAt:  "2026-01-01T00:00:03.120Z",
		EvaluationFinishedAt: "2026-01-01T00:00:03.130Z",
	}
	degraded, err := json.MarshalIndent(configured, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	fixture := filepath.Join(t.TempDir(), "degraded.json")
	if err := os.WriteFile(fixture, degraded, 0o600); err != nil {
		t.Fatal(err)
	}

	code, stdout, stderr := runCLI(t, "--fixture", fixture, "--format", "json")
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if !strings.Contains(stdout, `"status": "degraded"`) || !strings.Contains(stdout, `"classification": "unclassifiable"`) {
		t.Fatalf("degradation is not explicit: %s", stdout)
	}
}

func runCLI(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	command := exec.Command(researchBinary, args...)
	command.Dir = repositoryRoot
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	if err == nil {
		return 0, stdout.String(), stderr.String()
	}
	var exitError *exec.ExitError
	if !errors.As(err, &exitError) {
		t.Fatalf("run research command: %v", err)
	}
	return exitError.ExitCode(), stdout.String(), stderr.String()
}
