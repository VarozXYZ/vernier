package aggregatorlatencycompare_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

var commandBinary string

func TestMain(m *testing.M) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	directory, err := os.MkdirTemp("", "vernier-aggregator-compare-test-*")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	extension := ""
	if runtime.GOOS == "windows" {
		extension = ".exe"
	}
	commandBinary = filepath.Join(directory, "aggregator-latency-compare"+extension)
	command := exec.Command("go", "build", "-o", commandBinary, "./cmd/aggregator-latency-compare")
	command.Dir = root
	if output, buildErr := command.CombinedOutput(); buildErr != nil {
		fmt.Fprintf(os.Stderr, "build aggregator compare: %v\n%s", buildErr, output)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(directory)
	os.Exit(code)
}

func TestHelpDocumentsSafeDefaultsAndBothFlows(t *testing.T) {
	command := exec.Command(commandBinary, "-help")
	output, _ := command.CombinedOutput()
	text := string(output)
	for _, expected := range []string{"split, swap, or both", "(default 1m30s)", "(default 5s)"} {
		if !strings.Contains(text, expected) {
			t.Fatalf("help missing %q:\n%s", expected, text)
		}
	}
}

func TestInvalidModeFailsBeforeReadingPrivateConfiguration(t *testing.T) {
	command := exec.Command(commandBinary, "-mode", "invalid", "-env-file", "missing-private-file")
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("command unexpectedly succeeded:\n%s", output)
	}
	text := string(output)
	if !strings.Contains(text, "usage: go run ./cmd/aggregator-latency-compare") || strings.Contains(text, "load missing-private-file") {
		t.Fatalf("unexpected output:\n%s", text)
	}
}
