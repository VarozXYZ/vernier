package acrossbridgecanary_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

var canaryBinary string

func TestMain(m *testing.M) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	directory, err := os.MkdirTemp("", "vernier-across-canary-test-*")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	extension := ""
	if runtime.GOOS == "windows" {
		extension = ".exe"
	}
	canaryBinary = filepath.Join(directory, "across-bridge-canary"+extension)
	command := exec.Command("go", "build", "-o", canaryBinary, "./cmd/across-bridge-canary")
	command.Dir = root
	if output, buildErr := command.CombinedOutput(); buildErr != nil {
		fmt.Fprintf(os.Stderr, "build canary: %v\n%s", buildErr, output)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(directory)
	os.Exit(code)
}

func TestArmRequiresExactAmountConfirmationBeforeReadingConfiguration(t *testing.T) {
	command := exec.Command(
		canaryBinary,
		"--config", "missing-private-config.yaml",
		"--env-file", "missing-private-env.test",
		"--direction", "evm-to-solana",
		"--amount-units", "1000000",
		"--confirm-amount-units", "999999",
		"--arm",
	)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("armed command unexpectedly succeeded:\n%s", output)
	}
	text := string(output)
	if !strings.Contains(text, "exactly match --amount-units") ||
		strings.Contains(text, "cannot load local environment") {
		t.Fatalf("arm barrier did not run first:\n%s", text)
	}
}

func TestConfirmationWithoutArmIsRejected(t *testing.T) {
	command := exec.Command(
		canaryBinary,
		"--direction", "evm-to-solana",
		"--amount-units", "1000000",
		"--confirm-amount-units", "1000000",
	)
	output, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "requires --arm") {
		t.Fatalf("unexpected output:\n%s", output)
	}
}
