package swapbuildcanary_test

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
	directory, err := os.MkdirTemp("", "vernier-swap-canary-test-*")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	extension := ""
	if runtime.GOOS == "windows" {
		extension = ".exe"
	}
	canaryBinary = filepath.Join(directory, "swap-build-canary"+extension)
	command := exec.Command("go", "build", "-o", canaryBinary, "./cmd/swap-build-canary")
	command.Dir = root
	if output, buildErr := command.CombinedOutput(); buildErr != nil {
		fmt.Fprintf(os.Stderr, "build canary: %v\n%s", buildErr, output)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(directory)
	os.Exit(code)
}

func TestArmRequiresExactAmountConfirmationBeforeConfigurationOrNetwork(t *testing.T) {
	command := exec.Command(
		canaryBinary,
		"--config", "missing-private-config.yaml",
		"--env-file", "missing-private-env.test",
		"--market", "synthetic-market",
		"--side", "buy",
		"--amount-units", "1000000",
		"--confirm-amount-units", "999999",
		"--arm",
	)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("armed command unexpectedly succeeded:\n%s", output)
	}
	text := string(output)
	if !strings.Contains(text, "exactly match --amount-units") {
		t.Fatalf("unexpected output:\n%s", text)
	}
	if strings.Contains(text, "cannot load local environment") ||
		strings.Contains(text, "read configuration") {
		t.Fatalf("arm barrier did not run before configuration access:\n%s", text)
	}
}

func TestConfirmationIsRejectedWithoutArm(t *testing.T) {
	command := exec.Command(
		canaryBinary,
		"--config", "missing-private-config.yaml",
		"--market", "synthetic-market",
		"--side", "buy",
		"--amount-units", "1000000",
		"--confirm-amount-units", "1000000",
	)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("command unexpectedly succeeded:\n%s", output)
	}
	if !strings.Contains(
		string(output),
		"--confirm-amount-units is only valid with --arm",
	) {
		t.Fatalf("unexpected output:\n%s", output)
	}
}

func TestInvalidSolanaBroadcastTransportIsRejectedBeforeConfiguration(t *testing.T) {
	command := exec.Command(
		canaryBinary,
		"--config", "missing-private-config.yaml",
		"--market", "synthetic-market",
		"--side", "buy",
		"--amount-units", "1000000",
		"--solana-broadcast", "automatic",
	)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("command unexpectedly succeeded:\n%s", output)
	}
	text := string(output)
	if !strings.Contains(
		text,
		"--solana-broadcast must be rpc or helius-sender",
	) {
		t.Fatalf("unexpected output:\n%s", text)
	}
	if strings.Contains(text, "cannot load local environment") {
		t.Fatalf("transport validation did not run before configuration:\n%s", text)
	}
}

func TestHeliusSenderIsTheDefaultSolanaBroadcastTransport(t *testing.T) {
	command := exec.Command(canaryBinary, "--help")
	output, _ := command.CombinedOutput()
	text := string(output)
	if !strings.Contains(text, "-solana-broadcast string") ||
		!strings.Contains(text, `(default "helius-sender")`) {
		t.Fatalf("Helius Sender is not the documented default:\n%s", text)
	}
}
