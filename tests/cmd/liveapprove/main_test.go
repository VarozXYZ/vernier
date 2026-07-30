package liveapprove_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

var approvalBinary string

func TestMain(m *testing.M) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	directory, err := os.MkdirTemp("", "vernier-live-approve-test-*")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	extension := ""
	if runtime.GOOS == "windows" {
		extension = ".exe"
	}
	approvalBinary = filepath.Join(directory, "live-approve"+extension)
	command := exec.Command("go", "build", "-o", approvalBinary, "./cmd/live-approve")
	command.Dir = root
	if output, buildErr := command.CombinedOutput(); buildErr != nil {
		fmt.Fprintf(os.Stderr, "build live-approve: %v\n%s", buildErr, output)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(directory)
	os.Exit(code)
}

func TestArmRequiresOwnerConfirmationBeforeConfigurationOrNetwork(t *testing.T) {
	command := exec.Command(
		approvalBinary,
		"--config", "missing-private-config.yaml",
		"--env-file", "missing-private-env.test",
		"--arm",
	)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("armed command unexpectedly succeeded:\n%s", output)
	}
	text := string(output)
	if !strings.Contains(text, "--arm requires --confirm-owner") {
		t.Fatalf("unexpected output:\n%s", text)
	}
	if strings.Contains(text, "cannot load local environment") ||
		strings.Contains(text, "read configuration") {
		t.Fatalf("arm barrier did not run before configuration access:\n%s", text)
	}
}

func TestOwnerConfirmationWithoutArmIsRejected(t *testing.T) {
	command := exec.Command(
		approvalBinary,
		"--config", "missing-private-config.yaml",
		"--confirm-owner", "0x0000000000000000000000000000000000000001",
	)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("command unexpectedly succeeded:\n%s", output)
	}
	if !strings.Contains(string(output), "--confirm-owner requires --arm") {
		t.Fatalf("unexpected output:\n%s", output)
	}
}

func TestHelpExplainsInfiniteApprovalAndAuditDefault(t *testing.T) {
	command := exec.Command(approvalBinary, "--help")
	output, _ := command.CombinedOutput()
	text := string(output)
	for _, expected := range []string{
		"approve(MaxUint256)",
		"-confirm-owner string",
		"-config string",
	} {
		if !strings.Contains(text, expected) {
			t.Fatalf("help does not contain %q:\n%s", expected, text)
		}
	}
}
