package live_test

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestVersionDoesNotRequirePrivateComposition(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "run", "./cmd/live", "--version")
	command.Dir = root
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run Live version: %v\n%s", err, output)
	}
	if got := strings.TrimSpace(string(output)); got != "vernier dev" {
		t.Fatalf("version=%q", got)
	}
}

func TestRefuelOnceRejectsUnknownChainBeforeLoadingSecrets(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(
		"go", "run", "./cmd/live",
		"--refuel-once", "unknown-chain",
	)
	command.Dir = root
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatal("expected invalid refuel chain")
	}
	if !strings.Contains(
		string(output),
		"-refuel-once must be solana or polygon",
	) {
		t.Fatalf("output=%s", output)
	}
}

func TestRecoverOnlyRequiresArmBeforeLoadingSecrets(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "run", "./cmd/live", "-recover-only")
	command.Dir = root
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatal("expected recovery-only without arm to fail")
	}
	if !strings.Contains(string(output), "-recover-only requires -arm") {
		t.Fatalf("output=%s", output)
	}
}

func TestOnceRequiresArmBeforeLoadingSecrets(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "run", "./cmd/live", "-once")
	command.Dir = root
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatal("expected one-operation mode without arm to fail")
	}
	if !strings.Contains(string(output), "-once requires -arm") {
		t.Fatalf("output=%s", output)
	}
}
