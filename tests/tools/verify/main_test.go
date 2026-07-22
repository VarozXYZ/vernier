package verify_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestVerifierRejectsNestedEnvFiles(t *testing.T) {
	repositoryRoot, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	extension := ""
	if runtime.GOOS == "windows" {
		extension = ".exe"
	}
	verifier := filepath.Join(t.TempDir(), "verify"+extension)
	build := exec.Command("go", "build", "-o", verifier, "./tools/verify")
	build.Dir = repositoryRoot
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build verifier: %v\n%s", err, output)
	}

	for _, filename := range []string{".env", ".env.local"} {
		t.Run(filename, func(t *testing.T) {
			testRepository := t.TempDir()
			runGit(t, testRepository, "init", "--quiet")
			nested := filepath.Join(testRepository, "nested")
			if err := os.MkdirAll(nested, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(nested, filename), []byte("test-value\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			relative := "nested/" + filename
			runGit(t, testRepository, "add", "--force", relative)

			var stdout, stderr bytes.Buffer
			command := exec.Command(verifier)
			command.Dir = testRepository
			command.Stdout = &stdout
			command.Stderr = &stderr
			if err := command.Run(); err == nil {
				t.Fatalf("verifier accepted %s\nstdout:\n%s", relative, stdout.String())
			}
			if !strings.Contains(stderr.String(), "private paths are tracked: "+relative) {
				t.Fatalf("unexpected verifier failure\nstdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String())
			}
		})
	}
}

func TestVerifierRejectsSetupConfiguration(t *testing.T) {
	repositoryRoot, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	extension := ""
	if runtime.GOOS == "windows" {
		extension = ".exe"
	}
	verifier := filepath.Join(t.TempDir(), "verify"+extension)
	build := exec.Command("go", "build", "-o", verifier, "./tools/verify")
	build.Dir = repositoryRoot
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build verifier: %v\n%s", err, output)
	}
	testRepository := t.TempDir()
	runGit(t, testRepository, "init", "--quiet")
	path := filepath.Join(testRepository, "config", "example")
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "pool.json"), []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, testRepository, "add", "--force", "config/example/pool.json")
	command := exec.Command(verifier)
	command.Dir = testRepository
	if output, err := command.CombinedOutput(); err == nil || !strings.Contains(string(output), "config/example/pool.json") {
		t.Fatalf("verifier did not reject local configuration: %v\n%s", err, output)
	}
}

func runGit(t *testing.T, directory string, arguments ...string) {
	t.Helper()
	command := exec.Command("git", arguments...)
	command.Dir = directory
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(arguments, " "), err, output)
	}
}
