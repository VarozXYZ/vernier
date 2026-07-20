// Command verify runs the repository's complete local and CI verification contract.
package main

import (
	"bytes"
	"fmt"
	"go/format"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

func main() {
	checks := []struct {
		name string
		fn   func() error
	}{
		{"public tree", checkPublicTree},
		{"Go formatting", checkGoFormatting},
		{"text formatting", checkTextFormatting},
		{"go vet", command("go", "vet", "./...")},
		{"tests", command("go", "test", "./...")},
		{"race tests", raceTests},
		{"build", command("go", "build", "./...")},
		{"staticcheck", command("go", "tool", "staticcheck", "./...")},
		{"govulncheck", command("go", "tool", "govulncheck", "./...")},
		{"GitHub Actions", command("go", "tool", "actionlint")},
	}

	for _, check := range checks {
		fmt.Printf("==> %s\n", check.name)
		if err := check.fn(); err != nil {
			fmt.Fprintf(os.Stderr, "verify: %s: %v\n", check.name, err)
			os.Exit(1)
		}
	}
}

func raceTests() error {
	out, err := exec.Command("go", "env", "CGO_ENABLED").Output()
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(out)) != "1" {
		fmt.Println("skip: race detector requires CGO_ENABLED=1")
		return nil
	}
	return command("go", "test", "-race", "./...")()
}

func command(name string, args ...string) func() error {
	return func() error {
		cmd := exec.Command(name, args...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		return cmd.Run()
	}
}

func repositoryFiles() ([]string, error) {
	cmd := exec.Command("git", "ls-files", "-z", "--cached", "--others", "--exclude-standard")
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	parts := bytes.Split(out, []byte{0})
	files := make([]string, 0, len(parts))
	for _, part := range parts {
		if len(part) > 0 {
			files = append(files, string(part))
		}
	}
	sort.Strings(files)
	return files, nil
}

func trackedFiles() ([]string, error) {
	cmd := exec.Command("git", "ls-files", "-z")
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	parts := bytes.Split(out, []byte{0})
	files := make([]string, 0, len(parts))
	for _, part := range parts {
		if len(part) > 0 {
			files = append(files, string(part))
		}
	}
	return files, nil
}

func checkPublicTree() error {
	files, err := trackedFiles()
	if err != nil {
		return err
	}

	var forbidden []string
	for _, file := range files {
		path := strings.ToLower(filepath.ToSlash(file))
		base := filepath.Base(path)
		if path == "agents.md" || strings.HasSuffix(path, "/agents.md") ||
			path == "agents.override.md" || strings.HasSuffix(path, "/agents.override.md") ||
			strings.HasPrefix(path, "docs/") || base == ".env" || strings.HasPrefix(base, ".env.") {
			forbidden = append(forbidden, file)
		}
	}
	if len(forbidden) > 0 {
		return fmt.Errorf("private paths are tracked: %s", strings.Join(forbidden, ", "))
	}
	return nil
}

func checkGoFormatting() error {
	files, err := repositoryFiles()
	if err != nil {
		return err
	}

	var unformatted []string
	for _, file := range files {
		if filepath.Ext(file) != ".go" {
			continue
		}
		source, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		formatted, err := format.Source(source)
		if err != nil {
			return fmt.Errorf("%s: %w", file, err)
		}
		if !bytes.Equal(source, formatted) {
			unformatted = append(unformatted, file)
		}
	}
	if len(unformatted) > 0 {
		return fmt.Errorf("run gofmt on: %s", strings.Join(unformatted, ", "))
	}
	return nil
}

func checkTextFormatting() error {
	files, err := repositoryFiles()
	if err != nil {
		return err
	}

	for _, file := range files {
		ext := strings.ToLower(filepath.Ext(file))
		if ext != ".md" && ext != ".yml" && ext != ".yaml" && ext != ".json" {
			continue
		}
		data, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		if len(data) > 0 && data[len(data)-1] != '\n' {
			return fmt.Errorf("%s: missing final newline", file)
		}
		for lineNumber, line := range bytes.Split(data, []byte{'\n'}) {
			if len(line) > 0 && (line[len(line)-1] == ' ' || line[len(line)-1] == '\t') {
				return fmt.Errorf("%s:%d: trailing whitespace", file, lineNumber+1)
			}
		}
	}
	return nil
}
