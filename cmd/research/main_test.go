package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunTextMatchesGolden(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{
		"--fixture", filepath.Join("..", "..", "examples", "synthetic", "two-market.json"),
		"--format", "text",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
	want, err := os.ReadFile(filepath.Join("testdata", "research.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stdout.Bytes(), want) {
		t.Fatalf("text output differs from golden\n--- got ---\n%s\n--- want ---\n%s", stdout.Bytes(), want)
	}
}

func TestRunExitCodes(t *testing.T) {
	fixture := filepath.Join("..", "..", "examples", "synthetic", "two-market.json")
	for name, args := range map[string][]string{
		"missing fixture": {"--fixture", "missing.json"},
		"bad format":      {"--fixture", fixture, "--format", "yaml"},
	} {
		t.Run(name, func(t *testing.T) {
			if code := run(context.Background(), args, &bytes.Buffer{}, &bytes.Buffer{}); code != 2 {
				t.Fatalf("got exit %d, want 2", code)
			}
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if code := run(ctx, []string{"--fixture", fixture}, &bytes.Buffer{}, &bytes.Buffer{}); code != 1 {
		t.Fatalf("canceled run got exit %d, want 1", code)
	}
}

func TestRunReturnsSuccessForExplicitDegradation(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "examples", "synthetic", "two-market.json"))
	if err != nil {
		t.Fatal(err)
	}
	degraded := strings.Replace(string(data), `"sequence": 2`, `"sequence": 3`, 1)
	fixture := filepath.Join(t.TempDir(), "degraded.json")
	if err := os.WriteFile(fixture, []byte(degraded), 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"--fixture", fixture, "--format", "json"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"status": "degraded"`) || !strings.Contains(stdout.String(), `"classification": "unclassifiable"`) {
		t.Fatalf("degradation is not explicit: %s", stdout.String())
	}
}
