package configuration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/VarozXYZ/vernier/runtime/configuration"
)

func TestLoadEnvFileExpandsPreviouslyLoadedVariables(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime.env")
	if err := os.WriteFile(
		path,
		[]byte("PRIMARY=https://primary.example\nFANOUT=\"${PRIMARY},https://secondary.example\"\n"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	values := map[string]string{}
	lookup := func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
	set := func(key, value string) error {
		values[key] = value
		return nil
	}
	if err := configuration.LoadEnvFile(path, lookup, set); err != nil {
		t.Fatal(err)
	}
	if values["FANOUT"] !=
		"https://primary.example,https://secondary.example" {
		t.Fatalf("expanded fanout = %q", values["FANOUT"])
	}
}

func TestLoadEnvFileRejectsUnsetVariableReference(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime.env")
	if err := os.WriteFile(
		path,
		[]byte("FANOUT=${MISSING_RPC}\n"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	values := map[string]string{}
	err := configuration.LoadEnvFile(
		path,
		func(key string) (string, bool) {
			value, ok := values[key]
			return value, ok
		},
		func(key, value string) error {
			values[key] = value
			return nil
		},
	)
	if err == nil || !strings.Contains(err.Error(), "unset variable") {
		t.Fatalf("unset reference error = %v", err)
	}
}

func TestReadIsolatedEnvFileDoesNotInheritProcessValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "isolated.env")
	if err := os.WriteFile(
		path,
		[]byte("LOCAL_KEY=setup-secret\nFANOUT=${LOCAL_KEY},secondary\n"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FOREIGN_PRIVATE_KEY", "must-not-leak")
	lookup, err := configuration.ReadIsolatedEnvFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := lookup("FOREIGN_PRIVATE_KEY"); ok {
		t.Fatal("isolated environment inherited a process variable")
	}
	if value, ok := lookup("FANOUT"); !ok || value != "setup-secret,secondary" {
		t.Fatalf("same-file expansion = %q, %t", value, ok)
	}
}
