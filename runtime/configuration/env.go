package configuration

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

var envFileKey = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
var envFileReference = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// LoadEnvFile loads local test/runtime secrets without overriding variables
// already present in the process environment.
func LoadEnvFile(path string, lookup LookupEnv, set func(string, string) error) error {
	if strings.TrimSpace(path) == "" || lookup == nil || set == nil {
		return fmt.Errorf("environment file path, lookup, and setter are required")
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		key, value, found := strings.Cut(line, "=")
		key = strings.TrimSpace(key)
		if !found || !envFileKey.MatchString(key) {
			return fmt.Errorf("invalid environment entry")
		}
		if _, exists := lookup(key); exists {
			continue
		}
		value = strings.TrimSpace(value)
		if len(value) >= 2 && (value[0] == '\'' && value[len(value)-1] == '\'' ||
			value[0] == '"' && value[len(value)-1] == '"') {
			if value[0] == '\'' {
				value = value[1 : len(value)-1]
			} else {
				value, err = strconv.Unquote(value)
				if err != nil {
					return fmt.Errorf("invalid quoted environment value")
				}
			}
		}
		missing := ""
		value = envFileReference.ReplaceAllStringFunc(
			value,
			func(reference string) string {
				name := reference[2 : len(reference)-1]
				resolved, ok := lookup(name)
				if !ok {
					missing = name
					return ""
				}
				return resolved
			},
		)
		if missing != "" {
			return fmt.Errorf("environment entry references unset variable %q", missing)
		}
		if err := set(key, value); err != nil {
			return err
		}
	}
	return scanner.Err()
}
