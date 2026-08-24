// Package safeerr removes credentials from errors before they cross an
// operational boundary such as logs, notifications, or durable journals.
package safeerr

import (
	"errors"
	"net/url"
	"regexp"
	"strings"
)

var (
	endpointPattern = regexp.MustCompile(`(?i)\b(?:https?|wss?)://[^\s"']+`)
	secretPattern   = regexp.MustCompile(`(?i)(api[-_]?key|access[-_]?token|token|secret)=([^&\s]+)`)
)

// Message returns a diagnostic string that preserves the endpoint host and
// path while removing URL userinfo, query parameters, fragments, and common
// inline credential assignments.
func Message(err error) string {
	if err == nil {
		return ""
	}
	return Sanitize(err.Error())
}

// Error returns a new sanitized error. Callers should use it only at an
// external boundary because replacing an error intentionally drops its type.
func Error(err error) error {
	if err == nil {
		return nil
	}
	return errors.New(Message(err))
}

func Sanitize(value string) string {
	value = endpointPattern.ReplaceAllStringFunc(value, sanitizeEndpoint)
	return secretPattern.ReplaceAllString(value, `${1}=[REDACTED]`)
}

func sanitizeEndpoint(raw string) string {
	// Error strings commonly put punctuation immediately after a URL. Keep it
	// outside the parse so diagnostics retain their original grammar.
	trimmed := strings.TrimRight(raw, ".,;:)]}")
	suffix := raw[len(trimmed):]
	parsed, err := url.Parse(trimmed)
	if err != nil || parsed.Host == "" {
		return "[REDACTED_ENDPOINT]" + suffix
	}
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.ForceQuery = false
	parsed.Fragment = ""
	return parsed.String() + suffix
}
