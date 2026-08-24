package safeerr_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/VarozXYZ/vernier/internal/safeerr"
)

func TestMessageRedactsCredentialsButKeepsEndpointIdentity(t *testing.T) {
	const secret = "private-value"
	message := safeerr.Message(errors.New(
		"rpc call on https://rpc.example/v1/?api-key=" + secret +
			" failed; access_token=" + secret,
	))
	if strings.Contains(message, secret) || strings.Contains(message, "?") {
		t.Fatalf("credential leaked: %s", message)
	}
	if !strings.Contains(message, "https://rpc.example/v1/") ||
		!strings.Contains(message, "access_token=[REDACTED]") {
		t.Fatalf("diagnostic context was lost: %s", message)
	}
}
