package livecompare_test

import (
	"testing"

	"github.com/VarozXYZ/vernier/runtime/configuration"
	"github.com/VarozXYZ/vernier/runtime/livecompare"
)

func TestReferenceQuoteOverrideMustUseConfiguredSource(t *testing.T) {
	config := configuration.ParsedConfig{QuoteSources: map[string]configuration.ResolvedQuoteSource{
		"external": {ID: "external", Kind: "jupiter"},
	}}
	if _, err := livecompare.New(config, nil, livecompare.Options{ReferenceQuoteOverride: "missing"}); err == nil {
		t.Fatal("missing reference source was accepted")
	}
	if _, err := livecompare.New(config, nil, livecompare.Options{ReferenceQuoteOverride: "external"}); err != nil {
		t.Fatalf("configured reference source rejected: %v", err)
	}
	if _, err := livecompare.New(config, nil, livecompare.Options{ReferenceQuoteOverride: "off"}); err != nil {
		t.Fatalf("off override rejected: %v", err)
	}
}
