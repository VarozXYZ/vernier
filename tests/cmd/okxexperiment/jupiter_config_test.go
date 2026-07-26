package okxexperiment_test

import (
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/cmd/okxexperiment"
)

func TestJupiterRequestUsesConfiguredTokensAndAmount(t *testing.T) {
	settings := okxexperiment.Settings{
		FromToken:          "mint-in",
		ToToken:            "mint-out",
		Amount:             "1000000",
		JupiterSlippageBPS: 75,
	}
	request := settings.JupiterRequest()
	if request.InputMint != "mint-in" || request.OutputMint != "mint-out" || request.Amount != "1000000" || request.SlippageBPS != 75 {
		t.Fatalf("unexpected Jupiter request: %+v", request)
	}
}

func TestJupiterSourceRequiresAPIKey(t *testing.T) {
	if _, err := (okxexperiment.Settings{}).JupiterSource(time.Second); err == nil {
		t.Fatal("expected missing Jupiter API key error")
	}
}

func TestJupiterSourceAcceptsAPIKey(t *testing.T) {
	settings := okxexperiment.Settings{JupiterAPIKey: "test-key", JupiterSlippageBPS: 50}
	if _, err := settings.JupiterSource(time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestJupiterRestrictedRequestUsesConfiguredDexLabels(t *testing.T) {
	settings := okxexperiment.Settings{
		FromToken:              "mint-in",
		ToToken:                "mint-out",
		Amount:                 "1000000",
		JupiterSlippageBPS:     50,
		JupiterRestrictedDexes: "Meteora,Meteora DLMM,Orca V2",
	}
	request, err := settings.JupiterRestrictedRequest()
	if err != nil {
		t.Fatal(err)
	}
	if request.Dexes != "Meteora,Meteora DLMM,Orca V2" {
		t.Fatalf("restricted Jupiter dexes = %q", request.Dexes)
	}
}
