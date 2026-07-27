package zeroxexperiment_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/VarozXYZ/vernier/adapters/quote/zerox"
	"github.com/VarozXYZ/vernier/cmd/zeroxexperiment"
)

func TestDefaultEnvironmentFileIsTestScoped(t *testing.T) {
	if zeroxexperiment.DefaultEnvFile != ".env.test" {
		t.Fatalf("unexpected default environment file %q", zeroxexperiment.DefaultEnvFile)
	}
}

func TestRequestUsesConfiguredRobinhoodValues(t *testing.T) {
	settings := zeroxexperiment.Settings{
		ChainID:     zerox.RobinhoodChainID,
		SellToken:   "0x1000000000000000000000000000000000000001",
		BuyToken:    "0x2000000000000000000000000000000000000002",
		SellAmount:  "1000000",
		Taker:       "0x3000000000000000000000000000000000000003",
		SlippageBPS: 75,
	}
	request := settings.Request()
	if request.ChainID != "4663" || request.SellAmount != "1000000" || request.SlippageBPS != 75 {
		t.Fatalf("unexpected 0x request: %+v", request)
	}
}

func TestLocalSettingsDoNotRequireProviderCredentials(t *testing.T) {
	keys := []string{
		"ZEROX_API_KEY", "ZEROX_CHAIN_ID", "ZEROX_SELL_TOKEN", "ZEROX_BUY_TOKEN",
		"ZEROX_SELL_AMOUNT", "ZEROX_TAKER_ADDRESS", "ZEROX_BUY_TOKEN_DECIMALS",
		"ZEROX_BUY_TOKEN_SYMBOL",
	}
	type previousEnvironment struct {
		value  string
		exists bool
	}
	previous := make(map[string]previousEnvironment, len(keys))
	for _, key := range keys {
		value, exists := os.LookupEnv(key)
		previous[key] = previousEnvironment{value: value, exists: exists}
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		for _, key := range keys {
			value := previous[key]
			if value.exists {
				_ = os.Setenv(key, value.value)
			} else {
				_ = os.Unsetenv(key)
			}
		}
	})

	path := filepath.Join(t.TempDir(), ".env.test")
	contents := "ZEROX_SELL_TOKEN=0x1000000000000000000000000000000000000001\n" +
		"ZEROX_BUY_TOKEN=0x2000000000000000000000000000000000000002\n" +
		"ZEROX_SELL_AMOUNT=1000000\n" +
		"ZEROX_BUY_TOKEN_DECIMALS=18\n" +
		"ZEROX_BUY_TOKEN_SYMBOL=TOKEN_OUT\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	settings, err := zeroxexperiment.LoadLocalSettings(path)
	if err != nil {
		t.Fatal(err)
	}
	if settings.APIKey != "" || settings.Taker != "" {
		t.Fatal("local settings unexpectedly required provider credentials")
	}
	if settings.ChainID != zerox.RobinhoodChainID || settings.SellAmount != "1000000" {
		t.Fatalf("unexpected local settings: %+v", settings)
	}
}
