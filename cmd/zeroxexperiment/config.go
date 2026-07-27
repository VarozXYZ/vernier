// Package zeroxexperiment contains local configuration for standalone 0x
// latency commands. It is not part of Research composition.
package zeroxexperiment

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/VarozXYZ/vernier/adapters/quote/zerox"
)

const DefaultEnvFile = ".env.test"

var envKey = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type Settings struct {
	APIKey      string
	ChainID     string
	SellToken   string
	BuyToken    string
	SellAmount  string
	Taker       string
	SlippageBPS int
	BuyDecimals string
	BuySymbol   string
	Samples     int
	Interval    time.Duration
}

func LoadSettings(path string) (Settings, error) {
	if strings.TrimSpace(path) == "" {
		path = DefaultEnvFile
	}
	if err := loadEnvFile(path); err != nil {
		return Settings{}, fmt.Errorf("load %s: %w", path, err)
	}
	settings := settingsFromEnvironment()
	for name, value := range map[string]string{
		"ZEROX_API_KEY":            settings.APIKey,
		"ZEROX_SELL_TOKEN":         settings.SellToken,
		"ZEROX_BUY_TOKEN":          settings.BuyToken,
		"ZEROX_SELL_AMOUNT":        settings.SellAmount,
		"ZEROX_TAKER_ADDRESS":      settings.Taker,
		"ZEROX_BUY_TOKEN_DECIMALS": settings.BuyDecimals,
	} {
		if strings.TrimSpace(value) == "" {
			return Settings{}, fmt.Errorf("missing %s in %s or process environment", name, path)
		}
	}
	var err error
	settings.SlippageBPS, err = positiveInt("ZEROX_SLIPPAGE_BPS", os.Getenv("ZEROX_SLIPPAGE_BPS"), 50)
	if err != nil {
		return Settings{}, err
	}
	if settings.SlippageBPS > 10_000 {
		return Settings{}, fmt.Errorf("ZEROX_SLIPPAGE_BPS must be <= 10000")
	}
	settings.Samples, err = positiveInt("ZEROX_LATENCY_SAMPLES", os.Getenv("ZEROX_LATENCY_SAMPLES"), 20)
	if err != nil {
		return Settings{}, err
	}
	intervalMS, err := positiveInt("ZEROX_REQUEST_INTERVAL_MS", os.Getenv("ZEROX_REQUEST_INTERVAL_MS"), 1000)
	if err != nil {
		return Settings{}, err
	}
	settings.Interval = time.Duration(intervalMS) * time.Millisecond
	return settings, nil
}

// LoadLocalSettings loads only the pair and amount metadata shared by local
// watcher experiments. It deliberately does not require provider credentials,
// taker addresses, slippage, or request pacing.
func LoadLocalSettings(path string) (Settings, error) {
	if strings.TrimSpace(path) == "" {
		path = DefaultEnvFile
	}
	if err := loadEnvFile(path); err != nil {
		return Settings{}, fmt.Errorf("load %s: %w", path, err)
	}
	settings := settingsFromEnvironment()
	for name, value := range map[string]string{
		"ZEROX_SELL_TOKEN":         settings.SellToken,
		"ZEROX_BUY_TOKEN":          settings.BuyToken,
		"ZEROX_SELL_AMOUNT":        settings.SellAmount,
		"ZEROX_BUY_TOKEN_DECIMALS": settings.BuyDecimals,
	} {
		if strings.TrimSpace(value) == "" {
			return Settings{}, fmt.Errorf("missing %s in %s or process environment", name, path)
		}
	}
	return settings, nil
}

func settingsFromEnvironment() Settings {
	settings := Settings{
		APIKey:      os.Getenv("ZEROX_API_KEY"),
		ChainID:     os.Getenv("ZEROX_CHAIN_ID"),
		SellToken:   os.Getenv("ZEROX_SELL_TOKEN"),
		BuyToken:    os.Getenv("ZEROX_BUY_TOKEN"),
		SellAmount:  os.Getenv("ZEROX_SELL_AMOUNT"),
		Taker:       os.Getenv("ZEROX_TAKER_ADDRESS"),
		BuyDecimals: os.Getenv("ZEROX_BUY_TOKEN_DECIMALS"),
		BuySymbol:   os.Getenv("ZEROX_BUY_TOKEN_SYMBOL"),
	}
	if strings.TrimSpace(settings.ChainID) == "" {
		settings.ChainID = zerox.RobinhoodChainID
	}
	return settings
}

func (s Settings) Source() (*zerox.Source, error) {
	return zerox.New(zerox.Config{APIKey: s.APIKey})
}

func (s Settings) Request() zerox.Request {
	return zerox.Request{
		ChainID:     s.ChainID,
		SellToken:   s.SellToken,
		BuyToken:    s.BuyToken,
		SellAmount:  s.SellAmount,
		Taker:       s.Taker,
		SlippageBPS: uint16(s.SlippageBPS),
	}
}

func positiveInt(name, raw string, fallback int) (int, error) {
	if strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return value, nil
}

func loadEnvFile(path string) error {
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
		if !found || !envKey.MatchString(key) {
			return fmt.Errorf("invalid environment entry")
		}
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		value = strings.TrimSpace(value)
		if len(value) >= 2 && ((value[0] == '\'' && value[len(value)-1] == '\'') || (value[0] == '"' && value[len(value)-1] == '"')) {
			if value[0] == '\'' {
				value = value[1 : len(value)-1]
			} else {
				value, err = strconv.Unquote(value)
				if err != nil {
					return fmt.Errorf("invalid quoted environment value")
				}
			}
		}
		if err := os.Setenv(key, value); err != nil {
			return err
		}
	}
	return scanner.Err()
}
