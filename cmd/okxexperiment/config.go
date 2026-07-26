// Package okxexperiment contains shared local configuration for the explicit
// OKX quote measurement commands. It is not part of Research composition.
package okxexperiment

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/VarozXYZ/vernier/adapters/quote/jupiter"
	"github.com/VarozXYZ/vernier/adapters/quote/okx"
)

const (
	DefaultEnvFile = ".env.test"

	// These are Jupiter's current DEX labels for the Meteora and Orca
	// protocols. They are labels accepted by the quote endpoint, not program
	// IDs, and are kept in the environment so the experiment does not need a
	// catalog request for every sample.
	DefaultJupiterRestrictedDexes = "Meteora,Meteora DAMM v2,Meteora DLMM,Orca V1,Orca V2,Whirlpool"
)

var envKey = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type Settings struct {
	APIKey                 string
	SecretKey              string
	Passphrase             string
	ProjectID              string
	JupiterAPIKey          string
	JupiterAPIKeys         []string
	JupiterKeyPool         *jupiter.APIKeyPool
	JupiterSlippageBPS     int
	JupiterRestrictedDexes string
	JupiterOutputDecimals  string
	JupiterOutputSymbol    string
	JupiterUserPublicKey   string
	OKXUserWalletAddress   string
	OKXSlippage            string
	FromToken              string
	ToToken                string
	Amount                 string
	RestrictedDexIDs       string
	ChainIndex             string
	Samples                int
	Interval               time.Duration
}

func LoadSettings(path string) (Settings, error) {
	if strings.TrimSpace(path) == "" {
		path = DefaultEnvFile
	}
	if err := loadEnvFile(path); err != nil {
		return Settings{}, fmt.Errorf("load %s: %w", path, err)
	}
	settings := Settings{
		APIKey:                 os.Getenv("OKX_API_KEY"),
		SecretKey:              os.Getenv("OKX_SECRET_KEY"),
		Passphrase:             os.Getenv("OKX_API_PASSPHRASE"),
		ProjectID:              os.Getenv("OKX_PROJECT_ID"),
		JupiterAPIKey:          os.Getenv("JUPITER_API_KEY"),
		JupiterRestrictedDexes: os.Getenv("JUPITER_RESTRICTED_DEXES"),
		JupiterOutputDecimals:  os.Getenv("JUPITER_OUTPUT_DECIMALS"),
		JupiterOutputSymbol:    os.Getenv("JUPITER_OUTPUT_SYMBOL"),
		JupiterUserPublicKey:   os.Getenv("JUPITER_USER_PUBLIC_KEY"),
		OKXUserWalletAddress:   os.Getenv("OKX_USER_WALLET_ADDRESS"),
		OKXSlippage:            os.Getenv("OKX_SLIPPAGE"),
		FromToken:              os.Getenv("OKX_FROM_TOKEN_ADDRESS"),
		ToToken:                os.Getenv("OKX_TO_TOKEN_ADDRESS"),
		Amount:                 os.Getenv("OKX_AMOUNT"),
		RestrictedDexIDs:       os.Getenv("OKX_RESTRICTED_DEX_IDS"),
		ChainIndex:             os.Getenv("OKX_CHAIN_INDEX"),
	}
	if settings.ChainIndex == "" {
		settings.ChainIndex = okx.SolanaChainIndex
	}
	if strings.TrimSpace(settings.JupiterRestrictedDexes) == "" {
		settings.JupiterRestrictedDexes = DefaultJupiterRestrictedDexes
	}
	if strings.TrimSpace(settings.OKXSlippage) == "" {
		settings.OKXSlippage = "0.5"
	}
	var err error
	rawJupiterKeys := strings.TrimSpace(os.Getenv("JUPITER_API_KEYS"))
	if rawJupiterKeys == "" {
		rawJupiterKeys = strings.TrimSpace(settings.JupiterAPIKey)
	}
	if rawJupiterKeys != "" {
		settings.JupiterAPIKeys, err = parseJupiterAPIKeys(rawJupiterKeys)
		if err != nil {
			return Settings{}, err
		}
		settings.JupiterKeyPool, err = jupiter.NewAPIKeyPool(settings.JupiterAPIKeys)
		if err != nil {
			return Settings{}, err
		}
	}
	for name, value := range map[string]string{
		"OKX_API_KEY":            settings.APIKey,
		"OKX_SECRET_KEY":         settings.SecretKey,
		"OKX_API_PASSPHRASE":     settings.Passphrase,
		"OKX_FROM_TOKEN_ADDRESS": settings.FromToken,
		"OKX_TO_TOKEN_ADDRESS":   settings.ToToken,
		"OKX_AMOUNT":             settings.Amount,
	} {
		if strings.TrimSpace(value) == "" {
			return Settings{}, fmt.Errorf("missing %s in %s or process environment", name, path)
		}
	}
	settings.Samples, err = positiveInt("OKX_LATENCY_SAMPLES", os.Getenv("OKX_LATENCY_SAMPLES"), 20)
	if err != nil {
		return Settings{}, err
	}
	intervalMS, err := positiveInt("OKX_REQUEST_INTERVAL_MS", os.Getenv("OKX_REQUEST_INTERVAL_MS"), 1000)
	if err != nil {
		return Settings{}, err
	}
	settings.Interval = time.Duration(intervalMS) * time.Millisecond
	settings.JupiterSlippageBPS, err = positiveInt("JUPITER_SLIPPAGE_BPS", os.Getenv("JUPITER_SLIPPAGE_BPS"), jupiter.DefaultSlippageBPS)
	if err != nil {
		return Settings{}, err
	}
	if settings.JupiterSlippageBPS > 10_000 {
		return Settings{}, fmt.Errorf("JUPITER_SLIPPAGE_BPS must be <= 10000")
	}
	return settings, nil
}

func (s Settings) Source(interval time.Duration) (*okx.Source, error) {
	return okx.New(okx.Config{
		ID:              "okx-experiment",
		APIKey:          s.APIKey,
		SecretKey:       s.SecretKey,
		Passphrase:      s.Passphrase,
		ProjectID:       s.ProjectID,
		ChainIndex:      s.ChainIndex,
		RequestInterval: interval,
	})
}

// SourceWithoutLimiter creates the OKX source used by the unthrottled
// quote-to-instructions experiment. The experiment still schedules one pair
// per second, but this source does not add a second client-side queue.
func (s Settings) SourceWithoutLimiter() (*okx.Source, error) {
	limiter, err := okx.NewSpacingLimiter(0)
	if err != nil {
		return nil, err
	}
	return okx.New(okx.Config{
		ID:         "okx-experiment-unthrottled",
		APIKey:     s.APIKey,
		SecretKey:  s.SecretKey,
		Passphrase: s.Passphrase,
		ProjectID:  s.ProjectID,
		ChainIndex: s.ChainIndex,
		Limiter:    limiter,
	})
}

func (s Settings) Request() okx.QuoteRequest {
	return okx.QuoteRequest{
		ChainIndex:       s.ChainIndex,
		FromTokenAddress: s.FromToken,
		ToTokenAddress:   s.ToToken,
		Amount:           s.Amount,
	}
}

func (s Settings) SwapInstructionRequest() okx.SwapInstructionRequest {
	return okx.SwapInstructionRequest{
		ChainIndex:        s.ChainIndex,
		FromTokenAddress:  s.FromToken,
		ToTokenAddress:    s.ToToken,
		Amount:            s.Amount,
		Slippage:          s.OKXSlippage,
		UserWalletAddress: s.OKXUserWalletAddress,
	}
}

// JupiterSource creates the direct Jupiter client used only by the standalone
// OKX-vs-Jupiter experiment. It is intentionally not wired into Research.
func (s Settings) JupiterSource(interval time.Duration) (*jupiter.QuoteSource, error) {
	pool, err := s.jupiterKeyPool()
	if err != nil {
		return nil, err
	}
	return jupiter.NewQuoteSource(jupiter.QuoteConfig{
		ID:              "jupiter-experiment",
		APIKeyPool:      pool,
		SlippageBPS:     uint16(s.JupiterSlippageBPS),
		RequestInterval: interval,
	})
}

// JupiterSourceWithoutLimiter creates the Jupiter quote source used by the
// unthrottled quote-to-swap experiment. API-key rotation remains enabled.
func (s Settings) JupiterSourceWithoutLimiter() (*jupiter.QuoteSource, error) {
	pool, err := s.jupiterKeyPool()
	if err != nil {
		return nil, err
	}
	limiter, err := jupiter.NewQuoteSpacingLimiter(0)
	if err != nil {
		return nil, err
	}
	return jupiter.NewQuoteSource(jupiter.QuoteConfig{
		ID:          "jupiter-experiment-unthrottled",
		APIKeyPool:  pool,
		SlippageBPS: uint16(s.JupiterSlippageBPS),
		Limiter:     limiter,
	})
}

func (s Settings) JupiterSwapSource(interval time.Duration) (*jupiter.SwapSource, error) {
	pool, err := s.jupiterKeyPool()
	if err != nil {
		return nil, err
	}
	return jupiter.NewSwapSource(jupiter.SwapConfig{
		ID:              "jupiter-swap-experiment",
		APIKeyPool:      pool,
		RequestInterval: interval,
	})
}

// JupiterSwapSourceWithoutLimiter creates the Jupiter swap source used by
// the unthrottled quote-to-swap experiment. API-key rotation remains enabled.
func (s Settings) JupiterSwapSourceWithoutLimiter() (*jupiter.SwapSource, error) {
	pool, err := s.jupiterKeyPool()
	if err != nil {
		return nil, err
	}
	limiter, err := jupiter.NewQuoteSpacingLimiter(0)
	if err != nil {
		return nil, err
	}
	return jupiter.NewSwapSource(jupiter.SwapConfig{
		ID:         "jupiter-swap-experiment-unthrottled",
		APIKeyPool: pool,
		Limiter:    limiter,
	})
}

func (s Settings) JupiterKeyPoolSize() int {
	if s.JupiterKeyPool != nil {
		return s.JupiterKeyPool.Len()
	}
	if len(s.JupiterAPIKeys) > 0 {
		return len(s.JupiterAPIKeys)
	}
	if strings.TrimSpace(s.JupiterAPIKey) != "" {
		return 1
	}
	return 0
}

func (s Settings) jupiterKeyPool() (*jupiter.APIKeyPool, error) {
	if s.JupiterKeyPool != nil {
		return s.JupiterKeyPool, nil
	}
	keys := append([]string(nil), s.JupiterAPIKeys...)
	if len(keys) == 0 && strings.TrimSpace(s.JupiterAPIKey) != "" {
		keys = []string{s.JupiterAPIKey}
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("missing JUPITER_API_KEY in the environment")
	}
	return jupiter.NewAPIKeyPool(keys)
}

func (s Settings) JupiterRequest() jupiter.QuoteRequest {
	return jupiter.QuoteRequest{
		InputMint:   s.FromToken,
		OutputMint:  s.ToToken,
		Amount:      s.Amount,
		SlippageBPS: uint16(s.JupiterSlippageBPS),
	}
}

func (s Settings) JupiterRestrictedRequest() (jupiter.QuoteRequest, error) {
	dexes := strings.TrimSpace(s.JupiterRestrictedDexes)
	if dexes == "" {
		return jupiter.QuoteRequest{}, fmt.Errorf("missing JUPITER_RESTRICTED_DEXES in the environment")
	}
	request := s.JupiterRequest()
	request.Dexes = dexes
	return request, nil
}

func (s Settings) RestrictedRequest() (okx.QuoteRequest, error) {
	dexIDs := strings.TrimSpace(s.RestrictedDexIDs)
	if dexIDs == "" {
		return okx.QuoteRequest{}, fmt.Errorf("missing OKX_RESTRICTED_DEX_IDS in the environment")
	}
	request := s.Request()
	request.DexIDs = dexIDs
	return request, nil
}

func positiveInt(name, raw string, fallback int) (int, error) {
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return value, nil
}

func parseJupiterAPIKeys(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	keys := make([]string, len(parts))
	for index, part := range parts {
		key := strings.TrimSpace(part)
		if key == "" {
			return nil, fmt.Errorf("JUPITER_API_KEYS contains an empty key at index %d", index)
		}
		keys[index] = key
	}
	return keys, nil
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
