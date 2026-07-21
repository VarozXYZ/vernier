// Package coingecko reads external price observations from CoinGecko.
package coingecko

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/VarozXYZ/vernier/domain/market"
	priceport "github.com/VarozXYZ/vernier/ports/price"
)

const (
	PublicBaseURL = "https://api.coingecko.com/api/v3"
	ProBaseURL    = "https://pro-api.coingecko.com/api/v3"
)

type Client interface {
	Do(*http.Request) (*http.Response, error)
}

type Config struct {
	ID           market.SourceID
	Base         market.AssetID
	Quote        market.AssetID
	CoinID       string
	Currency     string
	BaseURL      string
	APIKey       string
	APIKeyHeader string
	Client       Client
	Clock        func() time.Time
}

type Source struct {
	id           market.SourceID
	base         market.AssetID
	quote        market.AssetID
	coinID       string
	currency     string
	baseURL      string
	apiKey       string
	apiKeyHeader string
	client       Client
	clock        func() time.Time
}

func New(config Config) (*Source, error) {
	if config.ID == "" || config.Base == "" || config.Quote == "" || config.Base == config.Quote ||
		strings.TrimSpace(config.CoinID) == "" || strings.TrimSpace(config.Currency) == "" {
		return nil, fmt.Errorf("CoinGecko source requires an ID, pair, coin ID, and currency")
	}
	if config.BaseURL == "" {
		config.BaseURL = PublicBaseURL
	}
	parsed, err := url.Parse(config.BaseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("CoinGecko base URL is invalid")
	}
	if config.APIKeyHeader != "" && config.APIKey == "" || config.APIKeyHeader == "" && config.APIKey != "" {
		return nil, fmt.Errorf("CoinGecko API key and header must be configured together")
	}
	if config.Client == nil {
		config.Client = http.DefaultClient
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	return &Source{
		id: config.ID, base: config.Base, quote: config.Quote, coinID: config.CoinID,
		currency: strings.ToLower(config.Currency), baseURL: strings.TrimRight(config.BaseURL, "/"),
		apiKey: config.APIKey, apiKeyHeader: config.APIKeyHeader, client: config.Client, clock: config.Clock,
	}, nil
}

func (s *Source) ID() market.SourceID { return s.id }

func (s *Source) Observe(ctx context.Context, request priceport.Request) (market.PriceObservation, error) {
	if request.Base != s.base || request.Quote != s.quote {
		return market.PriceObservation{}, fmt.Errorf("CoinGecko source %q does not provide %s/%s", s.id, request.Base, request.Quote)
	}
	endpoint, _ := url.Parse(s.baseURL + "/simple/price")
	query := endpoint.Query()
	query.Set("ids", s.coinID)
	query.Set("vs_currencies", s.currency)
	query.Set("include_last_updated_at", "true")
	query.Set("precision", "full")
	endpoint.RawQuery = query.Encode()
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return market.PriceObservation{}, fmt.Errorf("build CoinGecko request: %w", err)
	}
	if s.apiKey != "" {
		httpRequest.Header.Set(s.apiKeyHeader, s.apiKey)
	}
	response, err := s.client.Do(httpRequest)
	if err != nil {
		return market.PriceObservation{}, fmt.Errorf("request CoinGecko price: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return market.PriceObservation{}, fmt.Errorf("CoinGecko returned HTTP %d", response.StatusCode)
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 1<<20))
	decoder.UseNumber()
	var payload map[string]map[string]json.Number
	if err := decoder.Decode(&payload); err != nil {
		return market.PriceObservation{}, fmt.Errorf("decode CoinGecko price: %w", err)
	}
	coin, ok := payload[s.coinID]
	if !ok {
		return market.PriceObservation{}, fmt.Errorf("CoinGecko response omitted coin %q", s.coinID)
	}
	value, err := exactDecimal(coin[s.currency].String())
	if err != nil || value.Sign() <= 0 {
		return market.PriceObservation{}, fmt.Errorf("CoinGecko returned invalid %s price", s.currency)
	}
	updatedUnix, err := coin["last_updated_at"].Int64()
	if err != nil || updatedUnix <= 0 {
		return market.PriceObservation{}, fmt.Errorf("CoinGecko returned invalid update time")
	}
	return market.NewPriceObservation(
		s.id, s.base, s.quote, value, "coingecko:coin/"+s.coinID,
		time.Unix(updatedUnix, 0).UTC(), s.clock().UTC(),
	)
}

func exactDecimal(text string) (*big.Rat, error) {
	if text == "" || len(text) > 1024 {
		return nil, fmt.Errorf("empty decimal")
	}
	mantissa, exponentText, hasExponent := strings.Cut(strings.ToLower(text), "e")
	exponent := int64(0)
	var err error
	if hasExponent {
		exponent, err = strconv.ParseInt(exponentText, 10, 32)
		if err != nil {
			return nil, err
		}
		if exponent < -1000 || exponent > 1000 {
			return nil, fmt.Errorf("decimal exponent is out of bounds")
		}
	}
	sign := ""
	if strings.HasPrefix(mantissa, "+") || strings.HasPrefix(mantissa, "-") {
		sign, mantissa = mantissa[:1], mantissa[1:]
	}
	whole, fraction, hasPoint := strings.Cut(mantissa, ".")
	if !hasPoint {
		fraction = ""
	}
	digits := sign + whole + fraction
	numerator, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		return nil, fmt.Errorf("invalid decimal")
	}
	scale := int64(len(fraction)) - exponent
	if scale <= 0 {
		return new(big.Rat).SetInt(new(big.Int).Mul(numerator, new(big.Int).Exp(big.NewInt(10), big.NewInt(-scale), nil))), nil
	}
	return new(big.Rat).SetFrac(numerator, new(big.Int).Exp(big.NewInt(10), big.NewInt(scale), nil)), nil
}
