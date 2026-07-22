// Package jupiter provides optional, read-only Jupiter Swap API v2 Router
// validation. It never signs or broadcasts the returned transaction data.
package jupiter

import (
	"context"
	"crypto/sha256"
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
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

const DefaultBaseURL = "https://api.jup.ag"

type Client interface {
	Do(*http.Request) (*http.Response, error)
}
type Clock func() time.Time

type Config struct {
	ID           market.SourceID
	BaseURL      string
	Taker        string
	SlippageBPS  uint16
	MaxAccounts  uint16
	APIKey       string
	APIKeyHeader string
	TokenMints   map[market.TokenID]string
	Local        quoteport.Source
	Client       Client
	Clock        Clock
}

type Source struct {
	id           market.SourceID
	baseURL      string
	taker        string
	slippageBPS  uint16
	maxAccounts  uint16
	apiKey       string
	apiKeyHeader string
	mints        map[market.TokenID]string
	local        quoteport.Source
	client       Client
	clock        Clock
}

func New(config Config) (*Source, error) {
	if config.ID == "" || config.Taker == "" || config.Local == nil || len(config.TokenMints) == 0 {
		return nil, fmt.Errorf("Jupiter source requires id, public taker, token mints, and local source")
	}
	if config.BaseURL == "" {
		config.BaseURL = DefaultBaseURL
	}
	if config.Client == nil {
		config.Client = &http.Client{Timeout: 8 * time.Second}
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	if config.SlippageBPS > 10_000 {
		return nil, fmt.Errorf("invalid Jupiter slippage")
	}
	if config.MaxAccounts == 0 {
		config.MaxAccounts = 64
	}
	mints := make(map[market.TokenID]string, len(config.TokenMints))
	for token, mint := range config.TokenMints {
		if token == "" || strings.TrimSpace(mint) == "" {
			return nil, fmt.Errorf("Jupiter token mint mapping is incomplete")
		}
		mints[token] = mint
	}
	return &Source{id: config.ID, baseURL: strings.TrimRight(config.BaseURL, "/"), taker: config.Taker, slippageBPS: config.SlippageBPS, maxAccounts: config.MaxAccounts, apiKey: config.APIKey, apiKeyHeader: config.APIKeyHeader, mints: mints, local: config.Local, client: config.Client, clock: config.Clock}, nil
}

func (s *Source) ID() market.SourceID { return s.id }
func (s *Source) Quote(ctx context.Context, input quoteport.Input) (market.Quote, error) {
	return s.local.Quote(ctx, input)
}

func (s *Source) QuoteWithReference(ctx context.Context, input quoteport.Input) (quoteport.ReferenceResult, error) {
	local, err := s.local.Quote(ctx, input)
	if err != nil {
		return quoteport.ReferenceResult{}, err
	}
	started := s.clock().UTC()
	evidence := quoteport.ReferenceEvidence{Provider: s.id, Status: quoteport.ReferenceUnavailable, Latency: 0}
	inputMint, inputOK := s.mints[input.TokenIn]
	outputMint, outputOK := s.mints[input.TokenOut]
	if !inputOK || !outputOK {
		evidence.Error = "token mint mapping is missing"
		return quoteport.ReferenceResult{Local: local, Evidence: evidence}, nil
	}
	query := url.Values{}
	query.Set("inputMint", inputMint)
	query.Set("outputMint", outputMint)
	query.Set("amount", input.AmountIn.Units().String())
	query.Set("taker", s.taker)
	query.Set("slippageBps", strconv.FormatUint(uint64(s.slippageBPS), 10))
	query.Set("maxAccounts", strconv.FormatUint(uint64(s.maxAccounts), 10))
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, s.baseURL+"/swap/v2/build?"+query.Encode(), nil)
	if err != nil {
		evidence.Error = err.Error()
		evidence.Latency = s.clock().Sub(started)
		return quoteport.ReferenceResult{Local: local, Evidence: evidence}, nil
	}
	if s.apiKey != "" {
		header := s.apiKeyHeader
		if header == "" {
			header = "x-api-key"
		}
		request.Header.Set(header, s.apiKey)
	}
	response, err := s.client.Do(request)
	if err != nil {
		evidence.Error = err.Error()
		evidence.Latency = s.clock().Sub(started)
		return quoteport.ReferenceResult{Local: local, Evidence: evidence}, nil
	}
	defer response.Body.Close()
	body, readErr := io.ReadAll(response.Body)
	evidence.Latency = s.clock().Sub(started)
	if readErr != nil {
		evidence.Error = readErr.Error()
		return quoteport.ReferenceResult{Local: local, Evidence: evidence}, nil
	}
	evidence.ResponseHash = sha256.Sum256(body)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		evidence.Error = fmt.Sprintf("HTTP status %s", response.Status)
		return quoteport.ReferenceResult{Local: local, Evidence: evidence}, nil
	}
	var payload buildResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		evidence.Error = err.Error()
		return quoteport.ReferenceResult{Local: local, Evidence: evidence}, nil
	}
	if payload.OutAmount == "" {
		evidence.Error = "Jupiter response has no outAmount"
		return quoteport.ReferenceResult{Local: local, Evidence: evidence}, nil
	}
	outUnits, ok := new(big.Int).SetString(payload.OutAmount, 10)
	if !ok || outUnits.Sign() < 0 {
		evidence.Error = "Jupiter outAmount is not a non-negative integer"
		return quoteport.ReferenceResult{Local: local, Evidence: evidence}, nil
	}
	amountOut, err := market.NewTokenAmount(input.TokenOut, outUnits)
	if err != nil {
		evidence.Error = err.Error()
		return quoteport.ReferenceResult{Local: local, Evidence: evidence}, nil
	}
	evidence.Status = quoteport.ReferenceAvailable
	evidence.AmountOut = amountOut
	evidence.ContextSlot = payload.ContextSlot
	for _, hop := range payload.RoutePlan {
		evidence.Route = append(evidence.Route, quoteport.ReferenceHop{AMM: hop.SwapInfo.AMMKey, Label: hop.SwapInfo.Label, InputMint: hop.SwapInfo.InputMint, OutputMint: hop.SwapInfo.OutputMint, InAmount: hop.SwapInfo.InAmount, OutAmount: hop.SwapInfo.OutAmount})
	}
	return quoteport.ReferenceResult{Local: local, Evidence: evidence}, nil
}

type buildResponse struct {
	OutAmount   string `json:"outAmount"`
	ContextSlot uint64 `json:"contextSlot"`
	RoutePlan   []struct {
		SwapInfo struct {
			AMMKey     string `json:"ammKey"`
			Label      string `json:"label"`
			InputMint  string `json:"inputMint"`
			OutputMint string `json:"outputMint"`
			InAmount   string `json:"inAmount"`
			OutAmount  string `json:"outAmount"`
		} `json:"swapInfo"`
	} `json:"routePlan"`
}

var _ quoteport.ReferenceSource = (*Source)(nil)
