package jupiter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

const DefaultBuildPath = "/swap/v2/build"

type BuildConfig struct {
	ID                     market.SourceID
	BaseURL                string
	BuildPath              string
	Taker                  string
	Payer                  string
	APIKeys                []string
	APIKeyPool             *APIKeyPool
	APIKeyHeader           string
	TokenMints             map[market.TokenID]string
	SlippageBPS            uint16
	MaxAccounts            uint16
	TipAmount              string
	ComputePricePercentile string
	BlockhashSlotsToExpiry uint16
	Client                 Client
	Clock                  Clock
}

// BuildSource validates one profitable ExactIn leg and retains the complete
// instruction response only in its in-memory Artifact.
type BuildSource struct {
	id                     market.SourceID
	baseURL                string
	path                   string
	taker                  string
	payer                  string
	keyPool                *APIKeyPool
	apiKeyHeader           string
	mints                  map[market.TokenID]string
	slippageBPS            uint16
	maxAccounts            uint16
	tipAmount              string
	computePricePercentile string
	blockhashSlots         uint16
	client                 Client
	clock                  Clock
}

func NewBuildSource(config BuildConfig) (*BuildSource, error) {
	if config.ID == "" || strings.TrimSpace(config.Taker) == "" || len(config.TokenMints) == 0 {
		return nil, fmt.Errorf("jupiter build source requires ID, taker, and token mints")
	}
	if config.BaseURL == "" {
		config.BaseURL = DefaultBaseURL
	}
	baseURL, err := url.Parse(strings.TrimRight(config.BaseURL, "/"))
	if err != nil || baseURL.Scheme == "" || baseURL.Host == "" || baseURL.RawQuery != "" || baseURL.Fragment != "" {
		return nil, fmt.Errorf("invalid jupiter build base URL")
	}
	if config.BuildPath == "" {
		config.BuildPath = DefaultBuildPath
	}
	if !strings.HasPrefix(config.BuildPath, "/") {
		return nil, fmt.Errorf("jupiter build path must start with /")
	}
	if config.SlippageBPS > 10_000 || config.MaxAccounts > 64 ||
		config.BlockhashSlotsToExpiry > 300 {
		return nil, fmt.Errorf("jupiter build request limits are invalid")
	}
	if config.SlippageBPS == 0 {
		config.SlippageBPS = DefaultSlippageBPS
	}
	if config.MaxAccounts == 0 {
		config.MaxAccounts = 64
	}
	if config.BlockhashSlotsToExpiry == 0 {
		config.BlockhashSlotsToExpiry = 150
	}
	if strings.TrimSpace(config.TipAmount) != "" {
		tip, ok := new(big.Int).SetString(config.TipAmount, 10)
		if !ok || tip.Sign() <= 0 {
			return nil, fmt.Errorf("jupiter build tip amount must be positive integer lamports")
		}
	}
	keyPool := config.APIKeyPool
	if keyPool == nil {
		if len(config.APIKeys) == 0 {
			return nil, fmt.Errorf("jupiter build requires API keys")
		}
		keyPool, err = NewAPIKeyPool(config.APIKeys)
		if err != nil {
			return nil, err
		}
	}
	mints := make(map[market.TokenID]string, len(config.TokenMints))
	for token, mint := range config.TokenMints {
		if token == "" || strings.TrimSpace(mint) == "" {
			return nil, fmt.Errorf("jupiter build token mapping is incomplete")
		}
		mints[token] = strings.TrimSpace(mint)
	}
	if config.Client == nil {
		config.Client = &http.Client{Timeout: 8 * time.Second}
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	return &BuildSource{
		id: config.ID, baseURL: strings.TrimRight(baseURL.String(), "/"), path: config.BuildPath,
		taker: strings.TrimSpace(config.Taker), payer: strings.TrimSpace(config.Payer), keyPool: keyPool,
		apiKeyHeader: config.APIKeyHeader, mints: mints, slippageBPS: config.SlippageBPS,
		maxAccounts: config.MaxAccounts, tipAmount: strings.TrimSpace(config.TipAmount),
		computePricePercentile: strings.TrimSpace(config.ComputePricePercentile),
		blockhashSlots:         config.BlockhashSlotsToExpiry, client: config.Client, clock: config.Clock,
	}, nil
}

func (s *BuildSource) Validate(
	ctx context.Context,
	request executionport.ValidationRequest,
) (executionport.Artifact, error) {
	return s.validate(ctx, request, s.maxAccounts)
}

func (s *BuildSource) ValidateCompact(
	ctx context.Context,
	request executionport.ValidationRequest,
	previous executionport.Artifact,
) (executionport.Artifact, error) {
	current := s.maxAccounts
	if text := strings.TrimSpace(previous.Metadata["max_accounts"]); text != "" {
		parsed, err := strconv.ParseUint(text, 10, 16)
		if err != nil || parsed == 0 || parsed > 64 {
			return executionport.Artifact{}, fmt.Errorf(
				"jupiter compact rebuild has invalid previous account limit",
			)
		}
		current = uint16(parsed)
	}
	next := nextCompactAccountLimit(current)
	if next == 0 {
		return executionport.Artifact{}, fmt.Errorf(
			"jupiter build cannot be compacted below %d accounts",
			current,
		)
	}
	return s.validate(ctx, request, next)
}

func (s *BuildSource) validate(
	ctx context.Context,
	request executionport.ValidationRequest,
	maxAccounts uint16,
) (executionport.Artifact, error) {
	if err := request.Leg.Validate(); err != nil {
		return executionport.Artifact{}, err
	}
	discovery := request.Discovery
	if discovery.Mode != market.QuoteModeExactInput || discovery.AmountIn.Token() != request.Leg.Input.Token() ||
		discovery.AmountIn.Units().Cmp(request.Leg.Input.Units()) != 0 {
		return executionport.Artifact{}, fmt.Errorf("jupiter build requires matching ExactIn discovery quote")
	}
	inputMint, inputOK := s.mints[discovery.AmountIn.Token()]
	outputMint, outputOK := s.mints[discovery.AmountOut.Token()]
	if !inputOK || !outputOK {
		return executionport.Artifact{}, fmt.Errorf("jupiter build token mint mapping is missing")
	}
	query := url.Values{}
	query.Set("inputMint", inputMint)
	query.Set("outputMint", outputMint)
	query.Set("amount", discovery.AmountIn.String())
	query.Set("taker", s.taker)
	query.Set("slippageBps", strconv.FormatUint(uint64(s.slippageBPS), 10))
	query.Set("maxAccounts", strconv.FormatUint(uint64(maxAccounts), 10))
	query.Set("blockhashSlotsToExpiry", strconv.FormatUint(uint64(s.blockhashSlots), 10))
	if s.payer != "" {
		query.Set("payer", s.payer)
	}
	if s.tipAmount != "" {
		query.Set("tipAmount", s.tipAmount)
	}
	if s.computePricePercentile != "" {
		query.Set("computeUnitPricePercentile", s.computePricePercentile)
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, s.baseURL+s.path+"?"+query.Encode(), nil)
	if err != nil {
		return executionport.Artifact{}, err
	}
	header := s.apiKeyHeader
	if header == "" {
		header = "x-api-key"
	}
	httpRequest.Header.Set(header, s.keyPool.Next())
	response, err := s.client.Do(httpRequest)
	if err != nil {
		return executionport.Artifact{}, err
	}
	if response == nil || response.Body == nil {
		return executionport.Artifact{}, fmt.Errorf("jupiter build HTTP client returned an empty response")
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxQuoteResponseBytes+1))
	if err != nil {
		return executionport.Artifact{}, err
	}
	if len(body) > maxQuoteResponseBytes {
		return executionport.Artifact{}, fmt.Errorf("jupiter build response exceeds %d bytes", maxQuoteResponseBytes)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return executionport.Artifact{}, parseAPIErrorFor("build", response.StatusCode, body)
	}
	var payload buildArtifactResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		return executionport.Artifact{}, fmt.Errorf("decode jupiter build response: %w", err)
	}
	if payload.InputMint != inputMint || payload.OutputMint != outputMint || payload.InAmount != discovery.AmountIn.String() {
		return executionport.Artifact{}, fmt.Errorf("jupiter build response changed fixed input")
	}
	outputUnits, ok := new(big.Int).SetString(payload.OutAmount, 10)
	if !ok || outputUnits.Sign() <= 0 || payload.SwapInstruction.ProgramID == "" {
		return executionport.Artifact{}, fmt.Errorf("jupiter build response is incomplete")
	}
	output, err := market.NewTokenAmount(discovery.AmountOut.Token(), outputUnits)
	if err != nil {
		return executionport.Artifact{}, err
	}
	position := discovery.SourcePosition
	if payload.ContextSlot > 0 {
		position = market.SourcePosition{Kind: ContextSlotPositionKind, Value: payload.ContextSlot}
	}
	validated, err := market.NewQuote(market.Quote{
		Source: s.id, Market: request.Leg.Market,
		SnapshotVersion: discovery.SnapshotVersion, SnapshotHash: discovery.SnapshotHash,
		SourcePosition: position, ResponseHash: sha256.Sum256(body),
		Purpose: market.QuotePurposeLiveValidation, Mode: market.QuoteModeExactInput,
		Quality: market.QuoteQualityExact, AmountIn: discovery.AmountIn, AmountOut: output,
		QuotedAt: s.clock().UTC(),
	})
	if err != nil {
		return executionport.Artifact{}, err
	}
	blockhash := payload.BlockhashWithMetadata.String()
	if blockhash == "" || payload.BlockhashWithMetadata.LastValidBlockHeight == 0 {
		return executionport.Artifact{}, fmt.Errorf("jupiter build response has no blockhash metadata")
	}
	return executionport.Artifact{
		Leg: request.Leg, ValidatedQuote: validated, Payload: append([]byte(nil), body...),
		Metadata: map[string]string{
			"kind":         "jupiter_build_v2",
			"max_accounts": strconv.FormatUint(uint64(maxAccounts), 10),
		},
		BuiltAt: s.clock().UTC(), Blockhash: blockhash,
		LastValidBlockHeight: payload.BlockhashWithMetadata.LastValidBlockHeight,
	}, nil
}

func nextCompactAccountLimit(current uint16) uint16 {
	switch {
	case current > 48:
		return 48
	case current > 32:
		return 32
	case current > 24:
		return 24
	default:
		return 0
	}
}

type apiInstruction struct {
	ProgramID string `json:"programId"`
	Accounts  []struct {
		Pubkey     string `json:"pubkey"`
		IsWritable bool   `json:"isWritable"`
		IsSigner   bool   `json:"isSigner"`
	} `json:"accounts"`
	Data string `json:"data"`
}

type buildArtifactResponse struct {
	InputMint              string              `json:"inputMint"`
	OutputMint             string              `json:"outputMint"`
	InAmount               string              `json:"inAmount"`
	OutAmount              string              `json:"outAmount"`
	OtherAmountThreshold   string              `json:"otherAmountThreshold"`
	ContextSlot            uint64              `json:"contextSlot"`
	ComputeInstructions    []apiInstruction    `json:"computeBudgetInstructions"`
	SetupInstructions      []apiInstruction    `json:"setupInstructions"`
	SwapInstruction        apiInstruction      `json:"swapInstruction"`
	CleanupInstruction     *apiInstruction     `json:"cleanupInstruction"`
	OtherInstructions      []apiInstruction    `json:"otherInstructions"`
	TipInstruction         *apiInstruction     `json:"tipInstruction"`
	AddressesByLookupTable map[string][]string `json:"addressesByLookupTableAddress"`
	BlockhashWithMetadata  blockhashMetadata   `json:"blockhashWithMetadata"`
}

type blockhashMetadata struct {
	Blockhash            json.RawMessage `json:"blockhash"`
	LastValidBlockHeight uint64          `json:"lastValidBlockHeight"`
}

func (m blockhashMetadata) String() string {
	var text string
	if json.Unmarshal(m.Blockhash, &text) == nil && strings.TrimSpace(text) != "" {
		return strings.TrimSpace(text)
	}
	var bytes []byte
	if json.Unmarshal(m.Blockhash, &bytes) == nil && len(bytes) > 0 {
		return hex.EncodeToString(bytes)
	}
	return ""
}

var _ executionport.Validator = (*BuildSource)(nil)
var _ executionport.CompactValidator = (*BuildSource)(nil)
