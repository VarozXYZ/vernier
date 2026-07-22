package observev3

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/VarozXYZ/vernier/adapters/chain/evm"
	"github.com/VarozXYZ/vernier/adapters/feed/evmlogs"
	"github.com/VarozXYZ/vernier/adapters/feed/sourceorder"
	"github.com/VarozXYZ/vernier/adapters/market/uniswapv3"
	"github.com/VarozXYZ/vernier/core/marketstate"
	"github.com/VarozXYZ/vernier/domain/market"
	feedport "github.com/VarozXYZ/vernier/ports/feed"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

var ErrParityMismatch = errors.New("local Uniswap V3 quote differs from QuoterV2")

type Options struct {
	Format  string
	Updates int
	Output  io.Writer
	Clock   func() time.Time
}

type Observer struct {
	config    ParsedConfig
	network   evm.Network
	venue     *uniswapv3.Adapter
	feed      *evmlogs.Feed
	mirror    *marketstate.Mirror
	local     *uniswapv3.Quoter
	reference *uniswapv3.ReferenceQuoter
	options   Options
}

type QuoteEvidence struct {
	TokenIn     string `json:"token_in"`
	TokenOut    string `json:"token_out"`
	AmountIn    string `json:"amount_in"`
	LocalAmount string `json:"local_amount_out"`
	Reference   string `json:"reference_amount_out"`
	Parity      bool   `json:"parity"`
}

type SnapshotRecord struct {
	Type           string          `json:"type"`
	ConfigHash     string          `json:"config_hash"`
	NetworkAdapter string          `json:"network_adapter"`
	VenueAdapter   string          `json:"venue_adapter"`
	MarketID       string          `json:"market_id"`
	Block          uint64          `json:"block"`
	BlockHash      string          `json:"block_hash"`
	Version        uint64          `json:"version"`
	StateHash      string          `json:"state_hash"`
	Health         market.Health   `json:"health"`
	SqrtPriceX96   string          `json:"sqrt_price_x96"`
	Tick           int32           `json:"tick"`
	Liquidity      string          `json:"liquidity"`
	CoverageMin    int32           `json:"coverage_min_word"`
	CoverageMax    int32           `json:"coverage_max_word"`
	Quotes         []QuoteEvidence `json:"quotes"`
}

type HealthRecord struct {
	Type       string        `json:"type"`
	ConfigHash string        `json:"config_hash"`
	MarketID   string        `json:"market_id"`
	Health     market.Health `json:"health"`
	Reason     string        `json:"reason,omitempty"`
	ObservedAt time.Time     `json:"observed_at"`
}

func New(config ParsedConfig, network evm.Network, options Options) (*Observer, error) {
	if network == nil || network.ID() != config.Network.ID {
		return nil, fmt.Errorf("observe-v3 requires configured network %q", config.Network.ID)
	}
	if options.Output == nil {
		return nil, fmt.Errorf("observe-v3 output is required")
	}
	if options.Format != "text" && options.Format != "jsonl" {
		return nil, fmt.Errorf("observe-v3 format must be text or jsonl")
	}
	if options.Updates < 0 {
		return nil, fmt.Errorf("observe-v3 updates cannot be negative")
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	probes := make([]uniswapv3.CoverageProbe, len(config.QuoteInputs))
	for index, input := range config.QuoteInputs {
		probes[index] = uniswapv3.CoverageProbe{
			ZeroForOne: input.TokenIn == config.Token0ID,
			AmountIn:   new(big.Int).Set(config.ProbeInput[index]),
		}
	}
	venue, err := uniswapv3.NewAdapter(uniswapv3.OnChainConfig{
		Pool: config.Pool, MaxTickWords: config.MaxTickWords, Probes: probes,
	})
	if err != nil {
		return nil, err
	}
	mirror, err := marketstate.NewMirror(
		market.MarketID(config.MarketID), market.SourceID(config.Network.ID+"/logs"),
		uniswapv3.Reducer{}, sourceorder.NewMonotonic(evmlogs.BlockPositionKind, false), options.Clock,
	)
	if err != nil {
		return nil, err
	}
	feed, err := evmlogs.New(evmlogs.Config{
		Market: market.MarketID(config.MarketID), Source: market.SourceID(config.Network.ID + "/logs"),
		Network: network, Venue: venue, Clock: options.Clock,
	})
	if err != nil {
		return nil, err
	}
	candidate := market.Market{
		ID: market.MarketID(config.MarketID), BaseToken: market.TokenID(config.Token0ID),
		QuoteToken: market.TokenID(config.Token1ID),
	}
	local, err := uniswapv3.NewQuoter("uniswap-v3/local", candidate, market.TokenID(config.Token0ID), market.TokenID(config.Token1ID))
	if err != nil {
		return nil, err
	}
	reference, err := uniswapv3.NewReferenceQuoter(config.QuoterV2)
	if err != nil {
		return nil, err
	}
	return &Observer{
		config: config, network: network, venue: venue, feed: feed, mirror: mirror,
		local: local, reference: reference, options: options,
	}, nil
}

func (o *Observer) Run(ctx context.Context) error {
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	sink := &observerSink{observer: o, cancel: cancel}
	err := o.feed.Run(child, sink)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

type observerSink struct {
	observer      *Observer
	cancel        context.CancelFunc
	activeUpdates int
}

func (s *observerSink) Publish(ctx context.Context, event market.MarketEvent) error {
	result, err := s.observer.mirror.Apply(ctx, event)
	if err != nil {
		return err
	}
	if result.Disposition == feedport.ApplyDispositionIgnoredStale {
		return nil
	}
	snapshot := result.Snapshot
	evidence, err := s.observer.quote(ctx, event, &snapshot)
	if err != nil && !errors.Is(err, ErrParityMismatch) {
		return err
	}
	record := makeSnapshotRecord(s.observer.config, snapshot, evidence)
	if writeErr := s.observer.writeSnapshot(record); writeErr != nil {
		return writeErr
	}
	if err != nil {
		return err
	}
	s.activeUpdates++
	if s.observer.options.Updates > 0 && s.activeUpdates >= s.observer.options.Updates {
		s.cancel()
	}
	return nil
}

func (s *observerSink) Reset(ctx context.Context, event market.MarketEvent) error {
	result, err := s.observer.mirror.Reset(ctx, event)
	if err != nil {
		return err
	}
	if result.Disposition == feedport.ApplyDispositionIgnoredStale {
		return nil
	}
	snapshot := result.Snapshot
	evidence, err := s.observer.quote(ctx, event, &snapshot)
	if err != nil && !errors.Is(err, ErrParityMismatch) {
		return err
	}
	if writeErr := s.observer.writeSnapshot(makeSnapshotRecord(s.observer.config, snapshot, evidence)); writeErr != nil {
		return writeErr
	}
	return err
}

func (s *observerSink) SetHealth(ctx context.Context, update feedport.HealthUpdate) error {
	if err := s.observer.mirror.SetHealth(ctx, update); err != nil {
		return err
	}
	return s.observer.writeHealth(HealthRecord{
		Type: "health", ConfigHash: s.observer.config.Hash, MarketID: s.observer.config.MarketID,
		Health: update.Health, Reason: update.Reason, ObservedAt: update.ObservedAt.UTC(),
	})
}

func (o *Observer) quote(ctx context.Context, event market.MarketEvent, snapshot *market.MarketSnapshot) ([]QuoteEvidence, error) {
	evidence, err := o.quoteSnapshot(ctx, *snapshot)
	if errors.Is(err, uniswapv3.ErrInsufficientTickCoverage) {
		state, ok := snapshot.Data().(uniswapv3.Snapshot)
		if !ok {
			return nil, fmt.Errorf("observe-v3 received incompatible snapshot %T", snapshot.Data())
		}
		block := evm.BlockReference{Number: event.Position.Value, Hash: common.HexToHash(event.Reference.Value)}
		expanded, expandErr := o.venue.ExpandCoverage(ctx, o.network, block, state)
		if expandErr != nil {
			return nil, expandErr
		}
		event.Data = expanded
		result, applyErr := o.mirror.Apply(ctx, event)
		if applyErr != nil {
			return nil, applyErr
		}
		*snapshot = result.Snapshot
		return o.quoteSnapshot(ctx, *snapshot)
	}
	return evidence, err
}

func (o *Observer) quoteSnapshot(ctx context.Context, snapshot market.MarketSnapshot) ([]QuoteEvidence, error) {
	info, ok := o.venue.PoolInfo()
	if !ok {
		return nil, fmt.Errorf("uniswap V3 pool metadata is unavailable")
	}
	metadata := snapshot.Metadata()
	block := evm.BlockReference{Number: metadata.EventPosition.Value, Hash: common.HexToHash(metadata.EventReference.Value)}
	result := make([]QuoteEvidence, len(o.config.QuoteInputs))
	parity := true
	for index, input := range o.config.QuoteInputs {
		zeroForOne := input.TokenIn == o.config.Token0ID
		tokenOutID := o.config.Token0ID
		tokenInAddress, tokenOutAddress := info.Token1, info.Token0
		if zeroForOne {
			tokenOutID = o.config.Token1ID
			tokenInAddress, tokenOutAddress = info.Token0, info.Token1
		}
		amount, amountErr := market.NewTokenAmount(market.TokenID(input.TokenIn), o.config.ProbeInput[index])
		if amountErr != nil {
			return nil, amountErr
		}
		localQuote, quoteErr := o.local.Quote(ctx, quoteport.Input{
			Snapshot: snapshot, TokenIn: market.TokenID(input.TokenIn), TokenOut: market.TokenID(tokenOutID),
			AmountIn: amount, Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: o.options.Clock().UTC(),
		})
		if quoteErr != nil {
			return nil, quoteErr
		}
		reference, quoteErr := o.reference.QuoteExactInputSingle(
			ctx, o.network, block, tokenInAddress, tokenOutAddress, o.config.ProbeInput[index], info.Fee,
		)
		if quoteErr != nil {
			return nil, quoteErr
		}
		matches := localQuote.AmountOut.Units().Cmp(reference) == 0
		parity = parity && matches
		result[index] = QuoteEvidence{
			TokenIn: input.TokenIn, TokenOut: tokenOutID, AmountIn: input.Amount,
			LocalAmount: localQuote.AmountOut.String(), Reference: reference.String(), Parity: matches,
		}
	}
	if !parity {
		return result, ErrParityMismatch
	}
	return result, nil
}

func makeSnapshotRecord(config ParsedConfig, snapshot market.MarketSnapshot, quotes []QuoteEvidence) SnapshotRecord {
	metadata := snapshot.Metadata()
	state := snapshot.Data().(uniswapv3.Snapshot)
	return SnapshotRecord{
		Type: "snapshot", ConfigHash: config.Hash, NetworkAdapter: config.NetworkAdapter,
		VenueAdapter: config.VenueAdapter, MarketID: config.MarketID,
		Block: metadata.EventPosition.Value, BlockHash: metadata.EventReference.Value,
		Version: metadata.Version, StateHash: hex.EncodeToString(metadata.StateHash[:]), Health: metadata.Health,
		SqrtPriceX96: state.SqrtPriceX96().String(), Tick: state.Tick(), Liquidity: state.Liquidity().String(),
		CoverageMin: state.Coverage().MinWord(), CoverageMax: state.Coverage().MaxWord(), Quotes: quotes,
	}
}

func (o *Observer) writeSnapshot(record SnapshotRecord) error {
	if o.options.Format == "jsonl" {
		return writeJSONLine(o.options.Output, record)
	}
	_, err := fmt.Fprintf(
		o.options.Output,
		"snapshot block=%d hash=%s version=%d state=%s tick=%d liquidity=%s coverage=%d..%d parity=%t\n",
		record.Block, record.BlockHash, record.Version, record.StateHash, record.Tick,
		record.Liquidity, record.CoverageMin, record.CoverageMax, allParity(record.Quotes),
	)
	return err
}

func (o *Observer) writeHealth(record HealthRecord) error {
	if o.options.Format == "jsonl" {
		return writeJSONLine(o.options.Output, record)
	}
	_, err := fmt.Fprintf(o.options.Output, "health status=%s reason=%s observed_at=%s\n", record.Health, record.Reason, record.ObservedAt.Format(time.RFC3339Nano))
	return err
}

func writeJSONLine(output io.Writer, value any) error {
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(value)
}

func allParity(quotes []QuoteEvidence) bool {
	for _, quote := range quotes {
		if !quote.Parity {
			return false
		}
	}
	return true
}
