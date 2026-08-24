package livecompare

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/VarozXYZ/vernier/adapters/chain/evm"
	kyberexecution "github.com/VarozXYZ/vernier/adapters/execution/kyberswap"
	quoteadapter "github.com/VarozXYZ/vernier/adapters/quote/kyberswap"
	"github.com/VarozXYZ/vernier/core/strategy"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	domainexecution "github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	persistenceport "github.com/VarozXYZ/vernier/ports/persistence"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

type prefundedValidationCoordinator struct {
	runner      *Runner
	strategy    *strategy.PrefundedParallel
	registry    *market.Registry
	store       persistenceport.ExecutableValidationStore
	remote      eventRefreshedRuntime
	localID     market.MarketID
	localSource quoteport.Source
	snapshots   func() ([]market.MarketSnapshot, bool)
	sender      common.Address
}

func newPrefundedValidationCoordinator(r *Runner, candidate evaluationStrategy, registry *market.Registry, store persistenceport.TrackingStore,
	local map[market.MarketID]routeRuntime, remote map[market.MarketID]eventRefreshedRuntime) (*prefundedValidationCoordinator, error) {
	if !r.config.ExecutableValidationEnabled {
		return nil, nil
	}
	prefunded, ok := candidate.(*strategy.PrefundedParallel)
	if !ok {
		return nil, fmt.Errorf("executable validation requires prefunded_parallel strategy")
	}
	journal, ok := any(store).(persistenceport.ExecutableValidationStore)
	if !ok {
		return nil, fmt.Errorf("opportunity store has no executable validation journal")
	}
	if len(local) != 1 || len(remote) != 1 {
		return nil, fmt.Errorf("executable validation requires one local and one remote market")
	}
	senderText, exists := r.lookup(r.config.ExecutableValidationEVMSenderEnv)
	if !exists || !common.IsHexAddress(senderText) || common.HexToAddress(senderText) == (common.Address{}) {
		return nil, fmt.Errorf("executable validation sender environment is unset or invalid")
	}
	var localID market.MarketID
	var localSource quoteport.Source
	for id, runtime := range local {
		localID, localSource = id, runtime.source
	}
	var remoteRuntime eventRefreshedRuntime
	for _, runtime := range remote {
		remoteRuntime = runtime
	}
	if remoteRuntime.kyberMarket == nil || remoteRuntime.kyberClient == nil {
		return nil, fmt.Errorf("executable validation requires KyberSwap route retention")
	}
	return &prefundedValidationCoordinator{runner: r, strategy: prefunded, registry: registry, store: journal, remote: remoteRuntime,
		localID: localID, localSource: localSource, sender: common.HexToAddress(senderText), snapshots: func() ([]market.MarketSnapshot, bool) { return eventStreamSnapshots(r.config.Markets, local, remote) }}, nil
}

func (c *prefundedValidationCoordinator) validate(ctx context.Context, window arbitrage.WindowID, sequence uint64, direction arbitrage.Direction, candidate arbitrage.Candidate, openingSnapshots []market.MarketSnapshot, bypassProfitThreshold bool) (*arbitrage.ExecutableValidationRound, *arbitrage.Candidate) {
	if c == nil {
		return nil, nil
	}
	now := c.runner.clock().UTC()
	round := &arbitrage.ExecutableValidationRound{ID: fmt.Sprintf("%s/validation/%d", window, sequence), WindowID: window, PointSequence: sequence,
		Direction: direction, RequestedAt: now, Status: arbitrage.ValidationUnavailable, DiscoveryOutput: candidate.Output,
		DiscoveryNet: candidate.NetPnL, Threshold: candidate.EffectiveThreshold, RemoteMarket: c.remote.config.ID, LocalMarket: c.localID}
	for _, snapshot := range openingSnapshots {
		if snapshot.Metadata().Market == c.localID {
			round.InitialLocalSnapshot = snapshot.Metadata()
		}
	}
	finishFailure := func(stage, class string) *arbitrage.ExecutableValidationRound {
		round.FailureStage, round.FailureClass, round.Error = stage, class, class
		round.Status = arbitrage.ValidationRejected
		if class == "rate_limited" || class == "provider_error" || class == "data_degraded" {
			round.Status = arbitrage.ValidationUnavailable
		}
		_ = c.store.RecordExecutableValidationRound(ctx, round)
		return round
	}
	remoteQuote := candidate.SellQuote
	side := domainexecution.LegSell
	if direction.BuyMarket == c.remote.config.ID {
		remoteQuote, side = candidate.BuyQuote, domainexecution.LegBuy
	}
	remoteSnapshot := snapshotByID(openingSnapshots, c.remote.config.ID)
	slots, ok := c.runner.config.ExecutableValidationEVMTokenSlots[remoteQuote.AmountIn.Token()]
	if !ok {
		return finishFailure("configuration", "input_token_slots_missing"), nil
	}
	inputAddress := c.remote.config.Base.AddressText
	if remoteQuote.AmountIn.Token() == c.remote.config.Quote.Token.ID {
		inputAddress = c.remote.config.Quote.AddressText
	}
	network := c.runner.networks[c.remote.config.Chain]
	provider, ok := network.(interface{ StateOverrideRPC() evm.StateOverrideRPC })
	if !ok || provider.StateOverrideRPC() == nil {
		return finishFailure("simulation", "state_override_unavailable"), nil
	}
	simulator, err := evm.NewERC20StateOverrideSimulator(evm.ERC20StateOverrideSimulatorConfig{Client: provider.StateOverrideRPC(), Token: common.HexToAddress(inputAddress), Owner: c.sender, BalanceSlot: slots.BalanceSlot, AllowanceSlot: slots.AllowanceSlot})
	if err != nil {
		return finishFailure("configuration", "state_override_invalid"), nil
	}
	profile := c.runner.config.QuoteSources[c.remote.config.QuoteSource]
	validator, err := kyberexecution.New(kyberexecution.Config{ID: remoteQuote.Source, ChainSlug: profile.ChainSlug, Sender: c.sender,
		TokenAddresses: map[market.TokenID]string{c.remote.config.Base.Token.ID: c.remote.config.Base.AddressText, c.remote.config.Quote.Token.ID: c.remote.config.Quote.AddressText},
		SlippageBPS:    profile.SlippageBPS, GasExecutionMode: "fixed", FixedExecutionGasLimit: kyberexecution.DefaultSwapGasLimit,
		GasCostMode: "fixed", FixedCostGasLimit: kyberexecution.DefaultSwapExpectedGasUsed, Source: c.remote.kyberClient,
		DiscoveryRoutes: c.remote.kyberMarket, Simulator: simulator, Clock: c.runner.clock,
		AllowedRouters: c.runner.executableAllowedRouters})
	if err != nil {
		return finishFailure("configuration", "validator_invalid"), nil
	}
	artifact, err := validator.Validate(ctx, executionport.ValidationRequest{Operation: domainexecution.OperationID(round.ID),
		Leg:       domainexecution.Leg{ID: "remote", Side: side, Chain: market.ChainID(c.remote.config.Chain), Account: "research-simulation", Market: c.remote.config.ID, Input: remoteQuote.AmountIn, ExpectedOutput: remoteQuote.AmountOut},
		Discovery: remoteQuote, Snapshot: remoteSnapshot, RequestedAt: now})
	if err != nil {
		return finishFailure(validationFailureStage(err), validationFailureClass(err)), nil
	}
	round.BuildFinishedAt, _ = time.Parse(time.RFC3339Nano, artifact.Metadata["build_finished_at"])
	round.SimulationFinishedAt, _ = time.Parse(time.RFC3339Nano, artifact.Metadata["simulation_finished_at"])
	round.BuildAttempts, _ = strconv.Atoi(artifact.Metadata["build_attempts"])
	round.RouteHTTPStatus, _ = strconv.Atoi(artifact.Metadata["route_http_status"])
	round.BuildHTTPStatus, _ = strconv.Atoi(artifact.Metadata["build_http_status"])
	routeNanos, _ := strconv.ParseInt(artifact.Metadata["route_duration_nanos"], 10, 64)
	buildNanos, _ := strconv.ParseInt(artifact.Metadata["build_duration_nanos"], 10, 64)
	round.RouteDuration, round.BuildDuration = time.Duration(routeNanos), time.Duration(buildNanos)
	round.RouteHash, round.BuildHash = artifact.Metadata["route_response_hash"], artifact.Metadata["build_response_hash"]
	round.RouteFinishedAt = remoteQuote.QuotedAt.Add(round.RouteDuration)
	validatedQuantity, amountErr := artifact.ValidatedQuote.AmountOut.ToAssetQuantity(tokenByID(c.registry, artifact.ValidatedQuote.AmountOut.Token()))
	if amountErr == nil {
		round.BuildOutput = validatedQuantity
	}
	fresh, ready := c.snapshots()
	if !ready {
		return finishFailure("local_recapture", "data_degraded"), nil
	}
	localSnapshot := snapshotByID(fresh, c.localID)
	if localSnapshot.Metadata().Health != market.HealthHealthy {
		return finishFailure("local_recapture", "data_degraded"), nil
	}
	c.runner.rememberSimulationSnapshots(fresh)
	round.LocalRecapturedAt, round.FinalLocalSnapshot = c.runner.clock().UTC(), localSnapshot.Metadata()
	localDiscovery := candidate.BuyQuote
	if direction.SellMarket == c.localID {
		localDiscovery = candidate.SellQuote
	}
	localQuote, err := c.localSource.Quote(ctx, quoteport.Input{Snapshot: localSnapshot, TokenIn: localDiscovery.AmountIn.Token(), TokenOut: localDiscovery.AmountOut.Token(), AmountIn: localDiscovery.AmountIn, Purpose: market.QuotePurposeLiveValidation, QuotedAt: c.runner.clock().UTC()})
	if err != nil {
		return finishFailure("local_requote", "local_quote_error"), nil
	}
	var buy, sell market.Quote
	if direction.BuyMarket == c.remote.config.ID {
		buy = artifact.ValidatedQuote
		sell = localQuote
	} else {
		buy = localQuote
		sell = artifact.ValidatedQuote
	}
	byID := make(map[market.MarketID]market.MarketSnapshot, 2)
	for _, snapshot := range fresh {
		byID[snapshot.Metadata().Market] = snapshot
	}
	byID[c.remote.config.ID] = remoteSnapshot
	final, err := c.strategy.RevalueCandidate(direction, candidate, buy, sell, byID, c.runner.clock().UTC())
	if err != nil {
		return finishFailure("recalculation", "economic_invariant"), nil
	}
	round.RecalculatedAt, round.FinalNet = c.runner.clock().UTC(), final.NetPnL
	comparison, compareErr := final.NetPnL.Cmp(final.EffectiveThreshold)
	if bypassProfitThreshold || (compareErr == nil && comparison >= 0 && final.NetPnL.Sign() > 0) {
		round.Status = arbitrage.ValidationConfirmed
		c.runner.retainExecutableArtifact(remoteQuote, artifact.ValidatedQuote, artifact)
	} else {
		round.Status = arbitrage.ValidationDisappeared
	}
	if err := c.store.RecordExecutableValidationRound(ctx, round); err != nil {
		c.runner.logger.Warn("executable validation persistence failed", "window", window, "error", err)
	}
	if round.Status != arbitrage.ValidationConfirmed {
		return round, nil
	}
	copy := final
	return round, &copy
}

func snapshotByID(snapshots []market.MarketSnapshot, id market.MarketID) market.MarketSnapshot {
	for _, snapshot := range snapshots {
		if snapshot.Metadata().Market == id {
			return snapshot
		}
	}
	return market.MarketSnapshot{}
}
func tokenByID(registry *market.Registry, id market.TokenID) market.Token {
	token, _ := registry.Token(id)
	return token
}
func validationFailureStage(err error) string {
	if strings.Contains(strings.ToLower(err.Error()), "build") {
		return "build"
	}
	return "simulation"
}
func validationFailureClass(err error) string {
	var api *quoteadapter.APIError
	if errors.As(err, &api) {
		if api.RateLimited() {
			return "rate_limited"
		}
		return "provider_error"
	}
	var slip *executionport.SlippageThresholdError
	if errors.As(err, &slip) {
		return "build_deteriorated"
	}
	return "simulation_reverted"
}
