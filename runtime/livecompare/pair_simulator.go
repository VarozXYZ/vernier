package livecompare

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/VarozXYZ/vernier/adapters/chain/evm"
	"github.com/VarozXYZ/vernier/adapters/market/meteora/dlmm"
	"github.com/VarozXYZ/vernier/adapters/market/uniswapv3"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
	simulationport "github.com/VarozXYZ/vernier/ports/simulation"
	"github.com/VarozXYZ/vernier/runtime/configuration"
	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gagliardetto/solana-go"
)

// pairSimulator runs both legs independently. It never signs, sends, or
// mutates chain state: the EVM leg uses eth_call state overrides and the
// Solana leg uses simulateTransaction with signature verification disabled.
type pairSimulator struct {
	runner *Runner
}

// A simulation only needs the snapshot of the currently running round and the
// newest pending round. Keeping every event snapshot would retain full pool
// states indefinitely in a long-lived research process.
const maxSimulationSnapshots = 64

func newPairSimulator(runner *Runner) *pairSimulator { return &pairSimulator{runner: runner} }

func (s *pairSimulator) marketConfig(id market.MarketID) (configuration.ResolvedMarket, bool) {
	for _, configured := range s.runner.config.Markets {
		if configured.ID == id {
			return configured, true
		}
	}
	return configuration.ResolvedMarket{}, false
}

func (s *pairSimulator) SimulatePair(ctx context.Context, request arbitrage.SimulationRequest) (arbitrage.SimulationRound, error) {
	if err := request.Validate(); err != nil {
		return arbitrage.SimulationRound{}, err
	}
	started := s.runner.clock().UTC()
	round := arbitrage.SimulationRound{
		ID:       fmt.Sprintf("simulation-%s-%d", request.WindowID, request.PointSequence),
		WindowID: request.WindowID, PointSequence: request.PointSequence,
		RequestedAt: request.RequestedAt, StartedAt: started,
		Status: arbitrage.SimulationPending, Buy: request.Buy, Sell: request.Sell,
		LocalQualified: request.LocalQualified, LocalNetPnL: request.LocalNetPnL,
		LocalThreshold: request.LocalThreshold,
	}

	round.Buy, round.Sell = simulateLegPair(ctx, request.Buy, request.Sell, s.simulateLeg)

	if round.Buy.Status == arbitrage.SimulationConfirmed && round.Sell.Status == arbitrage.SimulationConfirmed {
		round.Status = arbitrage.SimulationConfirmed
		round.SimulatedNetPnL = request.LocalNetPnL
	} else {
		failureClass, message := simulationFailure(round.Buy, round.Sell)
		round.FailureClass, round.Error = failureClass, message
		if failureClass == arbitrage.SimulationFailureInfrastructure || failureClass == arbitrage.SimulationFailureFixture {
			round.Status = arbitrage.SimulationUnavailable
		} else {
			round.Status = arbitrage.SimulationRejected
		}
	}
	round.FinishedAt = s.runner.clock().UTC()
	return round, nil
}

func (s *pairSimulator) simulateLeg(ctx context.Context, leg arbitrage.SimulationLeg) arbitrage.SimulationLeg {
	leg.StartedAt = s.runner.clock().UTC()
	configured, ok := s.marketConfig(leg.Market)
	if !ok {
		return s.finishLeg(leg, arbitrage.SimulationUnavailable, arbitrage.SimulationFailureFixture, "simulation market is not configured", nil)
	}
	chain, ok := s.runner.config.Chains[configured.Venue.Chain]
	if !ok {
		return s.finishLeg(leg, arbitrage.SimulationUnavailable, arbitrage.SimulationFailureFixture, "simulation chain is not configured", nil)
	}
	var result *big.Int
	var err error
	switch chain.Kind {
	case "solana":
		result, err = s.simulateSolana(ctx, configured, leg)
	case "evm":
		result, err = s.simulateEVM(ctx, configured, leg)
	default:
		err = fmt.Errorf("unsupported simulation chain kind %q", chain.Kind)
	}
	if err != nil {
		class := classifySimulationError(err)
		status := arbitrage.SimulationRejected
		if class == arbitrage.SimulationFailureInfrastructure || class == arbitrage.SimulationFailureFixture {
			status = arbitrage.SimulationUnavailable
		}
		return s.finishLeg(leg, status, class, err.Error(), nil)
	}
	return s.finishLeg(leg, arbitrage.SimulationConfirmed, arbitrage.SimulationFailureNone, "", result)
}

func (s *pairSimulator) simulateSolana(ctx context.Context, configured configuration.ResolvedMarket, leg arbitrage.SimulationLeg) (*big.Int, error) {
	network := s.runner.solanaNetworks[configured.Venue.Chain]
	if network == nil || len(configured.Path) != 1 {
		return nil, fmt.Errorf("solana simulation requires one configured pool hop")
	}
	return s.simulateSolanaWithSnapshot(ctx, configured, leg)
}

func (s *pairSimulator) simulateSolanaWithSnapshot(ctx context.Context, configured configuration.ResolvedMarket, leg arbitrage.SimulationLeg) (*big.Int, error) {
	snapshot, ok := s.runner.simulationSnapshot(leg)
	if !ok {
		return nil, fmt.Errorf("solana simulation snapshot is unavailable")
	}
	child, err := childMarketSnapshot(snapshot, configured.ID)
	if err != nil {
		return nil, err
	}
	if configured.Path[0].Venue.Kind != "meteora_dlmm" {
		return nil, fmt.Errorf("unsupported Solana simulation venue %q", configured.Path[0].Venue.Kind)
	}
	ownerText, ok := s.runner.lookup(s.runner.config.SimulationSolanaOwnerEnv)
	if !ok || strings.TrimSpace(ownerText) == "" {
		return nil, fmt.Errorf("solana simulation owner environment is unset")
	}
	owner, err := solana.PublicKeyFromBase58(strings.TrimSpace(ownerText))
	if err != nil {
		return nil, fmt.Errorf("decode Solana simulation owner: %w", err)
	}
	inputAddress, err := configuredTokenAddressText(configured, leg.Input.Token())
	if err != nil {
		return nil, err
	}
	outputAddress, err := configuredTokenAddressText(configured, leg.LocalOutput.Token())
	if err != nil {
		return nil, err
	}
	raw, err := dlmm.BuildSimulationTransaction(ctx, s.runner.solanaNetworks[configured.Venue.Chain], dlmm.SimulationConfig{
		Pool: configured.Path[0].Venue.PoolText, TokenX: configured.Path[0].In.AddressText,
		TokenY: configured.Path[0].Out.AddressText, ComputeLimit: s.runner.config.SimulationSolanaComputeLimit,
	}, child, inputAddress, outputAddress, leg.Input.Units(), leg.LocalOutput.Units(), owner)
	if err != nil {
		return nil, err
	}
	if err := s.runner.solanaNetworks[configured.Venue.Chain].SimulateTransactionWithoutSignatureVerification(ctx, raw); err != nil {
		return nil, err
	}
	return leg.LocalOutput.Units(), nil
}

func (s *pairSimulator) simulateEVM(ctx context.Context, configured configuration.ResolvedMarket, leg arbitrage.SimulationLeg) (*big.Int, error) {
	if len(configured.Path) != 1 || configured.Path[0].Venue.Kind != "uniswap_v3" && configured.Path[0].Venue.Kind != "pancakeswap_v3" {
		return nil, fmt.Errorf("unsupported EVM simulation venue %q", configured.Venue.Kind)
	}
	ownerText, ok := s.runner.lookup(s.runner.config.SimulationEVMOwnerEnv)
	if !ok || strings.TrimSpace(ownerText) == "" {
		return nil, fmt.Errorf("EVM simulation owner environment is unset")
	}
	routerText, ok := s.runner.lookup(s.runner.config.SimulationEVMRouterEnv)
	if !ok || strings.TrimSpace(routerText) == "" {
		return nil, fmt.Errorf("EVM simulation router environment is unset")
	}
	owner := common.HexToAddress(strings.TrimSpace(ownerText))
	router := common.HexToAddress(strings.TrimSpace(routerText))
	if owner == (common.Address{}) || router == (common.Address{}) {
		return nil, fmt.Errorf("EVM simulation owner or router is invalid")
	}
	network, ok := s.runner.networks[configured.Venue.Chain].(interface{ StateOverrideRPC() evm.StateOverrideRPC })
	if !ok || network.StateOverrideRPC() == nil {
		return nil, fmt.Errorf("EVM network does not expose state override RPC")
	}
	inputAddress, err := configuredTokenAddress(configured, leg.Input.Token())
	if err != nil {
		return nil, err
	}
	outputAddress, err := configuredTokenAddress(configured, leg.LocalOutput.Token())
	if err != nil {
		return nil, err
	}
	child, err := childMarketSnapshot(s.runner.simulationSnapshotValue(leg), configured.ID)
	if err != nil {
		return nil, err
	}
	if s.runner.simulationSnapshotValue(leg).Data() == nil {
		return nil, fmt.Errorf("EVM simulation snapshot is unavailable")
	}
	state, ok := child.Data().(uniswapv3.Snapshot)
	if !ok {
		return nil, fmt.Errorf("EVM simulation snapshot is not Uniswap V3 state")
	}
	params := struct {
		TokenIn           common.Address
		TokenOut          common.Address
		Fee               *big.Int
		Recipient         common.Address
		Deadline          *big.Int
		AmountIn          *big.Int
		AmountOutMinimum  *big.Int
		SqrtPriceLimitX96 *big.Int
	}{inputAddress, outputAddress, new(big.Int).SetUint64(uint64(state.FeePips())), owner, new(big.Int).Add(new(big.Int).SetInt64(time.Now().Unix()), big.NewInt(60)), leg.Input.Units(), leg.LocalOutput.Units(), new(big.Int)}
	parsed, err := abi.JSON(strings.NewReader(`[ {"inputs":[{"components":[{"internalType":"address","name":"tokenIn","type":"address"},{"internalType":"address","name":"tokenOut","type":"address"},{"internalType":"uint24","name":"fee","type":"uint24"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMinimum","type":"uint256"},{"internalType":"uint160","name":"sqrtPriceLimitX96","type":"uint160"}],"name":"params","type":"tuple"}],"name":"exactInputSingle","outputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"}],"stateMutability":"nonpayable","type":"function"} ]`))
	if err != nil {
		return nil, err
	}
	data, err := parsed.Pack("exactInputSingle", params)
	if err != nil {
		return nil, fmt.Errorf("encode EVM simulation call: %w", err)
	}
	balanceSlot := s.runner.config.SimulationEVMBalanceSlot
	allowanceSlot := s.runner.config.SimulationEVMAllowanceSlot
	if slots, ok := s.runner.config.SimulationEVMTokenSlots[leg.Input.Token()]; ok {
		balanceSlot, allowanceSlot = slots.BalanceSlot, slots.AllowanceSlot
	}
	simulator, err := evm.NewERC20StateOverrideSimulator(evm.ERC20StateOverrideSimulatorConfig{
		Client: network.StateOverrideRPC(), Token: inputAddress, Owner: owner,
		BalanceSlot: balanceSlot, AllowanceSlot: allowanceSlot,
	})
	if err != nil {
		return nil, err
	}
	result, err := simulator.CallContract(ctx, geth.CallMsg{From: owner, To: &router, Gas: s.runner.config.SimulationEVMGasLimit, Data: data}, nil)
	if err != nil {
		return nil, err
	}
	if len(result) >= 32 {
		return new(big.Int).SetBytes(result[len(result)-32:]), nil
	}
	return leg.LocalOutput.Units(), nil
}

type simulatedLegResult struct {
	buy bool
	leg arbitrage.SimulationLeg
}

func simulateLegPair(ctx context.Context, buy, sell arbitrage.SimulationLeg, simulate func(context.Context, arbitrage.SimulationLeg) arbitrage.SimulationLeg) (arbitrage.SimulationLeg, arbitrage.SimulationLeg) {
	results := make(chan simulatedLegResult, 2)
	go func() { results <- simulatedLegResult{buy: true, leg: simulate(ctx, buy)} }()
	go func() { results <- simulatedLegResult{buy: false, leg: simulate(ctx, sell)} }()
	for range 2 {
		result := <-results
		if result.buy {
			buy = result.leg
		} else {
			sell = result.leg
		}
	}
	return buy, sell
}

func (s *pairSimulator) finishLeg(leg arbitrage.SimulationLeg, status arbitrage.SimulationStatus, failure arbitrage.SimulationFailureClass, message string, output *big.Int) arbitrage.SimulationLeg {
	leg.Status, leg.FailureClass, leg.Error = status, failure, message
	if output != nil {
		leg.SimulatedOutput, _ = market.NewTokenAmount(leg.LocalOutput.Token(), output)
	}
	leg.FinishedAt = s.runner.clock().UTC()
	return leg
}

func simulationFailure(buy, sell arbitrage.SimulationLeg) (arbitrage.SimulationFailureClass, string) {
	for _, leg := range []arbitrage.SimulationLeg{buy, sell} {
		if leg.Status != arbitrage.SimulationConfirmed {
			return leg.FailureClass, fmt.Sprintf("%s: %s", leg.Market, leg.Error)
		}
	}
	return arbitrage.SimulationFailureNone, ""
}

func classifySimulationError(err error) arbitrage.SimulationFailureClass {
	if err == nil {
		return arbitrage.SimulationFailureNone
	}
	text := strings.ToLower(err.Error())
	for _, marker := range []string{"revert", "instructionerror", "custom program error", "insufficient", "transfer failed", "slippage"} {
		if strings.Contains(text, marker) {
			return arbitrage.SimulationFailureExecution
		}
	}
	for _, marker := range []string{"owner environment", "router environment", "not configured", "unsupported", "snapshot", "token direction", "invalid"} {
		if strings.Contains(text, marker) {
			return arbitrage.SimulationFailureFixture
		}
	}
	return arbitrage.SimulationFailureInfrastructure
}

func configuredTokenAddress(configured configuration.ResolvedMarket, token market.TokenID) (common.Address, error) {
	if token == configured.Base.Token.ID {
		return configured.Base.Address, nil
	}
	if token == configured.Quote.Token.ID {
		return configured.Quote.Address, nil
	}
	for _, hop := range configured.Path {
		if token == hop.In.Token.ID {
			return hop.In.Address, nil
		}
		if token == hop.Out.Token.ID {
			return hop.Out.Address, nil
		}
	}
	return common.Address{}, fmt.Errorf("token %q is not part of market %q", token, configured.ID)
}

func configuredTokenAddressText(configured configuration.ResolvedMarket, token market.TokenID) (string, error) {
	if token == configured.Base.Token.ID {
		return configured.Base.AddressText, nil
	}
	if token == configured.Quote.Token.ID {
		return configured.Quote.AddressText, nil
	}
	for _, hop := range configured.Path {
		if token == hop.In.Token.ID {
			return hop.In.AddressText, nil
		}
		if token == hop.Out.Token.ID {
			return hop.Out.AddressText, nil
		}
	}
	return "", fmt.Errorf("token %q is not part of market %q", token, configured.ID)
}

func childMarketSnapshot(snapshot market.MarketSnapshot, marketID market.MarketID) (market.MarketSnapshot, error) {
	if bundle, ok := snapshot.Data().(market.SnapshotBundle); ok {
		for _, child := range bundle.Snapshots() {
			return child, nil
		}
		return market.MarketSnapshot{}, fmt.Errorf("route %q contains no child snapshot", marketID)
	}
	return snapshot, nil
}

// These two helpers are deliberately kept on Runner rather than in the
// domain. Simulation needs the immutable route snapshot, while the domain
// only stores its hash/version as evidence.
func (r *Runner) simulationSnapshot(leg arbitrage.SimulationLeg) (market.MarketSnapshot, bool) {
	r.simulationMu.RLock()
	defer r.simulationMu.RUnlock()
	snapshot, ok := r.simulationSnapshots[string(leg.Market)+"/"+fmt.Sprint(leg.SnapshotVersion)+"/"+fmt.Sprintf("%x", leg.SnapshotHash)]
	return snapshot, ok
}

func (r *Runner) simulationSnapshotValue(leg arbitrage.SimulationLeg) market.MarketSnapshot {
	snapshot, _ := r.simulationSnapshot(leg)
	return snapshot
}

// SnapshotForQuote returns the immutable snapshot retained for one discovery
// quote. Live uses it to rebuild a deterministic local intent after the remote
// build completes; it never returns a merely latest snapshot under the same
// identity.
func (r *Runner) SnapshotForQuote(quote market.Quote) (market.MarketSnapshot, bool) {
	r.simulationMu.RLock()
	defer r.simulationMu.RUnlock()
	key := string(quote.Market) + "/" + fmt.Sprint(quote.SnapshotVersion) + "/" + fmt.Sprintf("%x", quote.SnapshotHash)
	snapshot, ok := r.simulationSnapshots[key]
	return snapshot, ok
}

func simulationSnapshotKey(id market.MarketID, snapshot market.MarketSnapshot) string {
	metadata := snapshot.Metadata()
	return string(id) + "/" + fmt.Sprint(metadata.Version) + "/" + fmt.Sprintf("%x", metadata.StateHash)
}

func (r *Runner) rememberSimulationSnapshots(snapshots []market.MarketSnapshot) {
	r.simulationMu.Lock()
	defer r.simulationMu.Unlock()
	for _, snapshot := range snapshots {
		r.latestLocalSnapshots[snapshot.Metadata().Market] = snapshot
		key := simulationSnapshotKey(snapshot.Metadata().Market, snapshot)
		if _, exists := r.simulationSnapshots[key]; exists {
			r.simulationSnapshots[key] = snapshot
			continue
		}
		r.simulationSnapshots[key] = snapshot
		r.simulationSnapshotKeys = append(r.simulationSnapshotKeys, key)
		if len(r.simulationSnapshotKeys) > maxSimulationSnapshots {
			oldest := r.simulationSnapshotKeys[0]
			r.simulationSnapshotKeys = r.simulationSnapshotKeys[1:]
			delete(r.simulationSnapshots, oldest)
		}
	}
}

func (r *Runner) LatestSnapshot(id market.MarketID) (market.MarketSnapshot, bool) {
	r.simulationMu.RLock()
	defer r.simulationMu.RUnlock()
	snapshot, ok := r.latestLocalSnapshots[id]
	return snapshot, ok
}

func (r *Runner) simulationRequestForCandidate(windowID arbitrage.WindowID, sequence uint64, candidate arbitrage.Candidate, snapshots []market.MarketSnapshot, qualified bool) (arbitrage.SimulationRequest, bool) {
	find := func(quote market.Quote) (arbitrage.SimulationLeg, bool) {
		for _, snapshot := range snapshots {
			metadata := snapshot.Metadata()
			if metadata.Market != quote.Market {
				continue
			}
			hash := quote.SnapshotHash
			if hash == ([32]byte{}) {
				hash = metadata.StateHash
			}
			return arbitrage.SimulationLeg{
				Chain: chainForMarket(r.config, quote.Market), Market: quote.Market,
				Input: quote.AmountIn, LocalOutput: quote.AmountOut, Status: arbitrage.SimulationPending,
				SnapshotVersion: quote.SnapshotVersion, SnapshotHash: hash,
				Context: metadata.EventReference.Value, ContextPosition: metadata.EventPosition.Value,
			}, true
		}
		return arbitrage.SimulationLeg{}, false
	}
	buy, ok := find(candidate.BuyQuote)
	if !ok {
		return arbitrage.SimulationRequest{}, false
	}
	sell, ok := find(candidate.SellQuote)
	if !ok {
		return arbitrage.SimulationRequest{}, false
	}
	return arbitrage.SimulationRequest{
		WindowID: windowID, PointSequence: sequence, RequestedAt: r.clock().UTC(),
		Buy: buy, Sell: sell, LocalQualified: qualified,
		LocalNetPnL: candidate.NetPnL, LocalThreshold: candidate.EffectiveThreshold,
	}, true
}

func chainForMarket(config configuration.ParsedConfig, id market.MarketID) string {
	for _, configured := range config.Markets {
		if configured.ID == id {
			return configured.Venue.Chain
		}
	}
	return ""
}

var _ simulationport.PairSimulator = (*pairSimulator)(nil)
