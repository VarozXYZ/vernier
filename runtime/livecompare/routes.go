package livecompare

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/VarozXYZ/vernier/adapters/chain/evm"
	"github.com/VarozXYZ/vernier/adapters/feed/evmlogs"
	"github.com/VarozXYZ/vernier/adapters/feed/solanalogs"
	"github.com/VarozXYZ/vernier/adapters/feed/sourceorder"
	"github.com/VarozXYZ/vernier/adapters/market/aerodrome"
	"github.com/VarozXYZ/vernier/adapters/market/aerodromeslipstream"
	"github.com/VarozXYZ/vernier/adapters/market/constantproduct"
	"github.com/VarozXYZ/vernier/adapters/market/meteora/dlmm"
	"github.com/VarozXYZ/vernier/adapters/market/orcawhirlpool"
	"github.com/VarozXYZ/vernier/adapters/market/uniswapv2"
	"github.com/VarozXYZ/vernier/adapters/market/uniswapv3"
	"github.com/VarozXYZ/vernier/core/marketstate"
	"github.com/VarozXYZ/vernier/domain/market"
	feedport "github.com/VarozXYZ/vernier/ports/feed"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
	"github.com/VarozXYZ/vernier/runtime/configuration"
	"github.com/VarozXYZ/vernier/runtime/crosschain"
)

type routeChildRuntime struct {
	market    market.Market
	sourceID  market.SourceID
	mirror    *marketstate.Mirror
	source    quoteport.Source
	feed      feedport.Feed
	bootstrap func(context.Context) (market.EventData, market.SourcePosition, market.SourceReference, error)
}

type routeRuntime struct {
	config   configuration.ResolvedMarket
	route    *crosschain.Route
	children []routeChildRuntime
}

func (r *Runner) requiresRouteRuntime() bool {
	for _, configured := range r.config.Markets {
		if len(configured.Path) != 1 || r.config.Chains[configured.Venue.Chain].Kind == "solana" {
			return true
		}
	}
	return false
}

func (r *Runner) runRoutes(ctx context.Context) (Report, error) {
	startedAt := r.clock().UTC()
	r.logger.Info("route research started", "run", r.config.RunID, "markets", len(r.config.Markets))
	blocks, err := r.currentBlocks(ctx)
	if err != nil {
		return Report{}, err
	}
	slots, err := r.currentSlots(ctx)
	if err != nil {
		return Report{}, err
	}
	registry, setup, err := r.registry()
	if err != nil {
		return Report{}, err
	}
	maximum, err := market.NewAssetQuantity(r.sizingAsset(), r.config.MaximumSize)
	if err != nil {
		return Report{}, err
	}
	sources := make(map[market.MarketID]quoteport.Source, len(r.config.Markets))
	referenceSources := make(map[market.MarketID]quoteport.Source, len(r.config.Markets))
	snapshots := make([]market.MarketSnapshot, 0, len(r.config.Markets))
	for _, configured := range r.config.Markets {
		route, err := r.buildRoute(ctx, configured, registry, maximum, blocks, slots, startedAt)
		if err != nil {
			return Report{}, fmt.Errorf("bootstrap route %s: %w", configured.ID, err)
		}
		snapshot, ok := route.route.Snapshot()
		if !ok {
			return Report{}, fmt.Errorf("route %s did not publish a snapshot", configured.ID)
		}
		r.logger.Info("route bootstrap complete", "market", configured.ID, "hops", len(route.children), "version", snapshot.Metadata().Version)
		source := quoteport.Source(route.route.Source)
		if configured.ReferenceQuote != "" {
			reference, err := r.externalSource(configured, source)
			if err != nil {
				return Report{}, err
			}
			if reference != nil {
				source = reference
				referenceSources[configured.ID] = reference
			}
		}
		sources[configured.ID] = source
		snapshots = append(snapshots, snapshot)
	}
	costEvidence, cost, err := r.cost(ctx, blocks, startedAt)
	if err != nil {
		return Report{}, err
	}
	strategy, err := r.newStrategy(registry, setup, sources)
	if err != nil {
		return Report{}, err
	}
	research, err := r.evaluate(ctx, strategy, snapshots, cost, "route-evaluation/"+r.config.ResearchID, startedAt, nil)
	if err != nil {
		return Report{}, err
	}
	r.logger.Info("route local research complete", "opportunities", len(research.Opportunities))
	availableReferences := r.referenceSourcesFor(sources)
	for id, source := range referenceSources {
		availableReferences[id] = source
	}
	return Report{Research: research, Cost: costEvidence, Reference: validateReferences(ctx, research.Opportunities, snapshots, availableReferences, research, r.clock)}, nil
}

func (r *Runner) currentSlots(ctx context.Context) (map[string]uint64, error) {
	slots := make(map[string]uint64)
	for id, network := range r.solanaNetworks {
		slot, err := network.CurrentSlot(ctx)
		if err != nil {
			return nil, err
		}
		slots[id] = slot
	}
	return slots, nil
}

func (r *Runner) buildRoute(ctx context.Context, configured configuration.ResolvedMarket, registry *market.Registry, maximum market.AssetQuantity, blocks map[string]evm.BlockReference, slots map[string]uint64, now time.Time) (routeRuntime, error) {
	candidate, ok := registry.Market(configured.ID)
	if !ok {
		return routeRuntime{}, fmt.Errorf("registry is missing route market")
	}
	children := make([]routeChildRuntime, 0, len(configured.Path))
	routeChildren := make([]crosschain.Child, 0, len(configured.Path))
	for index, hop := range configured.Path {
		childID := market.MarketID(fmt.Sprintf("%s/hop/%d", configured.ID, index))
		childMarket := market.Market{ID: childID, Pair: candidate.Pair, Chain: market.ChainID(hop.Venue.Chain), Path: market.PathID(fmt.Sprintf("%s/path/%d", configured.ID, index)), BaseToken: hop.In.Token.ID, QuoteToken: hop.Out.Token.ID}
		sourceID := market.SourceID(fmt.Sprintf("%s/pool/%d", hop.Venue.Chain, index))
		child, err := r.buildChild(ctx, configured, hop, childMarket, sourceID, maximum, blocks, slots, now)
		if err != nil {
			return routeRuntime{}, err
		}
		children = append(children, child)
		routeChildren = append(routeChildren, crosschain.Child{Market: child.market, Mirror: child.mirror, Source: child.source})
	}
	route, err := crosschain.NewRoute(candidate, market.SourceID(string(configured.Venue.Chain)+"/route"), routeChildren, func() time.Time { return r.clock().UTC() })
	if err != nil {
		return routeRuntime{}, err
	}
	for index := range children {
		data, position, reference, err := children[index].bootstrap(ctx)
		if err != nil {
			return routeRuntime{}, fmt.Errorf("bootstrap hop %d: %w", index, err)
		}
		event, err := market.NewMarketEvent(market.MarketEvent{Market: children[index].market.ID, Source: children[index].sourceID, Position: position, Reference: reference, Finality: market.FinalityPreconfirmed, ReceivedAt: now, Data: data})
		if err != nil {
			return routeRuntime{}, err
		}
		if _, err := route.Apply(ctx, event); err != nil {
			return routeRuntime{}, err
		}
	}
	return routeRuntime{config: configured, route: route, children: children}, nil
}

func (r *Runner) buildChild(ctx context.Context, configured configuration.ResolvedMarket, hop configuration.ResolvedHop, childMarket market.Market, sourceID market.SourceID, maximum market.AssetQuantity, blocks map[string]evm.BlockReference, slots map[string]uint64, now time.Time) (routeChildRuntime, error) {
	if r.config.Chains[hop.Venue.Chain].Kind == "solana" {
		return r.buildSolanaChild(hop, childMarket, sourceID, slots[hop.Venue.Chain], now)
	}
	venue, reducer, source, err := r.composeEVMHop(hop, childMarket, maximum)
	if err != nil {
		return routeChildRuntime{}, err
	}
	mirror, err := marketstate.NewMirror(childMarket.ID, sourceID, reducer, sourceorder.NewMonotonic(evmlogs.BlockPositionKind, false), r.clock)
	if err != nil {
		return routeChildRuntime{}, err
	}
	network := r.networks[hop.Venue.Chain]
	block := blocks[hop.Venue.Chain]
	feed, err := evmlogs.New(evmlogs.Config{Market: childMarket.ID, Source: sourceID, Network: network, Venue: venue, Clock: r.clock, Logger: r.logger})
	if err != nil {
		return routeChildRuntime{}, err
	}
	return routeChildRuntime{market: childMarket, sourceID: sourceID, mirror: mirror, source: source, feed: feed, bootstrap: func(ctx context.Context) (market.EventData, market.SourcePosition, market.SourceReference, error) {
		data, err := venue.Bootstrap(ctx, network, block)
		return data, market.SourcePosition{Kind: evmlogs.BlockPositionKind, Value: block.Number}, market.SourceReference{Kind: evmlogs.BlockHashReferenceKind, Value: block.Hash.Hex()}, err
	}}, nil
}

func (r *Runner) buildSolanaChild(hop configuration.ResolvedHop, childMarket market.Market, sourceID market.SourceID, slot uint64, now time.Time) (routeChildRuntime, error) {
	network := r.solanaNetworks[hop.Venue.Chain]
	var decoder solanalogs.Decoder
	var reducer marketstate.Reducer
	var source quoteport.Source
	var err error
	switch hop.Venue.Kind {
	case "meteora_dlmm":
		decoder, err = dlmm.NewDecoder(hop.Venue.PoolText)
		if err == nil {
			source, err = dlmm.NewQuoter(sourceID+"/local", childMarket, hop.In.Token.ID, hop.Out.Token.ID)
		}
		reducer = dlmm.Reducer{}
	case "orca_whirlpool":
		decoder, err = orcawhirlpool.NewDecoder(hop.Venue.PoolText)
		if err == nil {
			source, err = orcawhirlpool.NewQuoter(sourceID+"/local", childMarket, hop.In.Token.ID, hop.Out.Token.ID)
		}
		reducer = orcawhirlpool.Reducer{}
	default:
		return routeChildRuntime{}, fmt.Errorf("unsupported Solana venue kind %q", hop.Venue.Kind)
	}
	if err != nil {
		return routeChildRuntime{}, err
	}
	mirror, err := marketstate.NewMirror(childMarket.ID, sourceID, reducer, sourceorder.NewMonotonic(solanalogs.SlotPositionKind, false), r.clock)
	if err != nil {
		return routeChildRuntime{}, err
	}
	feed, err := solanalogs.New(solanalogs.Config{Market: childMarket.ID, Source: sourceID, Pool: hop.Venue.PoolText, Network: network, Decoder: decoder, Clock: r.clock, Logger: r.logger})
	if err != nil {
		return routeChildRuntime{}, err
	}
	return routeChildRuntime{market: childMarket, sourceID: sourceID, mirror: mirror, source: source, feed: feed, bootstrap: func(ctx context.Context) (market.EventData, market.SourcePosition, market.SourceReference, error) {
		data, err := decoder.Bootstrap(ctx, network, slot)
		return data, market.SourcePosition{Kind: solanalogs.SlotPositionKind, Value: slot}, market.SourceReference{Kind: solanalogs.SignatureReferenceKind, Value: "bootstrap"}, err
	}}, nil
}

func (r *Runner) composeEVMHop(hop configuration.ResolvedHop, candidate market.Market, maximum market.AssetQuantity) (evmlogs.Venue, marketstate.Reducer, quoteport.Source, error) {
	configured := configuration.ResolvedMarket{ID: candidate.ID, Venue: hop.Venue, Base: hop.In, Quote: hop.Out, Path: []configuration.ResolvedHop{hop}}
	switch hop.Venue.Kind {
	case "uniswap_v2":
		adapter, err := uniswapv2.NewAdapter(uniswapv2.Config{Pool: hop.Venue.Pool, Factory: hop.Venue.Factory, BaseToken: hop.In.Address, QuoteToken: hop.Out.Address, FeeBPS: hop.Venue.FeeBPS})
		if err != nil {
			return nil, nil, nil, err
		}
		local, err := constantproduct.NewQuoter(market.SourceID(string(hop.Venue.ID)+"/local"), candidate)
		return adapter, constantproduct.Reducer{}, local, err
	case "aerodrome_volatile":
		adapter, err := aerodrome.NewAdapter(aerodrome.Config{Pool: hop.Venue.Pool, Factory: hop.Venue.Factory, BaseToken: hop.In.Address, QuoteToken: hop.Out.Address, FeeBPS: hop.Venue.FeeBPS})
		if err != nil {
			return nil, nil, nil, err
		}
		local, err := constantproduct.NewQuoter(market.SourceID(string(hop.Venue.ID)+"/local"), candidate)
		return adapter, constantproduct.Reducer{}, local, err
	case "uniswap_v3":
		maxBase, initialQuote, zeroForOne, err := v3Inputs(configured, hopMaximum(maximum, hop))
		if err != nil {
			return nil, nil, nil, err
		}
		adapter, err := uniswapv3.NewAdapter(uniswapv3.OnChainConfig{Pool: hop.Venue.Pool, MaxTickWords: hop.Venue.MaxTickWords, Probes: []uniswapv3.CoverageProbe{{ZeroForOne: zeroForOne, AmountIn: maxBase}, {ZeroForOne: !zeroForOne, AmountIn: initialQuote}}})
		if err != nil {
			return nil, nil, nil, err
		}
		token0, token1 := hop.In.Token.ID, hop.Out.Token.ID
		if !zeroForOne {
			token0, token1 = token1, token0
		}
		local, err := uniswapv3.NewQuoter(market.SourceID(string(hop.Venue.ID)+"/local"), candidate, token0, token1)
		return adapter, uniswapv3.Reducer{}, local, err
	case "aerodrome_slipstream":
		maxBase, initialQuote, zeroForOne, err := v3Inputs(configured, hopMaximum(maximum, hop))
		if err != nil {
			return nil, nil, nil, err
		}
		adapter, err := aerodromeslipstream.NewAdapter(aerodromeslipstream.Config{Pool: hop.Venue.Pool, Factory: hop.Venue.Factory, BaseToken: hop.In.Address, QuoteToken: hop.Out.Address, MaxTickWords: hop.Venue.MaxTickWords, Probes: []uniswapv3.CoverageProbe{{ZeroForOne: zeroForOne, AmountIn: maxBase}, {ZeroForOne: !zeroForOne, AmountIn: initialQuote}}})
		if err != nil {
			return nil, nil, nil, err
		}
		token0, token1 := hop.In.Token.ID, hop.Out.Token.ID
		if !zeroForOne {
			token0, token1 = token1, token0
		}
		local, err := uniswapv3.NewQuoter(market.SourceID(string(hop.Venue.ID)+"/local"), candidate, token0, token1)
		return adapter, uniswapv3.Reducer{}, local, err
	default:
		return nil, nil, nil, fmt.Errorf("unsupported EVM venue kind %q", hop.Venue.Kind)
	}
}

func hopMaximum(maximum market.AssetQuantity, hop configuration.ResolvedHop) market.AssetQuantity {
	if maximum.Asset() == hop.In.Token.Asset || maximum.Asset() == hop.Out.Token.Asset {
		return maximum
	}
	// A route's sizing asset belongs to its endpoints, not necessarily to each
	// intermediate hop. One whole intermediate token is a conservative coverage
	// probe; sizing remains owned by the route strategy.
	value, _ := market.NewAssetQuantity(hop.Out.Token.Asset, big.NewRat(1, 1))
	return value
}
