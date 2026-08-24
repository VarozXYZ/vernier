package strategy

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

// PrefundedParallel applies LiveEvaluator's independent-inventory economics
// to Research. Each generation fixes one quote notional and one separately
// valued base amount before any directional quote starts.
type PrefundedParallel struct {
	id                     arbitrage.StrategyID
	setup                  arbitrage.ArbitrageSetup
	registry               *market.Registry
	sources                map[market.MarketID]quoteport.Source
	notionals              []market.AssetQuantity
	legacyThreshold        market.AssetQuantity
	directionalThresholds  map[arbitrage.Direction]market.AssetQuantity
	thresholdFixed         market.AssetQuantity
	thresholdBPS           uint16
	clock                  Clock
	valuationMarket        market.MarketID
	valuations             *BaseValuationCache
	live                   *LiveEvaluator
	warmMu                 sync.Mutex
	requireQuoteConversion bool
	onEvaluationError      func(arbitrage.Direction, error)
	candidateEligible      func(arbitrage.Direction, arbitrage.Candidate) bool
	candidateCost          func(arbitrage.Direction, market.AssetQuantity, time.Time) (arbitrage.CostSnapshot, bool)
}

type PrefundedParallelConfig struct {
	ID                     arbitrage.StrategyID
	Setup                  arbitrage.ArbitrageSetup
	Registry               *market.Registry
	Sources                map[market.MarketID]quoteport.Source
	Notional               market.AssetQuantity
	Notionals              []market.AssetQuantity
	Threshold              market.AssetQuantity
	DirectionalThresholds  map[arbitrage.Direction]market.AssetQuantity
	ThresholdFixed         market.AssetQuantity
	ThresholdBPS           uint16
	ValuationMarket        market.MarketID
	Clock                  Clock
	RequireQuoteConversion bool
	// OnEvaluationError observes a failed local/provider evaluation without
	// changing its public economic classification. It must remain non-blocking.
	OnEvaluationError func(arbitrage.Direction, error)
	// CandidateEligible is an optional, memory-only capacity predicate. It is
	// evaluated after local quoting and before economic selection.
	CandidateEligible func(arbitrage.Direction, arbitrage.Candidate) bool
	CandidateCost     func(arbitrage.Direction, market.AssetQuantity, time.Time) (arbitrage.CostSnapshot, bool)
}

func NewPrefundedParallel(config PrefundedParallelConfig) (*PrefundedParallel, error) {
	if config.ID == "" || config.Registry == nil || config.Clock == nil || len(config.Setup.Markets()) != 2 || config.ValuationMarket == "" {
		return nil, fmt.Errorf("prefunded parallel strategy requires id, two markets, valuation market, registry, and clock")
	}
	pair, ok := config.Registry.Pair(config.Setup.Pair())
	notionals := append([]market.AssetQuantity(nil), config.Notionals...)
	if len(notionals) == 0 && config.Notional.Sign() > 0 {
		notionals = []market.AssetQuantity{config.Notional}
	}
	if !ok || len(notionals) == 0 || config.Threshold.Asset() != pair.QuoteAsset || config.Threshold.Sign() < 0 {
		return nil, fmt.Errorf("prefunded parallel notional and thresholds must use the quote asset")
	}
	for index, notional := range notionals {
		if notional.Asset() != pair.QuoteAsset || notional.Sign() <= 0 ||
			(index > 0 && !quantityGreater(notional, notionals[index-1])) {
			return nil, fmt.Errorf("prefunded parallel notionals must be strictly increasing quote quantities")
		}
	}
	if config.ThresholdBPS > 10_000 || (config.ThresholdBPS > 0 && (config.ThresholdFixed.Asset() != pair.QuoteAsset || config.ThresholdFixed.Sign() <= 0)) {
		return nil, fmt.Errorf("prefunded parallel percentage threshold is invalid")
	}
	for direction, threshold := range config.DirectionalThresholds {
		if direction.BuyMarket == direction.SellMarket || threshold.Asset() != pair.QuoteAsset || threshold.Sign() < 0 {
			return nil, fmt.Errorf("prefunded parallel directional threshold is invalid")
		}
	}
	cache, err := NewBaseValuationCache(pair.BaseAsset, pair.QuoteAsset)
	if err != nil {
		return nil, err
	}
	live, err := NewLiveEvaluator(LiveConfig{Setup: config.Setup, Registry: config.Registry, Sources: config.Sources, Clock: config.Clock})
	if err != nil {
		return nil, err
	}
	if _, ok := config.Sources[config.ValuationMarket]; !ok {
		return nil, fmt.Errorf("valuation market has no quote source")
	}
	return &PrefundedParallel{id: config.ID, setup: config.Setup, registry: config.Registry, sources: config.Sources,
		notionals: notionals, legacyThreshold: config.Threshold, directionalThresholds: config.DirectionalThresholds, thresholdFixed: config.ThresholdFixed,
		thresholdBPS: config.ThresholdBPS, valuationMarket: config.ValuationMarket, valuations: cache,
		live: live, clock: config.Clock, requireQuoteConversion: config.RequireQuoteConversion,
		onEvaluationError: config.OnEvaluationError, candidateEligible: config.CandidateEligible,
		candidateCost: config.CandidateCost}, nil
}

func (s *PrefundedParallel) ID() arbitrage.StrategyID { return s.id }

// RevalueCandidate combines a validated remote quote with the latest local
// quote while proving that neither prefunded input changed.
func (s *PrefundedParallel) RevalueCandidate(direction arbitrage.Direction, candidate arbitrage.Candidate, buy, sell market.Quote, snapshots map[market.MarketID]market.MarketSnapshot, at time.Time) (arbitrage.Candidate, error) {
	if candidate.Valuation == nil {
		return arbitrage.Candidate{}, fmt.Errorf("prefunded candidate valuation is missing")
	}
	hugeCost, _ := market.NewAssetQuantity(candidate.Cost.Amount.Asset(), new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(30), nil)))
	hugeBase, _ := market.NewAssetQuantity(candidate.Valuation.Base(), hugeCost.Rat())
	valued, err := s.live.Value(LiveEvaluationRequest{ID: "research-validation", Direction: direction, Snapshots: snapshots,
		Notional: candidate.Input, Valuation: *candidate.Valuation, Cost: candidate.Cost.Amount, Threshold: candidate.EffectiveThreshold,
		MaximumCost: hugeCost, MaximumBaseExposure: hugeBase, TriggeredAt: at,
		QuoteConversion: candidate.QuoteConversion}, buy, sell, at)
	if err != nil {
		return arbitrage.Candidate{}, err
	}
	output, err := candidate.Input.Add(valued.GrossPnL)
	if err != nil {
		return arbitrage.Candidate{}, err
	}
	result := candidate
	result.BuyQuote, result.SellQuote, result.Output = buy, sell, output
	result.GrossPnL, result.NetPnL = valued.GrossPnL, valued.NetPnL
	return result, nil
}

func (s *PrefundedParallel) EvaluateWithTiming(ctx context.Context, evaluation arbitrage.Evaluation) ([]arbitrage.Opportunity, EvaluationTiming, error) {
	if evaluation.Strategy() != s.id {
		return nil, EvaluationTiming{}, fmt.Errorf("evaluation targets strategy %q, expected %q", evaluation.Strategy(), s.id)
	}
	started := s.clock()
	valuation, err := s.valuation(ctx, evaluation)
	if err != nil {
		return s.unclassifiable(evaluation, "valuation_unavailable"), EvaluationTiming{Duration: nonNegative(s.clock().Sub(started))}, nil
	}
	directions := s.setup.Directions()
	results := make([]arbitrage.Opportunity, len(directions))
	timings := make([]DirectionTiming, len(directions))
	var wg sync.WaitGroup
	for i, direction := range directions {
		i, direction := i, direction
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], timings[i] = s.evaluateDirectionSizes(ctx, evaluation, direction, valuation)
		}()
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, EvaluationTiming{}, err
	}
	s.observe(results)
	best := -1
	for i := range results {
		if results[i].SelectedIndex < 0 {
			continue
		}
		candidate := results[i].Candidates[results[i].SelectedIndex]
		bestCandidate := arbitrage.Candidate{}
		if best >= 0 {
			bestCandidate = results[best].Candidates[results[best].SelectedIndex]
		}
		if best < 0 || quantityGreater(candidate.NetPnL, bestCandidate.NetPnL) {
			best = i
		}
	}
	for i := range results {
		if i != best && results[i].SelectedIndex >= 0 {
			results[i].SelectedIndex = -1
			results[i].Classification = arbitrage.ClassificationNoSpread
			results[i].Reasons = []string{"lower_prefunded_net_pnl"}
		}
	}
	return results, EvaluationTiming{Duration: nonNegative(s.clock().Sub(started)), Directions: timings}, nil
}

func (s *PrefundedParallel) EvaluatePinnedWithTiming(ctx context.Context, evaluation arbitrage.Evaluation, direction arbitrage.Direction, size market.AssetQuantity) (arbitrage.Opportunity, PinnedEvaluationTiming, error) {
	trace := PinnedEvaluationTiming{EvaluationStartedAt: s.clock().UTC()}
	if size.Asset() != s.notionals[0].Asset() || size.Sign() <= 0 {
		return arbitrage.Opportunity{}, trace, fmt.Errorf("prefunded pinned size must use the configured quote asset")
	}
	valuation, err := s.valuation(ctx, evaluation)
	if err != nil {
		opportunities := s.unclassifiable(evaluation, "valuation_unavailable")
		return opportunities[0], trace, nil
	}
	opportunity, timing := s.evaluateDirection(ctx, evaluation, direction, size, valuation)
	trace.Direction = timing
	if len(timing.Quotes) > 0 {
		trace.BuyStartedAt = evaluation.StartedAt()
		trace.BuyFinishedAt = s.clock().UTC()
		trace.SellStartedAt = trace.BuyStartedAt
		trace.SellFinishedAt = trace.BuyFinishedAt
	}
	trace.PnLStartedAt, trace.PnLFinishedAt, trace.EvaluationFinishedAt = trace.BuyFinishedAt, s.clock().UTC(), s.clock().UTC()
	s.observe([]arbitrage.Opportunity{opportunity})
	return opportunity, trace, nil
}

func (s *PrefundedParallel) evaluateDirectionSizes(ctx context.Context, evaluation arbitrage.Evaluation,
	direction arbitrage.Direction, valuation arbitrage.ValuationSnapshot) (arbitrage.Opportunity, DirectionTiming) {
	started := s.clock()
	var combined arbitrage.Opportunity
	combined.SelectedIndex = -1
	var timing DirectionTiming
	timing.Direction = direction
	capacityRejected := false
	bestQualified := -1
	type sizedResult struct {
		opportunity arbitrage.Opportunity
		timing      DirectionTiming
	}
	results := make([]sizedResult, len(s.notionals))
	var group sync.WaitGroup
	for index, notional := range s.notionals {
		index, notional := index, notional
		group.Add(1)
		go func() {
			defer group.Done()
			results[index].opportunity, results[index].timing = s.evaluateDirection(ctx, evaluation, direction, notional, valuation)
		}()
	}
	group.Wait()
	for _, result := range results {
		if err := ctx.Err(); err != nil {
			break
		}
		opportunity, candidateTiming := result.opportunity, result.timing
		if combined.Evaluation == "" {
			combined = opportunity
			combined.Candidates = nil
			combined.SelectedIndex = -1
		}
		timing.Quotes = append(timing.Quotes, candidateTiming.Quotes...)
		if len(opportunity.Candidates) == 0 {
			if len(combined.Reasons) == 0 {
				combined.Reasons = opportunity.Reasons
			}
			continue
		}
		candidate := opportunity.Candidates[0]
		index := len(combined.Candidates)
		combined.Candidates = append(combined.Candidates, candidate)
		if s.candidateEligible != nil && !s.candidateEligible(direction, candidate) {
			capacityRejected = true
			continue
		}
		if combined.SelectedIndex < 0 || quantityGreater(candidate.NetPnL, combined.Candidates[combined.SelectedIndex].NetPnL) {
			combined.SelectedIndex = index
		}
		if greaterOrEqual(candidate.NetPnL, candidate.EffectiveThreshold) && candidate.NetPnL.Sign() > 0 &&
			(bestQualified < 0 || quantityGreater(candidate.NetPnL, combined.Candidates[bestQualified].NetPnL)) {
			bestQualified = index
		}
	}
	timing.Duration = nonNegative(s.clock().Sub(started))
	combined.FinishedAt = s.clock().UTC()
	if combined.SelectedIndex < 0 {
		combined.Classification = arbitrage.ClassificationUnclassifiable
		if capacityRejected {
			combined.Reasons = []string{"insufficient_local_balance"}
		}
		return combined, timing
	}
	if bestQualified >= 0 {
		combined.SelectedIndex = bestQualified
	}
	selected := combined.Candidates[combined.SelectedIndex]
	combined.Threshold = selected.EffectiveThreshold
	switch {
	case selected.GrossPnL.Sign() <= 0:
		combined.Classification, combined.Reasons = arbitrage.ClassificationNoSpread, []string{"no_positive_gross_profit"}
	case selected.NetPnL.Sign() <= 0:
		combined.Classification, combined.Reasons = arbitrage.ClassificationObservedSpread, []string{"costs_exceed_gross_profit"}
	case !greaterOrEqual(selected.NetPnL, selected.EffectiveThreshold):
		combined.Classification, combined.Reasons = arbitrage.ClassificationEconomic, []string{"below_profit_threshold"}
	default:
		combined.Classification, combined.Reasons = arbitrage.ClassificationPolicyQualified, []string{"profit_threshold_met"}
	}
	return combined, timing
}

func (s *PrefundedParallel) evaluateDirection(ctx context.Context, evaluation arbitrage.Evaluation, direction arbitrage.Direction, notional market.AssetQuantity, valuation arbitrage.ValuationSnapshot) (arbitrage.Opportunity, DirectionTiming) {
	started := s.clock()
	opportunity := arbitrage.Opportunity{Evaluation: evaluation.ID(), Run: evaluation.Run(), ConfigHash: evaluation.ConfigHash(), Strategy: s.id,
		Direction: direction, Classification: arbitrage.ClassificationUnclassifiable, SelectedIndex: -1, TriggeredAt: evaluation.TriggeredAt(), StartedAt: evaluation.StartedAt()}
	opportunity.Trigger, opportunity.HasTrigger = evaluation.Trigger()
	snapshots := map[market.MarketID]market.MarketSnapshot{}
	for _, id := range s.setup.Markets() {
		if snapshot, ok := evaluation.Snapshot(id); ok {
			snapshots[id] = snapshot
			opportunity.Snapshots = append(opportunity.Snapshots, snapshot.Metadata())
		}
	}
	costSnapshot := evaluation.CostFor(direction)
	if s.candidateCost != nil {
		var ok bool
		costSnapshot, ok = s.candidateCost(direction, notional, evaluation.StartedAt())
		if !ok {
			opportunity.Reasons = []string{"cost_cache_stale"}
			opportunity.FinishedAt = s.clock().UTC()
			return opportunity, DirectionTiming{Direction: direction, Duration: nonNegative(s.clock().Sub(started))}
		}
	}
	cost := costSnapshot.Amount
	fixed, percentage, effective, err := s.threshold(direction, notional)
	if err != nil {
		opportunity.Reasons = []string{"threshold_invalid"}
		opportunity.FinishedAt = s.clock().UTC()
		return opportunity, DirectionTiming{Direction: direction, Duration: nonNegative(s.clock().Sub(started))}
	}
	hugeCost, _ := market.NewAssetQuantity(cost.Asset(), new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(30), nil)))
	hugeBase, _ := market.NewAssetQuantity(valuation.Base(), hugeCost.Rat())
	conversion, conversionErr := s.conversion(evaluation, direction)
	if conversionErr != nil {
		opportunity.Reasons = []string{"quote_conversion_unavailable"}
		opportunity.FinishedAt = s.clock().UTC()
		return opportunity, DirectionTiming{Direction: direction, Duration: nonNegative(s.clock().Sub(started))}
	}
	live, err := s.live.Evaluate(ctx, LiveEvaluationRequest{ID: string(evaluation.ID()) + "/" + string(direction.BuyMarket), Direction: direction, Snapshots: snapshots,
		Notional: notional, Valuation: valuation, Cost: cost, Threshold: effective, MaximumCost: hugeCost, MaximumBaseExposure: hugeBase, TriggeredAt: evaluation.TriggeredAt(),
		QuoteConversion: conversion})
	if err != nil {
		if s.onEvaluationError != nil {
			s.onEvaluationError(direction, fmt.Errorf("input %s: %w", notional, err))
		}
		opportunity.Reasons = []string{prefundedQuoteFailure(err)}
		opportunity.FinishedAt = s.clock().UTC()
		return opportunity, DirectionTiming{Direction: direction, Duration: nonNegative(s.clock().Sub(started))}
	}
	output, _ := notional.Add(live.GrossPnL)
	valuationCopy := valuation
	candidate := arbitrage.Candidate{Size: notional, Input: notional, Output: output, GrossPnL: live.GrossPnL, Cost: costSnapshot, NetPnL: live.NetPnL,
		FixedThreshold: fixed, PercentageThreshold: percentage, EffectiveThreshold: effective, BuyQuote: live.BuyQuote, SellQuote: live.SellQuote, Valuation: &valuationCopy}
	if conversion != nil {
		copy := *conversion
		candidate.QuoteConversion = &copy
	}
	opportunity.Candidates = []arbitrage.Candidate{candidate}
	opportunity.SelectedIndex = 0
	opportunity.Threshold = effective
	switch {
	case live.GrossPnL.Sign() <= 0:
		opportunity.Classification, opportunity.Reasons = arbitrage.ClassificationNoSpread, []string{"no_positive_gross_profit"}
	case live.NetPnL.Sign() <= 0:
		opportunity.Classification, opportunity.Reasons = arbitrage.ClassificationObservedSpread, []string{"costs_exceed_gross_profit"}
	case !greaterOrEqual(live.NetPnL, effective):
		opportunity.Classification, opportunity.Reasons = arbitrage.ClassificationEconomic, []string{"below_profit_threshold"}
	default:
		opportunity.Classification, opportunity.Reasons = arbitrage.ClassificationPolicyQualified, []string{"profit_threshold_met"}
	}
	opportunity.FinishedAt = s.clock().UTC()
	return opportunity, DirectionTiming{Direction: direction, Duration: nonNegative(s.clock().Sub(started))}
}

func (s *PrefundedParallel) conversion(evaluation arbitrage.Evaluation, direction arbitrage.Direction) (*market.QuoteConversionSnapshot, error) {
	buy, buyOK := s.registry.Market(direction.BuyMarket)
	sell, sellOK := s.registry.Market(direction.SellMarket)
	if !buyOK || !sellOK {
		return nil, fmt.Errorf("quote conversion markets are unavailable")
	}
	if buy.QuoteToken == sell.QuoteToken || !s.requireQuoteConversion {
		return nil, nil
	}
	snapshot, ok := evaluation.QuoteConversion(sell.QuoteToken, buy.QuoteToken)
	if !ok || !snapshot.ValidAt(evaluation.StartedAt()) {
		return nil, fmt.Errorf("quote conversion %s -> %s is unavailable", sell.QuoteToken, buy.QuoteToken)
	}
	return &snapshot, nil
}

func prefundedQuoteFailure(err error) string {
	var rateLimited interface{ RateLimited() bool }
	if errors.As(err, &rateLimited) && rateLimited.RateLimited() {
		return "remote_rate_limited"
	}
	return "provider_or_quote_unavailable"
}

func (s *PrefundedParallel) valuation(ctx context.Context, evaluation arbitrage.Evaluation) (arbitrage.ValuationSnapshot, error) {
	if snapshot, err := s.valuations.Snapshot(s.clock().UTC()); err == nil {
		return snapshot, nil
	}
	s.warmMu.Lock()
	defer s.warmMu.Unlock()
	if snapshot, err := s.valuations.Snapshot(s.clock().UTC()); err == nil {
		return snapshot, nil
	}
	configured, ok := s.registry.Market(s.valuationMarket)
	if !ok {
		return arbitrage.ValuationSnapshot{}, fmt.Errorf("valuation market missing")
	}
	base, _ := s.registry.Token(configured.BaseToken)
	quoteToken, _ := s.registry.Token(configured.QuoteToken)
	marketSnapshot, ok := evaluation.Snapshot(s.valuationMarket)
	if !ok || marketSnapshot.Metadata().Health != market.HealthHealthy {
		return arbitrage.ValuationSnapshot{}, fmt.Errorf("valuation snapshot unavailable")
	}
	input, err := s.notionals[len(s.notionals)-1].ToTokenAmount(quoteToken)
	if err != nil {
		return arbitrage.ValuationSnapshot{}, err
	}
	quoted, err := s.sources[s.valuationMarket].Quote(ctx, quoteport.Input{Snapshot: marketSnapshot, TokenIn: quoteToken.ID, TokenOut: base.ID, AmountIn: input, Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: s.clock().UTC()})
	if err != nil {
		return arbitrage.ValuationSnapshot{}, err
	}
	if err := s.valuations.Observe(string(s.valuationMarket), quoted, quoteToken, base, s.clock().UTC()); err != nil {
		return arbitrage.ValuationSnapshot{}, err
	}
	return s.valuations.Snapshot(s.clock().UTC())
}

func (s *PrefundedParallel) observe(opportunities []arbitrage.Opportunity) {
	for _, opportunity := range opportunities {
		if len(opportunity.Candidates) == 0 {
			continue
		}
		index := opportunity.SelectedIndex
		if index < 0 || index >= len(opportunity.Candidates) {
			continue
		}
		candidate := opportunity.Candidates[index]
		configured, ok := s.registry.Market(opportunity.Direction.BuyMarket)
		if !ok {
			continue
		}
		base, baseOK := s.registry.Token(configured.BaseToken)
		quoteToken, quoteOK := s.registry.Token(configured.QuoteToken)
		if !baseOK || !quoteOK {
			continue
		}
		_ = s.valuations.Observe(string(opportunity.Direction.BuyMarket), candidate.BuyQuote, quoteToken, base, s.clock().UTC())
	}
}

func (s *PrefundedParallel) threshold(direction arbitrage.Direction, input market.AssetQuantity) (market.AssetQuantity, market.AssetQuantity, market.AssetQuantity, error) {
	zero, _ := market.NewAssetQuantity(input.Asset(), new(big.Rat))
	if threshold, ok := s.directionalThresholds[direction]; ok {
		return threshold, zero, threshold, nil
	}
	if s.thresholdBPS == 0 {
		return s.legacyThreshold, zero, s.legacyThreshold, nil
	}
	percentage, err := market.NewAssetQuantity(input.Asset(), new(big.Rat).Quo(new(big.Rat).Mul(input.Rat(), new(big.Rat).SetInt64(int64(s.thresholdBPS))), new(big.Rat).SetInt64(10_000)))
	if err != nil {
		return market.AssetQuantity{}, market.AssetQuantity{}, market.AssetQuantity{}, err
	}
	effective := s.thresholdFixed
	if quantityGreater(percentage, effective) {
		effective = percentage
	}
	return s.thresholdFixed, percentage, effective, nil
}

func (s *PrefundedParallel) unclassifiable(evaluation arbitrage.Evaluation, reason string) []arbitrage.Opportunity {
	result := make([]arbitrage.Opportunity, 0, 2)
	for _, direction := range s.setup.Directions() {
		result = append(result, arbitrage.Opportunity{Evaluation: evaluation.ID(), Run: evaluation.Run(), ConfigHash: evaluation.ConfigHash(), Strategy: s.id, Direction: direction, Classification: arbitrage.ClassificationUnclassifiable, SelectedIndex: -1, Reasons: []string{reason}, TriggeredAt: evaluation.TriggeredAt(), StartedAt: evaluation.StartedAt(), FinishedAt: s.clock().UTC()})
	}
	return result
}

func quantityGreater(left, right market.AssetQuantity) bool {
	comparison, err := left.Cmp(right)
	return err == nil && comparison > 0
}

var _ interface {
	EvaluateWithTiming(context.Context, arbitrage.Evaluation) ([]arbitrage.Opportunity, EvaluationTiming, error)
} = (*PrefundedParallel)(nil)
