// Package live wires versioned market snapshots to the Live execution core.
// Feed and mirror factories remain shared runtime capabilities; this package
// contains no Research reports or window tracking.
package live

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	corelive "github.com/VarozXYZ/vernier/core/live"
	"github.com/VarozXYZ/vernier/core/saga"
	"github.com/VarozXYZ/vernier/core/strategy"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

type Trigger struct {
	Market      market.MarketID
	At          time.Time
	Rebootstrap bool
}

type SnapshotProvider interface {
	CurrentSnapshots() (map[market.MarketID]market.MarketSnapshot, bool)
}

type RunnerConfig struct {
	Engine              *corelive.Engine
	Confirmer           *saga.Confirmer
	Setup               arbitrage.ArbitrageSetup
	Registry            *market.Registry
	Sources             map[market.MarketID]quoteport.Source
	Snapshots           SnapshotProvider
	Valuation           *strategy.BaseValuationCache
	Notional            market.AssetQuantity
	Cost                market.AssetQuantity
	Costs               executionport.CostSnapshotSource
	Threshold           market.AssetQuantity
	MaximumCost         market.AssetQuantity
	MaximumBaseExposure market.AssetQuantity
	Clock               func() time.Time
	ID                  func(prefix string) string
	RecoveryGate        func(context.Context) error
	OnResult            func(corelive.BatchResult)
	Observer            Observer
	DryRun              bool
}

type Runner struct {
	config RunnerConfig
}

func New(config RunnerConfig) (*Runner, error) {
	if config.Engine == nil || config.Setup.ID() == "" || config.Registry == nil ||
		len(config.Sources) < 2 || config.Snapshots == nil || config.Valuation == nil ||
		config.Notional.Asset() == "" ||
		(config.Costs == nil && config.Cost.Asset() == "") || config.Threshold.Asset() == "" ||
		config.MaximumCost.Asset() == "" || config.MaximumBaseExposure.Asset() == "" ||
		config.Clock == nil || config.ID == nil || !config.DryRun && config.RecoveryGate == nil {
		return nil, fmt.Errorf("live runner configuration is incomplete")
	}
	if !config.DryRun && config.Confirmer == nil {
		return nil, fmt.Errorf("armed live runner requires a confirmer")
	}
	if config.OnResult == nil {
		config.OnResult = func(corelive.BatchResult) {}
	}
	if config.Observer == nil {
		config.Observer = noopObserver{}
	}
	return &Runner{config: config}, nil
}

func (r *Runner) Run(ctx context.Context, triggers <-chan Trigger) error {
	if triggers == nil {
		return fmt.Errorf("live trigger channel is required")
	}
	if !r.config.DryRun {
		if err := r.config.RecoveryGate(ctx); err != nil {
			return err
		}
	}
	if err := r.config.Engine.Warm(ctx); err != nil {
		return err
	}
	if !r.config.DryRun {
		if err := r.config.Confirmer.Warm(ctx); err != nil {
			return err
		}
	}
	snapshots, ready := r.config.Snapshots.CurrentSnapshots()
	if !ready {
		return fmt.Errorf("live markets are not ready")
	}
	if err := r.initializeValuation(ctx, snapshots); err != nil {
		return err
	}
	circuitOpen := false
	for {
		select {
		case <-ctx.Done():
			return nil
		case trigger, ok := <-triggers:
			if !ok {
				return nil
			}
			trigger = latestTrigger(trigger, triggers)
			if circuitOpen {
				r.config.Observer.Observe(Event{
					Kind: EventTriggerSkipped, At: r.config.Clock().UTC(),
					Trigger: trigger.Market, Reason: "manual circuit breaker is open",
				})
				continue
			}
			snapshots, ready = r.config.Snapshots.CurrentSnapshots()
			if !ready {
				continue
			}
			if trigger.Rebootstrap {
				r.config.Valuation.Reset()
				if err := r.initializeValuation(ctx, snapshots); err != nil {
					return err
				}
			}
			valuation, err := r.config.Valuation.Snapshot(r.config.Clock().UTC())
			if err != nil {
				return err
			}
			currentCost := r.config.Cost
			if r.config.Costs != nil {
				currentCost, err = r.config.Costs.Current(ctx)
				if err != nil {
					r.config.Observer.Observe(Event{
						Kind: EventTriggerSkipped, At: r.config.Clock().UTC(),
						Trigger: trigger.Market, Reason: err.Error(),
					})
					continue
				}
			}
			requests := make([]strategy.LiveEvaluationRequest, 0, len(r.config.Setup.Directions()))
			for _, direction := range r.config.Setup.Directions() {
				requests = append(requests, strategy.LiveEvaluationRequest{
					ID: r.config.ID("evaluation"), Direction: direction,
					Snapshots: snapshots, Notional: r.config.Notional, Valuation: valuation,
					Cost: currentCost, Threshold: r.config.Threshold,
					MaximumCost:         r.config.MaximumCost,
					MaximumBaseExposure: r.config.MaximumBaseExposure,
					TriggeredAt:         trigger.At.UTC(),
				})
			}
			result, err := r.config.Engine.EvaluateBatch(ctx, requests)
			r.observeDiscoveries(result.Discoveries)
			r.config.OnResult(result)
			r.config.Observer.Observe(Event{
				Kind: EventEvaluationCompleted, At: r.config.Clock().UTC(),
				Trigger: trigger.Market, Result: &result,
			})
			executed := result.Result.Execution
			if !r.config.DryRun && executed != nil && executed.ReconciliationRequired {
				operationID := executed.Operation.ID
				r.config.Observer.Observe(Event{
					Kind: EventConfirmationStarted, At: r.config.Clock().UTC(),
					Trigger: trigger.Market, Operation: operationID,
				})
				confirmationErr := r.config.Confirmer.Confirm(ctx, executed.Operation)
				r.config.Observer.Observe(Event{
					Kind: EventConfirmationEnded, At: r.config.Clock().UTC(),
					Trigger: trigger.Market, Operation: operationID,
					Reason: errorText(confirmationErr),
				})
				if confirmationErr != nil {
					circuitOpen = true
					r.config.Observer.Observe(Event{
						Kind: EventCircuitOpened, At: r.config.Clock().UTC(),
						Trigger: trigger.Market, Operation: operationID,
						Reason: confirmationErr.Error(),
					})
					continue
				}
				// A proven settlement supersedes a transport-level broadcast
				// error because both economic effects are now known.
				err = nil
			}
			if errors.Is(err, saga.ErrNoExecution) {
				continue
			}
			if err != nil {
				return err
			}
		}
	}
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func latestTrigger(initial Trigger, triggers <-chan Trigger) Trigger {
	latest := initial
	for {
		select {
		case candidate, ok := <-triggers:
			if !ok {
				return latest
			}
			latest = candidate
		default:
			return latest
		}
	}
}

func (r *Runner) initializeValuation(ctx context.Context, snapshots map[market.MarketID]market.MarketSnapshot) error {
	type result struct {
		marketID market.MarketID
		quote    market.Quote
		tokenIn  market.Token
		tokenOut market.Token
		err      error
	}
	results := make(chan result, len(r.config.Setup.Markets()))
	var group sync.WaitGroup
	for _, marketID := range r.config.Setup.Markets() {
		marketID := marketID
		candidate, ok := r.config.Registry.Market(marketID)
		if !ok {
			return fmt.Errorf("valuation market %q is not registered", marketID)
		}
		base, baseOK := r.config.Registry.Token(candidate.BaseToken)
		quoteToken, quoteOK := r.config.Registry.Token(candidate.QuoteToken)
		if !baseOK || !quoteOK {
			return fmt.Errorf("valuation market %q tokens are not registered", marketID)
		}
		input, err := r.config.Notional.ToTokenAmount(quoteToken)
		if err != nil {
			return err
		}
		group.Add(1)
		go func() {
			defer group.Done()
			quoted, quoteErr := r.config.Sources[marketID].Quote(ctx, quoteport.Input{
				Snapshot: snapshots[marketID], TokenIn: quoteToken.ID, TokenOut: base.ID,
				AmountIn: input, Purpose: market.QuotePurposeLiveDiscovery, QuotedAt: r.config.Clock().UTC(),
			})
			results <- result{marketID: marketID, quote: quoted, tokenIn: quoteToken, tokenOut: base, err: quoteErr}
		}()
	}
	group.Wait()
	close(results)
	for observed := range results {
		if observed.err != nil {
			return observed.err
		}
		if err := r.config.Valuation.Observe(
			string(observed.marketID)+"/bootstrap_buy", observed.quote,
			observed.tokenIn, observed.tokenOut, r.config.Clock().UTC(),
		); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) observeDiscoveries(discoveries []arbitrage.LiveOpportunity) {
	for _, discovery := range discoveries {
		for side, quote := range map[string]market.Quote{"buy": discovery.BuyQuote, "sell": discovery.SellQuote} {
			candidate, ok := r.config.Registry.Market(quote.Market)
			if !ok {
				continue
			}
			base, baseOK := r.config.Registry.Token(candidate.BaseToken)
			quoteToken, quoteOK := r.config.Registry.Token(candidate.QuoteToken)
			if !baseOK || !quoteOK {
				continue
			}
			tokenIn, tokenOut := quoteToken, base
			if quote.AmountIn.Token() == base.ID {
				tokenIn, tokenOut = base, quoteToken
			}
			_ = r.config.Valuation.Observe(
				string(quote.Market)+"/"+side, quote, tokenIn, tokenOut, r.config.Clock().UTC(),
			)
		}
	}
}
