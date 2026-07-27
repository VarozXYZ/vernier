// Package live coordinates discovery, executable validation, and durable
// parallel execution.
package live

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/core/saga"
	"github.com/VarozXYZ/vernier/core/strategy"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

type EngineConfig struct {
	Evaluator  *strategy.LiveEvaluator
	Planner    *saga.Planner
	Executor   *saga.Executor
	Validators map[market.MarketID]executionport.Validator
	Costs      executionport.CostEstimator
	Clock      func() time.Time
	ID         func(prefix string) string
	DryRun     bool
}

type Engine struct {
	config EngineConfig
}

type Result struct {
	Discovery       arbitrage.LiveOpportunity
	Validated       *arbitrage.LiveOpportunity
	Artifacts       map[execution.StepID]executionport.Artifact
	Execution       *saga.ExecutionResult
	ValidationAbort bool
	Timing          ValidationTiming
}

type BatchResult struct {
	Discoveries       []arbitrage.LiveOpportunity
	Selected          int
	Result            Result
	DiscoveryDuration time.Duration
}

type ValidationTiming struct {
	Attempts    int
	Validation  time.Duration
	Costing     time.Duration
	Revaluation time.Duration
	Total       time.Duration
}

func New(config EngineConfig) (*Engine, error) {
	if config.Evaluator == nil || config.Planner == nil ||
		len(config.Validators) < 2 || config.Costs == nil || config.Clock == nil || config.ID == nil {
		return nil, fmt.Errorf("live engine requires evaluator, planner, executor, validators, clock, and ID source")
	}
	if !config.DryRun && config.Executor == nil {
		return nil, fmt.Errorf("armed live engine requires a saga executor")
	}
	return &Engine{config: config}, nil
}

func (e *Engine) Warm(ctx context.Context) error {
	if e.config.DryRun {
		return nil
	}
	return e.config.Executor.Warm(ctx)
}

func (e *Engine) Evaluate(ctx context.Context, request strategy.LiveEvaluationRequest) (Result, error) {
	discovery, err := e.config.Evaluator.Evaluate(ctx, request)
	if err != nil {
		return Result{}, err
	}
	return e.executeDiscovered(ctx, request, discovery)
}

// EvaluateBatch discovers all configured directions concurrently, then
// validates and executes only the best net opportunity.
func (e *Engine) EvaluateBatch(ctx context.Context, requests []strategy.LiveEvaluationRequest) (BatchResult, error) {
	if len(requests) == 0 {
		return BatchResult{}, fmt.Errorf("live batch requires evaluation requests")
	}
	discoveryStarted := e.config.Clock()
	discoveries := make([]arbitrage.LiveOpportunity, len(requests))
	discoveryErrors := make([]error, len(requests))
	var group sync.WaitGroup
	for index, request := range requests {
		index, request := index, request
		group.Add(1)
		go func() {
			defer group.Done()
			discoveries[index], discoveryErrors[index] = e.config.Evaluator.Evaluate(ctx, request)
		}()
	}
	group.Wait()
	for _, err := range discoveryErrors {
		if err != nil {
			return BatchResult{}, err
		}
	}
	selected := -1
	for index, discovery := range discoveries {
		if !discovery.Profitable() {
			continue
		}
		if selected < 0 {
			selected = index
			continue
		}
		comparison, err := discovery.NetPnL.Cmp(discoveries[selected].NetPnL)
		if err != nil {
			return BatchResult{}, err
		}
		if comparison > 0 {
			selected = index
		}
	}
	result := BatchResult{
		Discoveries: discoveries, Selected: selected,
		DiscoveryDuration: elapsed(e.config.Clock, discoveryStarted),
	}
	if selected < 0 {
		return result, nil
	}
	executed, err := e.executeDiscovered(ctx, requests[selected], discoveries[selected])
	result.Result = executed
	return result, err
}

func (e *Engine) executeDiscovered(
	ctx context.Context,
	request strategy.LiveEvaluationRequest,
	discovery arbitrage.LiveOpportunity,
) (result Result, returnedErr error) {
	started := e.config.Clock()
	result = Result{Discovery: discovery}
	defer func() {
		result.Timing.Total = elapsed(e.config.Clock, started)
	}()
	if !discovery.Profitable() {
		return result, nil
	}
	for {
		result.Timing.Attempts++
		planID := execution.PlanID(e.config.ID("plan"))
		operationID := execution.OperationID(e.config.ID("operation"))
		discoveryPlan, planErr := e.config.Planner.Plan(planID, discovery)
		if planErr != nil {
			return result, planErr
		}
		validationStarted := e.config.Clock()
		artifacts, validationErr := e.validate(ctx, operationID, discoveryPlan, discovery, request)
		result.Timing.Validation += elapsed(e.config.Clock, validationStarted)
		if validationErr != nil {
			// Provider and local executable-validation failures abort this
			// opportunity, not the long-running Live runtime. No artifact,
			// reservation, persistence, or broadcast is allowed to follow.
			result.ValidationAbort = true
			return result, nil
		}
		costStarted := e.config.Clock()
		finalCost, costErr := e.config.Costs.Estimate(ctx, executionport.CostRequest{
			Opportunity: discovery, Artifacts: artifacts, RequestedAt: e.config.Clock().UTC(),
		})
		result.Timing.Costing += elapsed(e.config.Clock, costStarted)
		if costErr != nil {
			result.ValidationAbort = true
			return result, nil
		}
		validatedRequest := request
		validatedRequest.Cost = finalCost
		revaluationStarted := e.config.Clock()
		validated, valueErr := e.config.Evaluator.Value(
			validatedRequest,
			artifacts["buy"].ValidatedQuote,
			artifacts["sell"].ValidatedQuote,
			e.config.Clock().UTC(),
		)
		result.Timing.Revaluation += elapsed(e.config.Clock, revaluationStarted)
		if valueErr != nil {
			return result, valueErr
		}
		result.Validated = &validated
		result.Artifacts = cloneArtifacts(artifacts)
		if !validated.Profitable() {
			return result, nil
		}
		if e.config.DryRun {
			return result, nil
		}
		plan, planErr := e.config.Planner.Plan(planID, validated)
		if planErr != nil {
			return result, planErr
		}
		executed, executionErr := e.config.Executor.Execute(ctx, operationID, plan, artifacts)
		if errors.Is(executionErr, saga.ErrArtifactTooOld) {
			if err := ctx.Err(); err != nil {
				return result, err
			}
			continue
		}
		result.Execution = &executed
		return result, executionErr
	}
}

func cloneArtifacts(source map[execution.StepID]executionport.Artifact) map[execution.StepID]executionport.Artifact {
	result := make(map[execution.StepID]executionport.Artifact, len(source))
	for id, artifact := range source {
		artifact.Payload = append([]byte(nil), artifact.Payload...)
		artifact.Metadata = cloneMetadata(artifact.Metadata)
		if artifact.Allocation != nil {
			allocation := artifact.Allocation.Clone()
			artifact.Allocation = &allocation
		}
		result[id] = artifact
	}
	return result
}

func cloneMetadata(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func elapsed(clock func() time.Time, started time.Time) time.Duration {
	duration := clock().Sub(started)
	if duration < 0 {
		return 0
	}
	return duration
}

func (e *Engine) validate(
	ctx context.Context,
	operationID execution.OperationID,
	plan execution.SagaPlan,
	opportunity arbitrage.LiveOpportunity,
	request strategy.LiveEvaluationRequest,
) (map[execution.StepID]executionport.Artifact, error) {
	artifacts := make(map[execution.StepID]executionport.Artifact, 2)
	var (
		mu       sync.Mutex
		group    sync.WaitGroup
		firstErr error
	)
	for _, leg := range plan.Legs() {
		leg := leg
		validator := e.config.Validators[leg.Market]
		if validator == nil {
			return nil, fmt.Errorf("no validator for market %q", leg.Market)
		}
		group.Add(1)
		go func() {
			defer group.Done()
			quote := opportunity.BuyQuote
			if leg.Side == execution.LegSell {
				quote = opportunity.SellQuote
			}
			artifact, err := validator.Validate(ctx, executionport.ValidationRequest{
				Operation: operationID,
				Leg:       leg, Discovery: quote, Snapshot: request.Snapshots[leg.Market],
				RequestedAt: e.config.Clock().UTC(),
			})
			mu.Lock()
			defer mu.Unlock()
			if err != nil && firstErr == nil {
				firstErr = err
				return
			}
			if err == nil {
				artifacts[leg.ID] = artifact
			}
		}()
	}
	group.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return artifacts, nil
}
