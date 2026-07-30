package livecanary

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	sqlitestore "github.com/VarozXYZ/vernier/adapters/persistence/sqlite"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	notificationport "github.com/VarozXYZ/vernier/ports/notification"
	"github.com/VarozXYZ/vernier/runtime/livecompare"
)

// Runtime connects the read-only opportunity detector to a sequential
// executor. The detector never blocks on execution and production Live keeps
// accepting opportunities after completed operations.
type Runtime struct {
	runner           streamRunner
	manager          *Manager
	opportunityStore *sqlitestore.Store
	closers          []func()
	output           io.Writer
	notifier         *LiveNotifier
	reevaluate       chan time.Time
	gate             *RuntimeGate
	postFlowRefresh  func(context.Context) error
	recovery         sequentialRecovery
	refuel           *RefuelService
	execute          bool
	forcedDirection  *arbitrage.Direction

	closeOnce sync.Once
}

type streamRunner interface {
	RunStream(context.Context, livecompare.StreamOptions) error
}

type sequentialRecovery interface {
	RecoverActive(
		context.Context,
	) (executionport.SequentialResult, bool, error)
}

// NewRuntime composes a sequential Live runtime around an opportunity stream.
func NewRuntime(
	runner streamRunner,
	manager *Manager,
	opportunityStore *sqlitestore.Store,
	closers []func(),
	output io.Writer,
	notifier *LiveNotifier,
	reevaluate chan time.Time,
	execute bool,
	forcedDirection *arbitrage.Direction,
) (*Runtime, error) {
	return NewRuntimeWithGate(
		runner,
		manager,
		opportunityStore,
		closers,
		output,
		notifier,
		reevaluate,
		execute,
		forcedDirection,
		NewRuntimeGate(),
	)
}

func NewRuntimeWithGate(
	runner streamRunner,
	manager *Manager,
	opportunityStore *sqlitestore.Store,
	closers []func(),
	output io.Writer,
	notifier *LiveNotifier,
	reevaluate chan time.Time,
	execute bool,
	forcedDirection *arbitrage.Direction,
	gate *RuntimeGate,
) (*Runtime, error) {
	if runner == nil || manager == nil || opportunityStore == nil || output == nil {
		return nil, fmt.Errorf("sequential Live runtime composition is incomplete")
	}
	if gate == nil {
		return nil, fmt.Errorf("sequential Live runtime gate is required")
	}
	return &Runtime{
		runner: runner, manager: manager, opportunityStore: opportunityStore,
		closers: closers, output: output, notifier: notifier,
		reevaluate: reevaluate, gate: gate,
		execute: execute, forcedDirection: forcedDirection,
	}, nil
}

func (r *Runtime) Run(ctx context.Context) (runErr error) {
	startedAt := time.Now().UTC()
	if r.notifier != nil {
		r.notifier.NotifyRuntime(notificationport.LiveRuntimeEvent{
			Kind: notificationport.LiveRuntimeStarted,
			Mode: r.runtimeMode(), StartedAt: startedAt,
			OccurredAt: startedAt,
		})
		defer func() {
			stoppedAt := time.Now().UTC()
			r.notifier.NotifyRuntime(notificationport.LiveRuntimeEvent{
				Kind: notificationport.LiveRuntimeStopped,
				Mode: r.runtimeMode(), Reason: liveRuntimeStopReason(runErr),
				StartedAt: startedAt, OccurredAt: stoppedAt,
				Uptime: stoppedAt.Sub(startedAt),
			})
		}()
	}
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()

	streamResult := make(chan error, 1)
	reevaluate := r.reevaluate
	if reevaluate == nil {
		reevaluate = make(chan time.Time, 1)
	}
	startupState := RuntimeGateStarting
	if r.refuel != nil {
		if err := r.gate.Transition(
			startupState,
			RuntimeGateRefueling,
		); err != nil {
			return err
		}
		startupState = RuntimeGateRefueling
		if err := r.refuel.ReconcileActive(ctx); err != nil {
			_ = r.gate.Transition(
				RuntimeGateRefueling,
				RuntimeGateRecoveryBlocked,
			)
			return err
		}
	}
	if r.recovery != nil {
		if err := r.gate.Transition(
			startupState,
			RuntimeGateRecovering,
		); err != nil {
			return err
		}
		recovered, found, recoveryErr := r.recovery.RecoverActive(ctx)
		if recoveryErr != nil {
			_ = r.gate.Transition(
				RuntimeGateRecovering,
				RuntimeGateRecoveryBlocked,
			)
			return recoveryErr
		}
		if found {
			fmt.Fprintf(
				r.output,
				"live_recovery operation=%s status=completed stages=%d final_units=%s\n",
				recovered.Operation,
				len(recovered.Settlements),
				recovered.FinalAmount,
			)
		}
		if err := r.gate.Transition(
			RuntimeGateRecovering,
			RuntimeGateIdle,
		); err != nil {
			return err
		}
	} else if err := r.gate.Transition(
		startupState,
		RuntimeGateIdle,
	); err != nil {
		return err
	}
	defer r.stopGate()
	go func() {
		var outputMu sync.Mutex
		reports := 0
		forcedOffered := false
		streamResult <- r.runner.RunStream(streamCtx, livecompare.StreamOptions{
			Updates:              0,
			OpportunityStore:     r.opportunityStore,
			ReevaluationRequests: reevaluate,
			EvaluationGate:       r.gate,
			OnQualifiedOpportunity: func(opportunity arbitrage.Opportunity) error {
				if !r.execute || r.forcedDirection != nil {
					return nil
				}
				if err := r.gate.Transition(
					RuntimeGateIdle,
					RuntimeGateExecuting,
				); err != nil {
					return nil
				}
				accepted, err := r.manager.Offer(opportunity)
				if err != nil {
					_ = r.gate.Transition(
						RuntimeGateExecuting,
						RuntimeGateIdle,
					)
					return err
				}
				if !accepted {
					_ = r.gate.Transition(
						RuntimeGateExecuting,
						RuntimeGateIdle,
					)
				}
				if accepted {
					fmt.Fprintf(
						r.output,
						"live_operation status=accepted evaluation=%s direction=%s->%s\n",
						opportunity.Evaluation,
						opportunity.Direction.BuyMarket,
						opportunity.Direction.SellMarket,
					)
				}
				return nil
			},
			OnReport: func(report livecompare.Report) error {
				outputMu.Lock()
				var buffer bytes.Buffer
				err := livecompare.WriteTextWithOptions(&buffer, report, livecompare.OutputOptions{
					Calculations: livecompare.CalculationSummary,
					OmitCost:     reports > 0,
				})
				if err == nil {
					_, err = r.output.Write(buffer.Bytes())
				}
				if err == nil {
					reports++
				}
				outputMu.Unlock()
				if err != nil || r.forcedDirection == nil || forcedOffered {
					return err
				}
				opportunity, forceErr := ForceOpportunity(
					report.Research.Opportunities,
					*r.forcedDirection,
				)
				if forceErr != nil {
					return forceErr
				}
				if err := r.gate.Transition(
					RuntimeGateIdle,
					RuntimeGateExecuting,
				); err != nil {
					return err
				}
				accepted, forceErr := r.manager.Offer(opportunity)
				if forceErr != nil {
					_ = r.gate.Transition(
						RuntimeGateExecuting,
						RuntimeGateIdle,
					)
					return forceErr
				}
				if !accepted {
					_ = r.gate.Transition(
						RuntimeGateExecuting,
						RuntimeGateIdle,
					)
					return fmt.Errorf(
						"forced canary operation was not accepted",
					)
				}
				forcedOffered = true
				fmt.Fprintf(
					r.output,
					"live_operation status=accepted mode=forced_canary evaluation=%s direction=%s->%s\n",
					opportunity.Evaluation,
					opportunity.Direction.BuyMarket,
					opportunity.Direction.SellMarket,
				)
				return nil
			},
		})
	}()
	refuelResult := make(chan error, 1)
	if r.refuel != nil {
		r.refuel.SetAfter(func(callbackCtx context.Context) {
			r.requestPostFlowEvaluation(callbackCtx, reevaluate)
		})
		go func() {
			refuelResult <- r.refuel.Run(streamCtx)
		}()
	}

	for {
		select {
		case <-ctx.Done():
			cancelStream()
			<-streamResult
			return ctx.Err()
		case err := <-streamResult:
			return err
		case err := <-refuelResult:
			if err != nil && !errors.Is(err, context.Canceled) {
				cancelStream()
				<-streamResult
				return fmt.Errorf("gas refuel runtime: %w", err)
			}
		case event := <-r.manager.Events():
			if event.Err != nil && event.RetryEvaluation {
				action := "reevaluate_latest"
				if event.FromRetryEvaluation {
					action = "wait_for_next_trigger"
				}
				fmt.Fprintf(
					r.output,
					"live_result operation=%s status=aborted_before_first_settlement action=%s error=%q\n",
					event.Operation,
					action,
					event.Err,
				)
				if r.notifier != nil {
					r.notifier.Notify(notificationport.LiveExecutionEvent{
						Kind:       notificationport.LiveExecutionFailed,
						Operation:  string(event.Operation),
						State:      "aborted_retrying",
						Detail:     event.Err.Error(),
						OccurredAt: time.Now().UTC(),
					})
				}
				if err := r.maintainAfterOperation(
					ctx,
					RuntimeGateExecuting,
				); err != nil {
					cancelStream()
					<-streamResult
					return fmt.Errorf(
						"post-operation refuel: %w",
						err,
					)
				}
				if !event.FromRetryEvaluation {
					r.requestPostFlowEvaluation(ctx, reevaluate)
				}
				_ = r.gate.Transition(
					RuntimeGateExecuting,
					RuntimeGateIdle,
				)
				continue
			}
			if event.Err != nil {
				_ = r.gate.Transition(
					RuntimeGateExecuting,
					RuntimeGateRecovering,
				)
				if r.recovery == nil {
					_ = r.gate.Transition(
						RuntimeGateRecovering,
						RuntimeGateRecoveryBlocked,
					)
					cancelStream()
					<-streamResult
					return fmt.Errorf(
						"operation %s requires attention: %w",
						event.Operation,
						event.Err,
					)
				}
				recovered, found, recoveryErr :=
					r.recovery.RecoverActive(ctx)
				if recoveryErr != nil {
					_ = r.gate.Transition(
						RuntimeGateRecovering,
						RuntimeGateRecoveryBlocked,
					)
					cancelStream()
					<-streamResult
					return fmt.Errorf(
						"operation %s recovery blocked: %w",
						event.Operation,
						recoveryErr,
					)
				}
				if !found {
					return fmt.Errorf(
						"operation %s recovery journal is missing",
						event.Operation,
					)
				}
				fmt.Fprintf(
					r.output,
					"live_recovery operation=%s status=completed stages=%d final_units=%s\n",
					recovered.Operation,
					len(recovered.Settlements),
					recovered.FinalAmount,
				)
				if err := r.maintainAfterOperation(
					ctx,
					RuntimeGateRecovering,
				); err != nil {
					cancelStream()
					<-streamResult
					return fmt.Errorf(
						"post-recovery refuel: %w",
						err,
					)
				}
				r.requestPostFlowEvaluation(ctx, reevaluate)
				_ = r.gate.Transition(
					RuntimeGateRecovering,
					RuntimeGateIdle,
				)
				continue
			}
			fmt.Fprintf(
				r.output,
				"live_result operation=%s status=completed stages=%d final_units=%s "+
					"execution_cost=%s external_cost=%s gross_pnl=%s net_pnl=%s\n",
				event.Operation,
				len(event.Result.Settlements),
				event.Result.FinalAmount,
				event.Result.ExecutionCost,
				event.Result.ExternalCost,
				event.Result.RealizedGross,
				event.Result.RealizedNetPnL,
			)
			if r.forcedDirection != nil {
				cancelStream()
				<-streamResult
				return nil
			}
			// Events produced by this operation may have been evaluated while
			// Manager was busy and therefore deliberately not queued. Request
			// one fresh evaluation against the latest trigger/snapshots now
			// that the execution barrier is available again.
			if err := r.maintainAfterOperation(
				ctx,
				RuntimeGateExecuting,
			); err != nil {
				cancelStream()
				<-streamResult
				return fmt.Errorf("post-operation refuel: %w", err)
			}
			r.requestPostFlowEvaluation(ctx, reevaluate)
			_ = r.gate.Transition(
				RuntimeGateExecuting,
				RuntimeGateIdle,
			)
		}
	}
}

func (r *Runtime) maintainAfterOperation(
	ctx context.Context,
	owner RuntimeGateState,
) error {
	if r.refuel == nil {
		return nil
	}
	return r.refuel.MaintainAfterOperation(ctx, owner)
}

func (r *Runtime) SetPostFlowRefresh(refresh func(context.Context) error) {
	r.postFlowRefresh = refresh
}

// RecoverOnly reconciles and resumes one durable operation, then exits before
// starting feeds or admitting a new opportunity.
func (r *Runtime) RecoverOnly(ctx context.Context) error {
	startupState := RuntimeGateStarting
	if r.refuel != nil {
		if err := r.gate.Transition(
			startupState,
			RuntimeGateRefueling,
		); err != nil {
			return err
		}
		startupState = RuntimeGateRefueling
		if err := r.refuel.ReconcileActive(ctx); err != nil {
			_ = r.gate.Transition(
				RuntimeGateRefueling,
				RuntimeGateRecoveryBlocked,
			)
			return err
		}
	}
	if r.recovery == nil {
		return fmt.Errorf("sequential recovery is unavailable")
	}
	if err := r.gate.Transition(
		startupState,
		RuntimeGateRecovering,
	); err != nil {
		return err
	}
	recovered, found, err := r.recovery.RecoverActive(ctx)
	if err != nil {
		_ = r.gate.Transition(
			RuntimeGateRecovering,
			RuntimeGateRecoveryBlocked,
		)
		return err
	}
	if found {
		fmt.Fprintf(
			r.output,
			"live_recovery operation=%s status=completed stages=%d final_units=%s\n",
			recovered.Operation,
			len(recovered.Settlements),
			recovered.FinalAmount,
		)
	} else {
		fmt.Fprintln(r.output, "live_recovery status=idle active_operation=false")
	}
	if err := r.gate.Transition(
		RuntimeGateRecovering,
		RuntimeGateIdle,
	); err != nil {
		return err
	}
	r.stopGate()
	return nil
}

func (r *Runtime) SetRecovery(recovery sequentialRecovery) {
	r.recovery = recovery
}

func (r *Runtime) SetRefuel(refuel *RefuelService) {
	r.refuel = refuel
}

// RefuelOnce uses the normal signer-backed builders and simulators without
// starting discovery. When armed is false it never persists or broadcasts.
func (r *Runtime) RefuelOnce(
	ctx context.Context,
	chain market.ChainID,
	armed bool,
) (executionport.RefuelRecord, error) {
	if r.refuel == nil {
		return executionport.RefuelRecord{},
			fmt.Errorf("gas refuel is disabled")
	}
	if r.gate.State() == RuntimeGateStarting {
		if err := r.gate.Transition(
			RuntimeGateStarting,
			RuntimeGateRefueling,
		); err != nil {
			return executionport.RefuelRecord{}, err
		}
		if err := r.refuel.ReconcileActive(ctx); err != nil {
			_ = r.gate.Transition(
				RuntimeGateRefueling,
				RuntimeGateRecoveryBlocked,
			)
			return executionport.RefuelRecord{}, err
		}
		if err := r.gate.Transition(
			RuntimeGateRefueling,
			RuntimeGateIdle,
		); err != nil {
			return executionport.RefuelRecord{}, err
		}
	}
	return r.refuel.RefuelOnce(ctx, chain, armed)
}

func (r *Runtime) requestPostFlowEvaluation(
	ctx context.Context,
	reevaluate chan<- time.Time,
) {
	if r.postFlowRefresh != nil {
		refreshCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := r.postFlowRefresh(refreshCtx)
		cancel()
		if err != nil {
			fmt.Fprintf(
				r.output,
				"live_warning component=post_flow_cost_refresh error=%q\n",
				err,
			)
		}
	}
	select {
	case reevaluate <- time.Now().UTC():
	default:
	}
}

func (r *Runtime) stopGate() {
	state := r.gate.State()
	if state == RuntimeGateStopping {
		return
	}
	_ = r.gate.Transition(state, RuntimeGateStopping)
}

type synchronizedWriter struct {
	delegate io.Writer
	mu       sync.Mutex
}

func (w *synchronizedWriter) Write(payload []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.delegate.Write(payload)
}

func (r *Runtime) Close() error {
	var closeErr error
	r.closeOnce.Do(func() {
		r.manager.Close()
		if err := r.opportunityStore.Close(); err != nil {
			closeErr = err
		}
		for index := len(r.closers) - 1; index >= 0; index-- {
			r.closers[index]()
		}
		if r.notifier != nil {
			r.notifier.Close()
		}
	})
	return closeErr
}

func (r *Runtime) runtimeMode() string {
	switch {
	case r.forcedDirection != nil:
		return "canary"
	case !r.execute:
		return "observe"
	default:
		return "live"
	}
}

func liveRuntimeStopReason(err error) string {
	if err == nil {
		return "completed"
	}
	if errors.Is(err, context.Canceled) {
		return "operator/system stop"
	}
	reason := strings.TrimSpace(err.Error())
	if reason == "" {
		return "runtime error"
	}
	runes := []rune(reason)
	if len(runes) > 240 {
		reason = string(runes[:237]) + "..."
	}
	return "runtime error: " + reason
}
