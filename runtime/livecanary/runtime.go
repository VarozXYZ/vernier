package livecanary

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	sqlitestore "github.com/VarozXYZ/vernier/adapters/persistence/sqlite"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
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
	execute          bool
	forcedDirection  *arbitrage.Direction

	closeOnce sync.Once
}

type streamRunner interface {
	RunStream(context.Context, livecompare.StreamOptions) error
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
	if runner == nil || manager == nil || opportunityStore == nil || output == nil {
		return nil, fmt.Errorf("sequential Live runtime composition is incomplete")
	}
	return &Runtime{
		runner: runner, manager: manager, opportunityStore: opportunityStore,
		closers: closers, output: output, notifier: notifier,
		reevaluate: reevaluate,
		execute:    execute, forcedDirection: forcedDirection,
	}, nil
}

func (r *Runtime) Run(ctx context.Context) error {
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()

	streamResult := make(chan error, 1)
	reevaluate := r.reevaluate
	if reevaluate == nil {
		reevaluate = make(chan time.Time, 1)
	}
	go func() {
		var outputMu sync.Mutex
		reports := 0
		forcedOffered := false
		streamResult <- r.runner.RunStream(streamCtx, livecompare.StreamOptions{
			Updates:              0,
			OpportunityStore:     r.opportunityStore,
			ReevaluationRequests: reevaluate,
			OnQualifiedOpportunity: func(opportunity arbitrage.Opportunity) error {
				if !r.execute || r.forcedDirection != nil {
					return nil
				}
				accepted, err := r.manager.Offer(opportunity)
				if err != nil {
					return err
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
				accepted, forceErr := r.manager.Offer(opportunity)
				if forceErr != nil {
					return forceErr
				}
				if !accepted {
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

	for {
		select {
		case <-ctx.Done():
			cancelStream()
			<-streamResult
			return ctx.Err()
		case err := <-streamResult:
			return err
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
				if !event.FromRetryEvaluation {
					select {
					case reevaluate <- time.Now().UTC():
					default:
					}
				}
				continue
			}
			if event.Err != nil {
				cancelStream()
				<-streamResult
				if r.notifier != nil {
					r.notifier.Notify(notificationport.LiveExecutionEvent{
						Kind:       notificationport.LiveExecutionFailed,
						Operation:  string(event.Operation),
						State:      "manual_intervention_required",
						Detail:     event.Err.Error(),
						OccurredAt: time.Now().UTC(),
					})
				}
				return fmt.Errorf(
					"operation %s requires attention: %w",
					event.Operation,
					event.Err,
				)
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
			select {
			case reevaluate <- time.Now().UTC():
			default:
			}
		}
	}
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
	})
	return closeErr
}
