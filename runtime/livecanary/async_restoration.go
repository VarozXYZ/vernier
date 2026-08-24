package livecanary

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/core/live"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	persistenceport "github.com/VarozXYZ/vernier/ports/persistence"
)

type AsyncQuoteRestorerConfig struct {
	Context context.Context
	Journal interface {
		executionport.SequentialJournal
		executionport.SequentialRecoveryJournal
		persistenceport.RestorationJournal
	}
	Driver     executionport.SequentialStageDriver
	Observer   executionport.SequentialObserver
	OnCapacity func()
	Clock      func() time.Time
}

type AsyncQuoteRestorer struct {
	ctx        context.Context
	journal    AsyncQuoteRestorerConfigJournal
	driver     executionport.SequentialStageDriver
	observer   executionport.SequentialObserver
	onCapacity func()
	clock      func() time.Time
	gate       *live.RestorationGate
	mu         sync.Mutex
	uncertain  map[string]struct{}
	wg         sync.WaitGroup
	changes    chan struct{}
}

type AsyncQuoteRestorerConfigJournal interface {
	executionport.SequentialJournal
	executionport.SequentialRecoveryJournal
	persistenceport.RestorationJournal
}

func NewAsyncQuoteRestorer(config AsyncQuoteRestorerConfig) (*AsyncQuoteRestorer, error) {
	if config.Context == nil || config.Journal == nil || config.Driver == nil {
		return nil, fmt.Errorf("asynchronous quote restorer configuration is incomplete")
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	state, err := config.Journal.LoadRestoration(config.Context)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(state.QuoteJobs))
	for _, job := range state.QuoteJobs {
		ids = append(ids, job.ID)
	}
	gate, err := live.NewRestorationGate(live.RestorationSnapshot{BasePending: state.BasePending, QuoteJobs: ids})
	if err != nil {
		return nil, err
	}
	r := &AsyncQuoteRestorer{ctx: config.Context, journal: config.Journal, driver: config.Driver,
		observer: config.Observer, onCapacity: config.OnCapacity, clock: config.Clock, gate: gate,
		uncertain: make(map[string]struct{})}
	r.changes = make(chan struct{}, 1)
	for _, job := range state.QuoteJobs {
		if job.State == "outcome_unknown" {
			r.uncertain[job.ID] = struct{}{}
		}
	}
	return r, nil
}

// ResumePending is called only after the main operation recovery coordinator
// has finished. This ordering prevents the coordinator and the asynchronous
// worker from reconciling or persisting the same bridge settlement twice.
func (r *AsyncQuoteRestorer) ResumePending(ctx context.Context) error {
	state, err := r.journal.LoadRestoration(ctx)
	if err != nil {
		return err
	}
	if state.BasePending {
		snapshot, loadErr := r.journal.LoadSequentialRecovery(ctx, state.BaseOperation)
		if loadErr != nil {
			return loadErr
		}
		if snapshot.Operation.State == execution.SequentialCompleted {
			if err := r.journal.SetBaseRestoration(ctx, "", false); err != nil {
				return err
			}
			r.gate.CompleteBaseReturn()
		}
	}
	for _, job := range state.QuoteJobs {
		snapshot, loadErr := r.journal.LoadSequentialRecovery(ctx, job.Operation)
		if loadErr != nil {
			return loadErr
		}
		settled := false
		for _, settlement := range snapshot.Settlements {
			if settlement.Request.Stage.Ordinal == 4 && settlement.Request.Stage.Stage == execution.StageBridgeQuoteReturn {
				settled = true
				break
			}
		}
		if settled {
			if err := r.journal.FinishQuoteRestoration(ctx, job.ID, "delivered", r.clock().UTC()); err != nil {
				return err
			}
			r.mu.Lock()
			delete(r.uncertain, job.ID)
			r.mu.Unlock()
			r.gate.CompleteQuoteReturn(job.ID)
			continue
		}
		r.resume(job)
	}
	r.capacityChanged()
	return nil
}

func (r *AsyncQuoteRestorer) Admit(execution.SequentialPlan) error {
	r.mu.Lock()
	uncertain := len(r.uncertain) != 0
	r.mu.Unlock()
	if uncertain {
		return fmt.Errorf("quote restoration has an unknown outcome")
	}
	if !r.gate.CanEvaluate(true) {
		return fmt.Errorf("restoration capacity is unavailable")
	}
	return nil
}

func (r *AsyncQuoteRestorer) BeginBase(ctx context.Context, operation execution.OperationID) error {
	if err := r.gate.BeginOperation(); err != nil {
		return err
	}
	if err := r.journal.SetBaseRestoration(ctx, operation, true); err != nil {
		r.gate.CompleteBaseReturn()
		return err
	}
	return nil
}

func (r *AsyncQuoteRestorer) CompleteBase(ctx context.Context, operation execution.OperationID, stageErr error) error {
	if stageErr != nil {
		return r.journal.SetBaseRestoration(ctx, operation, true)
	}
	if err := r.journal.SetBaseRestoration(ctx, "", false); err != nil {
		return err
	}
	r.gate.CompleteBaseReturn()
	r.capacityChanged()
	return nil
}

func (r *AsyncQuoteRestorer) Start(_ context.Context, request execution.SequentialStageRequest,
	driver executionport.SequentialStageDriver, journal executionport.SequentialJournal) error {
	if driver == nil || journal == nil {
		return fmt.Errorf("asynchronous quote restoration driver or journal changed")
	}
	job := persistenceport.QuoteRestorationJob{ID: fmt.Sprintf("%s/quote-return", request.Operation), Operation: request.Operation,
		State: "pending", SourceChain: request.Stage.SourceChain, DestinationChain: request.Stage.DestinationChain,
		InputToken: request.Input.Token(), OutputToken: request.Stage.OutputToken, InputUnits: request.Input.Units(),
		CreatedAt: r.clock().UTC(), UpdatedAt: r.clock().UTC()}
	if err := r.gate.StartQuoteReturn(job.ID); err != nil {
		return err
	}
	if err := r.journal.StartQuoteRestoration(r.ctx, job); err != nil {
		r.gate.CompleteQuoteReturn(job.ID)
		return err
	}
	r.launch(job, request, nil)
	return nil
}

func (r *AsyncQuoteRestorer) resume(job persistenceport.QuoteRestorationJob) {
	input, err := market.NewTokenAmount(job.InputToken, job.InputUnits)
	if err != nil {
		return
	}
	request := execution.SequentialStageRequest{Operation: job.Operation, Plan: execution.PlanID("restored/" + string(job.Operation)),
		Stage: execution.SequentialStagePlan{Ordinal: 4, Stage: execution.StageBridgeQuoteReturn, Branch: execution.BranchMain,
			DependsOn: []int{2}, InputFromOrdinal: 2,
			SourceChain: job.SourceChain, DestinationChain: job.DestinationChain, InputToken: job.InputToken,
			OutputToken: job.OutputToken}, Input: input}
	snapshot, err := r.journal.LoadSequentialRecovery(r.ctx, job.Operation)
	if err != nil {
		return
	}
	r.launch(job, request, snapshot.Transactions)
}

func (r *AsyncQuoteRestorer) launch(job persistenceport.QuoteRestorationJob, request execution.SequentialStageRequest,
	records []executionport.SequentialTransactionRecord) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		if r.observer != nil {
			r.observer.StageStarted(request)
		}
		var settlement execution.SequentialStageSettlement
		var err error
		if len(records) > 0 {
			if recovery, ok := r.driver.(executionport.SequentialRecoveryDriver); ok {
				settlement, err = recovery.RecoverStage(r.ctx, request, records, r.journal)
			} else {
				err = fmt.Errorf("quote restoration driver has no recovery capability")
			}
		} else {
			settlement, err = r.driver.ExecuteStage(r.ctx, request, r.journal)
		}
		state := "delivered"
		if err != nil {
			state = "outcome_unknown"
		} else {
			if persistErr := r.journal.RecordStageSettlement(r.ctx, settlement); persistErr != nil {
				err, state = persistErr, "outcome_unknown"
			}
			if err == nil && r.observer != nil {
				r.observer.StageSettled(settlement)
			}
		}
		finishErr := r.finishQuoteRestoration(job.ID, state)
		if state == "delivered" && finishErr == nil {
			r.mu.Lock()
			delete(r.uncertain, job.ID)
			r.mu.Unlock()
			r.gate.CompleteQuoteReturn(job.ID)
			r.capacityChanged()
		} else {
			r.mu.Lock()
			r.uncertain[job.ID] = struct{}{}
			r.mu.Unlock()
		}
	}()
}

func (r *AsyncQuoteRestorer) finishQuoteRestoration(id, state string) error {
	ctx := context.WithoutCancel(r.ctx)
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		if err := r.journal.FinishQuoteRestoration(ctx, id, state, r.clock().UTC()); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if attempt == 4 {
			break
		}
		timer := time.NewTimer(time.Duration(1<<attempt) * 10 * time.Millisecond)
		<-timer.C
	}
	return fmt.Errorf("finish quote restoration %q as %s: %w", id, state, lastErr)
}

func (r *AsyncQuoteRestorer) capacityChanged() {
	select {
	case r.changes <- struct{}{}:
	default:
	}
	if r.onCapacity != nil {
		r.onCapacity()
	}
}
func (r *AsyncQuoteRestorer) CapacityAvailable() bool {
	r.mu.Lock()
	uncertain := len(r.uncertain) != 0
	r.mu.Unlock()
	return !uncertain && r.gate.CanEvaluate(true)
}
func (r *AsyncQuoteRestorer) CapacityChanges() <-chan struct{} { return r.changes }

// WaitForPending is used by bounded Live runs. A forced canary or --once run
// is successful only after every asynchronous quote-token return that was in
// flight has a durable delivered state. Unknown outcomes remain recoverable
// in the journal and are reported to the caller instead of being hidden by
// process shutdown.
func (r *AsyncQuoteRestorer) WaitForPending(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
	}
	state, err := r.journal.LoadRestoration(ctx)
	if err != nil {
		return fmt.Errorf("load restoration completion: %w", err)
	}
	if state.BasePending {
		return fmt.Errorf("critical base-token restoration remains pending")
	}
	if len(state.QuoteJobs) != 0 {
		return fmt.Errorf("%d quote-token restoration(s) remain pending or unknown", len(state.QuoteJobs))
	}
	return nil
}

func (r *AsyncQuoteRestorer) Close() { r.wg.Wait() }

var _ executionport.SequentialAsyncQuoteRestorer = (*AsyncQuoteRestorer)(nil)
var _ PlanAdmission = (*AsyncQuoteRestorer)(nil)
