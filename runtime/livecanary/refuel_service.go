package livecanary

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	notificationport "github.com/VarozXYZ/vernier/ports/notification"
	"github.com/VarozXYZ/vernier/runtime/configuration"
)

type RefuelService struct {
	policy    configuration.ResolvedGasRefuel
	gate      *RuntimeGate
	journal   executionport.RefuelJournal
	executors map[market.ChainID]executionport.RefuelExecutor
	notifier  *LiveNotifier
	clock     func() time.Time
	after     func(context.Context)
}

func NewRefuelService(
	policy configuration.ResolvedGasRefuel,
	gate *RuntimeGate,
	journal executionport.RefuelJournal,
	executors []executionport.RefuelExecutor,
	notifier *LiveNotifier,
	clock func() time.Time,
) (*RefuelService, error) {
	if !policy.Enabled {
		return nil, nil
	}
	if gate == nil || journal == nil || len(executors) != 2 ||
		policy.ThresholdUSD == nil || policy.TargetUSD == nil ||
		policy.MaxUSDC == nil || policy.PollInterval <= 0 ||
		policy.Cooldown <= 0 {
		return nil, fmt.Errorf("refuel service configuration is incomplete")
	}
	if clock == nil {
		clock = time.Now
	}
	byChain := make(
		map[market.ChainID]executionport.RefuelExecutor,
		len(executors),
	)
	for _, executor := range executors {
		if executor == nil || executor.Chain() == "" ||
			byChain[executor.Chain()] != nil {
			return nil, fmt.Errorf("refuel executors are invalid")
		}
		byChain[executor.Chain()] = executor
	}
	return &RefuelService{
		policy: policy, gate: gate, journal: journal,
		executors: byChain, notifier: notifier, clock: clock,
	}, nil
}

func (s *RefuelService) SetAfter(callback func(context.Context)) {
	if s != nil {
		s.after = callback
	}
}

func (s *RefuelService) ReconcileActive(ctx context.Context) error {
	if s == nil {
		return nil
	}
	record, found, err := s.journal.ActiveRefuel(ctx)
	if err != nil || !found {
		return err
	}
	executor := s.executors[record.Chain]
	if executor == nil {
		return fmt.Errorf(
			"active refuel on %s has no executor",
			record.Chain,
		)
	}
	reconciled, err := executor.Reconcile(ctx, record, s.journal)
	if err != nil {
		if executionport.RecoveryKind(err) ==
			executionport.RecoveryFailureUncertain {
			s.notify(
				notificationport.LiveExecutionRefuelUncertain,
				reconciled,
				err,
			)
			return fmt.Errorf(
				"refuel %s remains uncertain: %w",
				record.ID,
				err,
			)
		}
		s.notify(
			notificationport.LiveExecutionRefuelFailed,
			reconciled,
			err,
		)
		return nil
	}
	s.notify(
		notificationport.LiveExecutionRefuelCompleted,
		reconciled,
		nil,
	)
	return nil
}

func (s *RefuelService) Run(ctx context.Context) error {
	if s == nil {
		<-ctx.Done()
		return ctx.Err()
	}
	ticker := time.NewTicker(s.policy.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.poll(ctx); err != nil {
				return err
			}
		}
	}
}

func (s *RefuelService) RefuelOnce(
	ctx context.Context,
	chain market.ChainID,
	armed bool,
) (executionport.RefuelRecord, error) {
	if s == nil {
		return executionport.RefuelRecord{},
			fmt.Errorf("gas refuel is disabled")
	}
	executor := s.executors[chain]
	if executor == nil {
		return executionport.RefuelRecord{},
			fmt.Errorf("refuel chain %q is unavailable", chain)
	}
	if err := s.gate.Transition(
		RuntimeGateIdle,
		RuntimeGateRefueling,
	); err != nil {
		return executionport.RefuelRecord{}, err
	}
	spend, err := s.refuelSpend(ctx, executor, true)
	var record executionport.RefuelRecord
	if err == nil {
		if armed {
			record, err = executor.Execute(ctx, spend, s.journal)
		} else {
			record, err = executor.Preview(ctx, spend)
		}
	}
	if executionport.RecoveryKind(err) ==
		executionport.RecoveryFailureUncertain {
		_ = s.gate.Transition(
			RuntimeGateRefueling,
			RuntimeGateRecoveryBlocked,
		)
		return record, err
	}
	_ = s.gate.Transition(RuntimeGateRefueling, RuntimeGateIdle)
	return record, err
}

// EmergencyRefuel temporarily owns the recovery gate. A known failure returns
// control to recovery so it can keep its bounded backoff; an uncertain
// broadcast leaves the gate blocked because the shared nonce/balance cannot
// safely be reused.
func (s *RefuelService) EmergencyRefuel(
	ctx context.Context,
	chain market.ChainID,
) error {
	if s == nil {
		return fmt.Errorf("gas refuel is disabled")
	}
	executor := s.executors[chain]
	if executor == nil {
		return fmt.Errorf("refuel chain %q is unavailable", chain)
	}
	// Recovery can retry every few hundred milliseconds. A completed refuel is
	// durable, so never buy gas again during its cooldown merely because the
	// original stage has not observed the updated balance yet.
	if !s.refuelDue(ctx, chain) {
		return nil
	}
	if err := s.gate.Transition(
		RuntimeGateRecovering,
		RuntimeGateRefueling,
	); err != nil {
		return err
	}
	// A genuine recovery shortage may sit above the ordinary polling threshold,
	// so the first eligible emergency action still fills to the configured
	// target. The durable cooldown above prevents every recovery retry from
	// buying gas again.
	record, err := s.execute(ctx, executor, true)
	if executionport.RecoveryKind(err) ==
		executionport.RecoveryFailureUncertain {
		_ = s.gate.Transition(
			RuntimeGateRefueling,
			RuntimeGateRecoveryBlocked,
		)
		s.notify(
			notificationport.LiveExecutionRefuelUncertain,
			record,
			err,
		)
		return err
	}
	_ = s.gate.Transition(
		RuntimeGateRefueling,
		RuntimeGateRecovering,
	)
	if err != nil {
		s.notify(
			notificationport.LiveExecutionRefuelFailed,
			record,
			err,
		)
		return err
	}
	if record.Input.Token() == "" {
		return nil
	}
	s.notify(
		notificationport.LiveExecutionRefuelCompleted,
		record,
		nil,
	)
	return nil
}

// MaintainAfterOperation gives gas maintenance priority over the post-flow
// cost refresh and evaluation while keeping the original operational lease.
func (s *RefuelService) MaintainAfterOperation(
	ctx context.Context,
	owner RuntimeGateState,
) error {
	if s == nil {
		return nil
	}
	var due []executionport.RefuelExecutor
	for _, executor := range s.executors {
		if !s.refuelDue(ctx, executor.Chain()) {
			continue
		}
		balance, err := executor.Balance(ctx)
		if err != nil {
			s.notify(
				notificationport.LiveExecutionRefuelFailed,
				executionport.RefuelRecord{
					ID: fmt.Sprintf(
						"refuel-check-%s-%d",
						executor.Chain(),
						s.clock().UTC().Unix(),
					),
					Chain: executor.Chain(),
					State: executionport.RefuelFailed,
				},
				err,
			)
			continue
		}
		if balance.QuoteValue.Rat().Cmp(s.policy.ThresholdUSD) < 0 {
			due = append(due, executor)
		}
	}
	if len(due) == 0 {
		return nil
	}
	if err := s.gate.Transition(owner, RuntimeGateRefueling); err != nil {
		return err
	}
	for _, executor := range due {
		record, err := s.execute(ctx, executor, false)
		if executionport.RecoveryKind(err) ==
			executionport.RecoveryFailureUncertain {
			_ = s.gate.Transition(
				RuntimeGateRefueling,
				RuntimeGateRecoveryBlocked,
			)
			s.notify(
				notificationport.LiveExecutionRefuelUncertain,
				record,
				err,
			)
			return err
		}
		if err != nil {
			s.notify(
				notificationport.LiveExecutionRefuelFailed,
				record,
				err,
			)
			continue
		}
		s.notify(
			notificationport.LiveExecutionRefuelCompleted,
			record,
			nil,
		)
	}
	return s.gate.Transition(RuntimeGateRefueling, owner)
}

func (s *RefuelService) poll(ctx context.Context) error {
	if s.gate.State() != RuntimeGateIdle {
		return nil
	}
	for _, executor := range s.executors {
		if !s.refuelDue(ctx, executor.Chain()) {
			continue
		}
		balance, err := executor.Balance(ctx)
		if err != nil {
			s.notify(
				notificationport.LiveExecutionRefuelFailed,
				executionport.RefuelRecord{
					ID: fmt.Sprintf(
						"refuel-check-%s-%d",
						executor.Chain(),
						s.clock().UTC().Unix(),
					),
					Chain: executor.Chain(),
					State: executionport.RefuelFailed,
				},
				fmt.Errorf("read gas balance: %w", err),
			)
			continue
		}
		if balance.QuoteValue.Rat().Cmp(s.policy.ThresholdUSD) >= 0 {
			continue
		}
		if err := s.gate.Transition(
			RuntimeGateIdle,
			RuntimeGateRefueling,
		); err != nil {
			return nil
		}
		record, executeErr := s.execute(ctx, executor, false)
		if executionport.RecoveryKind(executeErr) ==
			executionport.RecoveryFailureUncertain {
			_ = s.gate.Transition(
				RuntimeGateRefueling,
				RuntimeGateRecoveryBlocked,
			)
			s.notify(
				notificationport.LiveExecutionRefuelUncertain,
				record,
				executeErr,
			)
			return executeErr
		}
		_ = s.gate.Transition(
			RuntimeGateRefueling,
			RuntimeGateIdle,
		)
		if executeErr != nil {
			s.notify(
				notificationport.LiveExecutionRefuelFailed,
				record,
				executeErr,
			)
		} else {
			s.notify(
				notificationport.LiveExecutionRefuelCompleted,
				record,
				nil,
			)
			if s.after != nil {
				s.after(ctx)
			}
		}
	}
	return nil
}

func (s *RefuelService) execute(
	ctx context.Context,
	executor executionport.RefuelExecutor,
	force bool,
) (executionport.RefuelRecord, error) {
	spend, err := s.refuelSpend(ctx, executor, force)
	if err != nil || spend.Asset() == "" {
		return s.identifyAttempt(
			executionport.RefuelRecord{},
			executor.Chain(),
		), err
	}
	record, err := executor.Execute(ctx, spend, s.journal)
	record = s.identifyAttempt(record, executor.Chain())
	// Only a failure carrying a valid transaction identity (or an explicit
	// outcome_unknown state) can represent an uncertain broadcast. Generic
	// pre-broadcast errors must remain known failures even if an implementation
	// forgot to attach a structured recovery kind.
	if err != nil &&
		executionport.RecoveryKind(err) == executionport.RecoveryFailureUncertain &&
		record.State != executionport.RefuelOutcomeUnknown &&
		record.Identity.Validate() != nil {
		err = executionport.NewRecoveryError(
			executionport.RecoveryFailureTemporary,
			err,
		)
	}
	return record, err
}

func (s *RefuelService) identifyAttempt(
	record executionport.RefuelRecord,
	chain market.ChainID,
) executionport.RefuelRecord {
	now := s.clock().UTC()
	if record.ID == "" {
		record.ID = fmt.Sprintf(
			"refuel-attempt-%s-%d",
			chain,
			now.UnixNano(),
		)
	}
	if record.Chain == "" {
		record.Chain = chain
	}
	if record.State == "" {
		record.State = executionport.RefuelFailed
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	if record.UpdatedAt.IsZero() {
		record.UpdatedAt = now
	}
	return record
}

func (s *RefuelService) refuelSpend(
	ctx context.Context,
	executor executionport.RefuelExecutor,
	force bool,
) (market.AssetQuantity, error) {
	balance, err := executor.Balance(ctx)
	if err != nil {
		return market.AssetQuantity{}, err
	}
	if !force &&
		balance.QuoteValue.Rat().Cmp(s.policy.ThresholdUSD) >= 0 {
		return market.AssetQuantity{}, nil
	}
	spend := new(big.Rat).Sub(
		s.policy.TargetUSD,
		balance.QuoteValue.Rat(),
	)
	if spend.Sign() <= 0 {
		return market.AssetQuantity{}, nil
	}
	if spend.Cmp(s.policy.MaxUSDC) > 0 {
		spend = new(big.Rat).Set(s.policy.MaxUSDC)
	}
	return market.NewAssetQuantity(
		balance.QuoteValue.Asset(),
		spend,
	)
}

func (s *RefuelService) refuelDue(
	ctx context.Context,
	chain market.ChainID,
) bool {
	record, found, err := s.journal.LastCompletedRefuel(ctx, chain)
	return err == nil &&
		(!found || s.clock().UTC().Sub(record.UpdatedAt) >= s.policy.Cooldown)
}

func (s *RefuelService) notify(
	kind notificationport.LiveExecutionEventKind,
	record executionport.RefuelRecord,
	err error,
) {
	if s.notifier == nil {
		return
	}
	detail := ""
	if err != nil {
		detail = err.Error()
	}
	s.notifier.Notify(notificationport.LiveExecutionEvent{
		Kind: kind, Operation: record.ID,
		State: string(record.State), SourceChain: string(record.Chain),
		Input:         record.Input.String(),
		Output:        record.NativeReceived.String(),
		ExecutionCost: record.Fee.String(),
		Detail:        detail, OccurredAt: s.clock().UTC(),
	})
}
