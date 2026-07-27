package saga

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/domain/execution"
	chainport "github.com/VarozXYZ/vernier/ports/chain"
)

type ConfirmerConfig struct {
	Settlement    *SettlementCoordinator
	WebSockets    map[execution.AccountID]chainport.ConfirmationSource
	Managers      map[execution.AccountID]chainport.TxManager
	FallbackAfter time.Duration
}

// Confirmer prefers transaction-bound WebSocket evidence and falls back to
// each TxManager's read-only reconciliation path. It never broadcasts.
type Confirmer struct {
	config ConfirmerConfig
}

func NewConfirmer(config ConfirmerConfig) (*Confirmer, error) {
	if config.Settlement == nil || len(config.WebSockets) < 2 || len(config.Managers) < 2 ||
		config.FallbackAfter <= 0 {
		return nil, fmt.Errorf("confirmer requires settlement, WebSockets, managers, and fallback timeout")
	}
	for account, source := range config.WebSockets {
		if account == "" || source == nil || config.Managers[account] == nil {
			return nil, fmt.Errorf("confirmer account binding is incomplete")
		}
	}
	return &Confirmer{config: config}, nil
}

func (c *Confirmer) Warm(ctx context.Context) error {
	var (
		group sync.WaitGroup
		errs  = make(chan error, len(c.config.WebSockets))
	)
	for _, source := range c.config.WebSockets {
		source := source
		group.Add(1)
		go func() {
			defer group.Done()
			if err := source.Warm(ctx); err != nil {
				errs <- err
			}
		}()
	}
	group.Wait()
	close(errs)
	var joined []error
	for err := range errs {
		joined = append(joined, err)
	}
	return errors.Join(joined...)
}

func (c *Confirmer) Confirm(ctx context.Context, operation execution.Operation) error {
	if err := c.config.Settlement.Register(operation); err != nil {
		return err
	}
	var (
		group sync.WaitGroup
		errs  = make(chan error, len(operation.Steps))
	)
	for _, step := range operation.Steps {
		step := step
		step.Operation = operation.ID
		group.Add(1)
		go func() {
			defer group.Done()
			settlement, err := c.awaitOrReconcile(ctx, step)
			if err != nil {
				errs <- err
				return
			}
			settlement.Operation = operation.ID
			settlement.Step = step.Leg.ID
			if settlement.Identity.Hash == "" {
				settlement.Identity = step.Identity
			}
			if settlement.Identity.Hash != step.Identity.Hash ||
				settlement.Identity.Chain != step.Identity.Chain ||
				settlement.Identity.Account != step.Identity.Account {
				errs <- fmt.Errorf("confirmation identity does not match durable operation step %q", step.Leg.ID)
				return
			}
			if err := c.config.Settlement.Observe(ctx, settlement); err != nil {
				errs <- err
			}
		}()
	}
	group.Wait()
	close(errs)
	var joined []error
	for err := range errs {
		joined = append(joined, err)
	}
	return errors.Join(joined...)
}

func (c *Confirmer) awaitOrReconcile(ctx context.Context, step execution.OperationStep) (execution.Settlement, error) {
	source := c.config.WebSockets[step.Leg.Account]
	manager := c.config.Managers[step.Leg.Account]
	if source == nil || manager == nil {
		return execution.Settlement{}, fmt.Errorf("confirmation binding for account %q is missing", step.Leg.Account)
	}
	websocketCtx, cancel := context.WithTimeout(ctx, c.config.FallbackAfter)
	settlement, websocketErr := source.Await(websocketCtx, step)
	cancel()
	if websocketErr == nil && websocketSettlementComplete(step, settlement) {
		return settlement, nil
	}
	return manager.Reconcile(ctx, step)
}

func websocketSettlementComplete(step execution.OperationStep, settlement execution.Settlement) bool {
	if settlement.Identity.Hash != step.Identity.Hash ||
		settlement.Identity.Chain != step.Identity.Chain ||
		settlement.Identity.Account != step.Identity.Account ||
		settlement.ObservedAt.IsZero() {
		return false
	}
	switch settlement.Technical {
	case execution.StateConfirmedSuccess:
		return settlement.Economic == execution.EconomicEffectVerified &&
			settlement.ActualIn.Token() == step.Leg.Input.Token() &&
			settlement.ActualOut.Token() == step.Leg.ExpectedOutput.Token()
	case execution.StateConfirmedRevert:
		return true
	default:
		return false
	}
}
