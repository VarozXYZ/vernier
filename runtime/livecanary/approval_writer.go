package livecanary

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	gethcrypto "github.com/ethereum/go-ethereum/crypto"

	corelive "github.com/VarozXYZ/vernier/core/live"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	chainport "github.com/VarozXYZ/vernier/ports/chain"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	persistenceport "github.com/VarozXYZ/vernier/ports/persistence"
)

type EVMApprovalWriter struct {
	Managers         map[market.ChainID]chainport.TxManager
	TokenAddresses   map[market.ChainID]map[market.TokenID]common.Address
	Journal          persistenceport.ApprovalJournal
	Clock            func() time.Time
	PollInterval     time.Duration
	Timeout          time.Duration
	GasLimit         uint64
	confirmedReverts map[string]struct{}
}

func reconcileApprovalStatus(
	ctx context.Context,
	manager chainport.TxManager,
	identity execution.TransactionIdentity,
	operationID string,
	token market.TokenID,
) (execution.TechnicalState, error) {
	if reconciler, ok := manager.(chainport.ReceiptStatusReconciler); ok {
		return reconciler.ReconcileReceiptStatus(ctx, identity)
	}
	input, _ := market.NewTokenAmount(token, big.NewInt(1))
	leg := execution.Leg{ID: "startup-approval-recovery", Side: execution.LegBuy,
		Chain: identity.Chain, Account: manager.Account(), Market: "startup-approval",
		Input: input, ExpectedOutput: input}
	step := execution.OperationStep{Operation: execution.OperationID(operationID), Leg: leg,
		Identity: identity, Technical: execution.StateBroadcastPossible,
		Economic: execution.EconomicReserved}
	settlement, err := manager.Reconcile(ctx, step)
	return settlement.Technical, err
}

func approvalEvidenceKey(requirement corelive.AllowanceRequirement) string {
	return string(requirement.Chain) + "\x00" + string(requirement.Token) + "\x00" + strings.ToLower(requirement.Spender)
}

// ReconcilePending must run before reading startup allowances. Possibly
// broadcast identities are reconciled by hash/nonce and are never rebuilt.
func (w *EVMApprovalWriter) ReconcilePending(ctx context.Context) error {
	if w.Journal == nil {
		return fmt.Errorf("approval recovery journal is unavailable")
	}
	records, err := w.Journal.LoadApprovalRecovery(ctx)
	if err != nil {
		return err
	}
	if w.confirmedReverts == nil {
		w.confirmedReverts = make(map[string]struct{})
	}
	for _, record := range records {
		requirement := corelive.AllowanceRequirement{Chain: record.Chain, Token: record.Token, Spender: record.Spender, Purpose: "startup approval recovery"}
		if record.State == "confirmed_revert" {
			if record.Amount.Cmp(corelive.MaximumAllowance) == 0 {
				w.confirmedReverts[approvalEvidenceKey(requirement)] = struct{}{}
			}
			continue
		}
		manager := w.Managers[record.Chain]
		if manager == nil {
			return fmt.Errorf("approval recovery manager for %s is unavailable", record.Chain)
		}
		record.Identity.Account = manager.Account()
		state, reconcileErr := reconcileApprovalStatus(
			ctx, manager, record.Identity, record.ID, record.Token,
		)
		if reconcileErr != nil {
			_ = w.Journal.SetApprovalState(context.WithoutCancel(ctx), record.ID, "outcome_unknown", time.Now().UTC())
			return fmt.Errorf("approval %s outcome remains unknown: %w", record.ID, reconcileErr)
		}
		switch state {
		case execution.StateConfirmedSuccess:
			if err := w.Journal.SetApprovalState(ctx, record.ID, "confirmed", time.Now().UTC()); err != nil {
				return err
			}
		case execution.StateConfirmedRevert:
			if err := w.Journal.SetApprovalState(ctx, record.ID, "confirmed_revert", time.Now().UTC()); err != nil {
				return err
			}
			if record.Amount.Cmp(corelive.MaximumAllowance) == 0 {
				w.confirmedReverts[approvalEvidenceKey(requirement)] = struct{}{}
			}
		default:
			_ = w.Journal.SetApprovalState(context.WithoutCancel(ctx), record.ID, "outcome_unknown", time.Now().UTC())
			return fmt.Errorf("approval %s outcome remains unknown", record.ID)
		}
	}
	return nil
}

func (w *EVMApprovalWriter) Approve(ctx context.Context, requirement corelive.AllowanceRequirement, amount *big.Int) error {
	manager := w.Managers[requirement.Chain]
	token := w.TokenAddresses[requirement.Chain][requirement.Token]
	if manager == nil || token == (common.Address{}) || w.Journal == nil || amount == nil || amount.Sign() < 0 ||
		!common.IsHexAddress(requirement.Spender) || common.HexToAddress(requirement.Spender) == (common.Address{}) {
		return fmt.Errorf("approval execution configuration is incomplete")
	}
	if amount.Cmp(corelive.MaximumAllowance) == 0 {
		key := approvalEvidenceKey(requirement)
		if _, demonstrated := w.confirmedReverts[key]; demonstrated {
			delete(w.confirmedReverts, key)
			return &corelive.ApprovalError{ConfirmedRevert: true, Err: fmt.Errorf("previous direct maximum approval confirmed reverted")}
		}
	}
	clock := w.Clock
	if clock == nil {
		clock = time.Now
	}
	gas := w.GasLimit
	if gas == 0 {
		gas = 100_000
	}
	spender := common.HexToAddress(requirement.Spender)
	payload := append(gethcrypto.Keccak256([]byte("approve(address,uint256)"))[:4], common.LeftPadBytes(spender.Bytes(), 32)...)
	payload = append(payload, common.LeftPadBytes(amount.Bytes(), 32)...)
	input, _ := market.NewTokenAmount(requirement.Token, new(big.Int).Set(amount))
	if input.IsZero() {
		input, _ = market.NewTokenAmount(requirement.Token, big.NewInt(1))
	}
	leg := execution.Leg{ID: "startup-approval", Side: execution.LegBuy, Chain: requirement.Chain,
		Account: manager.Account(), Market: "startup-approval", Input: input, ExpectedOutput: input}
	artifact := executionport.Artifact{Leg: leg, Payload: payload, Metadata: map[string]string{
		"to": token.Hex(), "value": "0", "gas_limit": fmt.Sprint(gas),
	}, BuiltAt: clock().UTC()}
	prepared, err := manager.Prepare(ctx, artifact)
	if err != nil {
		return err
	}
	now := clock().UTC()
	sum := sha256.Sum256([]byte(strings.Join([]string{string(requirement.Chain), string(requirement.Token), spender.Hex(), amount.String(), prepared.Identity.Hash}, "\x00")))
	id := "approval-" + hex.EncodeToString(sum[:16])
	if err := w.Journal.RecordApproval(ctx, persistenceport.ApprovalRecord{ID: id, Chain: requirement.Chain,
		Token: requirement.Token, Spender: spender.Hex(), Amount: new(big.Int).Set(amount), Identity: prepared.Identity,
		State: "prepared", CreatedAt: now, UpdatedAt: now}); err != nil {
		return err
	}
	broadcast, err := chainport.BroadcastPrimary(ctx, manager, prepared)
	if broadcast.Disposition == chainport.BroadcastRejected {
		_ = w.Journal.SetApprovalState(context.WithoutCancel(ctx), id, "broadcast_rejected", clock().UTC())
		if err == nil {
			err = fmt.Errorf("approval broadcast rejected")
		}
		return err
	}
	state := "broadcast"
	if broadcast.Disposition == chainport.BroadcastPossible {
		state = "outcome_unknown"
	}
	if stateErr := w.Journal.SetApprovalState(context.WithoutCancel(ctx), id, state, clock().UTC()); stateErr != nil {
		return stateErr
	}
	interval := w.PollInterval
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	timeout := w.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		technical, reconcileErr := reconcileApprovalStatus(
			waitCtx, manager, prepared.Identity, id, requirement.Token,
		)
		if reconcileErr == nil {
			switch technical {
			case execution.StateConfirmedSuccess:
				return w.Journal.SetApprovalState(context.WithoutCancel(ctx), id, "confirmed", clock().UTC())
			case execution.StateConfirmedRevert:
				_ = w.Journal.SetApprovalState(context.WithoutCancel(ctx), id, "confirmed_revert", clock().UTC())
				return &corelive.ApprovalError{ConfirmedRevert: true, Err: fmt.Errorf("approval transaction reverted")}
			}
		}
		select {
		case <-waitCtx.Done():
			_ = w.Journal.SetApprovalState(context.WithoutCancel(ctx), id, "outcome_unknown", clock().UTC())
			return &corelive.ApprovalError{Err: fmt.Errorf("approval outcome remains unknown: %w", waitCtx.Err())}
		case <-ticker.C:
		}
	}
}

var _ corelive.ApprovalWriter = (*EVMApprovalWriter)(nil)
