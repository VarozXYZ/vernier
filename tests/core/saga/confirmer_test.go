package saga_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/core/saga"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/inventory"
	"github.com/VarozXYZ/vernier/domain/market"
	chainport "github.com/VarozXYZ/vernier/ports/chain"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

type confirmationSource struct {
	block bool
	calls atomic.Int32
	now   time.Time
}

func (*confirmationSource) Warm(context.Context) error { return nil }

func (s *confirmationSource) Await(ctx context.Context, step execution.OperationStep) (execution.Settlement, error) {
	s.calls.Add(1)
	if s.block {
		<-ctx.Done()
		return execution.Settlement{}, ctx.Err()
	}
	return execution.Settlement{
		Identity: step.Identity, Technical: execution.StateConfirmedSuccess,
		Economic: execution.EconomicEffectVerified,
		ActualIn: step.Leg.Input, ActualOut: step.Leg.ExpectedOutput,
		ObservedAt: s.now, Evidence: "synthetic_websocket_event",
	}, nil
}

type reconciliationManager struct {
	account execution.AccountID
	calls   atomic.Int32
	now     time.Time
	input   market.TokenAmount
	output  market.TokenAmount
}

func (m *reconciliationManager) Account() execution.AccountID { return m.account }
func (*reconciliationManager) Warm(context.Context) error     { return nil }
func (*reconciliationManager) Prepare(context.Context, executionport.Artifact) (chainport.PreparedTransaction, error) {
	return chainport.PreparedTransaction{}, nil
}
func (*reconciliationManager) Broadcast(context.Context, chainport.PreparedTransaction) (chainport.BroadcastResult, error) {
	return chainport.BroadcastResult{}, nil
}
func (m *reconciliationManager) Reconcile(_ context.Context, step execution.OperationStep) (execution.Settlement, error) {
	m.calls.Add(1)
	return execution.Settlement{
		Identity: step.Identity, Technical: execution.StateConfirmedSuccess,
		Economic: execution.EconomicEffectVerified, ActualIn: m.input, ActualOut: m.output,
		ObservedAt: m.now, Evidence: "synthetic_rpc_receipt",
	}, nil
}

func TestConfirmerPrefersWebSocketAndFallsBackToRPC(t *testing.T) {
	now := time.Date(2026, 7, 27, 17, 0, 0, 0, time.UTC)
	plan := executablePlan(t, now)
	operation := preparedOperation(plan, now)
	owner, err := inventory.New(map[inventory.Key]market.TokenAmount{
		{Chain: "chain-a", Account: "account-a", Token: "quote-a"}: tokenAmount(t, "quote-a", 1_000),
		{Chain: "chain-a", Account: "account-a", Token: "base-a"}:  tokenAmount(t, "base-a", 1_000),
		{Chain: "chain-b", Account: "account-b", Token: "base-b"}:  tokenAmount(t, "base-b", 1_000),
		{Chain: "chain-b", Account: "account-b", Token: "quote-b"}: tokenAmount(t, "quote-b", 1_000),
	})
	if err != nil {
		t.Fatal(err)
	}
	var requirements []inventory.Requirement
	for _, leg := range plan.Legs() {
		requirements = append(requirements, inventory.Requirement{
			Key:    inventory.Key{Chain: leg.Chain, Account: leg.Account, Token: leg.Input.Token()},
			Amount: leg.Input,
		})
	}
	if _, err := owner.Reserve("operation-1", "operation-1", requirements); err != nil {
		t.Fatal(err)
	}
	gate := saga.NewOperationGate()
	if !gate.Restore(operation.ID) {
		t.Fatal("could not restore operation gate")
	}
	store := &settlementStore{}
	settlement, err := saga.NewSettlementCoordinator(store, owner, gate)
	if err != nil {
		t.Fatal(err)
	}
	websocketA := &confirmationSource{now: now}
	websocketB := &confirmationSource{block: true, now: now}
	managerA := &reconciliationManager{
		account: "account-a", now: now,
		input: operation.Steps[0].Leg.Input, output: operation.Steps[0].Leg.ExpectedOutput,
	}
	managerB := &reconciliationManager{
		account: "account-b", now: now,
		input: operation.Steps[1].Leg.Input, output: operation.Steps[1].Leg.ExpectedOutput,
	}
	confirmer, err := saga.NewConfirmer(saga.ConfirmerConfig{
		Settlement: settlement,
		WebSockets: map[execution.AccountID]chainport.ConfirmationSource{
			"account-a": websocketA,
			"account-b": websocketB,
		},
		Managers: map[execution.AccountID]chainport.TxManager{
			"account-a": managerA,
			"account-b": managerB,
		},
		FallbackAfter: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := confirmer.Confirm(context.Background(), operation); err != nil {
		t.Fatal(err)
	}
	if websocketA.calls.Load() != 1 || websocketB.calls.Load() != 1 ||
		managerA.calls.Load() != 0 || managerB.calls.Load() != 1 {
		t.Fatalf("confirmation calls wsA=%d wsB=%d rpcA=%d rpcB=%d",
			websocketA.calls.Load(), websocketB.calls.Load(), managerA.calls.Load(), managerB.calls.Load())
	}
	if _, active := gate.Active(); active {
		t.Fatal("complete WebSocket/RPC confirmation did not release operation gate")
	}
}

var (
	_ chainport.ConfirmationSource = (*confirmationSource)(nil)
	_ chainport.TxManager          = (*reconciliationManager)(nil)
)
