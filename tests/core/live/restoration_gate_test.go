package live_test

import (
	"testing"
	"time"

	corelive "github.com/VarozXYZ/vernier/core/live"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
)

func restorationTrigger(sequence uint64) arbitrage.TriggerMetadata {
	return arbitrage.TriggerMetadata{Market: "market", Source: "source",
		Position:  market.SourcePosition{Kind: "block", Value: sequence},
		Reference: market.SourceReference{Kind: "transaction", Value: "tx"}, At: time.Unix(int64(sequence), 0)}
}

func TestRestorationGateOnlyOverlapsTwoQuoteReturns(t *testing.T) {
	gate, err := corelive.NewRestorationGate(corelive.RestorationSnapshot{})
	if err != nil {
		t.Fatal(err)
	}
	if err := gate.BeginOperation(); err != nil {
		t.Fatal(err)
	}
	if gate.CanEvaluate(true) {
		t.Fatal("base pending did not close admission")
	}
	if err := gate.StartQuoteReturn("q1"); err != nil {
		t.Fatal(err)
	}
	gate.CompleteBaseReturn()
	if !gate.CanEvaluate(true) {
		t.Fatal("one quote return should allow another operation")
	}
	if err := gate.BeginOperation(); err != nil {
		t.Fatal(err)
	}
	if err := gate.StartQuoteReturn("q2"); err != nil {
		t.Fatal(err)
	}
	gate.CompleteBaseReturn()
	if gate.CanEvaluate(true) {
		t.Fatal("two quote returns did not exhaust capacity")
	}
	if err := gate.BeginOperation(); err == nil {
		t.Fatal("third operation was admitted")
	}
	if err := gate.StartQuoteReturn("q3"); err == nil {
		t.Fatal("third quote return was admitted")
	}
}

func TestRestorationGateCoalescesLatestTriggerAndRequestsFreshEvaluation(t *testing.T) {
	gate, _ := corelive.NewRestorationGate(corelive.RestorationSnapshot{BasePending: true})
	if err := gate.Coalesce(restorationTrigger(1)); err != nil {
		t.Fatal(err)
	}
	if err := gate.Coalesce(restorationTrigger(2)); err != nil {
		t.Fatal(err)
	}
	if _, ok := gate.TakeFreshEvaluationRequest(true, time.Now()); ok {
		t.Fatal("request escaped while base was pending")
	}
	gate.CompleteBaseReturn()
	trigger, ok := gate.TakeFreshEvaluationRequest(true, time.Now())
	if !ok || trigger.Position.Value != 2 {
		t.Fatalf("trigger=%+v ok=%t", trigger, ok)
	}
	if _, ok := gate.TakeFreshEvaluationRequest(true, time.Now()); ok {
		t.Fatal("coalesced request was replayed")
	}
}
