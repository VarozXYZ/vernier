package research_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
	runtimeresearch "github.com/VarozXYZ/vernier/runtime/research"
)

func TestTriggerGateDeduplicatesByChainAndTransaction(t *testing.T) {
	gate := runtimeresearch.NewTriggerGate(100)
	first := scheduledTrigger("chain-a", "market-a", "same-tx", 1)
	if !gate.Accept(first) {
		t.Fatal("first transaction was rejected")
	}
	if gate.Accept(first) {
		t.Fatal("same transaction on the same chain was not deduplicated")
	}
	otherChain := scheduledTrigger("chain-b", "market-b", "same-tx", 2)
	if !gate.Accept(otherChain) {
		t.Fatal("same hash on another chain was incorrectly deduplicated")
	}
}

func TestTriggerGateKeepsOnlyNewestPendingEvent(t *testing.T) {
	gate := runtimeresearch.NewTriggerGate(100)
	for index := 1; index <= 5; index++ {
		if !gate.OfferPending(scheduledTrigger("chain-a", "market-a", fmt.Sprintf("tx-%d", index), index)) {
			t.Fatalf("trigger %d was unexpectedly rejected", index)
		}
	}
	latest, ok := gate.TakePending()
	if !ok || latest.Metadata.Reference.Value != "tx-5" {
		t.Fatalf("pending trigger=%+v, want tx-5", latest)
	}
	if _, ok := gate.TakePending(); ok {
		t.Fatal("gate retained more than one pending evaluation")
	}
}

func TestTriggerGateDoesNotDeduplicateBootstrapOrSyntheticEvents(t *testing.T) {
	gate := runtimeresearch.NewTriggerGate(100)
	bootstrap := scheduledTrigger("chain-a", "market-a", "bootstrap", 1)
	for count := 0; count < 2; count++ {
		if !gate.Accept(bootstrap) {
			t.Fatal("bootstrap/health signal was deduplicated")
		}
	}
	synthetic := bootstrap
	synthetic.Metadata.Reference = market.SourceReference{Kind: "idle_timer", Value: "1"}
	if !gate.Accept(synthetic) {
		t.Fatal("first synthetic idle signal was deduplicated")
	}
	if !gate.Accept(synthetic) {
		t.Fatal("synthetic idle signal was deduplicated")
	}
}

func scheduledTrigger(chain string, marketID market.MarketID, transaction string, sequence int) runtimeresearch.ScheduledTrigger {
	at := time.Date(2026, 7, 27, 10, 0, sequence, 0, time.UTC)
	metadata := arbitrage.TriggerMetadata{
		Market: marketID, Source: "synthetic/feed",
		Position:  market.SourcePosition{Kind: "block", Value: uint64(sequence)},
		Reference: market.SourceReference{Kind: "evm_transaction_hash", Value: transaction}, At: at,
	}
	return runtimeresearch.ScheduledTrigger{
		Chain: chain, Market: marketID, TriggeredAt: at, Metadata: metadata, HasMetadata: true,
	}
}
