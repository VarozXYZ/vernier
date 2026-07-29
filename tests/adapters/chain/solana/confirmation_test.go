package solana_test

import (
	"context"
	"encoding/json"
	"math/big"
	"testing"
	"time"

	solanaadapter "github.com/VarozXYZ/vernier/adapters/chain/solana"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
)

type transactionSubscription struct {
	errors        chan error
	notifications chan solanaadapter.TransactionNotification
}

func (s *transactionSubscription) Err() <-chan error { return s.errors }
func (s *transactionSubscription) Notifications() <-chan solanaadapter.TransactionNotification {
	return s.notifications
}
func (*transactionSubscription) Unsubscribe() {}

type transactionSubscriber struct {
	subscription *transactionSubscription
}

func (s transactionSubscriber) SubscribeTransactions(context.Context, string) (solanaadapter.TransactionSubscription, error) {
	return s.subscription, nil
}

func TestSolanaConfirmationUsesBufferedTransactionBalanceDeltas(t *testing.T) {
	now := time.Date(2026, 7, 27, 20, 30, 0, 0, time.UTC)
	decoder, err := solanaadapter.NewSPLBalanceDecoder(solanaadapter.SPLBalanceDecoderConfig{
		Owner: "owner",
		TokenMints: map[market.TokenID]string{
			"quote": "quote-mint",
			"base":  "base-mint",
		},
		Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	subscription := &transactionSubscription{
		errors:        make(chan error),
		notifications: make(chan solanaadapter.TransactionNotification, 1),
	}
	source, err := solanaadapter.NewConfirmationSource(solanaadapter.ConfirmationSourceConfig{
		AccountAddress: "owner",
		Subscriber:     transactionSubscriber{subscription: subscription},
		Decoder:        decoder,
		Clock:          func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := source.Warm(ctx); err != nil {
		t.Fatal(err)
	}
	meta, _ := json.Marshal(map[string]any{
		"err":          nil,
		"fee":          5000,
		"preBalances":  []uint64{2_000_000},
		"postBalances": []uint64{1_000_000},
		"preTokenBalances": []any{
			tokenBalance("quote-mint", "owner", "1000"),
			tokenBalance("base-mint", "owner", "50"),
		},
		"postTokenBalances": []any{
			tokenBalance("quote-mint", "owner", "900"),
			tokenBalance("base-mint", "owner", "195"),
		},
	})
	subscription.notifications <- solanaadapter.TransactionNotification{
		Signature: "signature", Slot: 10, Meta: meta,
	}
	input, _ := market.NewTokenAmount("quote", big.NewInt(100))
	expected, _ := market.NewTokenAmount("base", big.NewInt(140))
	step := execution.OperationStep{
		Leg: execution.Leg{
			ID: "buy", Side: execution.LegBuy, Chain: "solana", Account: "account",
			Market: "market", Input: input, ExpectedOutput: expected,
		},
		Identity: execution.TransactionIdentity{
			Chain: "solana", Account: "account", Hash: "signature",
		},
	}
	settlement, err := source.Await(context.Background(), step)
	if err != nil {
		t.Fatal(err)
	}
	if settlement.Technical != execution.StateConfirmedSuccess ||
		settlement.Economic != execution.EconomicEffectVerified ||
		settlement.ActualIn.Units().Cmp(big.NewInt(100)) != 0 ||
		settlement.ActualOut.Units().Cmp(big.NewInt(145)) != 0 {
		t.Fatalf("settlement = %+v", settlement)
	}
	if len(settlement.Costs) != 2 ||
		settlement.Costs[0].Kind != "network_fee" ||
		settlement.Costs[1].Kind != "additional_payer_debit" ||
		settlement.Costs[0].Amount.Rat().Cmp(big.NewRat(5, 1_000_000)) != 0 ||
		settlement.Costs[1].Amount.Rat().Cmp(big.NewRat(995, 1_000_000)) != 0 {
		t.Fatalf("unexpected Solana costs: %+v", settlement.Costs)
	}
}

func tokenBalance(mint, owner, amount string) map[string]any {
	return map[string]any{
		"mint": mint, "owner": owner,
		"uiTokenAmount": map[string]any{"amount": amount},
	}
}

var _ solanaadapter.TransactionSubscriber = transactionSubscriber{}
