package acrossbridgecanary_test

import (
	"bytes"
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/adapters/crosschain/across"
	"github.com/VarozXYZ/vernier/internal/acrossbridgecanary"
)

type confirmationTestWatcher struct {
	balance           *big.Int
	awaitErr          error
	evidence          *acrossbridgecanary.DestinationEvidence
	recoveredEvidence *acrossbridgecanary.DestinationEvidence
}

func (w *confirmationTestWatcher) Await(
	ctx context.Context,
	_ *big.Int,
) (acrossbridgecanary.DestinationEvidence, error) {
	if w.awaitErr != nil {
		return acrossbridgecanary.DestinationEvidence{}, w.awaitErr
	}
	if w.evidence != nil {
		return *w.evidence, nil
	}
	<-ctx.Done()
	return acrossbridgecanary.DestinationEvidence{}, ctx.Err()
}

func TestDestinationBalanceEventCannotConfirmWithoutAcrossFill(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	target := big.NewInt(1_000_000)
	_, err := acrossbridgecanary.AwaitDestinationConfirmation(
		ctx,
		&bytes.Buffer{},
		&confirmationTestWatcher{
			balance: target,
			evidence: &acrossbridgecanary.DestinationEvidence{
				Balance: target,
				Source:  "solana_account_websocket",
			},
		},
		confirmationTestStatus{status: across.Status{
			State: across.DepositPending,
		}},
		"source",
		big.NewInt(0),
		target,
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v, want context deadline", err)
	}
}

func (w *confirmationTestWatcher) Balance(context.Context) (*big.Int, error) {
	return new(big.Int).Set(w.balance), nil
}

func (*confirmationTestWatcher) Close() {}

func (w *confirmationTestWatcher) TransferByIdentity(
	_ context.Context,
	identity string,
) (acrossbridgecanary.DestinationEvidence, bool, error) {
	if w.recoveredEvidence == nil || w.recoveredEvidence.Identity != identity {
		return acrossbridgecanary.DestinationEvidence{}, false, nil
	}
	return *w.recoveredEvidence, true, nil
}

type confirmationTestStatus struct {
	status across.Status
	err    error
}

func (s confirmationTestStatus) DepositStatus(
	context.Context,
	string,
) (across.Status, error) {
	return s.status, s.err
}

func TestAcrossFilledStatusAndBalanceConfirmWhenWebsocketMissesEvent(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var output bytes.Buffer
	evidence, err := acrossbridgecanary.AwaitDestinationConfirmation(
		ctx,
		&output,
		&confirmationTestWatcher{
			balance:  big.NewInt(1_000_000),
			awaitErr: errors.New("websocket event unavailable"),
		},
		confirmationTestStatus{status: across.Status{
			State:           across.DepositFilled,
			FillTransaction: "0xfill",
		}},
		"source",
		big.NewInt(0),
		big.NewInt(1_000_000),
	)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Identity != "0xfill" ||
		evidence.Balance.Cmp(big.NewInt(1_000_000)) != 0 ||
		evidence.Source != "across_status+destination_balance" {
		t.Fatalf("unexpected evidence: %+v", evidence)
	}
}

func TestAcrossFillReceiptConfirmsAttributedTransferDespiteLowerNetBalance(
	t *testing.T,
) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	before := big.NewInt(1_000_000)
	minimum := big.NewInt(500_000)
	actual := big.NewInt(510_000)
	evidence, err := acrossbridgecanary.AwaitDestinationConfirmation(
		ctx,
		&bytes.Buffer{},
		&confirmationTestWatcher{
			// A concurrent debit keeps the wallet below before+minimum even
			// though the correlated destination transfer was successful.
			balance:  big.NewInt(1_010_000),
			awaitErr: errors.New("websocket event was already consumed"),
			recoveredEvidence: &acrossbridgecanary.DestinationEvidence{
				Identity: "0xfill", Amount: actual,
				Balance: big.NewInt(1_010_000), Source: "evm_transfer_receipt",
			},
		},
		confirmationTestStatus{status: across.Status{
			State: across.DepositFilled, FillTransaction: "0xfill",
		}},
		"source",
		before,
		minimum,
	)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Amount == nil || evidence.Amount.Cmp(actual) != 0 ||
		evidence.Balance.Cmp(big.NewInt(1_510_000)) != 0 ||
		evidence.Source != "evm_transfer_receipt+across_status" {
		t.Fatalf("unexpected evidence: %+v", evidence)
	}
}
