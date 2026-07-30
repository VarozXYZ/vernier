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
	balance  *big.Int
	awaitErr error
}

func (w *confirmationTestWatcher) Await(
	ctx context.Context,
	_ *big.Int,
) (acrossbridgecanary.DestinationEvidence, error) {
	if w.awaitErr != nil {
		return acrossbridgecanary.DestinationEvidence{}, w.awaitErr
	}
	<-ctx.Done()
	return acrossbridgecanary.DestinationEvidence{}, ctx.Err()
}

func (w *confirmationTestWatcher) Balance(context.Context) (*big.Int, error) {
	return new(big.Int).Set(w.balance), nil
}

func (*confirmationTestWatcher) Close() {}

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
