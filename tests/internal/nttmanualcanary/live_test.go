package nttmanualcanary_test

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/internal/nttmanualcanary"
)

func TestRebaseTransferUnitsBetweenTokenPrecisions(t *testing.T) {
	t.Parallel()

	solanaToEVM, err := nttmanualcanary.RebaseTransferUnits(
		big.NewInt(4_983_035_000),
		9,
		18,
	)
	if err != nil {
		t.Fatal(err)
	}
	if solanaToEVM.String() != "4983035000000000000" {
		t.Fatalf("Solana to EVM units = %s", solanaToEVM)
	}

	evmToSolana, err := nttmanualcanary.RebaseTransferUnits(
		mustBigInt(t, "4983035000000000000"),
		18,
		9,
	)
	if err != nil {
		t.Fatal(err)
	}
	if evmToSolana.String() != "4983035000" {
		t.Fatalf("EVM to Solana units = %s", evmToSolana)
	}
}

func TestRebaseTransferUnitsRejectsDestinationDust(t *testing.T) {
	t.Parallel()

	_, err := nttmanualcanary.RebaseTransferUnits(
		mustBigInt(t, "4983035000000000001"),
		18,
		9,
	)
	if err == nil || !strings.Contains(err.Error(), "cannot be represented") {
		t.Fatalf("error = %v", err)
	}
}

func TestAwaitDestinationBalanceVisibilityWaitsForLaggingRPC(t *testing.T) {
	t.Parallel()

	responses := []*big.Int{
		big.NewInt(100),
		big.NewInt(100),
		big.NewInt(151),
	}
	index := 0
	after, delta, attempts, err := nttmanualcanary.AwaitDestinationBalanceVisibility(
		context.Background(),
		big.NewInt(100),
		big.NewInt(50),
		time.Second,
		time.Millisecond,
		func(context.Context) (*big.Int, error) {
			response := responses[index]
			if index < len(responses)-1 {
				index++
			}
			return new(big.Int).Set(response), nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 3 || after.String() != "151" || delta.String() != "51" {
		t.Fatalf(
			"attempts=%d after=%s delta=%s",
			attempts,
			after,
			delta,
		)
	}
}

func TestAwaitDestinationBalanceVisibilityRecoversFromReadError(t *testing.T) {
	t.Parallel()

	attempts := 0
	after, delta, observedAttempts, err := nttmanualcanary.AwaitDestinationBalanceVisibility(
		context.Background(),
		big.NewInt(100),
		big.NewInt(50),
		time.Second,
		time.Millisecond,
		func(context.Context) (*big.Int, error) {
			attempts++
			if attempts == 1 {
				return nil, errors.New("temporary RPC failure")
			}
			return big.NewInt(150), nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if observedAttempts != 2 || after.String() != "150" ||
		delta.String() != "50" {
		t.Fatalf(
			"attempts=%d after=%s delta=%s",
			observedAttempts,
			after,
			delta,
		)
	}
}

func TestAwaitDestinationBalanceVisibilityTimesOutWithoutDelta(t *testing.T) {
	t.Parallel()

	_, _, attempts, err := nttmanualcanary.AwaitDestinationBalanceVisibility(
		context.Background(),
		big.NewInt(100),
		big.NewInt(50),
		8*time.Millisecond,
		time.Millisecond,
		func(context.Context) (*big.Int, error) {
			return big.NewInt(100), nil
		},
	)
	if err == nil ||
		!strings.Contains(err.Error(), "before=100 latest=100 expected_delta=50") {
		t.Fatalf("attempts=%d error=%v", attempts, err)
	}
	if attempts < 2 {
		t.Fatalf("attempts=%d, want at least 2", attempts)
	}
}

func TestAwaitEVMSourceBalanceVisibilityWaitsForFreshRPCBlock(t *testing.T) {
	t.Parallel()

	type observation struct {
		balance *big.Int
		block   uint64
	}
	observations := []observation{
		{balance: big.NewInt(97), block: 100},
		{balance: big.NewInt(97), block: 100},
		{balance: big.NewInt(3_802), block: 101},
	}
	index := 0
	balance, block, attempts, err :=
		nttmanualcanary.AwaitEVMSourceBalanceVisibility(
			context.Background(),
			big.NewInt(3_800),
			time.Second,
			time.Millisecond,
			func(context.Context) (*big.Int, uint64, error) {
				observation := observations[index]
				if index < len(observations)-1 {
					index++
				}
				return new(big.Int).Set(observation.balance),
					observation.block,
					nil
			},
		)
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 3 || block != 101 || balance.String() != "3802" {
		t.Fatalf(
			"attempts=%d block=%d balance=%s",
			attempts,
			block,
			balance,
		)
	}
}

func TestAwaitEVMSourceBalanceVisibilityReportsStaleBlock(t *testing.T) {
	t.Parallel()

	_, block, attempts, err :=
		nttmanualcanary.AwaitEVMSourceBalanceVisibility(
			context.Background(),
			big.NewInt(3_800),
			8*time.Millisecond,
			time.Millisecond,
			func(context.Context) (*big.Int, uint64, error) {
				return big.NewInt(97), 100, nil
			},
		)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			"latest=97 required=3800 observed_block=100",
		) {
		t.Fatalf("attempts=%d block=%d error=%v", attempts, block, err)
	}
	if attempts < 2 || block != 100 {
		t.Fatalf("attempts=%d block=%d", attempts, block)
	}
}

func mustBigInt(t *testing.T, value string) *big.Int {
	t.Helper()
	result, ok := new(big.Int).SetString(value, 10)
	if !ok {
		t.Fatalf("invalid test integer %q", value)
	}
	return result
}
