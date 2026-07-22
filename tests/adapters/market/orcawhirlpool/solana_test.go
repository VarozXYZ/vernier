package orcawhirlpool_test

import (
	"context"
	"encoding/binary"
	"math/big"
	"testing"

	"github.com/VarozXYZ/vernier/adapters/chain/solana"
	"github.com/VarozXYZ/vernier/adapters/feed/solanalogs"
	"github.com/VarozXYZ/vernier/adapters/market/orcawhirlpool"
	"github.com/VarozXYZ/vernier/domain/market"
)

type fakeNetwork struct{ account solana.Account }

func (f fakeNetwork) CurrentSlot(context.Context) (uint64, error) { return 7, nil }
func (f fakeNetwork) SubscribeLogs(context.Context, string) (solana.LogsSubscription, error) {
	return nil, nil
}
func (f fakeNetwork) ReadAccount(context.Context, string) (solana.Account, error) {
	return f.account, nil
}

type programReaderNetwork struct {
	fakeNetwork
	accounts []solana.ProgramAccount
}

func (f programReaderNetwork) ReadProgramAccounts(_ context.Context, _ string, filters []solana.ProgramFilter) ([]solana.ProgramAccount, error) {
	if len(filters) == 0 || filters[0].DataSize == nil || *filters[0].DataSize != 9988 {
		return nil, nil
	}
	return f.accounts, nil
}

func TestWhirlpoolAccountDecoder(t *testing.T) {
	data := make([]byte, 653)
	copy(data, []byte{0x3f, 0x95, 0xd1, 0x0c, 0xe1, 0x80, 0x63, 0x09})
	binary.LittleEndian.PutUint16(data[41:43], 64)
	binary.LittleEndian.PutUint16(data[45:47], 30)
	binary.LittleEndian.PutUint64(data[49:57], 1000)
	binary.LittleEndian.PutUint64(data[65:73], 1<<32)
	binary.LittleEndian.PutUint32(data[81:85], 12)
	decoder, err := orcawhirlpool.NewDecoder("pool")
	if err != nil {
		t.Fatal(err)
	}
	event, err := decoder.Bootstrap(context.Background(), fakeNetwork{account: solana.Account{Data: data}}, 7)
	if err != nil {
		t.Fatal(err)
	}
	if event.EventKind() != "orca_whirlpool/state/v1" {
		t.Fatalf("unexpected event kind %q", event.EventKind())
	}
	child := market.Market{ID: "child", BaseToken: "a", QuoteToken: "b"}
	reducer := orcawhirlpool.Reducer{}
	state, _, err := reducer.Reduce(context.Background(), nil, event)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := state.(orcawhirlpool.Snapshot)
	if snapshot.TickSpacing() != 64 || snapshot.FeeBPS() != 1 || snapshot.SqrtPriceX64().Cmp(new(big.Int).Lsh(big.NewInt(1), 32)) != 0 {
		t.Fatalf("decoded Whirlpool state is incorrect")
	}
	_ = child
}

func TestWhirlpoolAccountDecodeDoesNotReadNetwork(t *testing.T) {
	data := make([]byte, 653)
	copy(data, []byte{0x3f, 0x95, 0xd1, 0x0c, 0xe1, 0x80, 0x63, 0x09})
	binary.LittleEndian.PutUint16(data[41:43], 64)
	binary.LittleEndian.PutUint16(data[45:47], 30)
	binary.LittleEndian.PutUint64(data[49:57], 1000)
	binary.LittleEndian.PutUint64(data[65:73], 1<<32)
	binary.LittleEndian.PutUint32(data[81:85], 12)
	decoder, err := orcawhirlpool.NewDecoder("pool")
	if err != nil {
		t.Fatal(err)
	}
	events, err := decoder.DecodeAccount(context.Background(), solana.AccountNotification{Slot: 8, Account: "pool", Value: solana.Account{Data: data}})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].EventKind() != "orca_whirlpool/pool/v1" {
		t.Fatalf("unexpected account events: %+v", events)
	}
}

func TestWhirlpoolProgramSubscriptionsDecodeDynamicArrays(t *testing.T) {
	pool := "11111111111111111111111111111111"
	decoder, err := orcawhirlpool.NewDecoder(pool)
	if err != nil {
		t.Fatal(err)
	}
	poolData := make([]byte, 653)
	copy(poolData, []byte{0x3f, 0x95, 0xd1, 0x0c, 0xe1, 0x80, 0x63, 0x09})
	binary.LittleEndian.PutUint16(poolData[41:43], 64)
	binary.LittleEndian.PutUint16(poolData[45:47], 30)
	binary.LittleEndian.PutUint64(poolData[49:57], 1000)
	binary.LittleEndian.PutUint64(poolData[65:73], 1<<32)
	binary.LittleEndian.PutUint32(poolData[81:85], 12)
	if _, err := decoder.Bootstrap(context.Background(), fakeNetwork{account: solana.Account{Data: poolData}}, 7); err != nil {
		t.Fatal(err)
	}
	if len(decoder.ProgramSubscriptions()) != 2 {
		t.Fatalf("program subscriptions = %d, want 2", len(decoder.ProgramSubscriptions()))
	}
	dynamic := make([]byte, 8+4+32+16+88*113)
	copy(dynamic, []byte{0x11, 0xd8, 0xf6, 0x8e, 0xe1, 0xc7, 0xda, 0x38})
	binary.LittleEndian.PutUint32(dynamic[8:12], 0)
	dynamic[60] = 1
	binary.LittleEndian.PutUint64(dynamic[61:69], 25)
	events, err := decoder.DecodeProgram(context.Background(), solana.ProgramNotification{Slot: 8, Account: "tick-array", Value: solana.Account{Data: dynamic}})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].EventKind() != "orca_whirlpool/tick_array/v1" {
		t.Fatalf("unexpected dynamic events: %+v", events)
	}
}

func TestWhirlpoolBootstrapEnumeratesProgramTickArrays(t *testing.T) {
	pool := "11111111111111111111111111111111"
	decoder, err := orcawhirlpool.NewDecoder(pool)
	if err != nil {
		t.Fatal(err)
	}
	poolData := make([]byte, 653)
	copy(poolData, []byte{0x3f, 0x95, 0xd1, 0x0c, 0xe1, 0x80, 0x63, 0x09})
	binary.LittleEndian.PutUint16(poolData[41:43], 64)
	binary.LittleEndian.PutUint16(poolData[45:47], 30)
	binary.LittleEndian.PutUint64(poolData[49:57], 1000)
	binary.LittleEndian.PutUint64(poolData[65:73], 1<<32)
	binary.LittleEndian.PutUint32(poolData[81:85], 12)
	tickArray := make([]byte, 9988)
	copy(tickArray, []byte{69, 97, 189, 190, 110, 7, 66, 187})
	binary.LittleEndian.PutUint32(tickArray[8:12], 0)
	tickArray[12] = 1
	binary.LittleEndian.PutUint64(tickArray[13:21], 25)
	programNetwork := programReaderNetwork{fakeNetwork: fakeNetwork{account: solana.Account{Data: poolData}}, accounts: []solana.ProgramAccount{{Account: "tick-array", Value: solana.Account{Data: tickArray}}}}
	event, err := decoder.Bootstrap(context.Background(), programNetwork, 7)
	if err != nil {
		t.Fatal(err)
	}
	snapshotData, _, err := (orcawhirlpool.Reducer{}).Reduce(context.Background(), nil, event)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := snapshotData.(orcawhirlpool.Snapshot)
	if !snapshot.FullCoverage() || snapshot.MinTick() != 0 || snapshot.MaxTick() != 5568 {
		t.Fatalf("unexpected enumerated coverage: full=%v min=%d max=%d", snapshot.FullCoverage(), snapshot.MinTick(), snapshot.MaxTick())
	}
}

var _ solanalogs.Network = fakeNetwork{}
