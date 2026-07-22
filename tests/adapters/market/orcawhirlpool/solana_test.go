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

var _ solanalogs.Network = fakeNetwork{}
