package dlmm_test

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/VarozXYZ/vernier/adapters/chain/solana"
	"github.com/VarozXYZ/vernier/adapters/feed/solanalogs"
	"github.com/VarozXYZ/vernier/adapters/market/meteora/dlmm"
)

type fakeNetwork struct{ accounts map[string]solana.Account }

func (f fakeNetwork) CurrentSlot(context.Context) (uint64, error) { return 9, nil }
func (f fakeNetwork) SubscribeLogs(context.Context, string) (solana.LogsSubscription, error) {
	return nil, nil
}
func (f fakeNetwork) ReadAccount(_ context.Context, address string) (solana.Account, error) {
	return f.accounts[address], nil
}
func (f fakeNetwork) ReadMultipleAccounts(_ context.Context, addresses []string) ([]solana.Account, error) {
	result := make([]solana.Account, len(addresses))
	for i, address := range addresses {
		result[i] = f.accounts[address]
	}
	return result, nil
}

func TestDLMMAccountDecoder(t *testing.T) {
	pool := solana.EncodePublicKey([32]byte{4})
	poolBytes := [32]byte{4}
	lbPair := make([]byte, 904)
	copy(lbPair, []byte{33, 11, 49, 98, 181, 101, 177, 13})
	binary.LittleEndian.PutUint32(lbPair[76:80], 0)
	binary.LittleEndian.PutUint16(lbPair[80:82], 10)
	// One initialized bin-array bitmap entry (index 0).
	lbPair[584] = 1
	decoder, err := dlmm.NewDecoder(pool)
	if err != nil {
		t.Fatal(err)
	}
	programAccounts := map[string]solana.Account{}
	// The decoder requests the PDA derived from the pool and the canonical
	// program; populate it lazily after observing the requested address.
	network := fakeNetwork{accounts: programAccounts}
	binArrayData := make([]byte, 10136)
	copy(binArrayData, []byte{92, 142, 92, 220, 5, 148, 70, 181})
	copy(binArrayData[24:56], poolBytes[:])
	binary.LittleEndian.PutUint64(binArrayData[56:64], 1_000)
	binary.LittleEndian.PutUint64(binArrayData[64:72], 2_000)
	binary.LittleEndian.PutUint64(binArrayData[72:80], 2)
	binary.LittleEndian.PutUint64(binArrayData[80:88], 0)
	// Account index 0 and its active bin are returned under the only address
	// requested by this synthetic bitmap.
	programID, err := solana.DecodePublicKey("LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo")
	if err != nil {
		t.Fatal(err)
	}
	address, _, err := solana.FindProgramAddress([][]byte{[]byte("bin_array"), poolBytes[:], make([]byte, 8)}, programID)
	if err != nil {
		t.Fatal(err)
	}
	network.accounts[solana.EncodePublicKey(address)] = solana.Account{Data: binArrayData}
	// The pool address is not used as a map key by the fake after construction.
	network.accounts[pool] = solana.Account{Data: lbPair}
	event, err := decoder.Bootstrap(context.Background(), network, 9)
	if err != nil {
		t.Fatal(err)
	}
	if event.EventKind() != "meteora_dlmm/state/v2" {
		t.Fatalf("unexpected event kind %q", event.EventKind())
	}
}

var _ solanalogs.Network = fakeNetwork{}
