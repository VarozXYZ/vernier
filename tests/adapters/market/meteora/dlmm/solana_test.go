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
	binary.LittleEndian.PutUint16(lbPair[80:82], 16)
	// Canonical StaticParameters and VariableParameters layout. Keeping the
	// accumulator non-zero while the reference is zero catches the historical
	// decoder bug that read offsets 40 and 44 as reference and index.
	binary.LittleEndian.PutUint16(lbPair[8:10], 1_875)
	binary.LittleEndian.PutUint16(lbPair[10:12], 30)
	binary.LittleEndian.PutUint16(lbPair[12:14], 600)
	binary.LittleEndian.PutUint16(lbPair[14:16], 5_000)
	binary.LittleEndian.PutUint32(lbPair[16:20], 30_000)
	binary.LittleEndian.PutUint32(lbPair[20:24], 350_000)
	binary.LittleEndian.PutUint32(lbPair[40:44], 350_000)
	binary.LittleEndian.PutUint32(lbPair[44:48], 0)
	binary.LittleEndian.PutUint32(lbPair[48:52], 0)
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
	data, _, err := (dlmm.Reducer{}).Reduce(context.Background(), nil, event)
	if err != nil {
		t.Fatal(err)
	}
	state := data.(dlmm.Snapshot)
	if got, want := state.FeeRateForBin(0), uint64(300_000); got != want {
		t.Fatalf("decoded active-bin fee rate = %d, want %d", got, want)
	}
}

var _ solanalogs.Network = fakeNetwork{}

type countingNetwork struct {
	fakeNetwork
	reads int
}

func (n *countingNetwork) ReadAccount(ctx context.Context, address string) (solana.Account, error) {
	n.reads++
	return n.fakeNetwork.ReadAccount(ctx, address)
}

func (n *countingNetwork) ReadMultipleAccounts(ctx context.Context, addresses []string) ([]solana.Account, error) {
	n.reads++
	return n.fakeNetwork.ReadMultipleAccounts(ctx, addresses)
}

func TestDLMMBatchUpdatesUseWebSocketDataWithoutHTTPReads(t *testing.T) {
	poolBytes := [32]byte{7}
	pool := solana.EncodePublicKey(poolBytes)
	lbPair := make([]byte, 904)
	copy(lbPair, []byte{33, 11, 49, 98, 181, 101, 177, 13})
	binary.LittleEndian.PutUint16(lbPair[80:82], 10)
	lbPair[584] = 1
	binArray := make([]byte, 10136)
	copy(binArray, []byte{92, 142, 92, 220, 5, 148, 70, 181})
	copy(binArray[24:56], poolBytes[:])
	binary.LittleEndian.PutUint64(binArray[56:64], 1_000)
	binary.LittleEndian.PutUint64(binArray[64:72], 2_000)
	binary.LittleEndian.PutUint64(binArray[72:80], 2)
	programID, _ := solana.DecodePublicKey("LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo")
	address, _, err := solana.FindProgramAddress([][]byte{[]byte("bin_array"), poolBytes[:], make([]byte, 8)}, programID)
	if err != nil {
		t.Fatal(err)
	}
	network := &countingNetwork{fakeNetwork: fakeNetwork{accounts: map[string]solana.Account{
		pool:                            {Data: lbPair},
		solana.EncodePublicKey(address): {Data: binArray},
	}}}
	decoder, err := dlmm.NewDecoder(pool)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decoder.Bootstrap(context.Background(), network, 9); err != nil {
		t.Fatal(err)
	}
	bootstrapReads := network.reads
	binary.LittleEndian.PutUint64(binArray[56:64], 1_500)
	events, err := decoder.DecodeAccountBatch(context.Background(), []solanalogs.AccountChange{{
		Slot: 10, Account: solana.EncodePublicKey(address), Value: solana.Account{Data: binArray}, Program: true,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if network.reads != bootstrapReads {
		t.Fatalf("account event performed HTTP reads: before=%d after=%d", bootstrapReads, network.reads)
	}
	if len(events) != 1 || events[0].EventKind() != "meteora_dlmm/state/v2" {
		t.Fatalf("unexpected batch events: %+v", events)
	}
	requests := decoder.ProgramSubscriptions()
	if len(requests) != 1 || len(requests[0].Filters) != 2 || requests[0].Filters[0].DataSize == nil || *requests[0].Filters[0].DataSize != 10136 {
		t.Fatalf("unexpected program subscription: %+v", requests)
	}
}

func TestDecoderMatchesOnlyEconomicLogs(t *testing.T) {
	decoder, err := dlmm.NewDecoder("synthetic-pool")
	if err != nil {
		t.Fatal(err)
	}
	for _, instruction := range []string{"Swap", "SwapExactOut", "AddLiquidityByWeight", "RemoveAllLiquidity"} {
		if !decoder.IsEconomicLog(solana.LogNotification{Logs: []string{"Program log: Instruction: " + instruction}}) {
			t.Fatalf("expected %s to be economic", instruction)
		}
	}
	for _, instruction := range []string{"ClaimFee", "InitializePosition", "ClosePosition"} {
		if decoder.IsEconomicLog(solana.LogNotification{Logs: []string{"Program log: Instruction: " + instruction}}) {
			t.Fatalf("expected %s to be administrative", instruction)
		}
	}
}
