package evmactivity_test

import (
	"context"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/VarozXYZ/vernier/adapters/chain/evm"
	"github.com/VarozXYZ/vernier/adapters/feed/evmactivity"
	"github.com/VarozXYZ/vernier/adapters/feed/evmlogs"
)

func TestVenueFiltersEconomicEventsAndCarriesTransactionIdentity(t *testing.T) {
	venue, err := evmactivity.NewVenue("synthetic-pool", "0x0000000000000000000000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	filter := venue.Filter()
	if filter.Address != common.HexToAddress("0x0000000000000000000000000000000000000001") || len(filter.Topics) != 3 {
		t.Fatalf("unexpected filter: %+v", filter)
	}
	tx := common.HexToHash("0x1234")
	data, err := venue.DecodeLog(context.Background(), nil, evm.BlockReference{Number: 1}, types.Log{
		TxHash: tx,
		Topics: []common.Hash{crypto.Keccak256Hash([]byte("Swap(address,address,int256,int256,uint160,uint128,int24)"))},
	})
	if err != nil {
		t.Fatal(err)
	}
	event := data.(evmactivity.TriggerEvent)
	reference := event.EventReference()
	if reference.Kind != evmlogs.TransactionReferenceKind || reference.Value != tx.Hex() {
		t.Fatalf("transaction reference was not preserved: %+v", reference)
	}
	if _, err := venue.DecodeLog(context.Background(), nil, evm.BlockReference{}, types.Log{
		Topics: []common.Hash{crypto.Keccak256Hash([]byte("Collect(address,address,int24,int24,uint128,uint128)"))},
	}); err == nil {
		t.Fatal("administrative/non-trigger event was accepted")
	}
}
