package wormholewtt_test

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/VarozXYZ/vernier/adapters/crosschain/wormholewtt"
)

func TestWTTBuildsTransferRedeemAndCompletionCheck(t *testing.T) {
	adapter, err := wormholewtt.NewEVMAdapter(common.HexToAddress("0x0000000000000000000000000000000000000010"))
	if err != nil {
		t.Fatal(err)
	}
	var recipient [32]byte
	copy(recipient[12:], common.HexToAddress("0x0000000000000000000000000000000000000020").Bytes())
	transfer, err := adapter.BuildTransfer(common.HexToAddress("0x0000000000000000000000000000000000000030"), big.NewInt(1_000_000), 30, recipient, 7)
	if err != nil || len(transfer) < 4 {
		t.Fatalf("transfer=%x err=%v", transfer, err)
	}
	redeem, err := adapter.BuildRedeem([]byte{1, 2, 3})
	if err != nil || len(redeem) < 4 {
		t.Fatalf("redeem=%x err=%v", redeem, err)
	}
	vaa := []byte{1, 0, 0, 0, 0, 0, 9, 8, 7}
	check, hash, err := adapter.BuildCompletionCheck(vaa)
	if err != nil || len(check) < 4 || hash == ([32]byte{}) {
		t.Fatalf("check=%x hash=%x err=%v", check, hash, err)
	}
}

func TestWTTParsesCanonicalCoreMessageIdentity(t *testing.T) {
	core := common.HexToAddress("0x0000000000000000000000000000000000000010")
	emitter := common.HexToAddress("0x0000000000000000000000000000000000000020")
	sequence := common.LeftPadBytes(big.NewInt(42).Bytes(), 32)
	receipt := &types.Receipt{Logs: []*types.Log{{Address: core, Topics: []common.Hash{
		crypto.Keccak256Hash([]byte("LogMessagePublished(address,uint64,uint32,bytes,uint8)")),
		common.BytesToHash(common.LeftPadBytes(emitter.Bytes(), 32)),
	}, Data: sequence}}}
	message, err := wormholewtt.ParseMessageID(receipt, core, emitter, 4)
	if err != nil {
		t.Fatal(err)
	}
	if message.EmitterChain != 4 || message.Sequence != 42 || common.BytesToAddress(message.EmitterAddress[12:]) != emitter {
		t.Fatalf("unexpected message: %+v", message)
	}
}

func TestWTTVAAHashRejectsTruncatedSignatureEnvelope(t *testing.T) {
	if _, err := wormholewtt.VAAHash([]byte{1, 0, 0, 0, 0, 1}); err == nil {
		t.Fatal("expected truncated VAA rejection")
	}
}

func TestWTTTrimsEighteenDecimalsToEight(t *testing.T) {
	amount, _ := new(big.Int).SetString("4473904820592314590", 10)
	transferable, dust, err := wormholewtt.TrimTransferAmount(amount, 18)
	if err != nil {
		t.Fatal(err)
	}
	if transferable.String() != "4473904820000000000" || dust.String() != "592314590" {
		t.Fatalf("transferable=%s dust=%s", transferable, dust)
	}
}
