package solana_test

import (
	"testing"

	"github.com/VarozXYZ/vernier/adapters/chain/solana"
)

func TestPublicKeyAndPDAEncoding(t *testing.T) {
	key := [32]byte{1, 2, 3, 4, 5}
	encoded := solana.EncodePublicKey(key)
	decoded, err := solana.DecodePublicKey(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != key {
		t.Fatalf("public key round trip changed bytes")
	}
	program := [32]byte{9}
	address, _, err := solana.FindProgramAddress([][]byte{[]byte("state"), key[:]}, program)
	if err != nil {
		t.Fatal(err)
	}
	if address == [32]byte{} {
		t.Fatalf("invalid PDA result")
	}
}
