package nttmanualcanary_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/VarozXYZ/vernier/internal/nttmanualcanary"
)

func TestLoadEVMApprovalTargetUsesPrivateProfileTokenAndManager(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ntt.yaml")
	profile := `schema_version: 1
guardian_rpc_urls:
  - https://guardian.invalid
solana:
  rpc_url_env: SYNTHETIC_SOLANA_RPC
  signer_env: SYNTHETIC_SOLANA_SIGNER
  wormhole_chain: 17
  manager: ComputeBudget111111111111111111111111111111
  transceiver: MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr
  wormhole_core: AddressLookupTab1e1111111111111111111111111
  token_mint: So11111111111111111111111111111111111111112
evm:
  rpc_url_env: SYNTHETIC_EVM_RPC
  signer_env: SYNTHETIC_EVM_SIGNER
  chain_id: "91337"
  wormhole_chain: 23
  token: "0x0000000000000000000000000000000000000011"
  manager: "0x0000000000000000000000000000000000000022"
  transceiver: "0x0000000000000000000000000000000000000033"
  wormhole_core: "0x0000000000000000000000000000000000000044"
`
	if err := os.WriteFile(path, []byte(profile), 0o600); err != nil {
		t.Fatal(err)
	}
	target, err := nttmanualcanary.LoadEVMApprovalTarget(path)
	if err != nil {
		t.Fatal(err)
	}
	if target.ChainID.String() != "91337" ||
		target.Token != common.HexToAddress("0x0000000000000000000000000000000000000011") ||
		target.Manager != common.HexToAddress("0x0000000000000000000000000000000000000022") {
		t.Fatalf("unexpected target: %+v", target)
	}
}
