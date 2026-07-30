package nttmanualcanary_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	solanago "github.com/gagliardetto/solana-go"
)

var canaryBinary string

func TestMain(m *testing.M) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	directory, err := os.MkdirTemp("", "vernier-ntt-canary-test-*")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	extension := ""
	if runtime.GOOS == "windows" {
		extension = ".exe"
	}
	canaryBinary = filepath.Join(directory, "ntt-manual-canary"+extension)
	command := exec.Command("go", "build", "-o", canaryBinary, "./cmd/ntt-manual-canary")
	command.Dir = root
	if output, buildErr := command.CombinedOutput(); buildErr != nil {
		fmt.Fprintf(os.Stderr, "build canary: %v\n%s", buildErr, output)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(directory)
	os.Exit(code)
}

func TestReadOnlySolanaSourcePreflight(t *testing.T) {
	directory := t.TempDir()
	configPath := filepath.Join(directory, "profile.yaml")
	envPath := filepath.Join(directory, ".env.test")
	config := `schema_version: 1
guardian_rpc_urls:
  - https://guardian.invalid
attestation_timeout_seconds: 1
solana:
  rpc_url_env: SYNTHETIC_SOLANA_RPC
  signer_env: SYNTHETIC_SOLANA_SIGNER
  wormhole_chain: 17
  manager: ComputeBudget111111111111111111111111111111
  transceiver: MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr
  wormhole_core: AddressLookupTab1e1111111111111111111111111
  token_mint: So11111111111111111111111111111111111111112
  token_program: TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA
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
	solanaSigner, err := solanago.NewRandomPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	env := fmt.Sprintf(
		"SYNTHETIC_SOLANA_SIGNER=%s\nSYNTHETIC_EVM_SIGNER=%s\n",
		solanaSigner.String(),
		"0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d",
	)
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(envPath, []byte(env), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(
		canaryBinary,
		"--config", configPath,
		"--env-file", envPath,
		"--direction", "solana-to-evm",
		"--amount-units", "123456",
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("canary failed: %v\n%s", err, output)
	}
	text := string(output)
	for _, expected := range []string{
		"source=solana",
		"destination=evm",
		"amount_units=123456",
		"instructions=3",
		"broadcast=disabled",
	} {
		if !strings.Contains(text, expected) {
			t.Fatalf("output does not contain %q:\n%s", expected, text)
		}
	}
}

func TestArmRequiresExactAmountConfirmationBeforeNetworkAccess(t *testing.T) {
	directory := t.TempDir()
	configPath := filepath.Join(directory, "profile.yaml")
	envPath := filepath.Join(directory, ".env.test")
	config := `schema_version: 1
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
	solanaSigner, err := solanago.NewRandomPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	env := fmt.Sprintf(
		"SYNTHETIC_SOLANA_SIGNER=%s\nSYNTHETIC_EVM_SIGNER=%s\n"+
			"SYNTHETIC_SOLANA_RPC=http://127.0.0.1:1\n"+
			"SYNTHETIC_EVM_RPC=http://127.0.0.1:1\n",
		solanaSigner.String(),
		"0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d",
	)
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(envPath, []byte(env), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(
		canaryBinary,
		"--config", configPath,
		"--env-file", envPath,
		"--direction", "solana-to-evm",
		"--amount-units", "123456",
		"--confirm-amount-units", "123455",
		"--arm",
	)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("armed command unexpectedly succeeded:\n%s", output)
	}
	text := string(output)
	if !strings.Contains(text, "exactly match --amount-units") {
		t.Fatalf("unexpected output:\n%s", text)
	}
	if strings.Contains(text, "connect") {
		t.Fatalf("arm barrier must fail before network access:\n%s", text)
	}
}

func TestArmRecoveryRequiresExactSourceTransactionConfirmation(t *testing.T) {
	command := exec.Command(
		canaryBinary,
		"--config", "unused.yaml",
		"--direction", "evm-to-solana",
		"--source-tx", "0x1234",
		"--arm",
	)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("armed command unexpectedly succeeded:\n%s", output)
	}
	if !strings.Contains(
		string(output),
		"armed recovery requires identical --source-tx and --confirm-source-tx",
	) {
		t.Fatalf("unexpected output:\n%s", output)
	}
}
