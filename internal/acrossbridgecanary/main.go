// Command across-bridge-canary obtains and validates a fresh Across source
// artifact. It remains read-only unless the explicit arm barriers are supplied.
package acrossbridgecanary

import (
	"context"
	"crypto/ecdsa"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/big"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	solanago "github.com/gagliardetto/solana-go"

	"github.com/VarozXYZ/vernier/adapters/crosschain/across"
	"github.com/VarozXYZ/vernier/runtime/configuration"
)

type direction string

const (
	evmToSolana direction = "evm-to-solana"
	solanaToEVM direction = "solana-to-evm"
)

func Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("across-bridge-canary", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "private Research manifest containing the two remote markets")
	envPath := flags.String("env-file", ".env.test", "local environment file")
	selectedDirection := flags.String("direction", "", "evm-to-solana or solana-to-evm")
	amount := flags.String("amount-units", "", "exact USDC amount in smallest units")
	slippage := flags.String("slippage", "auto", "Across slippage: auto or decimal 0..1")
	arm := flags.Bool("arm", false, "sign and broadcast the validated source transaction")
	confirmAmount := flags.String("confirm-amount-units", "", "must exactly match --amount-units when armed")
	resumeOperation := flags.String("resume-operation", "", "resume destination reconciliation for a persisted operation")
	storePath := flags.String("store", ".vernier/across-canary.sqlite", "WAL FULL operation journal")
	timeout := flags.Duration("confirmation-timeout", 10*time.Minute, "source and destination confirmation timeout")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	resumeMode := strings.TrimSpace(*resumeOperation) != ""
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "across-bridge-canary: unexpected positional arguments")
		return 2
	}
	if resumeMode {
		if *selectedDirection != "" || *amount != "" || *confirmAmount != "" || *arm {
			fmt.Fprintln(stderr, "across-bridge-canary: --resume-operation cannot be combined with direction, amount, confirmation, or arm")
			return 2
		}
	} else {
		if *selectedDirection != string(evmToSolana) && *selectedDirection != string(solanaToEVM) {
			fmt.Fprintln(stderr, "across-bridge-canary: valid --direction is required")
			return 2
		}
		if value, ok := new(big.Int).SetString(*amount, 10); !ok || value.Sign() <= 0 {
			fmt.Fprintln(stderr, "across-bridge-canary: --amount-units must be a positive integer")
			return 2
		}
		if *arm && *confirmAmount != *amount {
			fmt.Fprintln(stderr, "across-bridge-canary: --confirm-amount-units must exactly match --amount-units when armed")
			return 2
		}
		if !*arm && *confirmAmount != "" {
			fmt.Fprintln(stderr, "across-bridge-canary: --confirm-amount-units requires --arm")
			return 2
		}
	}
	if strings.TrimSpace(*configPath) == "" {
		fmt.Fprintln(stderr, "across-bridge-canary: --config is required")
		return 2
	}
	if err := configuration.LoadEnvFile(*envPath, os.LookupEnv, os.Setenv); err != nil {
		fmt.Fprintln(stderr, "across-bridge-canary: cannot load local environment")
		return 2
	}
	config, err := configuration.LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "across-bridge-canary: %v\n", err)
		return 2
	}
	apiKey, err := requiredEnv("ACROSS_API_KEY")
	if err != nil {
		fmt.Fprintf(stderr, "across-bridge-canary: %v\n", err)
		return 2
	}
	integratorID, err := requiredEnv("ACROSS_INTEGRATOR_ID")
	if err != nil {
		fmt.Fprintf(stderr, "across-bridge-canary: %v\n", err)
		return 2
	}
	client, err := across.New(across.Config{
		APIKey: apiKey, IntegratorID: integratorID,
	})
	if err != nil {
		fmt.Fprintf(stderr, "across-bridge-canary: %v\n", err)
		return 2
	}
	if resumeMode {
		if err := resumeArmed(
			ctx, stdout, config, client, strings.TrimSpace(*resumeOperation),
			*storePath, *timeout,
		); err != nil {
			fmt.Fprintf(stderr, "across-bridge-canary: %v\n", err)
			return 1
		}
		return 0
	}
	request, originLabel, destinationLabel, err := approvalRequest(
		config, direction(*selectedDirection), *amount, *slippage,
	)
	if err != nil {
		fmt.Fprintf(stderr, "across-bridge-canary: %v\n", err)
		return 2
	}
	started := time.Now()
	approval, err := client.Approval(ctx, request)
	if err != nil {
		fmt.Fprintf(stderr, "across-bridge-canary: %v\n", err)
		return 1
	}
	fmt.Fprintf(
		stdout,
		"approval=ok direction=%s origin=%s destination=%s input_units=%s expected_output_units=%s "+
			"minimum_output_units=%s provider=%s expected_fill=%s expires_at=%s latency=%s response_hash=%s\n",
		*selectedDirection, originLabel, destinationLabel, approval.InputAmount,
		approval.ExpectedOutputAmount, approval.MinimumOutputAmount,
		emptyAsUnknown(approval.BridgeProvider), approval.ExpectedFillTime,
		formatExpiry(approval.ExpiresAt), time.Since(started).Round(10*time.Microsecond),
		approval.ResponseHash[:16],
	)
	fmt.Fprintf(
		stdout,
		"artifact chain_id=%d approvals=%d spender=%s simulation_success=%s target=%s payload=%s fields=%s broadcast=disabled\n",
		approval.SwapTransaction.ChainID, len(approval.ApprovalTransactions),
		redactedTarget(approval.Allowance.Spender),
		formatSimulation(approval.SwapTransaction.SimulationSuccess),
		redactedTarget(approval.SwapTransaction.To),
		artifactKind(approval.SwapTransaction),
		strings.Join(approval.SwapTransaction.FieldNames(), ","),
	)
	if approval.SwapTransaction.ChainID == across.SolanaChainID {
		required, present, signatureErr := solanaArtifactSignatures(approval.SwapTransaction)
		if signatureErr != nil {
			fmt.Fprintf(stderr, "across-bridge-canary: inspect Solana artifact signatures: %v\n", signatureErr)
			return 1
		}
		fmt.Fprintf(stdout, "artifact_signatures required=%d provider_present=%d local_signature=pending\n", required, present)
	}
	if approval.SwapTransaction.ChainID == across.SolanaChainID &&
		approval.SwapTransaction.SimulationSuccess != nil &&
		!*approval.SwapTransaction.SimulationSuccess {
		fmt.Fprintln(stdout, "warning=provider_solana_simulation_unavailable action=local_rpc_simulation_required_before_signing")
	}
	if !*arm {
		return 0
	}
	if len(approval.ApprovalTransactions) != 0 {
		fmt.Fprintln(stderr, "across-bridge-canary: approval transactions are required; execute and confirm them separately before arming")
		return 1
	}
	if err := executeArmed(
		ctx, stdout, config, client, request, approval, direction(*selectedDirection),
		*storePath, *timeout, nil,
	); err != nil {
		fmt.Fprintf(stderr, "across-bridge-canary: %v\n", err)
		return 1
	}
	return 0
}

func approvalRequest(
	config configuration.ParsedConfig,
	selected direction,
	amount string,
	slippage string,
) (across.ApprovalRequest, string, string, error) {
	var solanaMarket, evmMarket *configuration.ResolvedMarket
	for index := range config.Markets {
		candidate := &config.Markets[index]
		chain := config.Chains[candidate.Chain]
		switch chain.Kind {
		case "solana":
			solanaMarket = candidate
		case "evm":
			evmMarket = candidate
		}
	}
	if solanaMarket == nil || evmMarket == nil {
		return across.ApprovalRequest{}, "", "", fmt.Errorf("configuration requires one Solana and one EVM market")
	}
	solanaPrivate, err := requiredEnv("SOLANA_PRIVATE_KEY")
	if err != nil {
		return across.ApprovalRequest{}, "", "", err
	}
	solanaSigner, err := parseSolanaPrivateKey(solanaPrivate)
	if err != nil {
		return across.ApprovalRequest{}, "", "", err
	}
	evmPrivate, err := requiredEnv("POLYGON_PRIVATE_KEY")
	if err != nil {
		return across.ApprovalRequest{}, "", "", err
	}
	evmSigner, err := parseEVMPrivateKey(evmPrivate)
	if err != nil {
		return across.ApprovalRequest{}, "", "", err
	}
	evmAddress := crypto.PubkeyToAddress(evmSigner.PublicKey).Hex()
	solanaAddress := solanaSigner.PublicKey().String()
	evmChainID := config.Chains[evmMarket.Chain].ChainID
	if evmChainID == nil || !evmChainID.IsUint64() || evmChainID.Sign() <= 0 {
		return across.ApprovalRequest{}, "", "", fmt.Errorf("EVM chain ID is invalid")
	}
	evmChain := evmChainID.Uint64()
	request := across.ApprovalRequest{
		Amount: amount, Slippage: slippage, RefundAddress: "",
	}
	if selected == evmToSolana {
		request.OriginChainID, request.DestinationChainID = evmChain, across.SolanaChainID
		request.InputToken, request.OutputToken = evmMarket.Quote.AddressText, solanaMarket.Quote.AddressText
		request.Depositor, request.Recipient, request.RefundAddress = evmAddress, solanaAddress, evmAddress
		return request, config.Chains[evmMarket.Chain].Label, config.Chains[solanaMarket.Chain].Label, nil
	}
	request.OriginChainID, request.DestinationChainID = across.SolanaChainID, evmChain
	request.InputToken, request.OutputToken = solanaMarket.Quote.AddressText, evmMarket.Quote.AddressText
	request.Depositor, request.Recipient, request.RefundAddress = solanaAddress, evmAddress, solanaAddress
	return request, config.Chains[solanaMarket.Chain].Label, config.Chains[evmMarket.Chain].Label, nil
}

func parseSolanaPrivateKey(value string) (solanago.PrivateKey, error) {
	value = strings.TrimSpace(value)
	if strings.HasPrefix(value, "[") {
		var raw []byte
		if err := json.Unmarshal([]byte(value), &raw); err == nil && len(raw) == 64 {
			return solanago.PrivateKey(raw), nil
		}
	}
	if parsed, err := solanago.PrivateKeyFromBase58(value); err == nil {
		return parsed, nil
	}
	return nil, fmt.Errorf("SOLANA_PRIVATE_KEY is invalid")
}

func parseEVMPrivateKey(value string) (*ecdsa.PrivateKey, error) {
	key, err := crypto.HexToECDSA(strings.TrimPrefix(strings.TrimSpace(value), "0x"))
	if err != nil {
		return nil, fmt.Errorf("POLYGON_PRIVATE_KEY is invalid")
	}
	return key, nil
}

func requiredEnv(name string) (string, error) {
	value, ok := os.LookupEnv(name)
	if !ok || strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("required environment %q is unset", name)
	}
	return strings.TrimSpace(value), nil
}

func formatSimulation(value *bool) string {
	if value == nil {
		return "unknown"
	}
	return strconv.FormatBool(*value)
}

func formatExpiry(value time.Time) string {
	if value.IsZero() {
		return "not_declared"
	}
	return value.Format(time.RFC3339)
}

func artifactKind(transaction across.Transaction) string {
	if transaction.Serialized != "" {
		return "serialized_transaction"
	}
	if transaction.Data != "" {
		if raw, err := base64.StdEncoding.DecodeString(transaction.Data); err == nil {
			if _, err := solanago.TransactionFromBytes(raw); err == nil {
				return "serialized_transaction_data"
			}
		}
		return "calldata"
	}
	return "unknown"
}

func solanaArtifactSignatures(transaction across.Transaction) (int, int, error) {
	encoded := transaction.Serialized
	if encoded == "" {
		encoded = transaction.Data
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return 0, 0, err
	}
	parsed, err := solanago.TransactionFromBytes(raw)
	if err != nil {
		return 0, 0, err
	}
	required := int(parsed.Message.Header.NumRequiredSignatures)
	var zero solanago.Signature
	present := 0
	for _, signature := range parsed.Signatures {
		if signature != zero {
			present++
		}
	}
	return required, present, nil
}

func redactedTarget(value string) string {
	if len(value) < 14 {
		return emptyAsUnknown(value)
	}
	return value[:8] + "..." + value[len(value)-6:]
}

func emptyAsUnknown(value string) string {
	if strings.TrimSpace(value) == "" {
		return "unknown"
	}
	return value
}
