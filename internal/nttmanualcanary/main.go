// Command ntt-manual-canary preflights and, only with an explicit arm barrier,
// executes a small direct Wormhole NTT transfer and its manual redemption.
package nttmanualcanary

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/big"
	"os"
	"strings"
	"time"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	gethcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	solanago "github.com/gagliardetto/solana-go"
	solanarpc "github.com/gagliardetto/solana-go/rpc"
	"gopkg.in/yaml.v3"

	"github.com/VarozXYZ/vernier/adapters/crosschain/wormholentt"
	crosschainport "github.com/VarozXYZ/vernier/ports/crosschain"
	"github.com/VarozXYZ/vernier/runtime/configuration"
)

type profile struct {
	SchemaVersion        int           `yaml:"schema_version"`
	GuardianRPCs         []string      `yaml:"guardian_rpc_urls"`
	GuardianRPCsEnv      string        `yaml:"guardian_rpc_urls_env"`
	AttestationTimeout   time.Duration `yaml:"-"`
	AttestationTimeoutS  int           `yaml:"attestation_timeout_seconds"`
	ConfirmationTimeoutS int           `yaml:"confirmation_timeout_seconds"`
	StorePath            string        `yaml:"store_path"`
	Solana               solanaProfile `yaml:"solana"`
	EVM                  evmProfile    `yaml:"evm"`
}

type solanaProfile struct {
	RPCURLEnv              string `yaml:"rpc_url_env"`
	SignerEnv              string `yaml:"signer_env"`
	WormholeChain          uint16 `yaml:"wormhole_chain"`
	Manager                string `yaml:"manager"`
	Transceiver            string `yaml:"transceiver"`
	WormholeCore           string `yaml:"wormhole_core"`
	TokenMint              string `yaml:"token_mint"`
	TokenProgram           string `yaml:"token_program"`
	MinimumBalanceLamports uint64 `yaml:"minimum_balance_lamports"`
	ComputeUnitLimit       uint32 `yaml:"compute_unit_limit"`
	ComputeUnitPrice       uint64 `yaml:"compute_unit_price_micro_lamports"`
}

type evmProfile struct {
	RPCURLEnv               string `yaml:"rpc_url_env"`
	SignerEnv               string `yaml:"signer_env"`
	ChainID                 string `yaml:"chain_id"`
	WormholeChain           uint16 `yaml:"wormhole_chain"`
	Token                   string `yaml:"token"`
	Manager                 string `yaml:"manager"`
	Transceiver             string `yaml:"transceiver"`
	WormholeCore            string `yaml:"wormhole_core"`
	MinimumNativeBalanceWei string `yaml:"minimum_native_balance_wei"`
	GasLimitMultiplierBPS   uint64 `yaml:"gas_limit_multiplier_bps"`
}

type direction string

const (
	solanaToEVM direction = "solana-to-evm"
	evmToSolana direction = "evm-to-solana"
)

func Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("ntt-manual-canary", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "private NTT profile")
	envPath := flags.String("env-file", ".env.test", "local environment file")
	directionText := flags.String("direction", "", "solana-to-evm or evm-to-solana")
	amountText := flags.String("amount-units", "", "fixed-integer source token amount")
	sourceTransaction := flags.String("source-tx", "", "resume from a confirmed source transaction")
	emitterChain := flags.Uint("emitter-chain", 0, "resume from a Wormhole emitter chain")
	emitterAddress := flags.String("emitter-address", "", "resume from a 32-byte hex emitter")
	sequence := flags.Uint64("sequence", 0, "resume from a Wormhole sequence")
	arm := flags.Bool("arm", false, "sign and broadcast the complete canary flow")
	confirmAmount := flags.String(
		"confirm-amount-units",
		"",
		"must exactly match --amount-units when --arm starts a source transfer",
	)
	confirmSource := flags.String(
		"confirm-source-tx",
		"",
		"must exactly match --source-tx when --arm resumes a confirmed transfer",
	)
	storePath := flags.String("operation-store", "", "durable SQLite canary journal")
	verifiedSignatureTx := flags.String(
		"verified-signature-tx",
		"",
		"confirmed Solana guardian-verification transaction to reuse",
	)
	simulate := flags.Bool(
		"simulate",
		false,
		"simulate the first destination transaction without broadcasting",
	)
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *configPath == "" || (*directionText != string(solanaToEVM) && *directionText != string(evmToSolana)) {
		fmt.Fprintln(stderr, "canary: --config and a valid --direction are required")
		return 2
	}
	resuming := *sourceTransaction != "" || *emitterChain != 0 ||
		*emitterAddress != "" || *sequence != 0
	if *arm {
		if resuming {
			if *sourceTransaction == "" || *confirmSource != *sourceTransaction {
				fmt.Fprintln(
					stderr,
					"canary: armed recovery requires identical --source-tx and --confirm-source-tx",
				)
				return 2
			}
		} else if *amountText == "" || *confirmAmount != *amountText {
			fmt.Fprintln(
				stderr,
				"canary: --arm requires --confirm-amount-units to exactly match --amount-units",
			)
			return 2
		}
	}
	if *simulate && !resuming {
		fmt.Fprintln(stderr, "canary: --simulate requires a source transaction or message")
		return 2
	}
	if *verifiedSignatureTx != "" &&
		(!*arm && !*simulate ||
			*directionText != string(evmToSolana) ||
			*sourceTransaction == "") {
		fmt.Fprintln(
			stderr,
			"canary: --verified-signature-tx requires evm-to-solana recovery",
		)
		return 2
	}
	if err := configuration.LoadEnvFile(*envPath, os.LookupEnv, os.Setenv); err != nil {
		fmt.Fprintln(stderr, "canary: cannot load local environment")
		return 2
	}
	config, err := loadProfile(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "canary: %v\n", err)
		return 2
	}
	solanaAdapter, evmAdapter, solanaPayer, evmSender, err := compose(config)
	if err != nil {
		fmt.Fprintf(stderr, "canary: %v\n", err)
		return 2
	}

	if *arm {
		if err := executeArmed(
			ctx,
			stdout,
			direction(*directionText),
			config,
			solanaAdapter,
			evmAdapter,
			solanaPayer,
			evmSender,
			*amountText,
			*sourceTransaction,
			*storePath,
			*verifiedSignatureTx,
			nil,
		); err != nil {
			fmt.Fprintf(stderr, "canary: %v\n", err)
			return 1
		}
		return 0
	}
	if !resuming {
		if *amountText == "" {
			fmt.Fprintln(stderr, "canary: --amount-units is required when not resuming a message")
			return 2
		}
		amount, ok := new(big.Int).SetString(*amountText, 10)
		if !ok || amount.Sign() <= 0 {
			fmt.Fprintln(stderr, "canary: --amount-units must be a positive fixed integer")
			return 2
		}
		if err := printSourcePreflight(
			ctx,
			stdout,
			direction(*directionText),
			config,
			solanaAdapter,
			evmAdapter,
			solanaPayer,
			evmSender,
			amount,
		); err != nil {
			fmt.Fprintf(stderr, "canary: %v\n", err)
			return 1
		}
		fmt.Fprintln(stdout, "broadcast=disabled")
		return 0
	}

	var message crosschainport.MessageID
	if *sourceTransaction != "" {
		if *emitterChain != 0 || *emitterAddress != "" || *sequence != 0 {
			fmt.Fprintln(stderr, "canary: --source-tx cannot be combined with emitter fields")
			return 2
		}
		message, err = messageFromSourceTransaction(
			ctx,
			direction(*directionText),
			config,
			solanaAdapter,
			evmAdapter,
			*sourceTransaction,
		)
	} else {
		if *emitterChain > 65_535 {
			fmt.Fprintln(stderr, "canary: emitter chain exceeds uint16")
			return 2
		}
		message, err = parseMessageID(uint16(*emitterChain), *emitterAddress, *sequence)
	}
	if err != nil {
		fmt.Fprintf(stderr, "canary: %v\n", err)
		return 2
	}
	if err := printRedemptionPreflight(
		ctx,
		stdout,
		direction(*directionText),
		config,
		solanaAdapter,
		evmAdapter,
		solanaPayer,
		message,
		*simulate,
		*verifiedSignatureTx,
	); err != nil {
		fmt.Fprintf(stderr, "canary: %v\n", err)
		return 1
	}
	fmt.Fprintln(stdout, "broadcast=disabled")
	return 0
}

func messageFromSourceTransaction(
	ctx context.Context,
	route direction,
	config profile,
	solanaAdapter *wormholentt.SolanaAdapter,
	evmAdapter *wormholentt.EVMAdapter,
	reference string,
) (crosschainport.MessageID, error) {
	switch route {
	case solanaToEVM:
		signature, err := solanago.SignatureFromBase58(strings.TrimSpace(reference))
		if err != nil {
			return crosschainport.MessageID{}, fmt.Errorf("invalid Solana source signature")
		}
		rpcURL, err := requiredEnv(config.Solana.RPCURLEnv)
		if err != nil {
			return crosschainport.MessageID{}, err
		}
		version := uint64(0)
		transaction, err := solanarpc.New(rpcURL).GetTransaction(
			ctx,
			signature,
			&solanarpc.GetTransactionOpts{
				Encoding:                       solanago.EncodingBase64,
				Commitment:                     solanarpc.CommitmentConfirmed,
				MaxSupportedTransactionVersion: &version,
			},
		)
		if err != nil || transaction.Meta == nil || transaction.Meta.Err != nil {
			return crosschainport.MessageID{}, fmt.Errorf("read successful Solana source transaction")
		}
		return solanaAdapter.MessageFromLogs(transaction.Meta.LogMessages)
	case evmToSolana:
		if !common.IsHexHash(reference) {
			return crosschainport.MessageID{}, fmt.Errorf("invalid EVM source transaction hash")
		}
		rpcURL, err := requiredEnv(config.EVM.RPCURLEnv)
		if err != nil {
			return crosschainport.MessageID{}, err
		}
		client, err := ethclient.DialContext(ctx, rpcURL)
		if err != nil {
			return crosschainport.MessageID{}, fmt.Errorf("connect EVM RPC: %w", err)
		}
		defer client.Close()
		receipt, err := client.TransactionReceipt(ctx, common.HexToHash(reference))
		if err != nil {
			return crosschainport.MessageID{}, fmt.Errorf("read EVM source receipt: %w", err)
		}
		return evmAdapter.MessageFromReceipt(receipt)
	default:
		return crosschainport.MessageID{}, fmt.Errorf("unsupported direction")
	}
}

func printSourcePreflight(
	ctx context.Context,
	output io.Writer,
	route direction,
	config profile,
	solanaAdapter *wormholentt.SolanaAdapter,
	evmAdapter *wormholentt.EVMAdapter,
	solanaPayer solanago.PublicKey,
	evmSender common.Address,
	amount *big.Int,
) error {
	switch route {
	case solanaToEVM:
		if !amount.IsUint64() {
			return fmt.Errorf("solana amount exceeds uint64")
		}
		plan, err := solanaAdapter.BuildTransferBurn(
			solanaPayer,
			amount.Uint64(),
			config.EVM.WormholeChain,
			wormholentt.EVMUniversalAddress(evmSender),
		)
		if err != nil {
			return err
		}
		fmt.Fprintf(
			output,
			"source=solana destination=evm amount_units=%s instructions=%d outbox=%s emitter=%s message=%s\n",
			amount,
			len(plan.Instructions),
			plan.Outbox.PublicKey(),
			plan.Emitter,
			plan.Message,
		)
	case evmToSolana:
		rpcURL, err := requiredEnv(config.EVM.RPCURLEnv)
		if err != nil {
			return err
		}
		client, err := ethclient.DialContext(ctx, rpcURL)
		if err != nil {
			return fmt.Errorf("connect EVM RPC: %w", err)
		}
		defer client.Close()
		query, err := evmAdapter.BuildDeliveryPriceCall(config.Solana.WormholeChain)
		if err != nil {
			return err
		}
		rawPrice, err := client.CallContract(ctx, geth.CallMsg{To: &query.To, Data: query.Data}, nil)
		if err != nil {
			return fmt.Errorf("query NTT delivery price: %w", err)
		}
		price, err := wormholentt.DecodeDeliveryPrice(rawPrice)
		if err != nil {
			return err
		}
		call, err := evmAdapter.BuildTransfer(
			amount,
			config.Solana.WormholeChain,
			wormholentt.SolanaUniversalAddress(solanaPayer),
			wormholentt.EVMUniversalAddress(evmSender),
			price,
		)
		if err != nil {
			return err
		}
		fmt.Fprintf(
			output,
			"source=evm destination=solana amount_units=%s delivery_value_wei=%s target=%s calldata_bytes=%d manual_instructions=%x\n",
			amount,
			price,
			call.To.Hex(),
			len(call.Data),
			wormholentt.ManualTransceiverInstructions(),
		)
	default:
		return fmt.Errorf("unsupported direction")
	}
	return nil
}

func printRedemptionPreflight(
	ctx context.Context,
	output io.Writer,
	route direction,
	config profile,
	solanaAdapter *wormholentt.SolanaAdapter,
	evmAdapter *wormholentt.EVMAdapter,
	solanaPayer solanago.PublicKey,
	message crosschainport.MessageID,
	simulate bool,
	verifiedSignatureTx string,
) error {
	endpoints, err := guardianEndpoints(config)
	if err != nil {
		return err
	}
	client, err := wormholentt.NewGuardianClient(wormholentt.GuardianClientConfig{
		Endpoints:    endpoints,
		PollInterval: 200 * time.Millisecond,
	})
	if err != nil {
		return err
	}
	waitCtx, cancel := context.WithTimeout(ctx, config.AttestationTimeout)
	defer cancel()
	started := time.Now()
	attestation, err := client.Await(waitCtx, message)
	if err != nil {
		return fmt.Errorf("await signed VAA: %w", err)
	}
	vaa, err := wormholentt.ParseVAA(attestation.Raw)
	if err != nil {
		return err
	}
	switch route {
	case solanaToEVM:
		call, err := evmAdapter.BuildRedeem(attestation.Raw)
		if err != nil {
			return err
		}
		fmt.Fprintf(
			output,
			"vaa=%s wait=%s destination=evm target=%s calldata_bytes=%d transactions=1\n",
			wormholentt.FingerprintVAA(attestation.Raw),
			time.Since(started).Round(time.Millisecond),
			call.To.Hex(),
			len(call.Data),
		)
	case evmToSolana:
		rpcURL, err := requiredEnv(config.Solana.RPCURLEnv)
		if err != nil {
			return err
		}
		rpcClient := solanarpc.New(rpcURL)
		account, err := rpcClient.GetAccountInfoWithOpts(
			ctx,
			solanaAdapter.GuardianSetAddress(vaa.GuardianSetIndex),
			&solanarpc.GetAccountInfoOpts{
				Encoding:   solanago.EncodingBase64,
				Commitment: solanarpc.CommitmentConfirmed,
			},
		)
		if err != nil {
			return fmt.Errorf("read Wormhole GuardianSet: %w", err)
		}
		guardians, err := wormholentt.DecodeGuardianSet(account.Value.Data.GetBinary())
		if err != nil {
			return err
		}
		var plan wormholentt.SolanaRedemptionPlan
		if verifiedSignatureTx == "" {
			plan, err = solanaAdapter.BuildRedemption(
				attestation.Raw,
				guardians.Keys,
				solanaPayer,
			)
		} else {
			signatureSet, setErr := verifiedSignatureSetFromTransaction(
				ctx,
				rpcClient,
				solanaAdapter,
				verifiedSignatureTx,
				vaa,
				len(guardians.Keys),
			)
			if setErr != nil {
				return setErr
			}
			plan, err = solanaAdapter.BuildRedemptionWithVerifiedSignatureSet(
				attestation.Raw,
				guardians.Keys,
				solanaPayer,
				signatureSet,
			)
			if err == nil {
				fmt.Fprintf(
					output,
					"guardian_verification=reused signature_set=%s evidence_tx=%s\n",
					signatureSet,
					verifiedSignatureTx,
				)
			}
		}
		if err != nil {
			return err
		}
		fmt.Fprintf(
			output,
			"vaa=%s wait=%s destination=solana guardian_batches=%d post_vaa=1 redeem=1 total_transactions=%d posted_vaa=%s inbox=%s\n",
			plan.VAAFingerprint,
			time.Since(started).Round(time.Millisecond),
			len(plan.Verify),
			len(plan.Verify)+2,
			plan.PostedVAA,
			plan.InboxItem,
		)
		if simulate {
			batch := plan.PostVAA
			if len(plan.Verify) > 0 {
				batch = plan.Verify[0]
			}
			if err := simulateSolanaBatch(
				ctx,
				output,
				config,
				batch.Kind,
				batch.Instructions,
				batch.Signers,
			); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported direction")
	}
	return nil
}

func loadProfile(path string) (profile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return profile{}, fmt.Errorf("read NTT profile: %w", err)
	}
	var result profile
	decoder := yaml.NewDecoder(strings.NewReader(string(data)))
	decoder.KnownFields(true)
	if err := decoder.Decode(&result); err != nil {
		return profile{}, fmt.Errorf("decode NTT profile: %w", err)
	}
	if result.SchemaVersion != 1 || (len(result.GuardianRPCs) == 0 && result.GuardianRPCsEnv == "") {
		return profile{}, fmt.Errorf("NTT profile requires schema version 1 and Guardian RPC endpoints")
	}
	if result.AttestationTimeoutS <= 0 {
		result.AttestationTimeoutS = 120
	}
	result.AttestationTimeout = time.Duration(result.AttestationTimeoutS) * time.Second
	if result.ConfirmationTimeoutS <= 0 {
		result.ConfirmationTimeoutS = 90
	}
	if result.StorePath == "" {
		result.StorePath = ".vernier/ntt-canary.sqlite"
	}
	if result.Solana.MinimumBalanceLamports == 0 {
		result.Solana.MinimumBalanceLamports = 20_000_000
	}
	if result.Solana.ComputeUnitLimit == 0 {
		result.Solana.ComputeUnitLimit = 1_400_000
	}
	if result.Solana.ComputeUnitPrice == 0 {
		result.Solana.ComputeUnitPrice = 1_000
	}
	if result.EVM.MinimumNativeBalanceWei == "" {
		result.EVM.MinimumNativeBalanceWei = "10000000000000000"
	}
	if result.EVM.GasLimitMultiplierBPS == 0 {
		result.EVM.GasLimitMultiplierBPS = 12_000
	}
	return result, nil
}

func compose(config profile) (
	*wormholentt.SolanaAdapter,
	*wormholentt.EVMAdapter,
	solanago.PublicKey,
	common.Address,
	error,
) {
	solanaPrivateText, err := requiredEnv(config.Solana.SignerEnv)
	if err != nil {
		return nil, nil, solanago.PublicKey{}, common.Address{}, err
	}
	solanaPrivate, err := parseSolanaPrivateKey(solanaPrivateText)
	if err != nil {
		return nil, nil, solanago.PublicKey{}, common.Address{}, err
	}
	solanaPayer := solanaPrivate.PublicKey()
	evmPrivateText, err := requiredEnv(config.EVM.SignerEnv)
	if err != nil {
		return nil, nil, solanago.PublicKey{}, common.Address{}, err
	}
	evmPrivate, err := gethcrypto.HexToECDSA(strings.TrimPrefix(strings.TrimSpace(evmPrivateText), "0x"))
	if err != nil {
		return nil, nil, solanago.PublicKey{}, common.Address{}, fmt.Errorf("invalid EVM private key")
	}
	evmSender := gethcrypto.PubkeyToAddress(evmPrivate.PublicKey)
	tokenProgram := solanago.TokenProgramID
	if strings.TrimSpace(config.Solana.TokenProgram) != "" {
		tokenProgram, err = solanago.PublicKeyFromBase58(config.Solana.TokenProgram)
		if err != nil {
			return nil, nil, solanago.PublicKey{}, common.Address{}, fmt.Errorf("invalid Solana token program")
		}
	}
	manager, err := parseSolanaKey("manager", config.Solana.Manager)
	if err != nil {
		return nil, nil, solanago.PublicKey{}, common.Address{}, err
	}
	transceiver, err := parseSolanaKey("transceiver", config.Solana.Transceiver)
	if err != nil {
		return nil, nil, solanago.PublicKey{}, common.Address{}, err
	}
	core, err := parseSolanaKey("Wormhole core", config.Solana.WormholeCore)
	if err != nil {
		return nil, nil, solanago.PublicKey{}, common.Address{}, err
	}
	mint, err := parseSolanaKey("token mint", config.Solana.TokenMint)
	if err != nil {
		return nil, nil, solanago.PublicKey{}, common.Address{}, err
	}
	solanaAdapter, err := wormholentt.NewSolanaAdapter(wormholentt.SolanaConfig{
		WormholeChain: config.Solana.WormholeChain,
		Manager:       manager,
		Transceiver:   transceiver,
		WormholeCore:  core,
		TokenMint:     mint,
		TokenProgram:  tokenProgram,
	})
	if err != nil {
		return nil, nil, solanago.PublicKey{}, common.Address{}, err
	}
	chainID, ok := new(big.Int).SetString(config.EVM.ChainID, 10)
	if !ok {
		return nil, nil, solanago.PublicKey{}, common.Address{}, fmt.Errorf("invalid EVM chain ID")
	}
	addresses := []string{
		config.EVM.Token,
		config.EVM.Manager,
		config.EVM.Transceiver,
		config.EVM.WormholeCore,
	}
	for _, address := range addresses {
		if !common.IsHexAddress(address) {
			return nil, nil, solanago.PublicKey{}, common.Address{}, fmt.Errorf("invalid EVM NTT address")
		}
	}
	evmAdapter, err := wormholentt.NewEVMAdapter(wormholentt.EVMConfig{
		ChainID:       chainID,
		WormholeChain: config.EVM.WormholeChain,
		Token:         common.HexToAddress(config.EVM.Token),
		Manager:       common.HexToAddress(config.EVM.Manager),
		Transceiver:   common.HexToAddress(config.EVM.Transceiver),
		WormholeCore:  common.HexToAddress(config.EVM.WormholeCore),
	})
	return solanaAdapter, evmAdapter, solanaPayer, evmSender, err
}

func parseMessageID(chain uint16, addressText string, sequence uint64) (crosschainport.MessageID, error) {
	if chain == 0 || sequence == 0 {
		return crosschainport.MessageID{}, fmt.Errorf("resume requires emitter chain, address, and sequence")
	}
	addressText = strings.TrimSpace(addressText)
	raw, err := hex.DecodeString(strings.TrimPrefix(addressText, "0x"))
	if err != nil || len(raw) != 32 {
		publicKey, keyErr := solanago.PublicKeyFromBase58(addressText)
		if keyErr != nil || publicKey.IsZero() {
			return crosschainport.MessageID{}, fmt.Errorf("emitter address must be 32-byte hex or Solana base58")
		}
		raw = publicKey.Bytes()
	}
	result := crosschainport.MessageID{EmitterChain: chain, Sequence: sequence}
	copy(result.EmitterAddress[:], raw)
	return result, nil
}

func requiredEnv(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("profile contains an empty environment variable name")
	}
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return "", fmt.Errorf("required environment %q is unset", name)
	}
	return value, nil
}

func splitList(value string) []string {
	return strings.FieldsFunc(value, func(character rune) bool {
		return character == ',' || character == ';' || character == '\n'
	})
}

func guardianEndpoints(config profile) ([]string, error) {
	if len(config.GuardianRPCs) > 0 {
		return append([]string(nil), config.GuardianRPCs...), nil
	}
	value, err := requiredEnv(config.GuardianRPCsEnv)
	if err != nil {
		return nil, err
	}
	return splitList(value), nil
}

func parseSolanaPrivateKey(value string) (solanago.PrivateKey, error) {
	value = strings.TrimSpace(value)
	if strings.HasPrefix(value, "[") {
		var bytes []byte
		if err := json.Unmarshal([]byte(value), &bytes); err != nil || len(bytes) != 64 {
			return nil, fmt.Errorf("invalid Solana private key")
		}
		privateKey := solanago.PrivateKey(append([]byte(nil), bytes...))
		if privateKey.PublicKey().IsZero() {
			return nil, fmt.Errorf("invalid Solana private key")
		}
		return privateKey, nil
	}
	privateKey, err := solanago.PrivateKeyFromBase58(value)
	if err != nil || privateKey.PublicKey().IsZero() {
		return nil, fmt.Errorf("invalid Solana private key")
	}
	return privateKey, nil
}

func parseSolanaKey(label, value string) (solanago.PublicKey, error) {
	result, err := solanago.PublicKeyFromBase58(strings.TrimSpace(value))
	if err != nil || result.IsZero() {
		return solanago.PublicKey{}, fmt.Errorf("invalid Solana %s", label)
	}
	return result, nil
}
