// Command swap-build-canary validates one small executable aggregator swap
// without broadcasting it. It loads private token addresses from an ignored
// setup manifest, signs only in memory, and runs an on-chain simulation.
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net/url"
	"os"
	"strings"
	"time"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	gethcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	bin "github.com/gagliardetto/binary"
	solanago "github.com/gagliardetto/solana-go"
	solanarpc "github.com/gagliardetto/solana-go/rpc"

	solanaadapter "github.com/VarozXYZ/vernier/adapters/chain/solana"
	sqlitestore "github.com/VarozXYZ/vernier/adapters/persistence/sqlite"
	"github.com/VarozXYZ/vernier/adapters/quote/jupiter"
	"github.com/VarozXYZ/vernier/adapters/quote/kyberswap"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	"github.com/VarozXYZ/vernier/runtime/configuration"
)

const (
	defaultJupiterBuildBaseURL = "https://api.jup.ag"
	defaultComputeUnitLimit    = uint32(1_400_000)
	defaultTipLamports         = "1000000"
	defaultMaxPriorityLamports = "500000"
	defaultPriorityPercentile  = "high"
	defaultConfirmationTimeout = 60 * time.Second
	defaultSwapCanaryStore     = ".vernier/swap-canary.sqlite"
	defaultHeliusSenderURLEnv  = "HELIUS_SENDER_URL"
)

type side string

const (
	buy  side = "buy"
	sell side = "sell"
)

type selectedSwap struct {
	Market     configuration.ResolvedMarket
	Source     configuration.ResolvedQuoteSource
	Chain      configuration.ResolvedChain
	Input      configuration.ResolvedToken
	Output     configuration.ResolvedToken
	Amount     *big.Int
	Side       side
	HTTPURL    string
	AmountText string
}

type armedCanary struct {
	Enabled     bool
	OperationID string
	Store       *sqlitestore.SwapCanaryStore
	Timeout     time.Duration
}

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("swap-build-canary", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "private Research manifest")
	envPath := flags.String("env-file", ".env.test", "local environment file")
	marketID := flags.String("market", "", "configured remote market")
	sideText := flags.String("side", "", "buy or sell")
	amountText := flags.String("amount-units", "", "fixed-integer input amount")
	tipLamports := flags.String(
		"jupiter-tip-lamports",
		defaultTipLamports,
		"Sender tip included in the signed Jupiter simulation",
	)
	computeLimit := flags.Uint(
		"solana-compute-unit-limit",
		uint(defaultComputeUnitLimit),
		"Solana compute-unit limit",
	)
	maxPriorityFeeLamports := flags.String(
		"solana-max-priority-fee-lamports",
		defaultMaxPriorityLamports,
		"maximum total Solana priority fee in lamports",
	)
	computePricePercentile := flags.String(
		"solana-compute-unit-price-percentile",
		defaultPriorityPercentile,
		"dynamic Jupiter compute-unit price percentile",
	)
	solanaBroadcast := flags.String(
		"solana-broadcast",
		"helius-sender",
		"Solana broadcast transport: rpc or helius-sender",
	)
	heliusSenderURLEnv := flags.String(
		"helius-sender-url-env",
		defaultHeliusSenderURLEnv,
		"environment variable containing the Helius Sender endpoint",
	)
	arm := flags.Bool("arm", false, "broadcast the signed canary after durable persistence")
	confirmAmount := flags.String(
		"confirm-amount-units",
		"",
		"must exactly repeat --amount-units when --arm is enabled",
	)
	operationStore := flags.String(
		"operation-store",
		defaultSwapCanaryStore,
		"SQLite journal for armed swap canaries",
	)
	confirmationTimeout := flags.Duration(
		"confirmation-timeout",
		defaultConfirmationTimeout,
		"maximum time to wait for an on-chain confirmation",
	)
	kyberSlippageBPS := flags.Int(
		"kyberswap-slippage-bps",
		-1,
		"override configured KyberSwap slippage for this canary only",
	)
	kyberGasEstimation := flags.Bool(
		"kyberswap-enable-gas-estimation",
		true,
		"ask KyberSwap to estimate the built route before returning calldata",
	)
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 || *configPath == "" || *marketID == "" ||
		(*sideText != string(buy) && *sideText != string(sell)) {
		fmt.Fprintln(
			stderr,
			"swap-build-canary: --config, --market, --side, and --amount-units are required",
		)
		return 2
	}
	amount, ok := new(big.Int).SetString(strings.TrimSpace(*amountText), 10)
	if !ok || amount.Sign() <= 0 {
		fmt.Fprintln(stderr, "swap-build-canary: --amount-units must be a positive integer")
		return 2
	}
	if *arm && strings.TrimSpace(*confirmAmount) != amount.String() {
		fmt.Fprintln(
			stderr,
			"swap-build-canary: --arm requires --confirm-amount-units to exactly match --amount-units",
		)
		return 2
	}
	if !*arm && strings.TrimSpace(*confirmAmount) != "" {
		fmt.Fprintln(
			stderr,
			"swap-build-canary: --confirm-amount-units is only valid with --arm",
		)
		return 2
	}
	if *confirmationTimeout <= 0 {
		fmt.Fprintln(stderr, "swap-build-canary: --confirmation-timeout must be positive")
		return 2
	}
	if *kyberSlippageBPS < -1 || *kyberSlippageBPS > 2_000 {
		fmt.Fprintln(
			stderr,
			"swap-build-canary: --kyberswap-slippage-bps must be between 0 and 2000",
		)
		return 2
	}
	if *computeLimit == 0 || *computeLimit > 1_400_000 {
		fmt.Fprintln(stderr, "swap-build-canary: invalid Solana compute-unit limit")
		return 2
	}
	*solanaBroadcast = strings.ToLower(strings.TrimSpace(*solanaBroadcast))
	if *solanaBroadcast != "rpc" && *solanaBroadcast != "helius-sender" {
		fmt.Fprintln(
			stderr,
			"swap-build-canary: --solana-broadcast must be rpc or helius-sender",
		)
		return 2
	}
	if err := configuration.LoadEnvFile(*envPath, os.LookupEnv, os.Setenv); err != nil {
		fmt.Fprintln(stderr, "swap-build-canary: cannot load local environment")
		return 2
	}
	config, err := configuration.LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "swap-build-canary: %v\n", err)
		return 2
	}
	selected, err := selectSwap(
		config,
		market.MarketID(strings.TrimSpace(*marketID)),
		side(*sideText),
		amount,
	)
	if err != nil {
		fmt.Fprintf(stderr, "swap-build-canary: %v\n", err)
		return 2
	}
	if selected.Source.Kind != "jupiter" &&
		*solanaBroadcast != "rpc" {
		fmt.Fprintln(
			stderr,
			"swap-build-canary: Helius Sender is only valid for a Jupiter/Solana market",
		)
		return 2
	}
	armed := armedCanary{Enabled: *arm, Timeout: *confirmationTimeout}
	if *arm {
		armed.OperationID, err = newSwapCanaryOperationID()
		if err != nil {
			fmt.Fprintf(stderr, "swap-build-canary: %v\n", err)
			return 1
		}
		armed.Store, err = sqlitestore.OpenSwapCanary(*operationStore)
		if err != nil {
			fmt.Fprintf(stderr, "swap-build-canary: %v\n", err)
			return 1
		}
		defer armed.Store.Close()
		if err := armed.Store.Create(ctx, sqlitestore.SwapCanaryOperation{
			ID: armed.OperationID, Provider: selected.Source.Kind,
			Market: string(selected.Market.ID), Side: string(selected.Side),
			AmountUnits: selected.Amount.String(), Status: "created",
			CreatedAt: time.Now().UTC(),
		}); err != nil {
			fmt.Fprintf(stderr, "swap-build-canary: create operation journal: %v\n", err)
			return 1
		}
	}
	broadcastMode := "disabled"
	if armed.Enabled {
		broadcastMode = "armed"
	}
	fmt.Fprintf(
		stdout,
		"canary provider=%s market=%s side=%s input=%s %s output=%s broadcast=%s",
		selected.Source.Kind,
		selected.Market.ID,
		selected.Side,
		formatUnits(selected.Amount, selected.Input.Token.Decimals),
		selected.Input.Token.Symbol,
		selected.Output.Token.Symbol,
		broadcastMode,
	)
	if armed.Enabled {
		fmt.Fprintf(
			stdout,
			" operation=%s journal=%s",
			armed.OperationID,
			*operationStore,
		)
	}
	fmt.Fprintln(stdout)
	switch selected.Source.Kind {
	case "jupiter":
		err = runJupiter(
			ctx,
			stdout,
			selected,
			strings.TrimSpace(*tipLamports),
			uint32(*computeLimit),
			strings.TrimSpace(*maxPriorityFeeLamports),
			strings.TrimSpace(*computePricePercentile),
			armed,
			*solanaBroadcast,
			strings.TrimSpace(*heliusSenderURLEnv),
		)
	case "kyberswap":
		err = runKyberSwap(
			ctx,
			stdout,
			selected,
			armed,
			*kyberSlippageBPS,
			*kyberGasEstimation,
		)
	default:
		err = fmt.Errorf("quote source %q has no executable canary", selected.Source.Kind)
	}
	if err != nil {
		if armed.Enabled {
			_ = armed.Store.Mark(
				context.WithoutCancel(ctx),
				armed.OperationID,
				"failed",
				err,
			)
		}
		fmt.Fprintf(stderr, "swap-build-canary: %v\n", err)
		return 1
	}
	if armed.Enabled {
		fmt.Fprintf(stdout, "result=completed operation=%s\n", armed.OperationID)
	} else {
		fmt.Fprintln(stdout, "result=ready_for_armed_canary broadcast=disabled")
	}
	return 0
}

func selectSwap(
	config configuration.ParsedConfig,
	id market.MarketID,
	selectedSide side,
	amount *big.Int,
) (selectedSwap, error) {
	var configured *configuration.ResolvedMarket
	for index := range config.Markets {
		if config.Markets[index].ID == id {
			configured = &config.Markets[index]
			break
		}
	}
	if configured == nil {
		return selectedSwap{}, fmt.Errorf("market %q is not in the active setup", id)
	}
	source, ok := config.QuoteSources[configured.QuoteSource]
	if !ok {
		return selectedSwap{}, fmt.Errorf("market %q has no remote quote source", id)
	}
	input, output := configured.Quote, configured.Base
	if selectedSide == sell {
		input, output = configured.Base, configured.Quote
	}
	chain, ok := config.Chains[configured.Chain]
	if !ok {
		return selectedSwap{}, fmt.Errorf("market %q chain is unavailable", id)
	}
	httpURL, err := requiredEnvironment(chain.HTTPURLEnv)
	if err != nil {
		return selectedSwap{}, err
	}
	return selectedSwap{
		Market: *configured, Source: source, Chain: chain,
		Input: input, Output: output,
		Amount: new(big.Int).Set(amount), Side: selectedSide, HTTPURL: httpURL,
		AmountText: amount.String(),
	}, nil
}

func runJupiter(
	ctx context.Context,
	output io.Writer,
	selected selectedSwap,
	tipLamports string,
	computeLimit uint32,
	maxPriorityFeeLamports string,
	computePricePercentile string,
	armed armedCanary,
	broadcastTransport string,
	senderURLEnv string,
) error {
	privateText, err := requiredEnvironment("SOLANA_PRIVATE_KEY")
	if err != nil {
		return err
	}
	privateKey, err := parseSolanaPrivateKey(privateText)
	if err != nil {
		return err
	}
	taker := privateKey.PublicKey()
	if takerText, exists := os.LookupEnv(selected.Source.TakerEnv); exists &&
		strings.TrimSpace(takerText) != "" &&
		strings.TrimSpace(takerText) != taker.String() {
		fmt.Fprintf(
			output,
			"configuration_warning field=%s configured=%s signer=%s "+
				"action=using_signer\n",
			selected.Source.TakerEnv,
			strings.TrimSpace(takerText),
			taker,
		)
	}
	keysText, err := requiredEnvironment(selected.Source.APIKeyEnv)
	if err != nil {
		return err
	}
	keys, err := splitValues(keysText)
	if err != nil {
		return err
	}
	tip, ok := new(big.Int).SetString(tipLamports, 10)
	if !ok || tip.Sign() <= 0 {
		return fmt.Errorf("jupiter tip must be a positive lamport integer")
	}
	maxPriorityFee, ok := new(big.Int).SetString(maxPriorityFeeLamports, 10)
	if !ok || maxPriorityFee.Sign() <= 0 || !maxPriorityFee.IsUint64() {
		return fmt.Errorf("jupiter priority-fee cap must be a positive uint64 integer")
	}
	if computePricePercentile == "" {
		return fmt.Errorf("jupiter compute-unit price percentile is required")
	}
	var senderManager *solanaadapter.TxManager
	var senderTipAccount solanago.PublicKey
	if broadcastTransport == "helius-sender" {
		senderEndpoint, err := requiredEnvironment(senderURLEnv)
		if err != nil {
			return err
		}
		minimumTip, err := heliusMinimumTip(senderEndpoint)
		if err != nil {
			return err
		}
		if tip.Cmp(minimumTip) < 0 {
			return fmt.Errorf(
				"helius Sender requires at least %s tip lamports for this route",
				minimumTip,
			)
		}
		if !tip.IsUint64() {
			return fmt.Errorf("helius Sender tip exceeds uint64 lamports")
		}
		senderTipAccount = solanaadapter.NextHeliusSenderTipAccount()
		senderManager, err = solanaadapter.NewTxManager(
			solanaadapter.TxManagerConfig{
				Chain: selected.Input.Token.Chain, Account: "manual-canary",
				PrivateKey: privateKey, SenderEndpoint: senderEndpoint,
				SenderTipAccount: senderTipAccount, SenderTipLamports: tip.Uint64(),
				ComputeUnitLimit:       computeLimit,
				MaxPriorityFeeLamports: maxPriorityFee.Uint64(),
				Reconciliation:         unusedSolanaReconciliation{},
				Clock:                  time.Now,
			},
		)
		if err != nil {
			return err
		}
		warmStarted := time.Now()
		if err := senderManager.Warm(ctx); err != nil {
			return err
		}
		fmt.Fprintf(
			output,
			"sender=ready transport=helius-sender endpoint=%s "+
				"tip_account=%s tip_lamports=%s priority_percentile=%s "+
				"max_priority_fee_lamports=%s latency=%s\n",
			senderHost(senderEndpoint),
			senderTipAccount,
			tip,
			computePricePercentile,
			maxPriorityFee,
			formatDuration(time.Since(warmStarted)),
		)
	}
	buildTipAmount := tip.String()
	if broadcastTransport == "helius-sender" {
		// Helius recognizes only its designated tip accounts. The local
		// assembler adds that transfer after Jupiter returns the swap.
		buildTipAmount = ""
	}
	source, err := jupiter.NewBuildSource(jupiter.BuildConfig{
		ID: jupiterBuildSourceID(selected.Source.ID),
		// Discovery may use another host. Swap V2 /build is served from the
		// authenticated Router API host.
		BaseURL: defaultJupiterBuildBaseURL,
		Taker:   taker.String(),
		APIKeys: keys,
		TokenMints: map[market.TokenID]string{
			selected.Input.Token.ID:  selected.Input.AddressText,
			selected.Output.Token.ID: selected.Output.AddressText,
		},
		SlippageBPS:            selected.Source.SlippageBPS,
		MaxAccounts:            selected.Source.MaxAccounts,
		TipAmount:              buildTipAmount,
		ComputePricePercentile: computePricePercentile,
		BlockhashSlotsToExpiry: 150,
	})
	if err != nil {
		return err
	}
	input, err := market.NewTokenAmount(selected.Input.Token.ID, selected.Amount)
	if err != nil {
		return err
	}
	placeholderOutput, err := market.NewTokenAmount(
		selected.Output.Token.ID,
		big.NewInt(1),
	)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	discovery, err := market.NewQuote(market.Quote{
		Source: "manual-canary", Market: selected.Market.ID,
		SnapshotVersion: 1, Purpose: market.QuotePurposeLiveDiscovery,
		Mode: market.QuoteModeExactInput, Quality: market.QuoteQualityExact,
		AmountIn: input, AmountOut: placeholderOutput, QuotedAt: now,
	})
	if err != nil {
		return err
	}
	leg := execution.Leg{
		ID: "canary", Side: sideToLeg(selected.Side), Chain: selected.Input.Token.Chain,
		Account: "manual-canary", Market: selected.Market.ID,
		Input: input, ExpectedOutput: placeholderOutput,
	}
	buildStarted := time.Now()
	artifact, err := source.Validate(ctx, executionport.ValidationRequest{
		Operation: "manual-canary", Leg: leg, Discovery: discovery,
		RequestedAt: now,
	})
	buildDuration := time.Since(buildStarted)
	if err != nil {
		return fmt.Errorf("jupiter /build: %w", err)
	}
	var raw []byte
	var signature, blockhash string
	if broadcastTransport == "helius-sender" {
		raw, signature, blockhash, err =
			solanaadapter.AssembleJupiterBuildForSender(
				artifact.Payload,
				privateKey,
				computeLimit,
				senderTipAccount,
				tip.Uint64(),
				maxPriorityFee.Uint64(),
			)
	} else {
		raw, signature, blockhash, err = solanaadapter.AssembleJupiterBuild(
			artifact.Payload,
			privateKey,
			computeLimit,
		)
	}
	if err != nil {
		return err
	}
	transaction, err := solanago.TransactionFromDecoder(bin.NewBinDecoder(raw))
	if err != nil {
		return fmt.Errorf("decode signed jupiter canary: %w", err)
	}
	if broadcastTransport == "helius-sender" &&
		!hasComputeUnitPrice(transaction) {
		return fmt.Errorf(
			"helius Sender transaction has no compute-unit-price instruction",
		)
	}
	client := solanarpc.New(selected.HTTPURL)
	simulationStarted := time.Now()
	simulation, err := client.SimulateTransactionWithOpts(
		ctx,
		transaction,
		&solanarpc.SimulateTransactionOpts{
			SigVerify: true, Commitment: solanarpc.CommitmentConfirmed,
		},
	)
	simulationDuration := time.Since(simulationStarted)
	if err != nil {
		return fmt.Errorf("simulate Jupiter transaction: %w", err)
	}
	if simulation == nil || simulation.Value == nil {
		return fmt.Errorf("simulate Jupiter transaction returned no result")
	}
	if simulation.Value.Err != nil {
		return fmt.Errorf(
			"jupiter simulation failed: %v logs=%v",
			simulation.Value.Err,
			simulation.Value.Logs,
		)
	}
	computeUnits := uint64(0)
	if simulation.Value.UnitsConsumed != nil {
		computeUnits = *simulation.Value.UnitsConsumed
	}
	fmt.Fprintf(
		output,
		"build=ok provider=jupiter input_units=%s output_units=%s "+
			"output=%s %s latency=%s\n",
		artifact.ValidatedQuote.AmountIn,
		artifact.ValidatedQuote.AmountOut,
		formatUnits(
			artifact.ValidatedQuote.AmountOut.Units(),
			selected.Output.Token.Decimals,
		),
		selected.Output.Token.Symbol,
		formatDuration(buildDuration),
	)
	fmt.Fprintf(
		output,
		"signed=ok chain=solana tx=%s blockhash=%s last_valid_height=%d bytes=%d\n",
		signature,
		blockhash,
		artifact.LastValidBlockHeight,
		len(raw),
	)
	fmt.Fprintf(
		output,
		"simulation=ok chain=solana compute_units=%d latency=%s\n",
		computeUnits,
		formatDuration(simulationDuration),
	)
	if armed.Enabled {
		return broadcastJupiterCanary(
			ctx,
			output,
			armed,
			client,
			transaction,
			raw,
			signature,
			blockhash,
			artifact.LastValidBlockHeight,
			artifact.ValidatedQuote.AmountIn.Units(),
			artifact.ValidatedQuote.AmountOut.Units(),
			selected,
			broadcastTransport,
			senderManager,
		)
	}
	return nil
}

func runKyberSwap(
	ctx context.Context,
	output io.Writer,
	selected selectedSwap,
	armed armedCanary,
	slippageOverride int,
	enableGasEstimation bool,
) error {
	privateText, err := requiredEnvironment("POLYGON_PRIVATE_KEY")
	if err != nil {
		return err
	}
	privateKey, err := gethcrypto.HexToECDSA(
		strings.TrimPrefix(strings.TrimSpace(privateText), "0x"),
	)
	if err != nil {
		return fmt.Errorf("invalid polygon private key")
	}
	sender := gethcrypto.PubkeyToAddress(privateKey.PublicKey)
	clientID, err := requiredEnvironment(selected.Source.ClientIDEnv)
	if err != nil {
		return err
	}
	source, err := kyberswap.New(kyberswap.Config{
		BaseURL: selected.Source.BaseURL, ClientID: clientID,
		Timeout: 15 * time.Second,
	})
	if err != nil {
		return err
	}
	routeStarted := time.Now()
	route, err := source.Route(ctx, kyberswap.RouteRequest{
		Chain:   selected.Source.ChainSlug,
		TokenIn: selected.Input.AddressText, TokenOut: selected.Output.AddressText,
		AmountIn: selected.Amount.String(), Origin: sender.Hex(),
	})
	routeDuration := time.Since(routeStarted)
	if err != nil {
		return fmt.Errorf("kyberswap route: %w", err)
	}
	buildStarted := time.Now()
	slippageBPS := selected.Source.SlippageBPS
	if slippageOverride >= 0 {
		slippageBPS = uint16(slippageOverride)
	}
	built, err := source.Build(ctx, kyberswap.BuildRequest{
		Route: route, Sender: sender.Hex(), Recipient: sender.Hex(),
		Origin: sender.Hex(), SlippageBPS: slippageBPS,
		EnableGasEstimation: enableGasEstimation,
	})
	buildDuration := time.Since(buildStarted)
	if err != nil {
		return fmt.Errorf("kyberswap build: %w", err)
	}
	router := common.HexToAddress(built.RouterAddress)
	rpc, err := ethclient.DialContext(ctx, selected.HTTPURL)
	if err != nil {
		return fmt.Errorf("connect polygon RPC: %w", err)
	}
	defer rpc.Close()
	balance, err := readERC20Uint(
		ctx,
		rpc,
		common.HexToAddress(selected.Input.AddressText),
		"70a08231",
		sender,
	)
	if err != nil {
		return fmt.Errorf("read kyberswap input balance: %w", err)
	}
	allowance, err := readAllowance(
		ctx,
		rpc,
		common.HexToAddress(selected.Input.AddressText),
		sender,
		router,
	)
	if err != nil {
		return fmt.Errorf("read kyberswap allowance: %w", err)
	}
	fmt.Fprintf(
		output,
		"route=ok provider=kyberswap input_units=%s output_units=%s "+
			"output=%s %s latency=%s\n",
		route.AmountIn,
		route.AmountOut,
		formatUnits(
			mustPositiveInteger(route.AmountOut),
			selected.Output.Token.Decimals,
		),
		selected.Output.Token.Symbol,
		formatDuration(routeDuration),
	)
	fmt.Fprintf(
		output,
		"build=ok provider=kyberswap router=%s calldata_bytes=%d "+
			"gas_hint=%s value_wei=%s slippage_bps=%d "+
			"server_gas_estimation=%t latency=%s\n",
		built.RouterAddress,
		(len(strings.TrimPrefix(built.Data, "0x")) / 2),
		built.Gas,
		built.TransactionValue,
		slippageBPS,
		enableGasEstimation,
		formatDuration(buildDuration),
	)
	fmt.Fprintf(
		output,
		"build_quote input_units=%s route_output_units=%s "+
			"build_output_units=%s output_change=%s selector=%s artifact_age=%s\n",
		built.AmountIn,
		route.AmountOut,
		built.AmountOut,
		emptyAsNone(built.OutputChange),
		calldataSelector(built.Data),
		formatDuration(time.Since(buildStarted)),
	)
	fmt.Fprintf(
		output,
		"readiness chain=polygon balance_units=%s allowance_units=%s required_units=%s\n",
		balance,
		allowance,
		selected.Amount,
	)
	if balance.Cmp(selected.Amount) < 0 {
		return fmt.Errorf("polygon input-token balance is below canary amount")
	}
	if allowance.Cmp(selected.Amount) < 0 {
		return fmt.Errorf(
			"polygon approval required: token=%s spender=%s amount_units=%s",
			selected.Input.AddressText,
			router.Hex(),
			selected.Amount,
		)
	}
	calldata, err := hex.DecodeString(strings.TrimPrefix(built.Data, "0x"))
	if err != nil {
		return fmt.Errorf("decode kyberswap calldata: %w", err)
	}
	value, ok := new(big.Int).SetString(built.TransactionValue, 10)
	if !ok || value.Sign() < 0 {
		return fmt.Errorf("kyberswap transaction value is invalid")
	}
	call := geth.CallMsg{
		From: sender, To: &router, Value: value, Data: calldata,
	}
	simulationStarted := time.Now()
	simulationResult, err := rpc.CallContract(ctx, call, nil)
	if err != nil {
		return fmt.Errorf("simulate kyberswap transaction: %w", err)
	}
	estimatedGas, err := rpc.EstimateGas(ctx, call)
	if err != nil {
		return fmt.Errorf("estimate kyberswap transaction gas: %w", err)
	}
	simulatedOutput, contractGas := abiSwapResult(simulationResult)
	fmt.Fprintf(
		output,
		"simulation=ok chain=polygon estimated_gas=%d output_units=%s "+
			"contract_gas_used=%s latency=%s\n",
		estimatedGas,
		simulatedOutput,
		contractGas,
		formatDuration(time.Since(simulationStarted)),
	)
	if armed.Enabled {
		return broadcastKyberCanary(
			ctx,
			output,
			armed,
			rpc,
			privateKey,
			sender,
			router,
			value,
			calldata,
			estimatedGas,
			selected,
			mustPositiveInteger(route.AmountOut),
		)
	}
	return nil
}

func readERC20Uint(
	ctx context.Context,
	client *ethclient.Client,
	token common.Address,
	selector string,
	account common.Address,
) (*big.Int, error) {
	selectorBytes, _ := hex.DecodeString(selector)
	data := append(selectorBytes, common.LeftPadBytes(account.Bytes(), 32)...)
	raw, err := client.CallContract(ctx, geth.CallMsg{To: &token, Data: data}, nil)
	if err != nil {
		return nil, err
	}
	if len(raw) != 32 {
		return nil, fmt.Errorf("ERC-20 query returned %d bytes", len(raw))
	}
	return new(big.Int).SetBytes(raw), nil
}

func readAllowance(
	ctx context.Context,
	client *ethclient.Client,
	token, owner, spender common.Address,
) (*big.Int, error) {
	selector, _ := hex.DecodeString("dd62ed3e")
	data := append(selector, common.LeftPadBytes(owner.Bytes(), 32)...)
	data = append(data, common.LeftPadBytes(spender.Bytes(), 32)...)
	raw, err := client.CallContract(ctx, geth.CallMsg{To: &token, Data: data}, nil)
	if err != nil {
		return nil, err
	}
	if len(raw) != 32 {
		return nil, fmt.Errorf("ERC-20 allowance returned %d bytes", len(raw))
	}
	return new(big.Int).SetBytes(raw), nil
}

func parseSolanaPrivateKey(value string) (solanago.PrivateKey, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, fmt.Errorf("solana private key is empty")
	}
	if strings.HasPrefix(value, "[") {
		var bytes []byte
		if err := json.Unmarshal([]byte(value), &bytes); err != nil || len(bytes) != 64 {
			return nil, fmt.Errorf("invalid Solana private key")
		}
		key := solanago.PrivateKey(append([]byte(nil), bytes...))
		if !key.PublicKey().IsZero() {
			return key, nil
		}
	}
	if key, err := solanago.PrivateKeyFromBase58(value); err == nil &&
		!key.PublicKey().IsZero() {
		return key, nil
	}
	return nil, fmt.Errorf("invalid Solana private key")
}

func splitValues(raw string) ([]string, error) {
	parts := strings.Split(strings.TrimSpace(raw), ",")
	values := make([]string, 0, len(parts))
	for index, part := range parts {
		value := strings.TrimSpace(part)
		if value == "" {
			return nil, fmt.Errorf("API key list contains an empty item at index %d", index)
		}
		values = append(values, value)
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("API key list is empty")
	}
	return values, nil
}

func requiredEnvironment(name string) (string, error) {
	if strings.TrimSpace(name) == "" {
		return "", fmt.Errorf("required environment name is empty")
	}
	value, ok := os.LookupEnv(name)
	if !ok || strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("required environment %q is unset", name)
	}
	return strings.TrimSpace(value), nil
}

func sideToLeg(value side) execution.LegSide {
	if value == sell {
		return execution.LegSell
	}
	return execution.LegBuy
}

func jupiterBuildSourceID(id string) market.SourceID {
	return market.SourceID(strings.TrimSpace(id) + "/build")
}

func mustPositiveInteger(value string) *big.Int {
	result, ok := new(big.Int).SetString(value, 10)
	if !ok || result.Sign() <= 0 {
		return new(big.Int)
	}
	return result
}

func formatUnits(units *big.Int, decimals uint8) string {
	if units == nil {
		return "0"
	}
	if decimals == 0 {
		return units.String()
	}
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	whole, fraction := new(big.Int), new(big.Int)
	whole.QuoRem(new(big.Int).Set(units), scale, fraction)
	text := fraction.Text(10)
	text = strings.Repeat("0", int(decimals)-len(text)) + text
	text = strings.TrimRight(text, "0")
	if text == "" {
		return whole.String()
	}
	return whole.String() + "." + text
}

func formatDuration(value time.Duration) string {
	return value.Round(10 * time.Microsecond).String()
}

func heliusMinimumTip(endpoint string) (*big.Int, error) {
	parsed, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("invalid Helius Sender endpoint")
	}
	minimum := int64(1_000_000)
	if strings.EqualFold(parsed.Query().Get("swqos_only"), "true") {
		minimum = 5_000
	}
	return big.NewInt(minimum), nil
}

func senderHost(endpoint string) string {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "invalid"
	}
	return parsed.Host
}

func hasComputeUnitPrice(transaction *solanago.Transaction) bool {
	if transaction == nil {
		return false
	}
	for _, instruction := range transaction.Message.Instructions {
		program, err := transaction.Message.ResolveProgramIDIndex(
			instruction.ProgramIDIndex,
		)
		if err == nil &&
			program.String() == "ComputeBudget111111111111111111111111111111" &&
			len(instruction.Data) == 9 &&
			instruction.Data[0] == 3 {
			return true
		}
	}
	return false
}

func emptyAsNone(value string) string {
	if strings.TrimSpace(value) == "" {
		return "none"
	}
	return strings.TrimSpace(value)
}

func calldataSelector(value string) string {
	value = strings.TrimPrefix(strings.TrimSpace(value), "0x")
	if len(value) < 8 {
		return "invalid"
	}
	return "0x" + value[:8]
}

func abiSwapResult(value []byte) (string, string) {
	if len(value) < 64 {
		return "unavailable", "unavailable"
	}
	return new(big.Int).SetBytes(value[:32]).String(),
		new(big.Int).SetBytes(value[32:64]).String()
}
