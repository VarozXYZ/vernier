// Package liveapproval discovers and prepares the persistent ERC-20
// allowances required by a configured sequential Live setup.
package liveapproval

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	gethcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	solanago "github.com/gagliardetto/solana-go"

	"github.com/VarozXYZ/vernier/adapters/crosschain/across"
	"github.com/VarozXYZ/vernier/adapters/quote/kyberswap"
	"github.com/VarozXYZ/vernier/internal/nttmanualcanary"
	"github.com/VarozXYZ/vernier/runtime/configuration"
)

const (
	defaultTimeout  = 2 * time.Minute
	approveSelector = "095ea7b3"
)

var maxUint256 = new(big.Int).Sub(
	new(big.Int).Lsh(big.NewInt(1), 256),
	big.NewInt(1),
)

var nearInfiniteAllowance = new(big.Int).Lsh(big.NewInt(1), 255)

type Target struct {
	Token       common.Address
	TokenSymbol string
	Spender     common.Address
	Purposes    []string
}

type discoveredSetup struct {
	Owner       common.Address
	PrivateKey  *ecdsa.PrivateKey
	ChainID     *big.Int
	RPCURL      string
	Targets     []Target
	ProbeAmount *big.Int
}

type discoveredPlan struct {
	Owner  common.Address
	Chains []discoveredSetup
}

func Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("live-approve", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "private Research/Live manifest")
	envPath := flags.String("env-file", ".env.test", "local environment file")
	arm := flags.Bool("arm", false, "sign and broadcast approve(MaxUint256) transactions")
	confirmOwner := flags.String(
		"confirm-owner",
		"",
		"must exactly match the derived EVM owner when --arm is used",
	)
	timeout := flags.Duration(
		"confirmation-timeout",
		defaultTimeout,
		"maximum wait for each approval receipt",
	)
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "live-approve: positional arguments are not supported")
		return 2
	}
	if strings.TrimSpace(*configPath) == "" {
		fmt.Fprintln(stderr, "live-approve: --config is required")
		return 2
	}
	if *timeout <= 0 {
		fmt.Fprintln(stderr, "live-approve: --confirmation-timeout must be positive")
		return 2
	}
	if !*arm && strings.TrimSpace(*confirmOwner) != "" {
		fmt.Fprintln(stderr, "live-approve: --confirm-owner requires --arm")
		return 2
	}
	if *arm && strings.TrimSpace(*confirmOwner) == "" {
		fmt.Fprintln(stderr, "live-approve: --arm requires --confirm-owner")
		return 2
	}
	if err := configuration.LoadEnvFile(*envPath, os.LookupEnv, os.Setenv); err != nil {
		fmt.Fprintln(stderr, "live-approve: cannot load local environment")
		return 2
	}
	plan, err := discoverPlan(ctx, *configPath, os.LookupEnv)
	if err != nil {
		fmt.Fprintf(stderr, "live-approve: %v\n", err)
		return 1
	}
	if *arm && !strings.EqualFold(strings.TrimSpace(*confirmOwner), plan.Owner.Hex()) {
		fmt.Fprintf(
			stderr,
			"live-approve: --confirm-owner must exactly match derived owner %s\n",
			plan.Owner.Hex(),
		)
		return 2
	}
	sent := 0
	skipped := 0
	required := 0
	for _, setup := range plan.Chains {
		client, dialErr := ethclient.DialContext(ctx, setup.RPCURL)
		if dialErr != nil {
			fmt.Fprintf(stderr, "live-approve: connect EVM RPC: %v\n", dialErr)
			return 1
		}
		networkChainID, chainErr := client.ChainID(ctx)
		if chainErr != nil || networkChainID.Cmp(setup.ChainID) != 0 {
			client.Close()
			fmt.Fprintf(stderr, "live-approve: RPC chain ID does not match configuration %s\n", setup.ChainID)
			return 1
		}
		fmt.Fprintf(stdout, "approval_setup chain_id=%s owner=%s targets=%d mode=%s\n",
			setup.ChainID, setup.Owner.Hex(), len(setup.Targets), map[bool]string{false: "audit", true: "armed"}[*arm])
		for index, target := range setup.Targets {
			allowance, readErr := readAllowance(ctx, client, target.Token, setup.Owner, target.Spender)
			if readErr != nil {
				client.Close()
				fmt.Fprintf(stderr, "live-approve: read allowance: %v\n", readErr)
				return 1
			}
			status := "required"
			if allowance.Cmp(nearInfiniteAllowance) >= 0 {
				status = "already_sufficient"
			}
			fmt.Fprintf(stdout,
				"target=%d/%d token=%s token_address=%s spender=%s purpose=%s allowance=%s status=%s\n",
				index+1, len(setup.Targets), target.TokenSymbol, target.Token.Hex(), target.Spender.Hex(),
				strings.Join(target.Purposes, ","), formatAllowance(allowance), status)
			if status == "already_sufficient" {
				skipped++
				continue
			}
			required++
			if !*arm {
				continue
			}
			if approveErr := approve(ctx, stdout, client, setup.PrivateKey, setup.ChainID, setup.Owner, target, *timeout); approveErr != nil {
				client.Close()
				fmt.Fprintf(stderr, "live-approve: %v\n", approveErr)
				return 1
			}
			sent++
		}
		client.Close()
	}
	if !*arm {
		fmt.Fprintf(stdout, "broadcast=disabled required=%d already_sufficient=%d\n", required, skipped)
		return 0
	}
	fmt.Fprintf(
		stdout,
		"result=completed approvals_sent=%d already_sufficient=%d owner=%s\n",
		sent,
		skipped,
		plan.Owner.Hex(),
	)
	return 0
}

func discoverPlan(ctx context.Context, manifestPath string, lookup configuration.LookupEnv) (discoveredPlan, error) {
	live, err := configuration.LoadLiveConfig(manifestPath)
	if err != nil {
		return discoveredPlan{}, err
	}
	if len(live.ApprovalSpenders) != 0 {
		return discoverExplicitPlan(manifestPath, lookup, live)
	}
	legacy, err := discover(ctx, manifestPath, lookup)
	if err != nil {
		return discoveredPlan{}, err
	}
	return discoveredPlan{Owner: legacy.Owner, Chains: []discoveredSetup{legacy}}, nil
}

func discoverExplicitPlan(manifestPath string, lookup configuration.LookupEnv, live configuration.ParsedLiveConfig) (discoveredPlan, error) {
	research, err := configuration.LoadConfig(manifestPath)
	if err != nil {
		return discoveredPlan{}, err
	}
	if research.Hash != live.Hash || research.SetupID != live.SetupID {
		return discoveredPlan{}, fmt.Errorf("research and Live manifests do not match")
	}
	endpoints, err := research.ResolveEndpoints(lookup)
	if err != nil {
		return discoveredPlan{}, err
	}
	type tokenInfo struct {
		address common.Address
		symbol  string
	}
	tokens := make(map[string]map[string]tokenInfo)
	addToken := func(chain string, id string, address common.Address, symbol string) {
		if tokens[chain] == nil {
			tokens[chain] = make(map[string]tokenInfo)
		}
		tokens[chain][id] = tokenInfo{address: address, symbol: symbol}
	}
	for _, configured := range research.Markets {
		addToken(configured.Chain, string(configured.Base.Token.ID), configured.Base.Address, configured.Base.Token.Symbol)
		addToken(configured.Chain, string(configured.Quote.Token.ID), configured.Quote.Address, configured.Quote.Token.Symbol)
	}
	for _, conversion := range live.QuoteConversions {
		addToken(conversion.Chain, string(conversion.TokenA.Token.ID), conversion.TokenA.Address, conversion.TokenA.Token.Symbol)
		addToken(conversion.Chain, string(conversion.TokenB.Token.ID), conversion.TokenB.Address, conversion.TokenB.Token.Symbol)
	}
	grouped := make(map[string][]Target)
	for _, requirement := range live.ApprovalSpenders {
		chain := string(requirement.Chain)
		token, ok := tokens[chain][string(requirement.Token)]
		if !ok || token.address == (common.Address{}) {
			return discoveredPlan{}, fmt.Errorf("approval token %s/%s is unavailable", chain, requirement.Token)
		}
		grouped[chain] = append(grouped[chain], Target{Token: token.address, TokenSymbol: token.symbol,
			Spender: requirement.Spender, Purposes: []string{requirement.Purpose}})
	}
	plan := discoveredPlan{}
	for chain, targets := range grouped {
		resolvedChain, ok := research.Chains[chain]
		if !ok || resolvedChain.Kind != "evm" || resolvedChain.ChainID == nil || resolvedChain.ChainID.Sign() <= 0 {
			return discoveredPlan{}, fmt.Errorf("approval chain %s is not a configured EVM chain", chain)
		}
		account, ok := live.Accounts[chain]
		if !ok {
			return discoveredPlan{}, fmt.Errorf("approval chain %s has no Live account", chain)
		}
		keyText, err := requiredEnv(lookup, account.SignerEnv)
		if err != nil {
			return discoveredPlan{}, err
		}
		privateKey, err := gethcrypto.HexToECDSA(strings.TrimPrefix(strings.TrimSpace(keyText), "0x"))
		if err != nil {
			return discoveredPlan{}, fmt.Errorf("invalid EVM private key for chain %s", chain)
		}
		owner := gethcrypto.PubkeyToAddress(privateKey.PublicKey)
		if plan.Owner == (common.Address{}) {
			plan.Owner = owner
		} else if plan.Owner != owner {
			return discoveredPlan{}, fmt.Errorf("explicit approval chains must use the same owner")
		}
		targets, err = normalizeTargets(targets)
		if err != nil {
			return discoveredPlan{}, err
		}
		plan.Chains = append(plan.Chains, discoveredSetup{Owner: owner, PrivateKey: privateKey,
			ChainID: new(big.Int).Set(resolvedChain.ChainID), RPCURL: endpoints[chain], Targets: targets})
	}
	if plan.Owner == (common.Address{}) || len(plan.Chains) == 0 {
		return discoveredPlan{}, fmt.Errorf("explicit approval allowlist is empty")
	}
	sort.Slice(plan.Chains, func(i, j int) bool { return plan.Chains[i].ChainID.Cmp(plan.Chains[j].ChainID) < 0 })
	return plan, nil
}

func discover(
	ctx context.Context,
	manifestPath string,
	lookup configuration.LookupEnv,
) (discoveredSetup, error) {
	research, err := configuration.LoadConfig(manifestPath)
	if err != nil {
		return discoveredSetup{}, err
	}
	live, err := configuration.LoadLiveConfig(manifestPath)
	if err != nil {
		return discoveredSetup{}, err
	}
	if research.Hash != live.Hash || research.SetupID != live.SetupID {
		return discoveredSetup{}, fmt.Errorf("research and Live manifests do not match")
	}
	endpoints, err := research.ResolveEndpoints(lookup)
	if err != nil {
		return discoveredSetup{}, err
	}
	var evmMarket, solanaMarket *configuration.ResolvedMarket
	for index := range research.Markets {
		candidate := &research.Markets[index]
		switch research.Chains[candidate.Chain].Kind {
		case "evm":
			evmMarket = candidate
		case "solana":
			solanaMarket = candidate
		}
	}
	if evmMarket == nil || solanaMarket == nil {
		return discoveredSetup{}, fmt.Errorf("setup requires one EVM and one Solana market")
	}
	evmChain := research.Chains[evmMarket.Chain]
	if evmChain.ChainID == nil || evmChain.ChainID.Sign() <= 0 {
		return discoveredSetup{}, fmt.Errorf("EVM chain ID is invalid")
	}
	evmAccount, ok := live.Accounts[evmMarket.Chain]
	if !ok {
		return discoveredSetup{}, fmt.Errorf("EVM Live account is missing")
	}
	evmKeyText, err := requiredEnv(lookup, evmAccount.SignerEnv)
	if err != nil {
		return discoveredSetup{}, err
	}
	privateKey, err := gethcrypto.HexToECDSA(
		strings.TrimPrefix(strings.TrimSpace(evmKeyText), "0x"),
	)
	if err != nil {
		return discoveredSetup{}, fmt.Errorf("invalid EVM private key")
	}
	owner := gethcrypto.PubkeyToAddress(privateKey.PublicKey)
	solanaAccount, ok := live.Accounts[solanaMarket.Chain]
	if !ok {
		return discoveredSetup{}, fmt.Errorf("solana Live account is missing")
	}
	solanaKeyText, err := requiredEnv(lookup, solanaAccount.SignerEnv)
	if err != nil {
		return discoveredSetup{}, err
	}
	solanaKey, err := parseSolanaPrivateKey(solanaKeyText)
	if err != nil {
		return discoveredSetup{}, err
	}
	probeAmount, err := amountUnits(live.CanaryInput, evmMarket.Quote.Token.Decimals)
	if err != nil {
		return discoveredSetup{}, fmt.Errorf("resolve approval probe amount: %w", err)
	}

	targets, err := discoverKyberTargets(
		ctx, research, *evmMarket, owner, probeAmount, lookup,
	)
	if err != nil {
		return discoveredSetup{}, err
	}
	if live.GasRefuel.Enabled {
		refuelTarget, refuelErr := discoverKyberRefuelTarget(
			ctx,
			research,
			*evmMarket,
			owner,
			probeAmount,
			lookup,
		)
		if refuelErr != nil {
			return discoveredSetup{}, refuelErr
		}
		targets = append(targets, refuelTarget)
	}
	nttPath := filepath.Join(filepath.Dir(manifestPath), live.BaseBridgeProfile)
	nttTarget, err := nttmanualcanary.LoadEVMApprovalTarget(nttPath)
	if err != nil {
		return discoveredSetup{}, fmt.Errorf("resolve NTT approval: %w", err)
	}
	if nttTarget.ChainID.Cmp(evmChain.ChainID) != 0 ||
		nttTarget.Token != evmMarket.Base.Address {
		return discoveredSetup{}, fmt.Errorf("NTT approval target does not match the EVM market")
	}
	targets = append(targets, Target{
		Token: nttTarget.Token, TokenSymbol: evmMarket.Base.Token.Symbol,
		Spender: nttTarget.Manager, Purposes: []string{"wormhole_ntt_base_bridge"},
	})

	acrossTarget, err := discoverAcrossTarget(
		ctx,
		research,
		*evmMarket,
		*solanaMarket,
		owner,
		solanaKey.PublicKey().String(),
		probeAmount,
		lookup,
	)
	if err != nil {
		return discoveredSetup{}, err
	}
	targets = append(targets, acrossTarget)
	targets, err = normalizeTargets(targets)
	if err != nil {
		return discoveredSetup{}, err
	}
	return discoveredSetup{
		Owner: owner, PrivateKey: privateKey,
		ChainID: new(big.Int).Set(evmChain.ChainID),
		RPCURL:  endpoints[evmMarket.Chain], Targets: targets,
		ProbeAmount: probeAmount,
	}, nil
}

func discoverKyberRefuelTarget(
	ctx context.Context,
	config configuration.ParsedConfig,
	configured configuration.ResolvedMarket,
	owner common.Address,
	quoteAmount *big.Int,
	lookup configuration.LookupEnv,
) (Target, error) {
	const nativePseudoAddress = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"
	sourceConfig, ok := config.QuoteSources[configured.QuoteSource]
	if !ok || sourceConfig.Kind != "kyberswap" {
		return Target{}, fmt.Errorf("EVM refuel market requires KyberSwap")
	}
	clientID, err := requiredEnv(lookup, sourceConfig.ClientIDEnv)
	if err != nil {
		return Target{}, err
	}
	source, err := kyberswap.New(kyberswap.Config{
		BaseURL: sourceConfig.BaseURL, ClientID: clientID,
		Timeout: 15 * time.Second,
	})
	if err != nil {
		return Target{}, err
	}
	route, err := source.Route(ctx, kyberswap.RouteRequest{
		Chain:   sourceConfig.ChainSlug,
		TokenIn: configured.Quote.AddressText, TokenOut: nativePseudoAddress,
		AmountIn: quoteAmount.String(), Origin: owner.Hex(),
	})
	if err != nil {
		return Target{}, fmt.Errorf("discover KyberSwap refuel router: %w", err)
	}
	built, err := source.Build(ctx, kyberswap.BuildRequest{
		Route: route, Sender: owner.Hex(), Recipient: owner.Hex(),
		Origin: owner.Hex(), SlippageBPS: 20, EnableGasEstimation: false,
	})
	if err != nil {
		return Target{}, fmt.Errorf("discover KyberSwap refuel spender: %w", err)
	}
	if !common.IsHexAddress(built.RouterAddress) {
		return Target{}, fmt.Errorf("KyberSwap returned an invalid refuel router")
	}
	return Target{
		Token:       configured.Quote.Address,
		TokenSymbol: configured.Quote.Token.Symbol,
		Spender:     common.HexToAddress(built.RouterAddress),
		Purposes:    []string{"kyberswap_gas_refuel"},
	}, nil
}

func discoverKyberTargets(
	ctx context.Context,
	config configuration.ParsedConfig,
	configured configuration.ResolvedMarket,
	owner common.Address,
	quoteAmount *big.Int,
	lookup configuration.LookupEnv,
) ([]Target, error) {
	sourceConfig, ok := config.QuoteSources[configured.QuoteSource]
	if !ok || sourceConfig.Kind != "kyberswap" {
		return nil, fmt.Errorf("EVM market requires KyberSwap")
	}
	clientID, err := requiredEnv(lookup, sourceConfig.ClientIDEnv)
	if err != nil {
		return nil, err
	}
	source, err := kyberswap.New(kyberswap.Config{
		BaseURL:  sourceConfig.BaseURL,
		ClientID: clientID,
		Timeout:  15 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	buy, err := source.Route(ctx, kyberswap.RouteRequest{
		Chain:    sourceConfig.ChainSlug,
		TokenIn:  configured.Quote.AddressText,
		TokenOut: configured.Base.AddressText,
		AmountIn: quoteAmount.String(),
		Origin:   owner.Hex(),
	})
	if err != nil {
		return nil, fmt.Errorf("discover KyberSwap buy router: %w", err)
	}
	buyBuild, err := source.Build(ctx, kyberswap.BuildRequest{
		Route: buy, Sender: owner.Hex(), Recipient: owner.Hex(), Origin: owner.Hex(),
		SlippageBPS: sourceConfig.SlippageBPS, EnableGasEstimation: false,
	})
	if err != nil {
		return nil, fmt.Errorf("discover KyberSwap buy spender: %w", err)
	}
	baseAmount, ok := new(big.Int).SetString(buy.AmountOut, 10)
	if !ok || baseAmount.Sign() <= 0 {
		return nil, fmt.Errorf("KyberSwap buy probe output is invalid")
	}
	sell, err := source.Route(ctx, kyberswap.RouteRequest{
		Chain:    sourceConfig.ChainSlug,
		TokenIn:  configured.Base.AddressText,
		TokenOut: configured.Quote.AddressText,
		AmountIn: baseAmount.String(),
		Origin:   owner.Hex(),
	})
	if err != nil {
		return nil, fmt.Errorf("discover KyberSwap sell router: %w", err)
	}
	sellBuild, err := source.Build(ctx, kyberswap.BuildRequest{
		Route: sell, Sender: owner.Hex(), Recipient: owner.Hex(), Origin: owner.Hex(),
		SlippageBPS: sourceConfig.SlippageBPS, EnableGasEstimation: false,
	})
	if err != nil {
		return nil, fmt.Errorf("discover KyberSwap sell spender: %w", err)
	}
	if !common.IsHexAddress(buyBuild.RouterAddress) ||
		!common.IsHexAddress(sellBuild.RouterAddress) {
		return nil, fmt.Errorf("KyberSwap returned an invalid router address")
	}
	return []Target{
		{
			Token:       configured.Quote.Address,
			TokenSymbol: configured.Quote.Token.Symbol,
			Spender:     common.HexToAddress(buyBuild.RouterAddress),
			Purposes:    []string{"kyberswap_buy"},
		},
		{
			Token:       configured.Base.Address,
			TokenSymbol: configured.Base.Token.Symbol,
			Spender:     common.HexToAddress(sellBuild.RouterAddress),
			Purposes:    []string{"kyberswap_sell"},
		},
	}, nil
}

func discoverAcrossTarget(
	ctx context.Context,
	config configuration.ParsedConfig,
	evmMarket configuration.ResolvedMarket,
	solanaMarket configuration.ResolvedMarket,
	owner common.Address,
	solanaRecipient string,
	amount *big.Int,
	lookup configuration.LookupEnv,
) (Target, error) {
	apiKey, err := requiredEnv(lookup, "ACROSS_API_KEY")
	if err != nil {
		return Target{}, err
	}
	integratorID, err := requiredEnv(lookup, "ACROSS_INTEGRATOR_ID")
	if err != nil {
		return Target{}, err
	}
	client, err := across.New(across.Config{
		APIKey:       apiKey,
		IntegratorID: integratorID,
	})
	if err != nil {
		return Target{}, err
	}
	chainID := config.Chains[evmMarket.Chain].ChainID
	if chainID == nil || !chainID.IsUint64() {
		return Target{}, fmt.Errorf("across EVM chain ID is invalid")
	}
	approval, err := client.Approval(ctx, across.ApprovalRequest{
		OriginChainID:      chainID.Uint64(),
		DestinationChainID: across.SolanaChainID,
		InputToken:         evmMarket.Quote.AddressText,
		OutputToken:        solanaMarket.Quote.AddressText,
		Amount:             amount.String(),
		Depositor:          owner.Hex(),
		Recipient:          solanaRecipient,
		RefundAddress:      owner.Hex(),
		Slippage:           "auto",
		CostOnly:           true,
	})
	if err != nil {
		return Target{}, fmt.Errorf("discover Across spender: %w", err)
	}
	spender, err := acrossSpender(approval, evmMarket.Quote.Address)
	if err != nil {
		return Target{}, err
	}
	if approval.Allowance.Token != "" &&
		(!common.IsHexAddress(approval.Allowance.Token) ||
			common.HexToAddress(approval.Allowance.Token) != evmMarket.Quote.Address) {
		return Target{}, fmt.Errorf("across allowance token does not match EVM quote token")
	}
	return Target{
		Token:       evmMarket.Quote.Address,
		TokenSymbol: evmMarket.Quote.Token.Symbol,
		Spender:     spender,
		Purposes:    []string{"across_quote_bridge"},
	}, nil
}

func acrossSpender(approval across.Approval, token common.Address) (common.Address, error) {
	if common.IsHexAddress(approval.Allowance.Spender) {
		spender := common.HexToAddress(approval.Allowance.Spender)
		if spender != (common.Address{}) {
			return spender, nil
		}
	}
	selector, _ := hex.DecodeString(approveSelector)
	for _, transaction := range approval.ApprovalTransactions {
		if !common.IsHexAddress(transaction.To) ||
			common.HexToAddress(transaction.To) != token {
			continue
		}
		raw, err := hex.DecodeString(strings.TrimPrefix(transaction.Data, "0x"))
		if err != nil || len(raw) < 68 || !bytes.Equal(raw[:4], selector) {
			continue
		}
		spender := common.BytesToAddress(raw[16:36])
		if spender != (common.Address{}) {
			return spender, nil
		}
	}
	return common.Address{}, fmt.Errorf("across did not identify its ERC-20 spender")
}

func normalizeTargets(input []Target) ([]Target, error) {
	byKey := make(map[string]Target, len(input))
	for _, target := range input {
		if target.Token == (common.Address{}) || target.Spender == (common.Address{}) ||
			strings.TrimSpace(target.TokenSymbol) == "" || len(target.Purposes) == 0 {
			return nil, fmt.Errorf("approval target is incomplete")
		}
		key := strings.ToLower(target.Token.Hex() + ":" + target.Spender.Hex())
		if current, ok := byKey[key]; ok {
			current.Purposes = append(current.Purposes, target.Purposes...)
			current.Purposes = uniqueStrings(current.Purposes)
			byKey[key] = current
			continue
		}
		target.Purposes = uniqueStrings(target.Purposes)
		byKey[key] = target
	}
	result := make([]Target, 0, len(byKey))
	for _, target := range byKey {
		result = append(result, target)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].TokenSymbol != result[j].TokenSymbol {
			return result[i].TokenSymbol < result[j].TokenSymbol
		}
		return result[i].Spender.Hex() < result[j].Spender.Hex()
	})
	return result, nil
}

func uniqueStrings(input []string) []string {
	seen := make(map[string]struct{}, len(input))
	result := make([]string, 0, len(input))
	for _, item := range input {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, exists := seen[item]; exists {
			continue
		}
		seen[item] = struct{}{}
		result = append(result, item)
	}
	sort.Strings(result)
	return result
}

func approve(
	ctx context.Context,
	output io.Writer,
	client *ethclient.Client,
	privateKey *ecdsa.PrivateKey,
	chainID *big.Int,
	owner common.Address,
	target Target,
	timeout time.Duration,
) error {
	data := approvalData(target.Spender)
	call := geth.CallMsg{
		From: owner,
		To:   &target.Token,
		Data: data,
	}
	gas, err := client.EstimateGas(ctx, call)
	if err != nil {
		return fmt.Errorf(
			"estimate %s approval for %s: %w",
			target.TokenSymbol,
			strings.Join(target.Purposes, ","),
			err,
		)
	}
	gas = gas + gas/5
	tip, err := client.SuggestGasTipCap(ctx)
	if err != nil {
		return fmt.Errorf("suggest approval priority fee: %w", err)
	}
	header, err := client.HeaderByNumber(ctx, nil)
	if err != nil {
		return fmt.Errorf("read approval base fee: %w", err)
	}
	if header.BaseFee == nil {
		return fmt.Errorf("approval chain does not expose EIP-1559 base fee")
	}
	feeCap := new(big.Int).Add(
		new(big.Int).Mul(header.BaseFee, big.NewInt(2)),
		tip,
	)
	call.Gas = gas
	call.GasTipCap = tip
	call.GasFeeCap = feeCap
	result, err := client.CallContract(ctx, call, nil)
	if err != nil {
		return fmt.Errorf("simulate %s approval: %w", target.TokenSymbol, err)
	}
	if len(result) != 0 && new(big.Int).SetBytes(result).Sign() == 0 {
		return fmt.Errorf("%s approval simulation returned false", target.TokenSymbol)
	}
	nonce, err := client.PendingNonceAt(ctx, owner)
	if err != nil {
		return fmt.Errorf("read approval nonce: %w", err)
	}
	transaction := types.NewTx(&types.DynamicFeeTx{
		ChainID:   chainID,
		Nonce:     nonce,
		GasTipCap: tip,
		GasFeeCap: feeCap,
		Gas:       gas,
		To:        &target.Token,
		Value:     new(big.Int),
		Data:      data,
	})
	signed, err := types.SignTx(
		transaction,
		types.LatestSignerForChainID(chainID),
		privateKey,
	)
	if err != nil {
		return fmt.Errorf("sign %s approval: %w", target.TokenSymbol, err)
	}
	fmt.Fprintf(
		output,
		"approval=prepared token=%s spender=%s tx=%s nonce=%d gas=%d\n",
		target.TokenSymbol,
		target.Spender.Hex(),
		signed.Hash().Hex(),
		nonce,
		gas,
	)
	if err := client.SendTransaction(ctx, signed); err != nil {
		return fmt.Errorf(
			"broadcast %s approval returned an uncertain result; check tx %s before rerunning: %w",
			target.TokenSymbol,
			signed.Hash().Hex(),
			err,
		)
	}
	fmt.Fprintf(
		output,
		"approval=broadcast token=%s tx=%s\n",
		target.TokenSymbol,
		signed.Hash().Hex(),
	)
	receipt, err := waitReceipt(ctx, client, signed.Hash(), timeout)
	if err != nil {
		return fmt.Errorf(
			"%s approval outcome is unknown for tx %s: %w",
			target.TokenSymbol,
			signed.Hash().Hex(),
			err,
		)
	}
	if receipt.Status != types.ReceiptStatusSuccessful {
		return fmt.Errorf(
			"%s approval reverted in tx %s",
			target.TokenSymbol,
			signed.Hash().Hex(),
		)
	}
	allowance, err := waitAllowanceAtReceipt(
		ctx,
		client,
		target.Token,
		owner,
		target.Spender,
		receipt.BlockNumber,
		15*time.Second,
	)
	if err != nil {
		return fmt.Errorf("verify %s approval: %w", target.TokenSymbol, err)
	}
	if allowance.Cmp(nearInfiniteAllowance) < 0 {
		return fmt.Errorf(
			"%s approval confirmed but allowance remains below the near-infinite threshold",
			target.TokenSymbol,
		)
	}
	fmt.Fprintf(
		output,
		"approval=confirmed token=%s spender=%s tx=%s block=%d gas_used=%d allowance=max_uint256\n",
		target.TokenSymbol,
		target.Spender.Hex(),
		signed.Hash().Hex(),
		receipt.BlockNumber.Uint64(),
		receipt.GasUsed,
	)
	return nil
}

func readAllowance(
	ctx context.Context,
	client interface {
		CallContract(context.Context, geth.CallMsg, *big.Int) ([]byte, error)
	},
	token common.Address,
	owner common.Address,
	spender common.Address,
) (*big.Int, error) {
	return readAllowanceAt(ctx, client, token, owner, spender, nil)
}

func readAllowanceAt(
	ctx context.Context,
	client interface {
		CallContract(context.Context, geth.CallMsg, *big.Int) ([]byte, error)
	},
	token common.Address,
	owner common.Address,
	spender common.Address,
	blockNumber *big.Int,
) (*big.Int, error) {
	data, _ := hex.DecodeString("dd62ed3e")
	data = append(data, common.LeftPadBytes(owner.Bytes(), 32)...)
	data = append(data, common.LeftPadBytes(spender.Bytes(), 32)...)
	raw, err := client.CallContract(
		ctx,
		geth.CallMsg{To: &token, Data: data},
		blockNumber,
	)
	if err != nil {
		return nil, err
	}
	if len(raw) != 32 {
		return nil, fmt.Errorf("ERC-20 allowance returned %d bytes", len(raw))
	}
	return new(big.Int).SetBytes(raw), nil
}

func waitAllowanceAtReceipt(
	ctx context.Context,
	client interface {
		CallContract(context.Context, geth.CallMsg, *big.Int) ([]byte, error)
	},
	token common.Address,
	owner common.Address,
	spender common.Address,
	blockNumber *big.Int,
	timeout time.Duration,
) (*big.Int, error) {
	if blockNumber == nil || blockNumber.Sign() < 0 || timeout <= 0 {
		return nil, fmt.Errorf("allowance receipt verification is invalid")
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var (
		lastAllowance *big.Int
		lastErr       error
	)
	for {
		allowance, err := readAllowanceAt(
			waitCtx,
			client,
			token,
			owner,
			spender,
			blockNumber,
		)
		if err == nil {
			lastAllowance = allowance
			if allowance.Cmp(nearInfiniteAllowance) >= 0 {
				return allowance, nil
			}
		} else {
			lastErr = err
		}
		select {
		case <-waitCtx.Done():
			if lastAllowance != nil {
				return lastAllowance, nil
			}
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, waitCtx.Err()
		case <-ticker.C:
		}
	}
}

func approvalData(spender common.Address) []byte {
	data, _ := hex.DecodeString(approveSelector)
	data = append(data, common.LeftPadBytes(spender.Bytes(), 32)...)
	data = append(data, common.LeftPadBytes(maxUint256.Bytes(), 32)...)
	return data
}

func waitReceipt(
	ctx context.Context,
	client interface {
		TransactionReceipt(context.Context, common.Hash) (*types.Receipt, error)
	},
	hash common.Hash,
	timeout time.Duration,
) (*types.Receipt, error) {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		receipt, err := client.TransactionReceipt(waitCtx, hash)
		if err == nil {
			return receipt, nil
		}
		if !errors.Is(err, geth.NotFound) {
			return nil, err
		}
		select {
		case <-waitCtx.Done():
			return nil, waitCtx.Err()
		case <-ticker.C:
		}
	}
}

func amountUnits(amount *big.Rat, decimals uint8) (*big.Int, error) {
	if amount == nil || amount.Sign() <= 0 {
		return nil, fmt.Errorf("amount must be positive")
	}
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	scaled := new(big.Rat).Mul(amount, new(big.Rat).SetInt(scale))
	if !scaled.IsInt() {
		return nil, fmt.Errorf("amount has more precision than the token supports")
	}
	return new(big.Int).Set(scaled.Num()), nil
}

func parseSolanaPrivateKey(value string) (solanago.PrivateKey, error) {
	value = strings.TrimSpace(value)
	if raw, err := hex.DecodeString(value); err == nil &&
		len(raw) == solanago.PrivateKeyLength {
		return solanago.PrivateKey(raw), nil
	}
	key, err := solanago.PrivateKeyFromBase58(value)
	if err != nil || len(key) != solanago.PrivateKeyLength {
		return nil, fmt.Errorf("invalid Solana private key")
	}
	return key, nil
}

func requiredEnv(lookup configuration.LookupEnv, name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("configuration contains an empty environment name")
	}
	value, ok := lookup(name)
	if !ok || strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("required environment %q is unset", name)
	}
	return strings.TrimSpace(value), nil
}

func formatAllowance(value *big.Int) string {
	if value.Cmp(maxUint256) == 0 {
		return "max_uint256"
	}
	if value.Cmp(nearInfiniteAllowance) >= 0 {
		return "near_infinite"
	}
	return value.String()
}
