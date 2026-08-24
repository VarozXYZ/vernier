package livecanary

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"

	corelive "github.com/VarozXYZ/vernier/core/live"
	"github.com/VarozXYZ/vernier/domain/market"
	"github.com/VarozXYZ/vernier/runtime/configuration"
)

// ComposeEVMHybridDryRun creates a read-only balance and allowance audit for
// a two-EVM prefunded-parallel setup. It intentionally cannot be upgraded to
// an armed runtime because no signer or transaction manager is composed.
func ComposeEVMHybridDryRun(config ComposeConfig) (*DryRunRuntime, error) {
	if config.LookupEnv == nil || config.Output == nil {
		return nil, fmt.Errorf("hybrid EVM dry-run configuration is incomplete")
	}
	local, remote, err := splitHybridMarkets(config.Research)
	if err != nil {
		return nil, err
	}
	markets := []configuration.ResolvedMarket{local, remote}
	endpoints, err := config.Research.ResolveEndpoints(config.LookupEnv)
	if err != nil {
		return nil, err
	}
	type chainAudit struct {
		market configuration.ResolvedMarket
		owner  common.Address
		client *ethclient.Client
		tokens map[market.TokenID]common.Address
	}
	audits := make(map[string]chainAudit, 2)
	closers := make([]func(), 0, 2)
	fail := func(cause error) (*DryRunRuntime, error) {
		for index := len(closers) - 1; index >= 0; index-- {
			closers[index]()
		}
		return nil, cause
	}
	for _, configured := range markets {
		chain := config.Research.Chains[configured.Chain]
		account, ok := config.Live.Accounts[configured.Chain]
		if !ok {
			return fail(fmt.Errorf("read-only Live account for %s is unavailable", configured.Chain))
		}
		owner, err := readOnlyAccountAddress(config.LookupEnv, account)
		if err != nil {
			return fail(fmt.Errorf("read-only Live account for %s has an invalid public address", configured.Chain))
		}
		client, err := ethclient.Dial(endpoints[configured.Chain])
		if err != nil {
			return fail(fmt.Errorf("dial %s dry-run endpoint: %w", configured.Chain, err))
		}
		closers = append(closers, client.Close)
		actualChain, err := client.ChainID(context.Background())
		if err != nil || actualChain.Cmp(chain.ChainID) != 0 {
			return fail(fmt.Errorf("%s dry-run endpoint returned the wrong chain", configured.Chain))
		}
		tokens := map[market.TokenID]common.Address{
			configured.Base.Token.ID: configured.Base.Address, configured.Quote.Token.ID: configured.Quote.Address,
		}
		for _, conversion := range config.Live.QuoteConversions {
			if conversion.Chain == configured.Chain {
				tokens[conversion.TokenA.Token.ID] = conversion.TokenA.Address
				tokens[conversion.TokenB.Token.ID] = conversion.TokenB.Address
			}
		}
		audits[configured.Chain] = chainAudit{market: configured, owner: owner, client: client, tokens: tokens}
	}
	runtime := &DryRunRuntime{close: func() {
		for index := len(closers) - 1; index >= 0; index-- {
			closers[index]()
		}
	}}
	runtime.run = func(ctx context.Context) error {
		for _, configuredBalance := range config.Live.Inventory {
			audit, ok := audits[configuredBalance.Chain]
			if !ok {
				return fmt.Errorf("inventory chain %s is unavailable", configuredBalance.Chain)
			}
			token, ok := audit.tokens[configuredBalance.Token.ID]
			if !ok {
				return fmt.Errorf("inventory token %s is unavailable", configuredBalance.Token.ID)
			}
			units, err := readERC20Word(ctx, audit.client, token, "balanceOf(address)", audit.owner)
			if err != nil {
				return err
			}
			fmt.Fprintf(config.Output, "live_dry_run kind=balance chain=%s token=%s units=%s\n", configuredBalance.Chain, configuredBalance.Token.ID, units)
		}
		for _, requirement := range config.Live.ApprovalSpenders {
			audit, ok := audits[string(requirement.Chain)]
			if !ok {
				return fmt.Errorf("allowance chain %s is unavailable", requirement.Chain)
			}
			token, ok := audit.tokens[requirement.Token]
			if !ok {
				return fmt.Errorf("allowance token %s is unavailable", requirement.Token)
			}
			units, err := readERC20Word(ctx, audit.client, token, "allowance(address,address)", audit.owner, requirement.Spender)
			if err != nil {
				return err
			}
			status := "insufficient"
			if units.Cmp(corelive.NearInfiniteAllowance) >= 0 {
				status = "sufficient"
			}
			fmt.Fprintf(config.Output, "live_dry_run kind=allowance chain=%s token=%s purpose=%s status=%s\n", requirement.Chain, requirement.Token, requirement.Purpose, status)
		}
		return nil
	}
	return runtime, nil
}

func readOnlyAccountAddress(lookup configuration.LookupEnv, account configuration.ResolvedLiveAccount) (common.Address, error) {
	if name := strings.TrimSpace(account.PublicAddressEnv); name != "" {
		value, err := requiredEnv(lookup, name)
		if err == nil && common.IsHexAddress(value) && common.HexToAddress(value) != (common.Address{}) {
			return common.HexToAddress(value), nil
		}
		return common.Address{}, fmt.Errorf("public address is invalid")
	}
	keyText, err := requiredEnv(lookup, account.SignerEnv)
	if err != nil {
		return common.Address{}, err
	}
	privateKey, err := crypto.HexToECDSA(strings.TrimPrefix(strings.TrimSpace(keyText), "0x"))
	if err != nil {
		return common.Address{}, err
	}
	return crypto.PubkeyToAddress(privateKey.PublicKey), nil
}

func readERC20Word(ctx context.Context, client *ethclient.Client, token common.Address, signature string, addresses ...common.Address) (*big.Int, error) {
	payload := append([]byte(nil), crypto.Keccak256([]byte(signature))[:4]...)
	for _, address := range addresses {
		payload = append(payload, common.LeftPadBytes(address.Bytes(), 32)...)
	}
	raw, err := client.CallContract(ctx, geth.CallMsg{To: &token, Data: payload}, nil)
	if err != nil {
		return nil, err
	}
	if len(raw) != 32 {
		return nil, fmt.Errorf("ERC-20 %s response has %d bytes", signature, len(raw))
	}
	return new(big.Int).SetBytes(raw), nil
}
