package livecanary

import (
	"fmt"
	"os"

	"github.com/ethereum/go-ethereum/common"
	"gopkg.in/yaml.v3"
)

type wttProfile struct {
	SchemaVersion  int                        `yaml:"schema_version"`
	PollIntervalMS int                        `yaml:"poll_interval_ms"`
	Chains         map[string]wttChainProfile `yaml:"chains"`
}

type wttChainProfile struct {
	WormholeChainID uint16 `yaml:"wormhole_chain_id"`
	CoreBridge      string `yaml:"core_bridge"`
	TokenBridge     string `yaml:"token_bridge"`
}

type resolvedWTTChain struct {
	WormholeChainID uint16
	CoreBridge      common.Address
	TokenBridge     common.Address
}

// publicWormholeMainnetVAAEndpoints follows the public mainnet Guardian RPC
// list maintained by Wormhole's Go SDK. WormholeScan implements the same
// signed-VAA lookup API and is the final availability fallback. These are
// public, non-secret protocol infrastructure endpoints, not setup-owned RPCs.
var publicWormholeMainnetVAAEndpoints = []string{
	"https://wormhole-v2-mainnet-api.mcf.rocks",
	"https://wormhole-v2-mainnet-api.chainlayer.network",
	"https://wormhole-v2-mainnet-api.staking.fund",
	"https://api.wormholescan.io/api/v1/vaas",
}

func loadWTTProfile(path string) (map[string]resolvedWTTChain, []string, int, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, 0, err
	}
	var profile wttProfile
	if err := yaml.Unmarshal(raw, &profile); err != nil {
		return nil, nil, 0, err
	}
	if profile.SchemaVersion != 1 || len(profile.Chains) != 2 || profile.PollIntervalMS <= 0 {
		return nil, nil, 0, fmt.Errorf("WTT profile is incomplete")
	}
	result := make(map[string]resolvedWTTChain, len(profile.Chains))
	for id, chain := range profile.Chains {
		if id == "" || chain.WormholeChainID == 0 || !common.IsHexAddress(chain.CoreBridge) ||
			!common.IsHexAddress(chain.TokenBridge) || common.HexToAddress(chain.CoreBridge) == (common.Address{}) ||
			common.HexToAddress(chain.TokenBridge) == (common.Address{}) {
			return nil, nil, 0, fmt.Errorf("WTT chain profile %q is invalid", id)
		}
		result[id] = resolvedWTTChain{WormholeChainID: chain.WormholeChainID,
			CoreBridge: common.HexToAddress(chain.CoreBridge), TokenBridge: common.HexToAddress(chain.TokenBridge)}
	}
	return result, append([]string(nil), publicWormholeMainnetVAAEndpoints...), profile.PollIntervalMS, nil
}
