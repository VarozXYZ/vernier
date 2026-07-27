package solanalogs

import (
	"context"
	"fmt"
	"strings"

	"github.com/VarozXYZ/vernier/adapters/chain/solana"
	"github.com/VarozXYZ/vernier/domain/market"
)

// TriggerDecoder turns activity from one already-filtered Solana pool log
// subscription into a causal Research trigger. It performs no account reads
// and deliberately carries no protocol-specific pool state.
type TriggerDecoder struct {
	Kind string
}

type TriggerEvent struct{}

func (TriggerEvent) EventKind() string { return "solana_pool_activity/v1" }

func (d TriggerDecoder) Bootstrap(context.Context, Network, uint64) (market.EventData, error) {
	if !supportedTriggerKind(d.Kind) {
		return nil, fmt.Errorf("unsupported Solana trigger kind %q", d.Kind)
	}
	return TriggerEvent{}, nil
}

func (d TriggerDecoder) Decode(_ context.Context, _ Network, notification solana.LogNotification) ([]market.EventData, error) {
	if !supportedTriggerKind(d.Kind) {
		return nil, fmt.Errorf("unsupported Solana trigger kind %q", d.Kind)
	}
	if d.Kind == "" {
		return []market.EventData{TriggerEvent{}}, nil
	}
	for _, line := range notification.Logs {
		normalized := strings.ToLower(strings.ReplaceAll(line, " ", "_"))
		compact := strings.ReplaceAll(normalized, "_", "")
		if strings.Contains(normalized, "swap") ||
			strings.Contains(normalized, "increase_liquidity") ||
			strings.Contains(normalized, "decrease_liquidity") ||
			strings.Contains(compact, "increaseliquidity") ||
			strings.Contains(compact, "decreaseliquidity") {
			return []market.EventData{TriggerEvent{}}, nil
		}
	}
	return nil, nil
}

func supportedTriggerKind(kind string) bool {
	return kind == "" || kind == "raydium_clmm" || kind == "orca_whirlpool"
}

var _ Decoder = TriggerDecoder{}
