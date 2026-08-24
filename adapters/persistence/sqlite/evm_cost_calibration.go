package sqlite

import (
	"context"
	"fmt"
	"time"

	"github.com/VarozXYZ/vernier/domain/market"
)

// EVMFlowTransaction is normalized receipt evidence for one completed stage.
// The cost oracle resolves the receipt through the configured chain manager;
// calldata and signed payloads never cross this boundary.
type EVMFlowTransaction struct {
	Chain     market.ChainID
	Identity  string
	UpdatedAt time.Time
}

// LatestEVMFlowTransactions returns confirmed transaction identities for one
// exact economic direction and stage phase. A matching durable settlement is
// required so reverted or merely broadcast transactions cannot calibrate gas.
func (s *SequentialLiveStore) LatestEVMFlowTransactions(
	ctx context.Context,
	buyMarket, sellMarket market.MarketID,
	ordinal int,
	phase string,
	limit int,
) ([]EVMFlowTransaction, error) {
	if buyMarket == "" || sellMarket == "" || buyMarket == sellMarket || ordinal < 1 || ordinal > 4 || phase == "" {
		return nil, fmt.Errorf("EVM flow calibration query is invalid")
	}
	if limit <= 0 {
		limit = 10
	}
	rows, err := s.db.QueryContext(ctx, `SELECT t.chain_name, t.identity, t.updated_at
		FROM sequential_live_transactions t
		JOIN sequential_live_plan_snapshots p ON p.operation_id=t.operation_id
		JOIN sequential_live_settlements s ON s.operation_id=t.operation_id AND s.ordinal=t.ordinal
		WHERE p.buy_market=? AND p.sell_market=? AND t.ordinal=? AND t.phase=?
			AND t.status='confirmed'
		ORDER BY t.updated_at DESC LIMIT ?`, buyMarket, sellMarket, ordinal, phase, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]EVMFlowTransaction, 0, limit)
	for rows.Next() {
		var item EVMFlowTransaction
		var updated string
		if err := rows.Scan(&item.Chain, &item.Identity, &updated); err != nil {
			return nil, err
		}
		item.UpdatedAt, err = time.Parse(time.RFC3339Nano, updated)
		if err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}
