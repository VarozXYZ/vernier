package constantproduct

import (
	"context"
	"crypto/sha256"
	"fmt"

	"github.com/VarozXYZ/vernier/domain/market"
)

type Reducer struct{}

func (Reducer) Reduce(ctx context.Context, _ market.SnapshotData, data market.EventData) (market.SnapshotData, [sha256.Size]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, [sha256.Size]byte{}, err
	}
	update, ok := data.(ReserveUpdate)
	if !ok {
		return nil, [sha256.Size]byte{}, fmt.Errorf("unsupported constant-product event payload %T", data)
	}
	if err := update.validate(); err != nil {
		return nil, [sha256.Size]byte{}, err
	}
	state := Snapshot{
		schemaVersion: snapshotSchemaVersion,
		baseReserve:   update.BaseReserve(), quoteReserve: update.QuoteReserve(), feeBPS: update.FeeBPS(),
	}
	payload := fmt.Sprintf("%d|%s|%s|%d", state.schemaVersion, state.baseReserve, state.quoteReserve, state.feeBPS)
	return state, sha256.Sum256([]byte(payload)), nil
}
