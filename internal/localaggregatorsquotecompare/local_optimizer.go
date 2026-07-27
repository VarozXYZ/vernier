package localaggregatorsquotecompare

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/VarozXYZ/vernier/cmd/okxexperiment"
	"github.com/VarozXYZ/vernier/core/sizing"
	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

type localSnapshotSet map[*watcher]market.MarketSnapshot

func captureLocalSnapshots(watchers []*watcher) (localSnapshotSet, error) {
	snapshots := make(localSnapshotSet, len(watchers))
	for _, current := range watchers {
		snapshot, ok := current.mirror.Current()
		if !ok || snapshot.Metadata().Health != market.HealthHealthy {
			return nil, fmt.Errorf("watcher %d has no healthy current snapshot", current.index)
		}
		snapshots[current] = snapshot
	}
	return snapshots, nil
}

func optimizeLocalReferenceGrid(
	ctx context.Context,
	watchers []*watcher,
	snapshots localSnapshotSet,
	totalInput *big.Int,
	quotedAt time.Time,
	divisions int,
) (sizing.TwoStageSplitResult, error) {
	direct, first, second, err := localSplitCurves(watchers, snapshots, quotedAt)
	if err != nil {
		return sizing.TwoStageSplitResult{}, err
	}
	request := sizing.GridTwoStageSplitRequest{
		TotalInput: totalInput, Divisions: divisions,
		Direct: direct, FirstStage: first, SecondStage: second,
	}
	return sizing.OptimizeTwoStageSplitGrid(ctx, request)
}

func optimizeLocalGridSplit(
	ctx context.Context,
	watchers []*watcher,
	snapshots localSnapshotSet,
	totalInput *big.Int,
	quotedAt time.Time,
	divisions int,
) (sizing.TwoStageSplitResult, error) {
	direct, first, second, err := localSplitCurves(watchers, snapshots, quotedAt)
	if err != nil {
		return sizing.TwoStageSplitResult{}, err
	}
	return sizing.OptimizeTwoStageSplitGrid(ctx, sizing.GridTwoStageSplitRequest{
		TotalInput: totalInput, Direct: direct, FirstStage: first, SecondStage: second,
		Divisions: divisions,
	})
}

func localSplitCurves(
	watchers []*watcher,
	snapshots localSnapshotSet,
	quotedAt time.Time,
) ([]sizing.SplitCurve, []sizing.SplitCurve, []sizing.SplitCurve, error) {
	var direct, first, second []sizing.SplitCurve
	for _, current := range watchers {
		snapshot, ok := snapshots[current]
		if !ok {
			return nil, nil, nil, fmt.Errorf("watcher %d snapshot is missing", current.index)
		}
		switch {
		case connects(current, inputTokenID, outputTokenID):
			direct = append(direct, splitCurve(current, snapshot, inputTokenID, outputTokenID, quotedAt))
		case connects(current, inputTokenID, intermediateTokenID):
			first = append(first, splitCurve(current, snapshot, inputTokenID, intermediateTokenID, quotedAt))
		case connects(current, intermediateTokenID, outputTokenID):
			second = append(second, splitCurve(current, snapshot, intermediateTokenID, outputTokenID, quotedAt))
		default:
			return nil, nil, nil, fmt.Errorf("watcher %d is outside the supported two-stage graph", current.index)
		}
	}
	if len(first) == 0 || len(second) == 0 {
		return nil, nil, nil, fmt.Errorf("local graph requires both intermediate stages")
	}
	return direct, first, second, nil
}

func splitCurve(
	current *watcher,
	snapshot market.MarketSnapshot,
	tokenIn, tokenOut market.TokenID,
	quotedAt time.Time,
) sizing.SplitCurve {
	return sizing.SplitCurve{
		ID: string(current.market.ID),
		Quote: func(ctx context.Context, amountIn *big.Int) (*big.Int, error) {
			amount, err := market.NewTokenAmount(tokenIn, amountIn)
			if err != nil {
				return nil, err
			}
			quoted, err := current.source.Quote(ctx, quoteport.Input{
				Snapshot: snapshot, TokenIn: tokenIn, TokenOut: tokenOut,
				AmountIn: amount, Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: quotedAt,
			})
			if err != nil {
				return nil, err
			}
			return quoted.AmountOut.Units(), nil
		},
	}
}

func connects(current *watcher, first, second market.TokenID) bool {
	return current.token0 == first && current.token1 == second ||
		current.token0 == second && current.token1 == first
}

func formatLocalSplitPlan(result sizing.TwoStageSplitResult, topology topologyConfig) (string, error) {
	direct, err := formatAllocationGroup(
		result.Direct,
		topology.Input.Decimals, topology.Input.Symbol,
		topology.Output.Decimals, topology.Output.Symbol,
	)
	if err != nil {
		return "", err
	}
	first, err := formatAllocationGroup(
		result.FirstStage,
		topology.Input.Decimals, topology.Input.Symbol,
		topology.Intermediate.Decimals, topology.Intermediate.Symbol,
	)
	if err != nil {
		return "", err
	}
	second, err := formatAllocationGroup(
		result.SecondStage,
		topology.Intermediate.Decimals, topology.Intermediate.Symbol,
		topology.Output.Decimals, topology.Output.Symbol,
	)
	if err != nil {
		return "", err
	}
	return "direct[" + direct + "];first[" + first + "];second[" + second + "]", nil
}

func formatAllocationGroup(
	allocations []sizing.SplitAllocation,
	inputDecimals uint8,
	inputSymbol string,
	outputDecimals uint8,
	outputSymbol string,
) (string, error) {
	parts := make([]string, 0, len(allocations))
	for _, allocation := range allocations {
		if allocation.AmountIn.Sign() == 0 {
			continue
		}
		input, err := okxexperiment.FormatBaseUnits(allocation.AmountIn.String(), fmt.Sprint(inputDecimals))
		if err != nil {
			return "", err
		}
		output, err := okxexperiment.FormatBaseUnits(allocation.AmountOut.String(), fmt.Sprint(outputDecimals))
		if err != nil {
			return "", err
		}
		parts = append(parts, fmt.Sprintf(
			"%s:%s_%s->%s_%s",
			allocation.CurveID,
			input, strings.ReplaceAll(strings.TrimSpace(inputSymbol), " ", "_"),
			output, strings.ReplaceAll(strings.TrimSpace(outputSymbol), " ", "_"),
		))
	}
	return strings.Join(parts, ","), nil
}

func usedAllocations(groups ...[]sizing.SplitAllocation) int {
	count := 0
	for _, group := range groups {
		for _, allocation := range group {
			if allocation.AmountIn.Sign() > 0 {
				count++
			}
		}
	}
	return count
}
