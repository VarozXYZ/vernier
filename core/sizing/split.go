package sizing

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"sync"

	"github.com/VarozXYZ/vernier/domain/market"
)

// SplitCurve is one immutable exact-input output curve. Quote must return the
// total output produced by swapping amountIn against one fixed pool snapshot.
type SplitCurve struct {
	ID    string
	Quote func(context.Context, *big.Int) (*big.Int, error)
}

type SplitAllocation struct {
	CurveID   string
	AmountIn  *big.Int
	AmountOut *big.Int
}

type SplitMetrics struct {
	CurveEvaluations     int
	CurveCacheHits       int
	ObjectiveEvaluations int
	ObjectiveCacheHits   int
	SecondStageSolves    int
	CoordinateSweeps     int
}

// TwoStageSplitRequest models direct input->output pools plus a fungible
// input->intermediate->output branch. Liquidity is owned by curves, not by
// nominal paths, so shared pools are evaluated exactly once per allocation.
type TwoStageSplitRequest struct {
	TotalInput      *big.Int
	Direct          []SplitCurve
	FirstStage      []SplitCurve
	SecondStage     []SplitCurve
	MaxSweeps       int
	MaxStarts       int
	Neighborhood    int
	ExhaustiveLimit int
}

type TwoStageSplitResult struct {
	Direct           []SplitAllocation
	FirstStage       []SplitAllocation
	SecondStage      []SplitAllocation
	TotalInput       *big.Int
	IntermediateOut  *big.Int
	TotalOutput      *big.Int
	InputResolution  *big.Int
	GridDivisions    int
	GridVerified     bool
	PairwiseOptimal  bool
	GloballyVerified bool
	Metrics          SplitMetrics
}

type memoCurve struct {
	id    string
	quote func(context.Context, *big.Int) (*big.Int, error)
	mu    sync.Mutex
	cache map[string]*big.Int
	calls int
	hits  int
}

type coordinateSettings struct {
	maxSweeps       int
	maxStarts       int
	neighborhood    int
	exhaustiveLimit int
}

type coordinateResult struct {
	allocation      []*big.Int
	value           *big.Int
	sweeps          int
	evaluations     int
	cacheHits       int
	pairwiseOptimal bool
	exhaustive      bool
}

type stagePlan struct {
	allocations []*big.Int
	output      *big.Int
	sweeps      int
	exhaustive  bool
}

type flowEvaluation struct {
	output       *big.Int
	directOut    *big.Int
	intermediate *big.Int
	second       stagePlan
}

// OptimizeTwoStageSplit finds a deterministic integer allocation at one
// minimum input unit resolution. It uses exhaustive search when the configured
// state space is small enough, otherwise deterministic multistart pairwise
// integer line searches. GloballyVerified is only true when every relevant
// allocation was exhaustively evaluated.
func OptimizeTwoStageSplit(ctx context.Context, request TwoStageSplitRequest) (TwoStageSplitResult, error) {
	if err := ctx.Err(); err != nil {
		return TwoStageSplitResult{}, err
	}
	if request.TotalInput == nil || request.TotalInput.Sign() <= 0 {
		return TwoStageSplitResult{}, fmt.Errorf("positive total split input is required")
	}
	if len(request.Direct) == 0 && len(request.FirstStage) == 0 {
		return TwoStageSplitResult{}, fmt.Errorf("at least one direct or first-stage curve is required")
	}
	if (len(request.FirstStage) == 0) != (len(request.SecondStage) == 0) {
		return TwoStageSplitResult{}, fmt.Errorf("both stages of the intermediate branch are required")
	}
	settings := coordinateSettings{
		maxSweeps:       request.MaxSweeps,
		maxStarts:       request.MaxStarts,
		neighborhood:    request.Neighborhood,
		exhaustiveLimit: request.ExhaustiveLimit,
	}
	if settings.maxSweeps == 0 {
		settings.maxSweeps = 64
	}
	if settings.neighborhood == 0 {
		settings.neighborhood = 32
	}
	if settings.maxStarts == 0 {
		settings.maxStarts = 3
	}
	if settings.exhaustiveLimit == 0 {
		settings.exhaustiveLimit = 10_000
	}
	if settings.maxSweeps < 1 || settings.maxSweeps > 1024 ||
		settings.maxStarts < 1 || settings.maxStarts > 128 ||
		settings.neighborhood < 1 || settings.neighborhood > 4096 ||
		settings.exhaustiveLimit < 1 || settings.exhaustiveLimit > 10_000_000 {
		return TwoStageSplitResult{}, fmt.Errorf("invalid split solver limits")
	}

	direct, err := newMemoCurves(request.Direct)
	if err != nil {
		return TwoStageSplitResult{}, fmt.Errorf("direct curves: %w", err)
	}
	first, err := newMemoCurves(request.FirstStage)
	if err != nil {
		return TwoStageSplitResult{}, fmt.Errorf("first-stage curves: %w", err)
	}
	second, err := newMemoCurves(request.SecondStage)
	if err != nil {
		return TwoStageSplitResult{}, fmt.Errorf("second-stage curves: %w", err)
	}
	if err := uniqueCurveIDs(direct, first, second); err != nil {
		return TwoStageSplitResult{}, err
	}
	inputCurves := append(append([]*memoCurve(nil), direct...), first...)
	secondCache := make(map[string]stagePlan)
	secondSolves := 0
	secondSweeps := 0
	allSecondGloballyVerified := true

	optimizeSecond := func(ctx context.Context, total *big.Int) (stagePlan, error) {
		if total.Sign() == 0 {
			return stagePlan{
				allocations: zeroAllocation(len(second)), output: new(big.Int), exhaustive: true,
			}, nil
		}
		key := total.String()
		if cached, ok := secondCache[key]; ok {
			return cloneStagePlan(cached), nil
		}
		plan, err := optimizeIndependent(ctx, total, second, settings)
		if err != nil {
			return stagePlan{}, fmt.Errorf("optimize second-stage split: %w", err)
		}
		secondSolves++
		secondSweeps += plan.sweeps
		if !plan.exhaustive {
			allSecondGloballyVerified = false
		}
		secondCache[key] = cloneStagePlan(plan)
		return plan, nil
	}

	evaluateFlow := func(ctx context.Context, allocation []*big.Int) (*big.Int, *flowEvaluation, error) {
		directOut := new(big.Int)
		for index := range direct {
			output, err := direct[index].at(ctx, allocation[index])
			if err != nil {
				return nil, nil, err
			}
			directOut.Add(directOut, output)
		}
		intermediate := new(big.Int)
		for index := range first {
			output, err := first[index].at(ctx, allocation[len(direct)+index])
			if err != nil {
				return nil, nil, err
			}
			intermediate.Add(intermediate, output)
		}
		secondPlan, err := optimizeSecond(ctx, intermediate)
		if err != nil {
			return nil, nil, err
		}
		total := new(big.Int).Add(new(big.Int).Set(directOut), secondPlan.output)
		evidence := &flowEvaluation{
			output: total, directOut: new(big.Int).Set(directOut),
			intermediate: new(big.Int).Set(intermediate), second: cloneStagePlan(secondPlan),
		}
		return new(big.Int).Set(total), evidence, nil
	}

	objective := func(ctx context.Context, allocation []*big.Int) (*big.Int, error) {
		value, _, err := evaluateFlow(ctx, allocation)
		return value, err
	}
	outer, err := coordinateOptimize(ctx, request.TotalInput, len(inputCurves), objective, settings)
	if err != nil {
		return TwoStageSplitResult{}, fmt.Errorf("optimize input split: %w", err)
	}
	_, finalEvidence, err := evaluateFlow(ctx, outer.allocation)
	if err != nil {
		return TwoStageSplitResult{}, err
	}
	// Integer pool math can make a dust-sized intermediate branch return zero.
	// Such an allocation may tie on total output, but it cannot be represented
	// as an executable route because its second stage has no positive input or
	// output. Prefer the best direct-only allocation in that case. This also
	// keeps every input unit accounted for instead of silently dropping dust.
	if len(direct) > 0 && (finalEvidence.intermediate.Sign() > 0 && finalEvidence.second.output.Sign() == 0 ||
		allocatedPositive(outer.allocation[len(direct):]) && finalEvidence.intermediate.Sign() == 0) {
		directPlan, directErr := optimizeIndependent(ctx, request.TotalInput, direct, settings)
		if directErr != nil {
			return TwoStageSplitResult{}, fmt.Errorf("optimize direct-only fallback: %w", directErr)
		}
		outer.allocation = append(cloneAllocation(directPlan.allocations), zeroAllocation(len(first))...)
		_, finalEvidence, err = evaluateFlow(ctx, outer.allocation)
		if err != nil {
			return TwoStageSplitResult{}, err
		}
	}

	directAllocations, err := materializeAllocations(ctx, direct, outer.allocation[:len(direct)])
	if err != nil {
		return TwoStageSplitResult{}, err
	}
	firstAllocations, err := materializeAllocations(ctx, first, outer.allocation[len(direct):])
	if err != nil {
		return TwoStageSplitResult{}, err
	}
	secondAllocations, err := materializeAllocations(ctx, second, finalEvidence.second.allocations)
	if err != nil {
		return TwoStageSplitResult{}, err
	}
	metrics := SplitMetrics{
		ObjectiveEvaluations: outer.evaluations,
		ObjectiveCacheHits:   outer.cacheHits,
		SecondStageSolves:    secondSolves,
		CoordinateSweeps:     outer.sweeps + secondSweeps,
	}
	for _, group := range [][]*memoCurve{direct, first, second} {
		for _, curve := range group {
			metrics.CurveEvaluations += curve.calls
			metrics.CurveCacheHits += curve.hits
		}
	}
	return TwoStageSplitResult{
		Direct: directAllocations, FirstStage: firstAllocations, SecondStage: secondAllocations,
		TotalInput:      new(big.Int).Set(request.TotalInput),
		IntermediateOut: new(big.Int).Set(finalEvidence.intermediate),
		TotalOutput:     new(big.Int).Set(finalEvidence.output),
		InputResolution: big.NewInt(1), PairwiseOptimal: outer.pairwiseOptimal,
		GloballyVerified: outer.exhaustive && allSecondGloballyVerified,
		Metrics:          metrics,
	}, nil
}

func allocatedPositive(allocation []*big.Int) bool {
	for _, amount := range allocation {
		if amount != nil && amount.Sign() > 0 {
			return true
		}
	}
	return false
}

func optimizeIndependent(
	ctx context.Context,
	total *big.Int,
	curves []*memoCurve,
	settings coordinateSettings,
) (stagePlan, error) {
	if len(curves) == 0 {
		if total.Sign() == 0 {
			return stagePlan{output: new(big.Int)}, nil
		}
		return stagePlan{}, fmt.Errorf("positive amount has no output curves")
	}
	objective := func(ctx context.Context, allocation []*big.Int) (*big.Int, error) {
		output := new(big.Int)
		for index, curve := range curves {
			value, err := curve.at(ctx, allocation[index])
			if err != nil {
				return nil, err
			}
			output.Add(output, value)
		}
		return output, nil
	}
	result, err := coordinateOptimize(ctx, total, len(curves), objective, settings)
	if err != nil {
		return stagePlan{}, err
	}
	return stagePlan{
		allocations: cloneAllocation(result.allocation),
		output:      new(big.Int).Set(result.value), sweeps: result.sweeps,
		exhaustive: result.exhaustive,
	}, nil
}

func coordinateOptimize(
	ctx context.Context,
	total *big.Int,
	count int,
	objective func(context.Context, []*big.Int) (*big.Int, error),
	settings coordinateSettings,
) (coordinateResult, error) {
	if count < 1 || total == nil || total.Sign() < 0 {
		return coordinateResult{}, fmt.Errorf("invalid coordinate optimization")
	}
	cache := make(map[string]*big.Int)
	evaluations := 0
	cacheHits := 0
	evaluate := func(allocation []*big.Int) (*big.Int, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key := allocationKey(allocation)
		if value, ok := cache[key]; ok {
			cacheHits++
			return new(big.Int).Set(value), nil
		}
		if !allocationConserves(allocation, total) {
			return nil, fmt.Errorf("candidate allocation does not conserve input")
		}
		value, err := objective(ctx, allocation)
		if err != nil {
			return nil, err
		}
		if value == nil || value.Sign() < 0 {
			return nil, fmt.Errorf("split objective returned invalid output")
		}
		evaluations++
		cache[key] = new(big.Int).Set(value)
		return new(big.Int).Set(value), nil
	}

	if count == 1 {
		allocation := zeroAllocation(1)
		allocation[0].Set(total)
		current, err := evaluate(allocation)
		if err != nil {
			return coordinateResult{}, err
		}
		return coordinateResult{
			allocation: allocation, value: current, sweeps: 1,
			evaluations: evaluations, cacheHits: cacheHits, pairwiseOptimal: true, exhaustive: true,
		}, nil
	}
	if compositionCountAtMost(total, count, settings.exhaustiveLimit) {
		allocation, value, err := exhaustiveCoordinates(ctx, total, count, evaluate)
		if err != nil {
			return coordinateResult{}, err
		}
		return coordinateResult{
			allocation: allocation, value: value, sweeps: 1,
			evaluations: evaluations, cacheHits: cacheHits, pairwiseOptimal: true, exhaustive: true,
		}, nil
	}
	if count == 2 {
		allocation := zeroAllocation(2)
		allocation[1].Set(total)
		best, value, err := maximizePair(ctx, allocation, 0, 1, total, evaluate, settings.neighborhood)
		if err != nil {
			return coordinateResult{}, err
		}
		allocation[0].Set(best)
		allocation[1].Sub(total, best)
		return coordinateResult{
			allocation: allocation, value: value, sweeps: 1,
			evaluations: evaluations, cacheHits: cacheHits, pairwiseOptimal: true,
		}, nil
	}

	type rankedStart struct {
		allocation []*big.Int
		value      *big.Int
	}
	starts := coordinateStarts(total, count)
	ranked := make([]rankedStart, len(starts))
	for index, start := range starts {
		value, err := evaluate(start)
		if err != nil {
			return coordinateResult{}, err
		}
		ranked[index] = rankedStart{allocation: start, value: value}
	}
	sort.Slice(ranked, func(left, right int) bool {
		comparison := ranked[left].value.Cmp(ranked[right].value)
		if comparison != 0 {
			return comparison > 0
		}
		return allocationKey(ranked[left].allocation) < allocationKey(ranked[right].allocation)
	})
	if len(ranked) > settings.maxStarts {
		ranked = ranked[:settings.maxStarts]
	}

	var bestAllocation []*big.Int
	var bestValue *big.Int
	totalSweeps := 0
	for _, start := range ranked {
		allocation := cloneAllocation(start.allocation)
		current, err := evaluate(allocation)
		if err != nil {
			return coordinateResult{}, err
		}
		converged := false
		for sweep := 0; sweep < settings.maxSweeps; sweep++ {
			improved := false
			for left := 0; left < count; left++ {
				for right := left + 1; right < count; right++ {
					pairTotal := new(big.Int).Add(allocation[left], allocation[right])
					best, pairValue, err := maximizePair(
						ctx, allocation, left, right, pairTotal, evaluate, settings.neighborhood,
					)
					if err != nil {
						return coordinateResult{}, err
					}
					if pairValue.Cmp(current) > 0 {
						allocation[left].Set(best)
						allocation[right].Sub(pairTotal, best)
						current = pairValue
						improved = true
					}
				}
			}
			totalSweeps++
			if !improved {
				converged = true
				break
			}
		}
		if !converged {
			return coordinateResult{}, fmt.Errorf("split solver did not converge after %d sweeps", settings.maxSweeps)
		}
		if bestValue == nil || current.Cmp(bestValue) > 0 ||
			current.Cmp(bestValue) == 0 && allocationKey(allocation) < allocationKey(bestAllocation) {
			bestAllocation = cloneAllocation(allocation)
			bestValue = new(big.Int).Set(current)
		}
	}
	return coordinateResult{
		allocation: bestAllocation, value: bestValue, sweeps: totalSweeps,
		evaluations: evaluations, cacheHits: cacheHits, pairwiseOptimal: true,
	}, nil
}

func compositionCountAtMost(total *big.Int, count, limit int) bool {
	if total == nil || total.Sign() < 0 || count < 1 {
		return false
	}
	if count == 1 {
		return limit >= 1
	}
	combinations := big.NewInt(1)
	limitValue := big.NewInt(int64(limit))
	for index := 1; index < count; index++ {
		divisor := big.NewInt(int64(index))
		factor := new(big.Int).Add(total, divisor)
		combinations.Mul(combinations, factor)
		combinations.Quo(combinations, divisor)
		if combinations.Cmp(limitValue) > 0 {
			return false
		}
	}
	return true
}

func exhaustiveCoordinates(
	ctx context.Context,
	total *big.Int,
	count int,
	evaluate func([]*big.Int) (*big.Int, error),
) ([]*big.Int, *big.Int, error) {
	if !total.IsInt64() {
		return nil, nil, fmt.Errorf("exhaustive split total exceeds int64")
	}
	allocation := zeroAllocation(count)
	var bestAllocation []*big.Int
	var bestValue *big.Int
	var walk func(int, int64) error
	walk = func(index int, remaining int64) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if index == count-1 {
			allocation[index].SetInt64(remaining)
			value, err := evaluate(allocation)
			if err != nil {
				return err
			}
			if bestValue == nil || value.Cmp(bestValue) > 0 ||
				value.Cmp(bestValue) == 0 && allocationKey(allocation) < allocationKey(bestAllocation) {
				bestAllocation = cloneAllocation(allocation)
				bestValue = new(big.Int).Set(value)
			}
			return nil
		}
		for value := int64(0); value <= remaining; value++ {
			allocation[index].SetInt64(value)
			if err := walk(index+1, remaining-value); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(0, total.Int64()); err != nil {
		return nil, nil, err
	}
	return bestAllocation, bestValue, nil
}

func coordinateStarts(total *big.Int, count int) [][]*big.Int {
	var starts [][]*big.Int
	seen := make(map[string]struct{})
	add := func(candidate []*big.Int) {
		key := allocationKey(candidate)
		if _, exists := seen[key]; exists {
			return
		}
		seen[key] = struct{}{}
		starts = append(starts, candidate)
	}
	for index := 0; index < count; index++ {
		candidate := zeroAllocation(count)
		candidate[index].Set(total)
		add(candidate)
	}
	equal := zeroAllocation(count)
	divisor := big.NewInt(int64(count))
	share, remainder := new(big.Int).QuoRem(total, divisor, new(big.Int))
	for index := range equal {
		equal[index].Set(share)
		if int64(index) < remainder.Int64() {
			equal[index].Add(equal[index], big.NewInt(1))
		}
	}
	add(equal)
	for left := 0; left < count; left++ {
		for right := left + 1; right < count; right++ {
			candidate := zeroAllocation(count)
			candidate[left].Rsh(new(big.Int).Set(total), 1)
			candidate[right].Sub(total, candidate[left])
			add(candidate)
		}
	}
	return starts
}

func maximizePair(
	ctx context.Context,
	base []*big.Int,
	left, right int,
	total *big.Int,
	evaluate func([]*big.Int) (*big.Int, error),
	neighborhood int,
) (*big.Int, *big.Int, error) {
	at := func(value *big.Int) (*big.Int, error) {
		candidate := cloneAllocation(base)
		candidate[left].Set(value)
		candidate[right].Sub(total, value)
		return evaluate(candidate)
	}
	low := new(big.Int)
	high := new(big.Int).Set(total)
	one := big.NewInt(1)
	for low.Cmp(high) < 0 {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		middle := new(big.Int).Rsh(new(big.Int).Add(low, high), 1)
		next := new(big.Int).Add(middle, one)
		middleValue, err := at(middle)
		if err != nil {
			return nil, nil, err
		}
		nextValue, err := at(next)
		if err != nil {
			return nil, nil, err
		}
		if nextValue.Cmp(middleValue) >= 0 {
			low.Set(next)
		} else {
			high.Set(middle)
		}
	}
	best := new(big.Int).Set(low)
	bestValue, err := at(best)
	if err != nil {
		return nil, nil, err
	}
	start := new(big.Int).Sub(best, big.NewInt(int64(neighborhood)))
	if start.Sign() < 0 {
		start.SetInt64(0)
	}
	end := new(big.Int).Add(best, big.NewInt(int64(neighborhood)))
	if end.Cmp(total) > 0 {
		end.Set(total)
	}
	for candidate := new(big.Int).Set(start); candidate.Cmp(end) <= 0; candidate.Add(candidate, one) {
		value, err := at(candidate)
		if err != nil {
			return nil, nil, err
		}
		if value.Cmp(bestValue) > 0 || value.Cmp(bestValue) == 0 && candidate.Cmp(best) < 0 {
			best.Set(candidate)
			bestValue.Set(value)
		}
	}
	return best, bestValue, nil
}

func newMemoCurves(configured []SplitCurve) ([]*memoCurve, error) {
	return newCurves(configured, true)
}

func newUncachedCurves(configured []SplitCurve) ([]*memoCurve, error) {
	return newCurves(configured, false)
}

func newCurves(configured []SplitCurve, cacheEnabled bool) ([]*memoCurve, error) {
	copyCurves := append([]SplitCurve(nil), configured...)
	sort.Slice(copyCurves, func(i, j int) bool { return copyCurves[i].ID < copyCurves[j].ID })
	result := make([]*memoCurve, len(copyCurves))
	for index, curve := range copyCurves {
		if strings.TrimSpace(curve.ID) == "" || curve.Quote == nil {
			return nil, fmt.Errorf("curve %d requires ID and quote function", index)
		}
		var cache map[string]*big.Int
		if cacheEnabled {
			cache = make(map[string]*big.Int)
		}
		result[index] = &memoCurve{id: curve.ID, quote: curve.Quote, cache: cache}
	}
	return result, nil
}

func uniqueCurveIDs(groups ...[]*memoCurve) error {
	seen := make(map[string]struct{})
	for _, group := range groups {
		for _, curve := range group {
			if _, exists := seen[curve.id]; exists {
				return fmt.Errorf("duplicate split curve ID %q", curve.id)
			}
			seen[curve.id] = struct{}{}
		}
	}
	return nil
}

func (c *memoCurve) at(ctx context.Context, amount *big.Int) (*big.Int, error) {
	if amount == nil || amount.Sign() < 0 {
		return nil, fmt.Errorf("curve %q received invalid input", c.id)
	}
	if amount.Sign() == 0 {
		return new(big.Int), nil
	}
	key := amount.String()
	if c.cache != nil {
		c.mu.Lock()
		if value, ok := c.cache[key]; ok {
			c.hits++
			c.mu.Unlock()
			return new(big.Int).Set(value), nil
		}
		c.mu.Unlock()
	}
	value, err := c.quote(ctx, new(big.Int).Set(amount))
	if err != nil {
		if !errors.Is(err, market.ErrQuoteOutputRoundsToZero) {
			return nil, fmt.Errorf("quote curve %q: %w", c.id, err)
		}
		value = new(big.Int)
	}
	if value == nil || value.Sign() < 0 {
		return nil, fmt.Errorf("curve %q returned invalid output", c.id)
	}
	c.mu.Lock()
	c.calls++
	if c.cache != nil {
		c.cache[key] = new(big.Int).Set(value)
	}
	c.mu.Unlock()
	return new(big.Int).Set(value), nil
}

func materializeAllocations(ctx context.Context, curves []*memoCurve, amounts []*big.Int) ([]SplitAllocation, error) {
	result := make([]SplitAllocation, len(curves))
	for index, curve := range curves {
		output, err := curve.at(ctx, amounts[index])
		if err != nil {
			return nil, err
		}
		result[index] = SplitAllocation{
			CurveID: curve.id, AmountIn: new(big.Int).Set(amounts[index]), AmountOut: output,
		}
	}
	return result, nil
}

func allocationKey(allocation []*big.Int) string {
	var builder strings.Builder
	for index, amount := range allocation {
		if index > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(amount.String())
	}
	return builder.String()
}

func allocationConserves(allocation []*big.Int, total *big.Int) bool {
	sum := new(big.Int)
	for _, amount := range allocation {
		if amount == nil || amount.Sign() < 0 {
			return false
		}
		sum.Add(sum, amount)
	}
	return sum.Cmp(total) == 0
}

func zeroAllocation(count int) []*big.Int {
	result := make([]*big.Int, count)
	for index := range result {
		result[index] = new(big.Int)
	}
	return result
}

func cloneAllocation(source []*big.Int) []*big.Int {
	result := make([]*big.Int, len(source))
	for index, amount := range source {
		result[index] = new(big.Int).Set(amount)
	}
	return result
}

func cloneStagePlan(source stagePlan) stagePlan {
	return stagePlan{
		allocations: cloneAllocation(source.allocations),
		output:      new(big.Int).Set(source.output), sweeps: source.sweeps,
		exhaustive: source.exhaustive,
	}
}
