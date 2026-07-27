package sizing

import (
	"context"
	"fmt"
	"math/big"
	"runtime"
	"sync"
)

// GridTwoStageSplitRequest configures a bounded-cost approximation of the
// same liquidity-owning graph modeled by TwoStageSplitRequest. Divisions is
// the number of input intervals considered per stage.
type GridTwoStageSplitRequest struct {
	TotalInput  *big.Int
	Direct      []SplitCurve
	FirstStage  []SplitCurve
	SecondStage []SplitCurve
	Divisions   int
}

type gridPlan struct {
	amounts []*big.Int
	output  *big.Int
	valid   bool
}

type gridFlow struct {
	direct       gridPlan
	first        gridPlan
	second       gridPlan
	intermediate *big.Int
	output       *big.Int
}

// OptimizeTwoStageSplitGrid performs deterministic dynamic programming on a
// bounded integer grid. GridVerified reports exhaustive optimality inside that
// declared grid. GloballyVerified remains reserved for exhaustive optimality at
// one minimum input unit.
func OptimizeTwoStageSplitGrid(ctx context.Context, request GridTwoStageSplitRequest) (TwoStageSplitResult, error) {
	if err := ctx.Err(); err != nil {
		return TwoStageSplitResult{}, err
	}
	if request.TotalInput == nil || request.TotalInput.Sign() <= 0 {
		return TwoStageSplitResult{}, fmt.Errorf("positive total grid split input is required")
	}
	if len(request.Direct) == 0 && len(request.FirstStage) == 0 {
		return TwoStageSplitResult{}, fmt.Errorf("at least one direct or first-stage grid curve is required")
	}
	if (len(request.FirstStage) == 0) != (len(request.SecondStage) == 0) {
		return TwoStageSplitResult{}, fmt.Errorf("both grid stages of the intermediate branch are required")
	}
	divisions := request.Divisions
	if divisions == 0 {
		divisions = 32
	}
	if divisions < 2 || divisions > 1024 {
		return TwoStageSplitResult{}, fmt.Errorf("grid divisions must be between 2 and 1024")
	}

	direct, err := newUncachedCurves(request.Direct)
	if err != nil {
		return TwoStageSplitResult{}, fmt.Errorf("direct grid curves: %w", err)
	}
	first, err := newUncachedCurves(request.FirstStage)
	if err != nil {
		return TwoStageSplitResult{}, fmt.Errorf("first-stage grid curves: %w", err)
	}
	second, err := newUncachedCurves(request.SecondStage)
	if err != nil {
		return TwoStageSplitResult{}, fmt.Errorf("second-stage grid curves: %w", err)
	}
	if err := uniqueCurveIDs(direct, first, second); err != nil {
		return TwoStageSplitResult{}, err
	}

	effective := effectiveGridDivisions(request.TotalInput, divisions)
	quantum, remainder := new(big.Int).QuoRem(
		request.TotalInput, big.NewInt(int64(effective)), new(big.Int),
	)
	directPlans, err := gridFamilyPlans(ctx, quantum, effective, direct)
	if err != nil {
		return TwoStageSplitResult{}, fmt.Errorf("optimize direct grid: %w", err)
	}
	firstPlans, err := gridFamilyPlans(ctx, quantum, effective, first)
	if err != nil {
		return TwoStageSplitResult{}, fmt.Errorf("optimize first-stage grid: %w", err)
	}

	secondSolves := 0
	objectiveEvaluations := 0
	allSecondExact := true
	evaluate := func(directPlan, firstPlan gridPlan) (gridFlow, bool, bool, error) {
		if !directPlan.valid || !firstPlan.valid {
			return gridFlow{}, false, true, nil
		}
		intermediate := new(big.Int).Set(firstPlan.output)
		secondPlan := gridPlan{amounts: zeroAllocation(len(second)), output: new(big.Int), valid: true}
		secondExact := true
		secondSolved := false
		if intermediate.Sign() > 0 {
			var err error
			secondPlan, secondExact, err = gridFamilyBest(ctx, intermediate, divisions, second)
			if err != nil {
				return gridFlow{}, false, false, err
			}
			secondSolved = true
		}
		return gridFlow{
			direct: directPlan, first: firstPlan, second: secondPlan,
			intermediate: intermediate,
			output:       new(big.Int).Add(new(big.Int).Set(directPlan.output), secondPlan.output),
		}, secondSolved, secondExact, nil
	}

	type evaluatedGridFlow struct {
		flow         gridFlow
		secondSolved bool
		secondExact  bool
		err          error
	}
	evaluated := make([]evaluatedGridFlow, effective+1)
	jobs := make(chan int)
	workers := min(runtime.GOMAXPROCS(0), effective+1)
	var wait sync.WaitGroup
	wait.Add(workers)
	for range workers {
		go func() {
			defer wait.Done()
			for firstUnits := range jobs {
				flow, secondSolved, secondExact, evaluateErr := evaluate(
					directPlans[effective-firstUnits], firstPlans[firstUnits],
				)
				evaluated[firstUnits] = evaluatedGridFlow{
					flow: flow, secondSolved: secondSolved, secondExact: secondExact, err: evaluateErr,
				}
			}
		}()
	}
	for firstUnits := 0; firstUnits <= effective; firstUnits++ {
		jobs <- firstUnits
	}
	close(jobs)
	wait.Wait()

	var best gridFlow
	for firstUnits := 0; firstUnits <= effective; firstUnits++ {
		current := evaluated[firstUnits]
		if current.err != nil {
			return TwoStageSplitResult{}, fmt.Errorf("evaluate grid flow: %w", current.err)
		}
		if current.flow.output == nil {
			continue
		}
		objectiveEvaluations++
		if current.secondSolved {
			secondSolves++
		}
		if !current.secondExact {
			allSecondExact = false
		}
		if betterGridFlow(current.flow, best) {
			best = cloneGridFlow(current.flow)
		}
	}
	if best.output == nil {
		return TwoStageSplitResult{}, fmt.Errorf("grid split found no feasible allocation")
	}

	if remainder.Sign() > 0 {
		baseDirect := cloneGridPlan(best.direct)
		baseFirst := cloneGridPlan(best.first)
		for index := range direct {
			candidateDirect := cloneGridPlan(baseDirect)
			candidateDirect.amounts[index].Add(candidateDirect.amounts[index], remainder)
			output, quoteErr := sumGridOutputs(ctx, direct, candidateDirect.amounts)
			if quoteErr != nil {
				return TwoStageSplitResult{}, quoteErr
			}
			candidateDirect.output = output
			candidate, secondSolved, secondExact, quoteErr := evaluate(candidateDirect, baseFirst)
			if quoteErr != nil {
				return TwoStageSplitResult{}, quoteErr
			}
			objectiveEvaluations++
			if secondSolved {
				secondSolves++
			}
			if !secondExact {
				allSecondExact = false
			}
			if betterGridFlow(candidate, best) {
				best = cloneGridFlow(candidate)
			}
		}
		for index := range first {
			candidateFirst := cloneGridPlan(baseFirst)
			candidateFirst.amounts[index].Add(candidateFirst.amounts[index], remainder)
			output, quoteErr := sumGridOutputs(ctx, first, candidateFirst.amounts)
			if quoteErr != nil {
				return TwoStageSplitResult{}, quoteErr
			}
			candidateFirst.output = output
			candidate, secondSolved, secondExact, quoteErr := evaluate(baseDirect, candidateFirst)
			if quoteErr != nil {
				return TwoStageSplitResult{}, quoteErr
			}
			objectiveEvaluations++
			if secondSolved {
				secondSolves++
			}
			if !secondExact {
				allSecondExact = false
			}
			if betterGridFlow(candidate, best) {
				best = cloneGridFlow(candidate)
			}
		}
	}

	directAllocations, err := materializeAllocations(ctx, direct, best.direct.amounts)
	if err != nil {
		return TwoStageSplitResult{}, err
	}
	firstAllocations, err := materializeAllocations(ctx, first, best.first.amounts)
	if err != nil {
		return TwoStageSplitResult{}, err
	}
	secondAllocations, err := materializeAllocations(ctx, second, best.second.amounts)
	if err != nil {
		return TwoStageSplitResult{}, err
	}
	metrics := SplitMetrics{
		ObjectiveEvaluations: objectiveEvaluations,
		SecondStageSolves:    secondSolves,
	}
	for _, group := range [][]*memoCurve{direct, first, second} {
		for _, curve := range group {
			metrics.CurveEvaluations += curve.calls
			metrics.CurveCacheHits += curve.hits
		}
	}
	return TwoStageSplitResult{
		Direct: directAllocations, FirstStage: firstAllocations, SecondStage: secondAllocations,
		TotalInput:       new(big.Int).Set(request.TotalInput),
		IntermediateOut:  new(big.Int).Set(best.intermediate),
		TotalOutput:      new(big.Int).Set(best.output),
		InputResolution:  new(big.Int).Set(quantum),
		GridDivisions:    effective,
		GridVerified:     remainder.Sign() == 0,
		GloballyVerified: quantum.Cmp(big.NewInt(1)) == 0 && allSecondExact,
		Metrics:          metrics,
	}, nil
}

func gridFamilyPlans(ctx context.Context, quantum *big.Int, divisions int, curves []*memoCurve) ([]gridPlan, error) {
	plans := make([]gridPlan, divisions+1)
	if len(curves) == 0 {
		plans[0] = gridPlan{amounts: nil, output: new(big.Int), valid: true}
		return plans, nil
	}
	quotes := make([][]*big.Int, len(curves))
	for curveIndex, curve := range curves {
		quotes[curveIndex] = make([]*big.Int, divisions+1)
		for units := 0; units <= divisions; units++ {
			amount := new(big.Int).Mul(quantum, big.NewInt(int64(units)))
			output, err := curve.at(ctx, amount)
			if err != nil {
				return nil, err
			}
			quotes[curveIndex][units] = output
		}
	}

	previous := make([]gridPlan, divisions+1)
	previous[0] = gridPlan{amounts: nil, output: new(big.Int), valid: true}
	for curveIndex := range curves {
		current := make([]gridPlan, divisions+1)
		for totalUnits := 0; totalUnits <= divisions; totalUnits++ {
			for curveUnits := 0; curveUnits <= totalUnits; curveUnits++ {
				prefix := previous[totalUnits-curveUnits]
				if !prefix.valid {
					continue
				}
				output := new(big.Int).Add(prefix.output, quotes[curveIndex][curveUnits])
				amounts := append(cloneAllocation(prefix.amounts), new(big.Int).Mul(
					quantum, big.NewInt(int64(curveUnits)),
				))
				candidate := gridPlan{amounts: amounts, output: output, valid: true}
				if betterGridPlan(candidate, current[totalUnits]) {
					current[totalUnits] = candidate
				}
			}
		}
		previous = current
	}
	return previous, nil
}

func gridFamilyBest(ctx context.Context, total *big.Int, divisions int, curves []*memoCurve) (gridPlan, bool, error) {
	if total.Sign() == 0 {
		return gridPlan{amounts: zeroAllocation(len(curves)), output: new(big.Int), valid: true}, true, nil
	}
	effective := effectiveGridDivisions(total, divisions)
	quantum, remainder := new(big.Int).QuoRem(total, big.NewInt(int64(effective)), new(big.Int))
	best, err := gridFamilyBestWithRemainder(ctx, quantum, remainder, effective, curves)
	if err != nil {
		return gridPlan{}, false, err
	}
	return best, quantum.Cmp(big.NewInt(1)) == 0, nil
}

// gridFamilyBestWithRemainder exhaustively evaluates the declared family grid.
// When the total is not divisible by the grid size, the indivisible remainder
// is assigned to each possible curve in turn. This is materially different
// from adding it only to the best remainder-free plan: that shortcut can miss
// the true optimum of the declared grid.
func gridFamilyBestWithRemainder(
	ctx context.Context,
	quantum *big.Int,
	remainder *big.Int,
	divisions int,
	curves []*memoCurve,
) (gridPlan, error) {
	if len(curves) == 0 {
		return gridPlan{}, fmt.Errorf("positive amount has no grid curves")
	}
	if len(curves) == 1 {
		amount := new(big.Int).Mul(quantum, big.NewInt(int64(divisions)))
		amount.Add(amount, remainder)
		output, err := curves[0].at(ctx, amount)
		if err != nil {
			return gridPlan{}, err
		}
		return gridPlan{
			amounts: []*big.Int{amount}, output: output, valid: true,
		}, nil
	}
	if len(curves) == 2 {
		recipients := []int{-1}
		if remainder.Sign() > 0 {
			recipients = []int{0, 1}
		}
		var best gridPlan
		for _, recipient := range recipients {
			for leftUnits := 0; leftUnits <= divisions; leftUnits++ {
				left := new(big.Int).Mul(quantum, big.NewInt(int64(leftUnits)))
				right := new(big.Int).Mul(quantum, big.NewInt(int64(divisions-leftUnits)))
				if recipient == 0 {
					left.Add(left, remainder)
				} else if recipient == 1 {
					right.Add(right, remainder)
				}
				leftOutput, err := curves[0].at(ctx, left)
				if err != nil {
					return gridPlan{}, err
				}
				rightOutput, err := curves[1].at(ctx, right)
				if err != nil {
					return gridPlan{}, err
				}
				candidate := gridPlan{
					amounts: []*big.Int{left, right},
					output:  new(big.Int).Add(leftOutput, rightOutput),
					valid:   true,
				}
				if betterGridPlan(candidate, best) {
					best = cloneGridPlan(candidate)
				}
			}
		}
		return best, nil
	}
	if remainder.Sign() == 0 {
		plans, err := gridFamilyPlansWithRemainder(ctx, quantum, nil, divisions, curves, -1)
		if err != nil {
			return gridPlan{}, err
		}
		return cloneGridPlan(plans[divisions]), nil
	}
	var best gridPlan
	for recipient := range curves {
		plans, err := gridFamilyPlansWithRemainder(
			ctx, quantum, remainder, divisions, curves, recipient,
		)
		if err != nil {
			return gridPlan{}, err
		}
		if betterGridPlan(plans[divisions], best) {
			best = cloneGridPlan(plans[divisions])
		}
	}
	return best, nil
}

func gridFamilyPlansWithRemainder(
	ctx context.Context,
	quantum *big.Int,
	remainder *big.Int,
	divisions int,
	curves []*memoCurve,
	remainderRecipient int,
) ([]gridPlan, error) {
	plans := make([]gridPlan, divisions+1)
	if len(curves) == 0 {
		plans[0] = gridPlan{amounts: nil, output: new(big.Int), valid: true}
		return plans, nil
	}
	quotes := make([][]*big.Int, len(curves))
	amounts := make([][]*big.Int, len(curves))
	for curveIndex, curve := range curves {
		quotes[curveIndex] = make([]*big.Int, divisions+1)
		amounts[curveIndex] = make([]*big.Int, divisions+1)
		for units := 0; units <= divisions; units++ {
			amount := new(big.Int).Mul(quantum, big.NewInt(int64(units)))
			if curveIndex == remainderRecipient && remainder != nil {
				amount.Add(amount, remainder)
			}
			output, err := curve.at(ctx, amount)
			if err != nil {
				return nil, err
			}
			amounts[curveIndex][units] = amount
			quotes[curveIndex][units] = output
		}
	}

	previous := make([]gridPlan, divisions+1)
	previous[0] = gridPlan{amounts: nil, output: new(big.Int), valid: true}
	for curveIndex := range curves {
		current := make([]gridPlan, divisions+1)
		for totalUnits := 0; totalUnits <= divisions; totalUnits++ {
			for curveUnits := 0; curveUnits <= totalUnits; curveUnits++ {
				prefix := previous[totalUnits-curveUnits]
				if !prefix.valid {
					continue
				}
				output := new(big.Int).Add(prefix.output, quotes[curveIndex][curveUnits])
				allocation := append(cloneAllocation(prefix.amounts), new(big.Int).Set(amounts[curveIndex][curveUnits]))
				candidate := gridPlan{amounts: allocation, output: output, valid: true}
				if betterGridPlan(candidate, current[totalUnits]) {
					current[totalUnits] = candidate
				}
			}
		}
		previous = current
	}
	return previous, nil
}

func sumGridOutputs(ctx context.Context, curves []*memoCurve, amounts []*big.Int) (*big.Int, error) {
	output := new(big.Int)
	for index, curve := range curves {
		quoted, err := curve.at(ctx, amounts[index])
		if err != nil {
			return nil, err
		}
		output.Add(output, quoted)
	}
	return output, nil
}

func effectiveGridDivisions(total *big.Int, requested int) int {
	if total.IsInt64() && total.Int64() < int64(requested) {
		return int(total.Int64())
	}
	return requested
}

func betterGridPlan(candidate, current gridPlan) bool {
	if !candidate.valid {
		return false
	}
	if !current.valid {
		return true
	}
	comparison := candidate.output.Cmp(current.output)
	return comparison > 0 || comparison == 0 && allocationKey(candidate.amounts) < allocationKey(current.amounts)
}

func betterGridFlow(candidate, current gridFlow) bool {
	if candidate.output == nil {
		return false
	}
	if current.output == nil {
		return true
	}
	comparison := candidate.output.Cmp(current.output)
	if comparison != 0 {
		return comparison > 0
	}
	candidateKey := allocationKey(append(cloneAllocation(candidate.direct.amounts), candidate.first.amounts...))
	currentKey := allocationKey(append(cloneAllocation(current.direct.amounts), current.first.amounts...))
	return candidateKey < currentKey
}

func cloneGridPlan(source gridPlan) gridPlan {
	return gridPlan{
		amounts: cloneAllocation(source.amounts),
		output:  cloneOptionalInt(source.output),
		valid:   source.valid,
	}
}

func cloneGridFlow(source gridFlow) gridFlow {
	return gridFlow{
		direct: cloneGridPlan(source.direct), first: cloneGridPlan(source.first),
		second: cloneGridPlan(source.second), intermediate: cloneOptionalInt(source.intermediate),
		output: cloneOptionalInt(source.output),
	}
}

func cloneOptionalInt(value *big.Int) *big.Int {
	if value == nil {
		return nil
	}
	return new(big.Int).Set(value)
}
