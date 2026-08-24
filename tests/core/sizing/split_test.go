package sizing_test

import (
	"context"
	"math/big"
	"math/rand"
	"testing"

	"github.com/VarozXYZ/vernier/core/sizing"
	"github.com/VarozXYZ/vernier/domain/market"
)

type integerCurve func(int64) int64

func TestHighPrecisionTwoStageSplitMatchesExhaustiveSearch(t *testing.T) {
	directFunctions := []integerCurve{
		constantProduct(70, 150),
		constantProduct(120, 250),
	}
	firstFunctions := []integerCurve{
		constantProduct(90, 50),
		constantProduct(140, 82),
	}
	secondFunctions := []integerCurve{
		constantProduct(45, 170),
		constantProduct(80, 290),
	}
	const total int64 = 24
	expected := exhaustiveTwoStage(total, directFunctions, firstFunctions, secondFunctions)
	result, err := sizing.OptimizeTwoStageSplit(context.Background(), sizing.TwoStageSplitRequest{
		TotalInput:  big.NewInt(total),
		Direct:      curves("direct", directFunctions),
		FirstStage:  curves("first", firstFunctions),
		SecondStage: curves("second", secondFunctions),
		MaxSweeps:   64, Neighborhood: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.TotalOutput.Cmp(big.NewInt(expected)) != 0 {
		t.Fatalf("solver output=%s exhaustive=%d", result.TotalOutput, expected)
	}
	if !result.PairwiseOptimal || result.InputResolution.Cmp(big.NewInt(1)) != 0 {
		t.Fatalf("unexpected precision evidence: optimal=%t resolution=%s", result.PairwiseOptimal, result.InputResolution)
	}
	if !result.GloballyVerified {
		t.Fatal("small state space should be globally verified by exhaustive search")
	}
	assertConservation(t, result)
}

func TestGridTwoStageSplitMatchesExhaustiveSearchAtUnitResolution(t *testing.T) {
	directFunctions := []integerCurve{
		constantProduct(70, 150),
		constantProduct(120, 250),
	}
	firstFunctions := []integerCurve{
		constantProduct(90, 50),
		constantProduct(140, 82),
	}
	secondFunctions := []integerCurve{
		constantProduct(45, 170),
		constantProduct(80, 290),
	}
	const total int64 = 24
	expected := exhaustiveTwoStage(total, directFunctions, firstFunctions, secondFunctions)
	result, err := sizing.OptimizeTwoStageSplitGrid(context.Background(), sizing.GridTwoStageSplitRequest{
		TotalInput:  big.NewInt(total),
		Direct:      curves("direct", directFunctions),
		FirstStage:  curves("first", firstFunctions),
		SecondStage: curves("second", secondFunctions),
		Divisions:   64,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.TotalOutput.Cmp(big.NewInt(expected)) != 0 {
		t.Fatalf("grid solver output=%s exhaustive=%d", result.TotalOutput, expected)
	}
	if !result.GridVerified || !result.GloballyVerified || result.InputResolution.Cmp(big.NewInt(1)) != 0 {
		t.Fatalf(
			"unexpected grid verification: grid=%t global=%t resolution=%s",
			result.GridVerified, result.GloballyVerified, result.InputResolution,
		)
	}
	assertConservation(t, result)
}

func TestSplitDoesNotDuplicateSharedSecondStageLiquidity(t *testing.T) {
	result, err := sizing.OptimizeTwoStageSplit(context.Background(), sizing.TwoStageSplitRequest{
		TotalInput: big.NewInt(109),
		FirstStage: curves("first", []integerCurve{
			constantProduct(100, 100),
			constantProduct(100, 100),
		}),
		SecondStage: curves("second", []integerCurve{
			constantProduct(100, 100),
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	var firstOutput, secondInput big.Int
	for _, allocation := range result.FirstStage {
		firstOutput.Add(&firstOutput, allocation.AmountOut)
	}
	for _, allocation := range result.SecondStage {
		secondInput.Add(&secondInput, allocation.AmountIn)
	}
	if firstOutput.Cmp(&secondInput) != 0 || firstOutput.Cmp(result.IntermediateOut) != 0 {
		t.Fatalf("intermediate flow duplicated: first=%s second=%s result=%s", &firstOutput, &secondInput, result.IntermediateOut)
	}
	assertConservation(t, result)
}

func TestSplitIsDeterministicAcrossCurveOrder(t *testing.T) {
	first := []sizing.SplitCurve{
		curve("z", constantProduct(100, 200)),
		curve("a", constantProduct(100, 200)),
	}
	second := []sizing.SplitCurve{curve("out", constantProduct(100, 200))}
	left, err := sizing.OptimizeTwoStageSplit(context.Background(), sizing.TwoStageSplitRequest{
		TotalInput: big.NewInt(25), FirstStage: first, SecondStage: second,
	})
	if err != nil {
		t.Fatal(err)
	}
	right, err := sizing.OptimizeTwoStageSplit(context.Background(), sizing.TwoStageSplitRequest{
		TotalInput: big.NewInt(25), FirstStage: []sizing.SplitCurve{first[1], first[0]}, SecondStage: second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if left.TotalOutput.Cmp(right.TotalOutput) != 0 || allocationText(left.FirstStage) != allocationText(right.FirstStage) {
		t.Fatalf("curve order changed result: left=%s/%s right=%s/%s", left.TotalOutput, allocationText(left.FirstStage), right.TotalOutput, allocationText(right.FirstStage))
	}
}

func TestSplitRejectsIncompleteIntermediateBranch(t *testing.T) {
	_, err := sizing.OptimizeTwoStageSplit(context.Background(), sizing.TwoStageSplitRequest{
		TotalInput: big.NewInt(1),
		FirstStage: curves("first", []integerCurve{constantProduct(10, 10)}),
	})
	if err == nil {
		t.Fatal("incomplete intermediate branch was accepted")
	}
}

func TestSplitResultBecomesExecutableAllocation(t *testing.T) {
	result := sizing.TwoStageSplitResult{
		Direct: []sizing.SplitAllocation{
			{CurveID: "direct", AmountIn: big.NewInt(20), AmountOut: big.NewInt(30)},
		},
		FirstStage: []sizing.SplitAllocation{
			{CurveID: "first", AmountIn: big.NewInt(80), AmountOut: big.NewInt(100)},
		},
		SecondStage: []sizing.SplitAllocation{
			{CurveID: "second-a", AmountIn: big.NewInt(60), AmountOut: big.NewInt(70)},
			{CurveID: "second-b", AmountIn: big.NewInt(40), AmountOut: big.NewInt(50)},
		},
		TotalInput: big.NewInt(100), IntermediateOut: big.NewInt(100), TotalOutput: big.NewInt(150),
	}
	allocation, err := sizing.BuildRouteAllocation(result, "quote", "middle", "base")
	if err != nil {
		t.Fatal(err)
	}
	if len(allocation.Groups) != 3 || allocation.Groups[2].Parent != allocation.Groups[1].ID {
		t.Fatalf("allocation groups = %+v", allocation.Groups)
	}
}

func TestSplitTreatsRoundedZeroOutputAsZeroValuedAllocation(t *testing.T) {
	result, err := sizing.OptimizeTwoStageSplit(context.Background(), sizing.TwoStageSplitRequest{
		TotalInput: big.NewInt(100),
		Direct: []sizing.SplitCurve{
			{
				ID: "rounded-zero",
				Quote: func(context.Context, *big.Int) (*big.Int, error) {
					return nil, market.ErrQuoteOutputRoundsToZero
				},
			},
			{
				ID: "productive",
				Quote: func(_ context.Context, input *big.Int) (*big.Int, error) {
					return new(big.Int).Set(input), nil
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.TotalOutput.Cmp(big.NewInt(100)) != 0 {
		t.Fatalf("unexpected split output %s", result.TotalOutput)
	}
	for _, allocation := range result.Direct {
		if allocation.CurveID == "rounded-zero" && allocation.AmountIn.Sign() != 0 {
			t.Fatalf("rounded-zero curve received input %s", allocation.AmountIn)
		}
	}
}

func TestSplitDropsDustIntermediateBranchThatCannotProduceOutput(t *testing.T) {
	result, err := sizing.OptimizeTwoStageSplit(context.Background(), sizing.TwoStageSplitRequest{
		TotalInput: big.NewInt(109),
		Direct:     []sizing.SplitCurve{curve("direct", func(input int64) int64 { return input / 10 })},
		FirstStage: []sizing.SplitCurve{curve("first", func(input int64) int64 {
			if input < 10 {
				return 0
			}
			return input / 20
		})},
		SecondStage: []sizing.SplitCurve{curve("second", func(input int64) int64 { return input })},
	})
	if err != nil {
		t.Fatal(err)
	}
	allocation, err := sizing.BuildRouteAllocation(result, "input", "middle", "output")
	if err != nil {
		t.Fatal(err)
	}
	if len(allocation.Groups) != 1 || allocation.Groups[0].ID != "direct" {
		t.Fatalf("dust intermediate branch was retained: %+v", allocation.Groups)
	}
	if allocation.Groups[0].Branches[0].PlannedInput.Cmp(big.NewInt(109)) != 0 {
		t.Fatalf("direct route input = %s, want 109", allocation.Groups[0].Branches[0].PlannedInput)
	}
}

func TestHighPrecisionSplitMatchesExhaustiveRandomizedCases(t *testing.T) {
	random := rand.New(rand.NewSource(7))
	for testCase := 0; testCase < 200; testCase++ {
		direct := []integerCurve{
			constantProduct(int64(20+random.Intn(100)), int64(30+random.Intn(200))),
			constantProduct(int64(20+random.Intn(100)), int64(30+random.Intn(200))),
		}
		first := []integerCurve{
			constantProduct(int64(20+random.Intn(100)), int64(30+random.Intn(200))),
			constantProduct(int64(20+random.Intn(100)), int64(30+random.Intn(200))),
		}
		second := []integerCurve{
			constantProduct(int64(20+random.Intn(100)), int64(30+random.Intn(200))),
			constantProduct(int64(20+random.Intn(100)), int64(30+random.Intn(200))),
		}
		total := int64(5 + random.Intn(11))
		expected := exhaustiveTwoStage(total, direct, first, second)
		result, err := sizing.OptimizeTwoStageSplit(context.Background(), sizing.TwoStageSplitRequest{
			TotalInput: big.NewInt(total), Direct: curves("direct", direct),
			FirstStage: curves("first", first), SecondStage: curves("second", second),
			Neighborhood: 16,
		})
		if err != nil {
			t.Fatalf("case %d: %v", testCase, err)
		}
		if result.TotalOutput.Cmp(big.NewInt(expected)) != 0 {
			t.Fatalf("case %d total=%d solver=%s exhaustive=%d", testCase, total, result.TotalOutput, expected)
		}
	}
}

func TestLargeSplitFallbackNeverLosesToBestSingleRoute(t *testing.T) {
	total := int64(20_000)
	directFunctions := []integerCurve{
		constantProduct(80_000, 55_000),
		constantProduct(70_000, 46_000),
	}
	firstFunctions := []integerCurve{
		constantProduct(90_000, 52_000),
		constantProduct(75_000, 45_000),
		constantProduct(68_000, 41_000),
	}
	secondFunctions := []integerCurve{
		constantProduct(65_000, 50_000),
		constantProduct(85_000, 68_000),
	}
	result, err := sizing.OptimizeTwoStageSplit(context.Background(), sizing.TwoStageSplitRequest{
		TotalInput:  big.NewInt(total),
		Direct:      curves("direct", directFunctions),
		FirstStage:  curves("first", firstFunctions),
		SecondStage: curves("second", secondFunctions),
		MaxStarts:   1, ExhaustiveLimit: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	bestSingle := int64(0)
	for _, direct := range directFunctions {
		if output := direct(total); output > bestSingle {
			bestSingle = output
		}
	}
	for _, first := range firstFunctions {
		intermediate := first(total)
		for _, second := range secondFunctions {
			if output := second(intermediate); output > bestSingle {
				bestSingle = output
			}
		}
	}
	if result.TotalOutput.Cmp(big.NewInt(bestSingle)) < 0 {
		t.Fatalf("split output %s is below best single route %d", result.TotalOutput, bestSingle)
	}
	if result.GloballyVerified {
		t.Fatal("large fallback search must not claim exhaustive global verification")
	}
	assertConservation(t, result)
}

func TestGridSplitHasBoundedEvaluationBudgetAndConservesFlow(t *testing.T) {
	direct := []sizing.SplitCurve{
		curve("direct-a", constantProduct(2_000_000_000, 1_100_000_000)),
		curve("direct-b", constantProduct(1_700_000_000, 900_000_000)),
	}
	first := []sizing.SplitCurve{
		curve("first-a", constantProduct(3_000_000_000, 1_500_000_000)),
		curve("first-b", constantProduct(2_400_000_000, 1_300_000_000)),
		curve("first-c", constantProduct(1_900_000_000, 1_000_000_000)),
		curve("first-d", constantProduct(1_600_000_000, 850_000_000)),
	}
	second := []sizing.SplitCurve{
		curve("second-a", constantProduct(2_800_000_000, 1_400_000_000)),
		curve("second-b", constantProduct(2_100_000_000, 1_150_000_000)),
	}
	result, err := sizing.OptimizeTwoStageSplitGrid(context.Background(), sizing.GridTwoStageSplitRequest{
		TotalInput: big.NewInt(10_000_000),
		Direct:     direct, FirstStage: first, SecondStage: second,
		Divisions: 32,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Metrics.CurveEvaluations > 5_000 {
		t.Fatalf("grid solver exceeded evaluation budget: %+v", result.Metrics)
	}
	if !result.GridVerified || result.GloballyVerified || result.InputResolution.Cmp(big.NewInt(312_500)) != 0 {
		t.Fatalf(
			"unexpected coarse-grid evidence: grid=%t global=%t resolution=%s",
			result.GridVerified, result.GloballyVerified, result.InputResolution,
		)
	}
	assertConservation(t, result)
}

func TestGridSplitExhaustsSecondStageRemainderPlacements(t *testing.T) {
	result, err := sizing.OptimizeTwoStageSplitGrid(context.Background(), sizing.GridTwoStageSplitRequest{
		TotalInput: big.NewInt(2),
		FirstStage: []sizing.SplitCurve{
			curve("first", func(input int64) int64 {
				if input == 2 {
					return 5
				}
				return input
			}),
		},
		SecondStage: []sizing.SplitCurve{
			curve("remainder-sensitive", func(input int64) int64 {
				if input == 3 {
					return 100
				}
				return 0
			}),
			curve("linear", func(input int64) int64 { return input }),
		},
		Divisions: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.TotalOutput.Cmp(big.NewInt(102)) != 0 {
		t.Fatalf("grid output=%s, want 102 from exhaustive remainder placement", result.TotalOutput)
	}
	if !result.GridVerified || result.GloballyVerified {
		t.Fatalf(
			"unexpected verification evidence: grid=%t global=%t",
			result.GridVerified, result.GloballyVerified,
		)
	}
	assertConservation(t, result)
}

func BenchmarkHighPrecisionTwoStageSplit(b *testing.B) {
	direct := []sizing.SplitCurve{
		curve("direct-a", constantProduct(2_000_000_000, 1_100_000_000)),
		curve("direct-b", constantProduct(1_700_000_000, 900_000_000)),
	}
	first := []sizing.SplitCurve{
		curve("first-a", constantProduct(3_000_000_000, 1_500_000_000)),
		curve("first-b", constantProduct(2_400_000_000, 1_300_000_000)),
		curve("first-c", constantProduct(1_900_000_000, 1_000_000_000)),
		curve("first-d", constantProduct(1_600_000_000, 850_000_000)),
	}
	second := []sizing.SplitCurve{
		curve("second-a", constantProduct(2_800_000_000, 1_400_000_000)),
		curve("second-b", constantProduct(2_100_000_000, 1_150_000_000)),
	}
	request := sizing.TwoStageSplitRequest{
		TotalInput: big.NewInt(10_000_000),
		Direct:     direct, FirstStage: first, SecondStage: second,
	}
	b.ResetTimer()
	for range b.N {
		result, err := sizing.OptimizeTwoStageSplit(context.Background(), request)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(result.Metrics.CurveEvaluations), "curve-evals/op")
		b.ReportMetric(float64(result.Metrics.ObjectiveEvaluations), "objectives/op")
	}
}

func BenchmarkGridTwoStageSplit(b *testing.B) {
	direct := []sizing.SplitCurve{
		curve("direct-a", constantProduct(2_000_000_000, 1_100_000_000)),
		curve("direct-b", constantProduct(1_700_000_000, 900_000_000)),
	}
	first := []sizing.SplitCurve{
		curve("first-a", constantProduct(3_000_000_000, 1_500_000_000)),
		curve("first-b", constantProduct(2_400_000_000, 1_300_000_000)),
		curve("first-c", constantProduct(1_900_000_000, 1_000_000_000)),
		curve("first-d", constantProduct(1_600_000_000, 850_000_000)),
	}
	second := []sizing.SplitCurve{
		curve("second-a", constantProduct(2_800_000_000, 1_400_000_000)),
		curve("second-b", constantProduct(2_100_000_000, 1_150_000_000)),
	}
	request := sizing.GridTwoStageSplitRequest{
		TotalInput: big.NewInt(10_000_000),
		Direct:     direct, FirstStage: first, SecondStage: second,
		Divisions: 32,
	}
	b.ResetTimer()
	for range b.N {
		result, err := sizing.OptimizeTwoStageSplitGrid(context.Background(), request)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(result.Metrics.CurveEvaluations), "curve-evals/op")
		b.ReportMetric(float64(result.Metrics.ObjectiveEvaluations), "objectives/op")
	}
}

// BenchmarkOperationalOnePlusOne mirrors the production topology: one direct
// branch and one two-hop branch. It is the latency guard used by local Live
// composition; the wider benchmark above remains a stress profile.
func BenchmarkOperationalOnePlusOne(b *testing.B) {
	request := sizing.TwoStageSplitRequest{
		TotalInput: big.NewInt(500_000_000),
		Direct: []sizing.SplitCurve{
			curve("direct", constantProduct(900_000_000, 700_000_000)),
		},
		FirstStage: []sizing.SplitCurve{
			curve("first", constantProduct(1_200_000_000, 800_000_000)),
		},
		SecondStage: []sizing.SplitCurve{
			curve("second", constantProduct(1_100_000_000, 900_000_000)),
		},
		MaxSweeps: 8, MaxStarts: 1, Neighborhood: 8,
	}
	b.ResetTimer()
	for range b.N {
		if _, err := sizing.OptimizeTwoStageSplit(context.Background(), request); err != nil {
			b.Fatal(err)
		}
	}
}

func curves(prefix string, functions []integerCurve) []sizing.SplitCurve {
	result := make([]sizing.SplitCurve, len(functions))
	for index, function := range functions {
		result[index] = curve(prefix+"-"+big.NewInt(int64(index)).String(), function)
	}
	return result
}

func curve(id string, function integerCurve) sizing.SplitCurve {
	return sizing.SplitCurve{
		ID: id,
		Quote: func(_ context.Context, input *big.Int) (*big.Int, error) {
			return big.NewInt(function(input.Int64())), nil
		},
	}
}

func constantProduct(reserveIn, reserveOut int64) integerCurve {
	return func(input int64) int64 {
		if input == 0 {
			return 0
		}
		return input * reserveOut / (reserveIn + input)
	}
}

func exhaustiveTwoStage(total int64, direct, first, second []integerCurve) int64 {
	inputCurves := append(append([]integerCurve(nil), direct...), first...)
	best := int64(-1)
	enumerateCompositions(total, len(inputCurves), func(allocation []int64) {
		output := int64(0)
		for index := range direct {
			output += direct[index](allocation[index])
		}
		intermediate := int64(0)
		for index := range first {
			intermediate += first[index](allocation[len(direct)+index])
		}
		stageBest := int64(-1)
		enumerateCompositions(intermediate, len(second), func(secondAllocation []int64) {
			candidate := int64(0)
			for index := range second {
				candidate += second[index](secondAllocation[index])
			}
			if candidate > stageBest {
				stageBest = candidate
			}
		})
		if stageBest > 0 {
			output += stageBest
		}
		if output > best {
			best = output
		}
	})
	return best
}

func enumerateCompositions(total int64, count int, visit func([]int64)) {
	values := make([]int64, count)
	var walk func(int, int64)
	walk = func(index int, remaining int64) {
		if index == count-1 {
			values[index] = remaining
			visit(append([]int64(nil), values...))
			return
		}
		for value := int64(0); value <= remaining; value++ {
			values[index] = value
			walk(index+1, remaining-value)
		}
	}
	walk(0, total)
}

func assertConservation(t *testing.T, result sizing.TwoStageSplitResult) {
	t.Helper()
	input := new(big.Int)
	for _, allocation := range result.Direct {
		input.Add(input, allocation.AmountIn)
	}
	for _, allocation := range result.FirstStage {
		input.Add(input, allocation.AmountIn)
	}
	if input.Cmp(result.TotalInput) != 0 {
		t.Fatalf("input not conserved: allocations=%s total=%s", input, result.TotalInput)
	}
	intermediate := new(big.Int)
	for _, allocation := range result.SecondStage {
		intermediate.Add(intermediate, allocation.AmountIn)
	}
	if intermediate.Cmp(result.IntermediateOut) != 0 {
		t.Fatalf("intermediate not conserved: allocations=%s total=%s", intermediate, result.IntermediateOut)
	}
}

func allocationText(allocations []sizing.SplitAllocation) string {
	var result string
	for _, allocation := range allocations {
		result += allocation.CurveID + "=" + allocation.AmountIn.String() + ";"
	}
	return result
}
