package okxexperiment

import (
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"
)

// AmountStatistics summarizes successful quote amounts in token units.
type AmountStatistics struct {
	Samples   int
	Min, Mean string
	P50, P95  string
	P99, Max  string
}

// FormatBaseUnits formats an integer token amount using the token's decimal
// precision. It deliberately uses integers so quote output is never rounded
// through floating point arithmetic.
func FormatBaseUnits(rawAmount, rawDecimals string) (string, error) {
	amount, decimals, err := parseBaseAmount(rawAmount, rawDecimals)
	if err != nil {
		return "", err
	}
	return formatAmount(amount, decimals), nil
}

// DifferenceBaseUnits subtracts right from left and formats the result using
// the same token precision.
func DifferenceBaseUnits(left, right, rawDecimals string) (string, error) {
	difference, err := SubtractBaseUnits(left, right)
	if err != nil {
		return "", err
	}
	_, decimals, err := parseBaseAmount(difference, rawDecimals)
	if err != nil {
		return "", err
	}
	differenceAmount, ok := new(big.Int).SetString(difference, 10)
	if !ok {
		return "", fmt.Errorf("invalid integer token amount %q", difference)
	}
	return formatAmount(differenceAmount, decimals), nil
}

// SubtractBaseUnits returns left-right without converting out of minimum
// token units. It is used to aggregate quote deltas without floating point.
func SubtractBaseUnits(left, right string) (string, error) {
	leftAmount, ok := new(big.Int).SetString(strings.TrimSpace(left), 10)
	if !ok {
		return "", fmt.Errorf("invalid integer token amount %q", left)
	}
	rightAmount, ok := new(big.Int).SetString(strings.TrimSpace(right), 10)
	if !ok {
		return "", fmt.Errorf("invalid integer token amount %q", right)
	}
	return new(big.Int).Sub(leftAmount, rightAmount).String(), nil
}

// SummarizeBaseUnits summarizes integer token amounts and renders every
// metric using the token's decimal precision. Percentiles use nearest rank.
func SummarizeBaseUnits(rawValues []string, rawDecimals string) (AmountStatistics, error) {
	if len(rawValues) == 0 {
		return AmountStatistics{}, fmt.Errorf("cannot summarize an empty amount sample")
	}
	_, decimals, err := parseBaseAmount(rawValues[0], rawDecimals)
	if err != nil {
		return AmountStatistics{}, err
	}
	values := make([]*big.Int, len(rawValues))
	sum := new(big.Int)
	for index, raw := range rawValues {
		value, _, err := parseBaseAmount(raw, rawDecimals)
		if err != nil {
			return AmountStatistics{}, err
		}
		values[index] = value
		sum.Add(sum, value)
	}
	sort.Slice(values, func(i, j int) bool { return values[i].Cmp(values[j]) < 0 })
	mean := new(big.Rat).SetFrac(sum, big.NewInt(int64(len(values))))
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	mean.Quo(mean, new(big.Rat).SetInt(scale))
	return AmountStatistics{
		Samples: len(values),
		Min:     formatAmount(values[0], decimals),
		Mean:    mean.FloatString(decimals),
		P50:     formatAmount(nearestRank(values, 50, 100), decimals),
		P95:     formatAmount(nearestRank(values, 95, 100), decimals),
		P99:     formatAmount(nearestRank(values, 99, 100), decimals),
		Max:     formatAmount(values[len(values)-1], decimals),
	}, nil
}

func nearestRank(sorted []*big.Int, numerator, denominator int) *big.Int {
	rank := (len(sorted)*numerator + denominator - 1) / denominator
	return sorted[rank-1]
}

func parseBaseAmount(rawAmount, rawDecimals string) (*big.Int, int, error) {
	amount, ok := new(big.Int).SetString(strings.TrimSpace(rawAmount), 10)
	if !ok {
		return nil, 0, fmt.Errorf("invalid integer token amount %q", rawAmount)
	}
	decimals, err := strconv.Atoi(strings.TrimSpace(rawDecimals))
	if err != nil || decimals < 0 {
		return nil, 0, fmt.Errorf("invalid token decimals %q", rawDecimals)
	}
	return amount, decimals, nil
}

func formatAmount(amount *big.Int, decimals int) string {
	negative := amount.Sign() < 0
	digits := new(big.Int).Abs(amount).String()
	if decimals == 0 {
		if negative {
			return "-" + digits
		}
		return digits
	}
	if len(digits) <= decimals {
		digits = strings.Repeat("0", decimals-len(digits)+1) + digits
	}
	pivot := len(digits) - decimals
	formatted := digits[:pivot] + "." + digits[pivot:]
	if negative {
		return "-" + formatted
	}
	return formatted
}
