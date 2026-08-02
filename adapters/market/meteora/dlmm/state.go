// Package dlmm implements the canonical local state and integer quote model
// for Meteora's Dynamic Liquidity Market Maker.
package dlmm

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math/big"
	"sort"
	"strings"

	"github.com/VarozXYZ/vernier/adapters/market/liquiditycurve"
	"github.com/VarozXYZ/vernier/domain/market"
)

const snapshotSchemaVersion uint16 = 2

type Bin struct {
	id                            int32
	reserveX                      *big.Int
	reserveY                      *big.Int
	priceX64                      *big.Int
	openOrderAmount               *big.Int
	processedOrderRemainingAmount *big.Int
	limitOrderAskSide             bool
}

func NewBin(id int32, reserveX, reserveY *big.Int) (Bin, error) {
	if reserveX == nil || reserveY == nil || reserveX.Sign() < 0 || reserveY.Sign() < 0 || reserveX.Sign() == 0 && reserveY.Sign() == 0 {
		return Bin{}, fmt.Errorf("bin reserves must be non-negative and not both zero")
	}
	price := new(big.Int).Lsh(new(big.Int).Set(reserveY), priceScaleBits)
	price.Quo(price, reserveX)
	if price.Sign() <= 0 {
		return Bin{}, fmt.Errorf("bin price rounds to zero")
	}
	return newBinWithProtocolLiquidity(id, reserveX, reserveY, price, new(big.Int), new(big.Int), false)
}

// NewBinWithPrice preserves the protocol's fixed-point bin price. It is used
// by on-chain decoders because a bin can be one-sided and its price cannot be
// reconstructed from the two reserve amounts alone.
func NewBinWithPrice(id int32, reserveX, reserveY, priceX64 *big.Int) (Bin, error) {
	return newBinWithProtocolLiquidity(id, reserveX, reserveY, priceX64, new(big.Int), new(big.Int), false)
}

// NewBinWithProtocolLiquidity preserves both market-making and limit-order
// liquidity from Meteora's canonical Bin account layout.
func NewBinWithProtocolLiquidity(id int32, reserveX, reserveY, priceX64, openOrderAmount, processedOrderRemainingAmount *big.Int, limitOrderAskSide bool) (Bin, error) {
	return newBinWithProtocolLiquidity(id, reserveX, reserveY, priceX64, openOrderAmount, processedOrderRemainingAmount, limitOrderAskSide)
}

func newBinWithProtocolLiquidity(id int32, reserveX, reserveY, priceX64, openOrderAmount, processedOrderRemainingAmount *big.Int, limitOrderAskSide bool) (Bin, error) {
	values := []*big.Int{reserveX, reserveY, priceX64, openOrderAmount, processedOrderRemainingAmount}
	for _, value := range values {
		if value == nil || value.Sign() < 0 {
			return Bin{}, fmt.Errorf("bin liquidity and price must be valid")
		}
	}
	if priceX64.Sign() <= 0 || reserveX.Sign() == 0 && reserveY.Sign() == 0 && openOrderAmount.Sign() == 0 && processedOrderRemainingAmount.Sign() == 0 {
		return Bin{}, fmt.Errorf("bin liquidity and price must be valid")
	}
	return Bin{
		id: id, reserveX: clone(reserveX), reserveY: clone(reserveY), priceX64: clone(priceX64),
		openOrderAmount: clone(openOrderAmount), processedOrderRemainingAmount: clone(processedOrderRemainingAmount), limitOrderAskSide: limitOrderAskSide,
	}, nil
}

func (b Bin) ID() int32                               { return b.id }
func (b Bin) ReserveX() *big.Int                      { return clone(b.reserveX) }
func (b Bin) ReserveY() *big.Int                      { return clone(b.reserveY) }
func (b Bin) PriceX64() *big.Int                      { return clone(b.priceX64) }
func (b Bin) OpenOrderAmount() *big.Int               { return clone(b.openOrderAmount) }
func (b Bin) ProcessedOrderRemainingAmount() *big.Int { return clone(b.processedOrderRemainingAmount) }
func (b Bin) LimitOrderAskSide() bool                 { return b.limitOrderAskSide }

const priceScaleBits uint = 64

type StateUpdate struct {
	activeID        int32
	binStep         uint16
	feeRate         uint64
	fixedFee        bool
	baseFactor      uint16
	filterPeriod    uint16
	decayPeriod     uint16
	reductionFactor uint16
	protocolShare   uint16
	baseFeePower    uint8
	functionType    uint8
	collectFeeMode  uint8
	legacyLO        bool
	variableControl uint32
	maxVolatility   uint32
	volatilityAcc   uint32
	volatilityRef   uint32
	indexReference  int32
	lastUpdateTime  int64
	bins            []Bin
}

type StaticParameters struct {
	BaseFactor        uint16
	FilterPeriod      uint16
	DecayPeriod       uint16
	ReductionFactor   uint16
	VariableControl   uint32
	MaxVolatility     uint32
	ProtocolShare     uint16
	BaseFeePower      uint8
	FunctionType      uint8
	CollectFeeMode    uint8
	LegacyLimitOrders bool
}

type VariableParameters struct {
	VolatilityAccumulator uint32
	VolatilityReference   uint32
	IndexReference        int32
	LastUpdateTimestamp   int64
}

func NewStateUpdate(activeID int32, binStep, feeBPS uint16, bins []Bin) (StateUpdate, error) {
	return NewStateUpdateWithFeeRate(activeID, binStep, uint64(feeBPS)*liquiditycurve.FeeRatePrecision/10_000, bins)
}

func NewStateUpdateWithFeeRate(activeID int32, binStep uint16, feeRate uint64, bins []Bin) (StateUpdate, error) {
	state := Snapshot{schemaVersion: snapshotSchemaVersion, activeID: activeID, binStep: binStep, feeRate: feeRate, fixedFee: true, bins: cloneBins(bins)}
	if err := state.validate(); err != nil {
		return StateUpdate{}, err
	}
	return StateUpdate{activeID: activeID, binStep: binStep, feeRate: feeRate, fixedFee: true, bins: cloneBins(bins)}, nil
}

func NewDynamicStateUpdate(activeID int32, binStep, baseFactor uint16, baseFeePower uint8, variableControl, maxVolatility, volatilityRef uint32, indexReference int32, bins []Bin) (StateUpdate, error) {
	return NewProtocolStateUpdate(activeID, binStep, StaticParameters{
		BaseFactor: baseFactor, VariableControl: variableControl, MaxVolatility: maxVolatility, BaseFeePower: baseFeePower,
	}, VariableParameters{
		VolatilityAccumulator: volatilityRef, VolatilityReference: volatilityRef, IndexReference: indexReference,
	}, bins)
}

func NewProtocolStateUpdate(activeID int32, binStep uint16, parameters StaticParameters, variable VariableParameters, bins []Bin) (StateUpdate, error) {
	rate := dynamicFeeRate(variable.IndexReference, activeID, binStep, parameters.BaseFactor, parameters.BaseFeePower, parameters.VariableControl, parameters.MaxVolatility, variable.VolatilityReference)
	state := Snapshot{
		schemaVersion: snapshotSchemaVersion, activeID: activeID, binStep: binStep, feeRate: rate,
		baseFactor: parameters.BaseFactor, filterPeriod: parameters.FilterPeriod, decayPeriod: parameters.DecayPeriod,
		reductionFactor: parameters.ReductionFactor, protocolShare: parameters.ProtocolShare, baseFeePower: parameters.BaseFeePower,
		functionType: parameters.FunctionType, collectFeeMode: parameters.CollectFeeMode,
		legacyLO:        parameters.LegacyLimitOrders,
		variableControl: parameters.VariableControl, maxVolatility: parameters.MaxVolatility,
		volatilityAcc: variable.VolatilityAccumulator, volatilityRef: variable.VolatilityReference,
		indexReference: variable.IndexReference, lastUpdateTime: variable.LastUpdateTimestamp, bins: cloneBins(bins),
	}
	if err := state.validate(); err != nil {
		return StateUpdate{}, err
	}
	return StateUpdate{
		activeID: activeID, binStep: binStep, feeRate: rate,
		baseFactor: parameters.BaseFactor, filterPeriod: parameters.FilterPeriod, decayPeriod: parameters.DecayPeriod,
		reductionFactor: parameters.ReductionFactor, protocolShare: parameters.ProtocolShare, baseFeePower: parameters.BaseFeePower,
		functionType: parameters.FunctionType, collectFeeMode: parameters.CollectFeeMode,
		legacyLO:        parameters.LegacyLimitOrders,
		variableControl: parameters.VariableControl, maxVolatility: parameters.MaxVolatility,
		volatilityAcc: variable.VolatilityAccumulator, volatilityRef: variable.VolatilityReference,
		indexReference: variable.IndexReference, lastUpdateTime: variable.LastUpdateTimestamp, bins: cloneBins(bins),
	}, nil
}
func (StateUpdate) EventKind() string { return "meteora_dlmm/state/v2" }

type SwapUpdate struct {
	activeID int32
	bins     []Bin
}

func NewSwapUpdate(activeID int32, bins []Bin) (SwapUpdate, error) {
	if len(bins) == 0 {
		return SwapUpdate{}, fmt.Errorf("swap update requires changed bins")
	}
	return SwapUpdate{activeID: activeID, bins: cloneBins(bins)}, nil
}
func (SwapUpdate) EventKind() string { return "meteora_dlmm/swap/v1" }

type LiquidityUpdate struct {
	id     int32
	deltaX *big.Int
	deltaY *big.Int
}

func NewLiquidityUpdate(id int32, deltaX, deltaY *big.Int) (LiquidityUpdate, error) {
	if deltaX == nil || deltaY == nil {
		return LiquidityUpdate{}, fmt.Errorf("liquidity deltas are required")
	}
	return LiquidityUpdate{id: id, deltaX: clone(deltaX), deltaY: clone(deltaY)}, nil
}
func (LiquidityUpdate) EventKind() string { return "meteora_dlmm/liquidity/v1" }

type Snapshot struct {
	schemaVersion   uint16
	activeID        int32
	binStep         uint16
	feeRate         uint64
	fixedFee        bool
	baseFactor      uint16
	filterPeriod    uint16
	decayPeriod     uint16
	reductionFactor uint16
	protocolShare   uint16
	baseFeePower    uint8
	functionType    uint8
	collectFeeMode  uint8
	legacyLO        bool
	variableControl uint32
	maxVolatility   uint32
	volatilityAcc   uint32
	volatilityRef   uint32
	indexReference  int32
	lastUpdateTime  int64
	bins            []Bin
}

func (Snapshot) SnapshotKind() string { return "meteora_dlmm/v2" }
func (s Snapshot) ActiveID() int32    { return s.activeID }
func (s Snapshot) BinStep() uint16    { return s.binStep }
func (s Snapshot) FeeRate() uint64    { return s.feeRate }
func (s Snapshot) FeeBPS() uint16 {
	return uint16((s.feeRate*10_000 + liquiditycurve.FeeRatePrecision - 1) / liquiditycurve.FeeRatePrecision)
}
func (s Snapshot) FeeRateForBin(id int32) uint64 {
	if s.fixedFee || s.variableControl == 0 || s.maxVolatility == 0 {
		return s.feeRate
	}
	return dynamicFeeRate(s.indexReference, id, s.binStep, s.baseFactor, s.baseFeePower, s.variableControl, s.maxVolatility, s.volatilityRef)
}
func (s Snapshot) Bins() []Bin { return cloneBins(s.bins) }

type quoteVariables struct {
	volatilityReference uint32
	indexReference      int32
}

func (s Snapshot) variablesAt(timestamp int64) (quoteVariables, error) {
	variables := quoteVariables{volatilityReference: s.volatilityRef, indexReference: s.indexReference}
	if s.fixedFee {
		return variables, nil
	}
	elapsed := timestamp - s.lastUpdateTime
	if elapsed < 0 {
		return quoteVariables{}, fmt.Errorf("quote timestamp predates Meteora state")
	}
	if elapsed < int64(s.filterPeriod) {
		return variables, nil
	}
	variables.indexReference = s.activeID
	if elapsed < int64(s.decayPeriod) {
		variables.volatilityReference = uint32(uint64(s.volatilityAcc) * uint64(s.reductionFactor) / 10_000)
	} else {
		variables.volatilityReference = 0
	}
	return variables, nil
}

func (s Snapshot) feeRateAtBin(id int32, variables quoteVariables) uint64 {
	if s.fixedFee || s.variableControl == 0 || s.maxVolatility == 0 {
		return s.feeRate
	}
	return dynamicFeeRate(variables.indexReference, id, s.binStep, s.baseFactor, s.baseFeePower, s.variableControl, s.maxVolatility, variables.volatilityReference)
}

func (s Snapshot) feeOnInput(swapForY bool) bool {
	switch s.collectFeeMode {
	case 1: // CollectFeeMode::OnlyY
		return !swapForY
	default: // unknown modes preserve the protocol's InputOnly fallback
		return true
	}
}

func (s Snapshot) supportsLimitOrders() bool {
	return s.functionType == 2 || s.functionType == 0 && s.legacyLO
}

func (s Snapshot) validate() error {
	if s.schemaVersion != snapshotSchemaVersion || s.binStep == 0 || s.feeRate >= liquiditycurve.FeeRatePrecision || len(s.bins) == 0 || !s.fixedFee && s.maxVolatility > 0 && s.variableControl > 0 && s.baseFactor == 0 {
		return fmt.Errorf("invalid Meteora DLMM state")
	}
	previous := int32(-1 << 31)
	for _, bin := range s.bins {
		if bin.reserveX == nil || bin.reserveY == nil || bin.priceX64 == nil || bin.openOrderAmount == nil || bin.processedOrderRemainingAmount == nil ||
			bin.id <= previous || bin.reserveX.Sign() < 0 || bin.reserveY.Sign() < 0 || bin.openOrderAmount.Sign() < 0 || bin.processedOrderRemainingAmount.Sign() < 0 || bin.priceX64.Sign() <= 0 ||
			bin.reserveX.Sign() == 0 && bin.reserveY.Sign() == 0 && bin.openOrderAmount.Sign() == 0 && bin.processedOrderRemainingAmount.Sign() == 0 {
			return fmt.Errorf("invalid or unsorted Meteora bin %d", bin.id)
		}
		previous = bin.id
	}
	return nil
}

type Reducer struct{}

func (Reducer) Reduce(ctx context.Context, previous market.SnapshotData, event market.EventData) (market.SnapshotData, [sha256.Size]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, [sha256.Size]byte{}, err
	}
	var next Snapshot
	switch update := event.(type) {
	case StateUpdate:
		next = Snapshot{
			schemaVersion: snapshotSchemaVersion, activeID: update.activeID, binStep: update.binStep, feeRate: update.feeRate, fixedFee: update.fixedFee,
			baseFactor: update.baseFactor, filterPeriod: update.filterPeriod, decayPeriod: update.decayPeriod, reductionFactor: update.reductionFactor,
			protocolShare: update.protocolShare, baseFeePower: update.baseFeePower, functionType: update.functionType, collectFeeMode: update.collectFeeMode,
			legacyLO:        update.legacyLO,
			variableControl: update.variableControl, maxVolatility: update.maxVolatility, volatilityAcc: update.volatilityAcc,
			volatilityRef: update.volatilityRef, indexReference: update.indexReference, lastUpdateTime: update.lastUpdateTime, bins: cloneBins(update.bins),
		}
	case SwapUpdate:
		current, err := require(previous)
		if err != nil {
			return nil, [sha256.Size]byte{}, err
		}
		next = current
		next.activeID = update.activeID
		next.bins = mergeBins(current.bins, update.bins)
	case LiquidityUpdate:
		current, err := require(previous)
		if err != nil {
			return nil, [sha256.Size]byte{}, err
		}
		next = current
		found := false
		for i := range next.bins {
			if next.bins[i].id != update.id {
				continue
			}
			next.bins[i].reserveX.Add(next.bins[i].reserveX, update.deltaX)
			next.bins[i].reserveY.Add(next.bins[i].reserveY, update.deltaY)
			found = true
		}
		if !found {
			return nil, [sha256.Size]byte{}, fmt.Errorf("meteora liquidity update references unknown bin %d", update.id)
		}
	default:
		return nil, [sha256.Size]byte{}, fmt.Errorf("unsupported meteora DLMM event %T", event)
	}
	if err := next.validate(); err != nil {
		return nil, [sha256.Size]byte{}, err
	}
	return next, hashState(next), nil
}

func require(previous market.SnapshotData) (Snapshot, error) {
	state, ok := previous.(Snapshot)
	if !ok || state.schemaVersion != snapshotSchemaVersion {
		return Snapshot{}, fmt.Errorf("meteora update requires a compatible snapshot")
	}
	state.bins = cloneBins(state.bins)
	return state, nil
}
func mergeBins(base, updates []Bin) []Bin {
	byID := make(map[int32]Bin, len(base)+len(updates))
	for _, bin := range base {
		byID[bin.id] = bin
	}
	for _, bin := range updates {
		byID[bin.id] = bin
	}
	result := make([]Bin, 0, len(byID))
	for _, bin := range byID {
		result = append(result, bin)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].id < result[j].id })
	return cloneBins(result)
}
func cloneBins(input []Bin) []Bin {
	result := make([]Bin, len(input))
	for i, bin := range input {
		result[i] = Bin{
			id: bin.id, reserveX: clone(bin.reserveX), reserveY: clone(bin.reserveY), priceX64: clone(bin.priceX64),
			openOrderAmount: clone(bin.openOrderAmount), processedOrderRemainingAmount: clone(bin.processedOrderRemainingAmount), limitOrderAskSide: bin.limitOrderAskSide,
		}
	}
	return result
}
func clone(value *big.Int) *big.Int {
	if value == nil {
		return nil
	}
	return new(big.Int).Set(value)
}
func hashState(state Snapshot) [sha256.Size]byte {
	var builder strings.Builder
	fmt.Fprintf(&builder, "%d|%d|%d|%d|%t|%d|%d|%d|%d|%d|%d|%d|%d|%t|%d|%d|%d|%d|%d|%d",
		state.schemaVersion, state.activeID, state.binStep, state.feeRate, state.fixedFee,
		state.baseFactor, state.filterPeriod, state.decayPeriod, state.reductionFactor, state.protocolShare,
		state.baseFeePower, state.functionType, state.collectFeeMode, state.legacyLO, state.variableControl, state.maxVolatility,
		state.volatilityAcc, state.volatilityRef, state.indexReference, state.lastUpdateTime)
	for _, bin := range state.bins {
		fmt.Fprintf(&builder, "|%d:%s:%s:%s:%s:%s:%t", bin.id, bin.reserveX, bin.reserveY, bin.priceX64, bin.openOrderAmount, bin.processedOrderRemainingAmount, bin.limitOrderAskSide)
	}
	return sha256.Sum256([]byte(builder.String()))
}

func dynamicFeeRate(activeID, binID int32, binStep, baseFactor uint16, baseFeePower uint8, variableControl, maxVolatility, volatilityRef uint32) uint64 {
	delta := uint64(absInt64(int64(activeID) - int64(binID)))
	volatility := uint64(volatilityRef) + delta*10_000
	if volatility > uint64(maxVolatility) {
		volatility = uint64(maxVolatility)
	}
	baseValue := new(big.Int).SetUint64(uint64(baseFactor))
	baseValue.Mul(baseValue, new(big.Int).SetUint64(uint64(binStep)))
	baseValue.Mul(baseValue, big.NewInt(10))
	for i := uint8(0); i < baseFeePower; i++ {
		baseValue.Mul(baseValue, big.NewInt(10))
	}
	variableNumerator := new(big.Int).SetUint64(uint64(variableControl))
	variableNumerator.Mul(variableNumerator, new(big.Int).SetUint64(uint64(binStep)))
	variableNumerator.Mul(variableNumerator, new(big.Int).SetUint64(uint64(binStep)))
	variableNumerator.Mul(variableNumerator, new(big.Int).SetUint64(volatility))
	variableNumerator.Mul(variableNumerator, new(big.Int).SetUint64(volatility))
	variableValue := ceilDiv(variableNumerator, new(big.Int).SetUint64(100_000_000_000))
	total := new(big.Int).Add(baseValue, variableValue)
	if total.Cmp(big.NewInt(100_000_000)) > 0 {
		return 100_000_000
	}
	return total.Uint64()
}

func absInt64(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}

func ceilDiv(numerator, denominator *big.Int) *big.Int {
	if numerator.Sign() == 0 {
		return new(big.Int)
	}
	adjusted := new(big.Int).Sub(new(big.Int).Set(denominator), big.NewInt(1))
	adjusted.Add(adjusted, numerator)
	return adjusted.Quo(adjusted, denominator)
}

var _ market.EventData = StateUpdate{}
var _ market.EventData = SwapUpdate{}
var _ market.EventData = LiquidityUpdate{}
var _ market.SnapshotData = Snapshot{}
