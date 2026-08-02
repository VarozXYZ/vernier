package dlmm

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"sync"

	"github.com/VarozXYZ/vernier/adapters/chain/solana"
	"github.com/VarozXYZ/vernier/adapters/feed/solanalogs"
	"github.com/VarozXYZ/vernier/domain/market"
)

var lbPairDiscriminator = [8]byte{33, 11, 49, 98, 181, 101, 177, 13}
var binArrayDiscriminator = [8]byte{92, 142, 92, 220, 5, 148, 70, 181}

const (
	meteoraProgram = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo"
	binArraySeed   = "bin_array"
	binArrayCount  = 70
	binSize        = 144
	binArraySize   = 10136
)

type AccountReader interface {
	ReadAccount(context.Context, string) (solana.Account, error)
	ReadMultipleAccounts(context.Context, []string) ([]solana.Account, error)
}

type Decoder struct {
	Pool     string
	mu       sync.RWMutex
	poolData []byte
	arrays   map[int64][]Bin
}

func NewDecoder(pool string) (*Decoder, error) {
	if pool == "" {
		return nil, fmt.Errorf("dlmm pool account is required")
	}
	return &Decoder{Pool: pool, arrays: make(map[int64][]Bin)}, nil
}

func (d *Decoder) Bootstrap(ctx context.Context, network solanalogs.Network, _ uint64) (market.EventData, error) {
	reader, ok := network.(AccountReader)
	if !ok {
		return nil, fmt.Errorf("solana network does not expose account reads")
	}
	account, err := reader.ReadAccount(ctx, d.Pool)
	if err != nil {
		return nil, err
	}
	return d.decode(ctx, reader, account.Data)
}

func (d *Decoder) Decode(ctx context.Context, network solanalogs.Network, _ solana.LogNotification) ([]market.EventData, error) {
	return nil, fmt.Errorf("DLMM log updates require account subscriptions")
}

// IsEconomicLog distinguishes pool activity that warrants a new Research
// evaluation from administrative instructions. Account subscriptions remain
// the source of truth for the resulting DLMM state.
func (*Decoder) IsEconomicLog(notification solana.LogNotification) bool {
	for _, line := range notification.Logs {
		normalized := strings.ToLower(strings.ReplaceAll(line, " ", ""))
		if !strings.Contains(normalized, "instruction:") {
			continue
		}
		instruction := strings.SplitN(normalized, "instruction:", 2)[1]
		liquidityChange := strings.Contains(instruction, "liquidity") &&
			(strings.HasPrefix(instruction, "add") || strings.HasPrefix(instruction, "remove") ||
				strings.HasPrefix(instruction, "increase") || strings.HasPrefix(instruction, "decrease"))
		if strings.Contains(instruction, "swap") || liquidityChange {
			return true
		}
	}
	return false
}

func (d *Decoder) AccountSubscriptions() []string { return []string{d.Pool} }

func (d *Decoder) ProgramSubscriptions() []solana.ProgramSubscriptionRequest {
	size := uint64(binArraySize)
	return []solana.ProgramSubscriptionRequest{{
		Program: meteoraProgram,
		Filters: []solana.ProgramFilter{
			{DataSize: &size},
			{Memcmp: &solana.ProgramMemcmp{Offset: 24, Bytes: d.Pool}},
		},
	}}
}

func (d *Decoder) DecodeAccountBatch(ctx context.Context, changes []solanalogs.AccountChange) ([]market.EventData, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, change := range changes {
		if change.Account == d.Pool && !change.Program {
			if len(change.Value.Data) < 904 || !equalBytes(change.Value.Data[:8], lbPairDiscriminator[:]) {
				return nil, fmt.Errorf("invalid DLMM lb pair account data")
			}
			d.poolData = append(d.poolData[:0], change.Value.Data...)
			continue
		}
		if !change.Program || len(change.Value.Data) == 0 {
			continue
		}
		if len(change.Value.Data) < binArraySize {
			return nil, fmt.Errorf("invalid DLMM bin array account data")
		}
		index := int64(binary.LittleEndian.Uint64(change.Value.Data[8:16]))
		pool, err := solana.DecodePublicKey(d.Pool)
		if err != nil {
			return nil, err
		}
		bins, err := parseBinArray(change.Value.Data, index, pool)
		if err != nil {
			return nil, err
		}
		d.arrays[index] = bins
	}
	if len(d.poolData) == 0 {
		return nil, nil
	}
	update, err := stateFromData(d.poolData, flattenBinArrays(d.arrays))
	if err != nil {
		return nil, err
	}
	return []market.EventData{update}, nil
}

func (d *Decoder) decode(ctx context.Context, reader AccountReader, data []byte) (StateUpdate, error) {
	if len(data) < 904 || !equalBytes(data[:8], lbPairDiscriminator[:]) {
		return StateUpdate{}, fmt.Errorf("invalid DLMM lb pair account data")
	}
	pool, err := solana.DecodePublicKey(d.Pool)
	if err != nil {
		return StateUpdate{}, err
	}
	activeID := int32(binary.LittleEndian.Uint32(data[76:80]))

	programID, err := solana.DecodePublicKey(meteoraProgram)
	if err != nil {
		return StateUpdate{}, err
	}
	indexes := make(map[int64]struct{})
	for word := 0; word < 16; word++ {
		bits := binary.LittleEndian.Uint64(data[584+word*8 : 592+word*8])
		for bit := 0; bit < 64; bit++ {
			if bits&(uint64(1)<<uint(bit)) != 0 {
				indexes[int64(word*64+bit)-512] = struct{}{}
			}
		}
	}
	indexes[floorDiv(int64(activeID), binArrayCount)] = struct{}{}
	ordered := make([]int64, 0, len(indexes))
	for index := range indexes {
		ordered = append(ordered, index)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	addresses := make([]string, 0, len(ordered))
	for _, index := range ordered {
		address, _, err := solana.FindProgramAddress([][]byte{[]byte(binArraySeed), pool[:], int64Bytes(index)}, programID)
		if err != nil {
			return StateUpdate{}, err
		}
		addresses = append(addresses, solana.EncodePublicKey(address))
	}
	accounts, err := reader.ReadMultipleAccounts(ctx, addresses)
	if err != nil {
		return StateUpdate{}, err
	}
	if len(accounts) != len(ordered) {
		return StateUpdate{}, fmt.Errorf("DLMM bin array response length mismatch")
	}
	bins := make([]Bin, 0, len(accounts)*binArrayCount)
	arrays := make(map[int64][]Bin, len(accounts))
	for i, account := range accounts {
		if len(account.Data) == 0 {
			continue
		}
		parsed, err := parseBinArray(account.Data, ordered[i], pool)
		if err != nil {
			return StateUpdate{}, err
		}
		arrays[ordered[i]] = parsed
		bins = append(bins, parsed...)
	}
	update, err := stateFromData(data, bins)
	if err != nil {
		return StateUpdate{}, err
	}
	d.mu.Lock()
	d.poolData = append([]byte(nil), data...)
	d.arrays = arrays
	d.mu.Unlock()
	return update, nil
}

func stateFromData(data []byte, bins []Bin) (StateUpdate, error) {
	if len(data) < 904 || !equalBytes(data[:8], lbPairDiscriminator[:]) {
		return StateUpdate{}, fmt.Errorf("invalid DLMM lb pair account data")
	}
	return NewProtocolStateUpdate(
		int32(binary.LittleEndian.Uint32(data[76:80])),
		binary.LittleEndian.Uint16(data[80:82]),
		StaticParameters{
			BaseFactor: binary.LittleEndian.Uint16(data[8:10]), FilterPeriod: binary.LittleEndian.Uint16(data[10:12]),
			DecayPeriod: binary.LittleEndian.Uint16(data[12:14]), ReductionFactor: binary.LittleEndian.Uint16(data[14:16]),
			VariableControl: binary.LittleEndian.Uint32(data[16:20]), MaxVolatility: binary.LittleEndian.Uint32(data[20:24]),
			ProtocolShare: binary.LittleEndian.Uint16(data[32:34]), BaseFeePower: data[34], FunctionType: data[35], CollectFeeMode: data[36],
			LegacyLimitOrders: allZero(data[264:296]) && allZero(data[408:440]),
		},
		VariableParameters{
			VolatilityAccumulator: binary.LittleEndian.Uint32(data[40:44]), VolatilityReference: binary.LittleEndian.Uint32(data[44:48]),
			IndexReference: int32(binary.LittleEndian.Uint32(data[48:52])), LastUpdateTimestamp: int64(binary.LittleEndian.Uint64(data[56:64])),
		},
		bins,
	)
}

func flattenBinArrays(arrays map[int64][]Bin) []Bin {
	result := make([]Bin, 0, len(arrays)*binArrayCount)
	for _, bins := range arrays {
		result = append(result, bins...)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].id < result[j].id })
	return result
}

func parseBinArray(data []byte, expectedIndex int64, pool [32]byte) ([]Bin, error) {
	if len(data) < 10136 || !equalBytes(data[:8], binArrayDiscriminator[:]) {
		return nil, fmt.Errorf("invalid DLMM bin array account data")
	}
	index := int64(binary.LittleEndian.Uint64(data[8:16]))
	if index != expectedIndex || !equalBytes(data[24:56], pool[:]) {
		return nil, fmt.Errorf("DLMM bin array identity mismatch")
	}
	result := make([]Bin, 0, binArrayCount)
	for offset := 56; offset+binSize <= len(data) && len(result) < binArrayCount; offset += binSize {
		x := new(big.Int).SetUint64(binary.LittleEndian.Uint64(data[offset : offset+8]))
		y := new(big.Int).SetUint64(binary.LittleEndian.Uint64(data[offset+8 : offset+16]))
		price := littleEndianInt(data[offset+16 : offset+32])
		openOrder := new(big.Int).SetUint64(binary.LittleEndian.Uint64(data[offset+112 : offset+120]))
		processedOrderRemaining := new(big.Int).SetUint64(binary.LittleEndian.Uint64(data[offset+128 : offset+136]))
		if x.Sign() == 0 && y.Sign() == 0 && openOrder.Sign() == 0 && processedOrderRemaining.Sign() == 0 {
			continue
		}
		binID := int32(index*binArrayCount + int64((offset-56)/binSize))
		bin, err := NewBinWithProtocolLiquidity(binID, x, y, price, openOrder, processedOrderRemaining, data[offset+140] != 0)
		if err != nil {
			return nil, err
		}
		result = append(result, bin)
	}
	return result, nil
}

func littleEndianInt(value []byte) *big.Int {
	copyValue := append([]byte(nil), value...)
	for left, right := 0, len(copyValue)-1; left < right; left, right = left+1, right-1 {
		copyValue[left], copyValue[right] = copyValue[right], copyValue[left]
	}
	return new(big.Int).SetBytes(copyValue)
}

func floorDiv(value int64, divisor int64) int64 {
	quotient := value / divisor
	remainder := value % divisor
	if remainder < 0 {
		quotient--
	}
	return quotient
}

func int64Bytes(value int64) []byte {
	result := make([]byte, 8)
	binary.LittleEndian.PutUint64(result, uint64(value))
	return result
}

func equalBytes(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func allZero(value []byte) bool {
	for _, item := range value {
		if item != 0 {
			return false
		}
	}
	return true
}

var _ solanalogs.Decoder = (*Decoder)(nil)
var _ solanalogs.EconomicLogMatcher = (*Decoder)(nil)
