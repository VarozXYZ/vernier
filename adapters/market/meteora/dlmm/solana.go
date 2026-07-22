package dlmm

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"sort"

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
)

type AccountReader interface {
	ReadAccount(context.Context, string) (solana.Account, error)
	ReadMultipleAccounts(context.Context, []string) ([]solana.Account, error)
}

type Decoder struct{ Pool string }

func NewDecoder(pool string) (*Decoder, error) {
	if pool == "" {
		return nil, fmt.Errorf("dlmm pool account is required")
	}
	return &Decoder{Pool: pool}, nil
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
	data, err := d.Bootstrap(ctx, network, 0)
	if err != nil {
		return nil, err
	}
	return []market.EventData{data}, nil
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
	binStep := binary.LittleEndian.Uint16(data[80:82])
	baseFactor := binary.LittleEndian.Uint16(data[8:10])
	variableControl := binary.LittleEndian.Uint32(data[16:20])
	volatility := binary.LittleEndian.Uint32(data[40:44])
	basePower := data[34]
	feeRate := uint64(baseFactor) * uint64(binStep) * 10
	for i := byte(0); i < basePower; i++ {
		feeRate *= 10
	}
	volatilityBin := uint64(volatility) * uint64(binStep)
	variable := uint64(0)
	if variableControl > 0 {
		variable = (uint64(variableControl)*volatilityBin*volatilityBin + 99_999_999_999) / 100_000_000_000
	}
	totalRate := feeRate + variable
	if totalRate > 100_000_000 {
		totalRate = 100_000_000
	}
	feeBPS := uint16((totalRate*10_000 + 999_999_999) / 1_000_000_000)
	if feeBPS >= 10_000 {
		return StateUpdate{}, fmt.Errorf("DLMM fee is outside quote model")
	}

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
	for i, account := range accounts {
		if len(account.Data) == 0 {
			continue
		}
		parsed, err := parseBinArray(account.Data, ordered[i], pool)
		if err != nil {
			return StateUpdate{}, err
		}
		bins = append(bins, parsed...)
	}
	return NewStateUpdate(activeID, binStep, feeBPS, bins)
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
		if x.Sign() == 0 && y.Sign() == 0 {
			continue
		}
		binID := int32(index*binArrayCount + int64((offset-56)/binSize))
		bin, err := NewBin(binID, x, y)
		if err != nil {
			return nil, err
		}
		result = append(result, bin)
	}
	return result, nil
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

var _ solanalogs.Decoder = (*Decoder)(nil)
