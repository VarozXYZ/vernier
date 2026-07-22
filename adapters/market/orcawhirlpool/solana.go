package orcawhirlpool

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"sort"
	"strconv"

	"github.com/VarozXYZ/vernier/adapters/chain/solana"
	"github.com/VarozXYZ/vernier/adapters/feed/solanalogs"
	"github.com/VarozXYZ/vernier/domain/market"
)

const whirlpoolDiscriminator = "\x3f\x95\xd1\x0c\xe1\x80\x63\x09"
const whirlpoolProgram = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"
const fixedTickArraySize = 88
const fixedTickSize = 113

// AccountReader is the optional read capability required by a protocol
// decoder. The feed itself remains limited to slots and logs.
type AccountReader interface {
	ReadAccount(context.Context, string) (solana.Account, error)
}

type MultipleAccountReader interface {
	ReadMultipleAccounts(context.Context, []string) ([]solana.Account, error)
}

type Decoder struct{ Pool string }

func NewDecoder(pool string) (*Decoder, error) {
	if pool == "" {
		return nil, fmt.Errorf("whirlpool pool account is required")
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
	if len(data) < 653 || string(data[:8]) != whirlpoolDiscriminator {
		return StateUpdate{}, fmt.Errorf("invalid whirlpool account data")
	}
	readU16 := func(offset int) uint16 { return binary.LittleEndian.Uint16(data[offset : offset+2]) }
	readI32 := func(offset int) int32 { return int32(binary.LittleEndian.Uint32(data[offset : offset+4])) }
	readU128 := func(offset int) *big.Int { return littleEndianInt(data[offset : offset+16]) }
	feeRate := uint32(readU16(45))
	tickSpacing := int32(readU16(41))
	currentTick := readI32(81)
	ticks, minTick, maxTick, err := d.readTicks(ctx, reader, currentTick, tickSpacing)
	if err != nil {
		return StateUpdate{}, err
	}
	return NewCoveredStateUpdateWithFeeRate(readU128(65), currentTick, readU128(49), feeRate, tickSpacing, ticks, false, minTick, maxTick)
}

func (d *Decoder) readTicks(ctx context.Context, reader AccountReader, currentTick, spacing int32) ([]Tick, int32, int32, error) {
	readerMany, ok := reader.(MultipleAccountReader)
	if !ok {
		// A reduced test reader may expose only the pool account. It cannot
		// establish tick coverage, so keep the state bounded to the current tick.
		return nil, currentTick, currentTick, nil
	}
	if spacing <= 0 {
		return nil, 0, 0, fmt.Errorf("invalid Whirlpool tick spacing")
	}
	arrayWidth := int64(fixedTickArraySize) * int64(spacing)
	start := int32(floorDiv(int64(currentTick), arrayWidth) * arrayWidth)
	pool, err := solana.DecodePublicKey(d.Pool)
	if err != nil {
		return nil, 0, 0, err
	}
	program, err := solana.DecodePublicKey(whirlpoolProgram)
	if err != nil {
		return nil, 0, 0, err
	}
	starts := []int32{start - int32(arrayWidth), start, start + int32(arrayWidth), start - 2*int32(arrayWidth), start + 2*int32(arrayWidth)}
	addresses := make([]string, len(starts))
	for i, value := range starts {
		address, _, err := solana.FindProgramAddress([][]byte{[]byte("tick_array"), pool[:], []byte(strconv.FormatInt(int64(value), 10))}, program)
		if err != nil {
			return nil, 0, 0, err
		}
		addresses[i] = solana.EncodePublicKey(address)
	}
	accounts, err := readerMany.ReadMultipleAccounts(ctx, addresses)
	if err != nil {
		return nil, 0, 0, err
	}
	var ticks []Tick
	min, max := int32(1<<31-1), int32(-1<<31)
	for _, account := range accounts {
		parsed, startTick, ok, err := parseFixedTickArray(account.Data, spacing)
		if err != nil {
			return nil, 0, 0, err
		}
		if !ok {
			continue
		}
		ticks = append(ticks, parsed...)
		arrayMax := startTick + int32((fixedTickArraySize-1))*spacing
		if startTick < min {
			min = startTick
		}
		if arrayMax > max {
			max = arrayMax
		}
	}
	if min > max {
		return nil, currentTick, currentTick, nil
	}
	if currentTick < min || currentTick > max {
		return nil, currentTick, currentTick, nil
	}
	sort.Slice(ticks, func(i, j int) bool { return ticks[i].index < ticks[j].index })
	return ticks, min, max, nil
}

func parseFixedTickArray(data []byte, spacing int32) ([]Tick, int32, bool, error) {
	if len(data) == 0 {
		return nil, 0, false, nil
	}
	minimum := 8 + 36 + fixedTickSize*fixedTickArraySize
	if len(data) < minimum {
		return nil, 0, false, nil // dynamic arrays are not decoded by this adapter yet
	}
	start := int32(binary.LittleEndian.Uint32(data[8:12]))
	result := make([]Tick, 0)
	for i := 0; i < fixedTickArraySize; i++ {
		offset := 12 + i*fixedTickSize
		if data[offset] == 0 {
			continue
		}
		index := start + int32(i)*spacing
		liquidityNet := signedLittleEndian(data[offset+1 : offset+17])
		tick, err := NewTick(index, liquidityNet)
		if err != nil {
			return nil, 0, false, err
		}
		result = append(result, tick)
	}
	return result, start, true, nil
}

func signedLittleEndian(value []byte) *big.Int {
	copyValue := append([]byte(nil), value...)
	negative := copyValue[len(copyValue)-1]&0x80 != 0
	for left, right := 0, len(copyValue)-1; left < right; left, right = left+1, right-1 {
		copyValue[left], copyValue[right] = copyValue[right], copyValue[left]
	}
	result := new(big.Int).SetBytes(copyValue)
	if negative {
		result.Sub(result, new(big.Int).Lsh(big.NewInt(1), uint(len(value)*8)))
	}
	return result
}

func floorDiv(value, divisor int64) int64 {
	quotient, remainder := value/divisor, value%divisor
	if remainder < 0 {
		quotient--
	}
	return quotient
}

func littleEndianInt(value []byte) *big.Int {
	copyValue := append([]byte(nil), value...)
	for left, right := 0, len(copyValue)-1; left < right; left, right = left+1, right-1 {
		copyValue[left], copyValue[right] = copyValue[right], copyValue[left]
	}
	return new(big.Int).SetBytes(copyValue)
}

var _ solanalogs.Decoder = (*Decoder)(nil)
