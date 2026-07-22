package orcawhirlpool

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"sync"

	"github.com/VarozXYZ/vernier/adapters/chain/solana"
	"github.com/VarozXYZ/vernier/adapters/feed/solanalogs"
	"github.com/VarozXYZ/vernier/domain/market"
)

const whirlpoolDiscriminator = "\x3f\x95\xd1\x0c\xe1\x80\x63\x09"
const whirlpoolProgram = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"
const fixedTickArraySize = 88
const fixedTickSize = 113
const fixedTickArrayAccountSize = 8 + 4 + fixedTickArraySize*fixedTickSize + 32
const dynamicTickArrayAccountSize = 8 + 4 + 32 + 16 + fixedTickArraySize*fixedTickSize
const fixedTickArrayDiscriminator = "Ea\xbd\xbe\x6e\x07B\xbb"
const dynamicTickArrayDiscriminator = "\x11\xd8\xf6\x8e\xe1\xc7\xda8"

// AccountReader is the optional read capability required by a protocol
// decoder. The feed itself remains limited to slots and logs.
type AccountReader interface {
	ReadAccount(context.Context, string) (solana.Account, error)
}

type MultipleAccountReader interface {
	ReadMultipleAccounts(context.Context, []string) ([]solana.Account, error)
}

type ProgramAccountReader interface {
	ReadProgramAccounts(context.Context, string, []solana.ProgramFilter) ([]solana.ProgramAccount, error)
}

type Decoder struct {
	Pool          string
	mu            sync.RWMutex
	tickSpacing   int32
	subscriptions []string
}

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

func (d *Decoder) AccountSubscriptions() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return append([]string(nil), d.subscriptions...)
}

func (d *Decoder) ProgramSubscriptions() []solana.ProgramSubscriptionRequest {
	pool, err := solana.DecodePublicKey(d.Pool)
	if err != nil {
		return nil
	}
	poolBytes := solana.EncodePublicKey(pool)
	fixedSize := uint64(fixedTickArrayAccountSize)
	dynamicSize := uint64(dynamicTickArrayAccountSize)
	return []solana.ProgramSubscriptionRequest{
		{Program: whirlpoolProgram, Filters: []solana.ProgramFilter{{DataSize: &fixedSize}, {Memcmp: &solana.ProgramMemcmp{Offset: fixedTickArrayAccountSize - 32, Bytes: poolBytes}}}},
		{Program: whirlpoolProgram, Filters: []solana.ProgramFilter{{DataSize: &dynamicSize}, {Memcmp: &solana.ProgramMemcmp{Offset: 12, Bytes: poolBytes}}}},
	}
}

func (d *Decoder) DecodeProgram(ctx context.Context, notification solana.ProgramNotification) ([]market.EventData, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.RLock()
	spacing := d.tickSpacing
	pool := d.Pool
	d.mu.RUnlock()
	if spacing <= 0 {
		return nil, fmt.Errorf("whirlpool tick spacing is unavailable")
	}
	poolKey, err := solana.DecodePublicKey(pool)
	if err != nil {
		return nil, err
	}
	ticks, start, ok, err := parseTickArray(notification.Value.Data, spacing, poolKey[:])
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("unsupported whirlpool tick array account")
	}
	update, err := newTickArrayUpdate(start, spacing, ticks)
	if err != nil {
		return nil, err
	}
	return []market.EventData{update}, nil
}

func (d *Decoder) Decode(ctx context.Context, _ solanalogs.Network, _ solana.LogNotification) ([]market.EventData, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("whirlpool log feed cannot update state without account data")
}

func (d *Decoder) DecodeAccount(ctx context.Context, notification solana.AccountNotification) ([]market.EventData, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if notification.Account == d.Pool {
		update, err := d.decodePool(notification.Value.Data)
		if err != nil {
			return nil, err
		}
		return []market.EventData{update}, nil
	}
	d.mu.RLock()
	spacing := d.tickSpacing
	d.mu.RUnlock()
	if spacing <= 0 {
		return nil, fmt.Errorf("whirlpool tick spacing is unavailable")
	}
	ticks, start, ok, err := parseFixedTickArray(notification.Value.Data, spacing)
	if !ok {
		pool, decodeErr := solana.DecodePublicKey(d.Pool)
		if decodeErr != nil {
			return nil, decodeErr
		}
		ticks, start, ok, err = parseTickArray(notification.Value.Data, spacing, pool[:])
	}
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("unsupported whirlpool tick array account")
	}
	update, err := newTickArrayUpdate(start, spacing, ticks)
	if err != nil {
		return nil, err
	}
	return []market.EventData{update}, nil
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
	ticks, minTick, maxTick, subscriptions, fullCoverage, err := d.readTicks(ctx, reader, currentTick, tickSpacing)
	if err != nil {
		return StateUpdate{}, err
	}
	if _, programReader := reader.(ProgramAccountReader); programReader {
		subscriptions = nil
	}
	d.mu.Lock()
	d.tickSpacing = tickSpacing
	d.subscriptions = append([]string{d.Pool}, subscriptions...)
	d.mu.Unlock()
	return NewCoveredStateUpdateWithFeeRate(readU128(65), currentTick, readU128(49), feeRate, tickSpacing, ticks, fullCoverage, minTick, maxTick)
}

func (d *Decoder) readTicks(ctx context.Context, reader AccountReader, currentTick, spacing int32) ([]Tick, int32, int32, []string, bool, error) {
	if programReader, ok := reader.(ProgramAccountReader); ok {
		return d.readProgramTicks(ctx, programReader, currentTick, spacing)
	}
	readerMany, ok := reader.(MultipleAccountReader)
	if !ok {
		// A reduced test reader may expose only the pool account. It cannot
		// establish tick coverage, so keep the state bounded to the current tick.
		return nil, currentTick, currentTick, nil, false, nil
	}
	if spacing <= 0 {
		return nil, 0, 0, nil, false, fmt.Errorf("invalid whirlpool tick spacing")
	}
	arrayWidth := int64(fixedTickArraySize) * int64(spacing)
	start := int32(floorDiv(int64(currentTick), arrayWidth) * arrayWidth)
	pool, err := solana.DecodePublicKey(d.Pool)
	if err != nil {
		return nil, 0, 0, nil, false, err
	}
	program, err := solana.DecodePublicKey(whirlpoolProgram)
	if err != nil {
		return nil, 0, 0, nil, false, err
	}
	starts := []int32{start - int32(arrayWidth), start, start + int32(arrayWidth), start - 2*int32(arrayWidth), start + 2*int32(arrayWidth)}
	addresses := make([]string, len(starts))
	for i, value := range starts {
		address, _, err := solana.FindProgramAddress([][]byte{[]byte("tick_array"), pool[:], []byte(strconv.FormatInt(int64(value), 10))}, program)
		if err != nil {
			return nil, 0, 0, nil, false, err
		}
		addresses[i] = solana.EncodePublicKey(address)
	}
	accounts, err := readerMany.ReadMultipleAccounts(ctx, addresses)
	if err != nil {
		return nil, 0, 0, nil, false, err
	}
	var ticks []Tick
	min, max := int32(1<<31-1), int32(-1<<31)
	for _, account := range accounts {
		parsed, startTick, ok, err := parseFixedTickArray(account.Data, spacing)
		if err != nil {
			return nil, 0, 0, nil, false, err
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
		return nil, currentTick, currentTick, addresses, false, nil
	}
	if currentTick < min || currentTick > max {
		return nil, currentTick, currentTick, addresses, false, nil
	}
	sort.Slice(ticks, func(i, j int) bool { return ticks[i].index < ticks[j].index })
	return ticks, min, max, addresses, false, nil
}

func (d *Decoder) readProgramTicks(ctx context.Context, reader ProgramAccountReader, currentTick, spacing int32) ([]Tick, int32, int32, []string, bool, error) {
	if spacing <= 0 {
		return nil, 0, 0, nil, false, fmt.Errorf("invalid whirlpool tick spacing")
	}
	pool, err := solana.DecodePublicKey(d.Pool)
	if err != nil {
		return nil, 0, 0, nil, false, err
	}
	fixedSize := uint64(fixedTickArrayAccountSize)
	dynamicSize := uint64(dynamicTickArrayAccountSize)
	queries := []struct {
		size   *uint64
		offset uint64
	}{
		{size: &fixedSize, offset: fixedTickArrayAccountSize - 32},
		{size: &dynamicSize, offset: 12},
	}
	var ticks []Tick
	addresses := make([]string, 0)
	min, max := int32(1<<31-1), int32(-1<<31)
	for _, query := range queries {
		accounts, readErr := reader.ReadProgramAccounts(ctx, whirlpoolProgram, []solana.ProgramFilter{{DataSize: query.size}, {Memcmp: &solana.ProgramMemcmp{Offset: query.offset, Bytes: solana.EncodePublicKey(pool)}}})
		if readErr != nil {
			return nil, 0, 0, nil, false, readErr
		}
		for _, account := range accounts {
			parsed, startTick, ok, parseErr := parseTickArray(account.Value.Data, spacing, pool[:])
			if parseErr != nil {
				return nil, 0, 0, nil, false, parseErr
			}
			if !ok {
				continue
			}
			ticks = append(ticks, parsed...)
			addresses = append(addresses, account.Account)
			arrayMax := startTick + int32(fixedTickArraySize-1)*spacing
			if startTick < min {
				min = startTick
			}
			if arrayMax > max {
				max = arrayMax
			}
		}
	}
	if min > max || currentTick < min || currentTick > max {
		return nil, currentTick, currentTick, addresses, false, nil
	}
	sort.Slice(ticks, func(i, j int) bool { return ticks[i].index < ticks[j].index })
	return ticks, min, max, addresses, true, nil
}

func (d *Decoder) decodePool(data []byte) (PoolUpdate, error) {
	if len(data) < 653 || string(data[:8]) != whirlpoolDiscriminator {
		return PoolUpdate{}, fmt.Errorf("invalid whirlpool account data")
	}
	feeRate := uint32(binary.LittleEndian.Uint16(data[45:47]))
	tick := int32(binary.LittleEndian.Uint32(data[81:85]))
	return newPoolUpdate(littleEndianInt(data[65:81]), tick, littleEndianInt(data[49:65]), feeRate)
}

func parseTickArray(data []byte, spacing int32, pool []byte) ([]Tick, int32, bool, error) {
	if len(data) >= fixedTickArrayAccountSize && string(data[:8]) == fixedTickArrayDiscriminator {
		if !bytes.Equal(data[fixedTickArrayAccountSize-32:fixedTickArrayAccountSize], pool) {
			return nil, 0, false, nil
		}
		return parseFixedTickArray(data, spacing)
	}
	if len(data) >= dynamicTickArrayAccountSize && string(data[:8]) == dynamicTickArrayDiscriminator {
		if !bytes.Equal(data[12:44], pool) {
			return nil, 0, false, nil
		}
		return parseDynamicTickArray(data, spacing)
	}
	return nil, 0, false, nil
}

func parseFixedTickArray(data []byte, spacing int32) ([]Tick, int32, bool, error) {
	if len(data) == 0 {
		return nil, 0, false, nil
	}
	minimum := 8 + 36 + fixedTickSize*fixedTickArraySize
	if len(data) < minimum || string(data[:8]) != fixedTickArrayDiscriminator {
		return nil, 0, false, nil
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

func parseDynamicTickArray(data []byte, spacing int32) ([]Tick, int32, bool, error) {
	if spacing <= 0 || len(data) < dynamicTickArrayAccountSize || string(data[:8]) != dynamicTickArrayDiscriminator {
		return nil, 0, false, nil
	}
	start := int32(binary.LittleEndian.Uint32(data[8:12]))
	result := make([]Tick, 0)
	for i := 0; i < fixedTickArraySize; i++ {
		offset := 60 + i*fixedTickSize
		switch data[offset] {
		case 0:
			continue
		case 1:
		default:
			return nil, 0, false, fmt.Errorf("invalid dynamic whirlpool tick state")
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
var _ solanalogs.AccountDecoder = (*Decoder)(nil)
var _ solanalogs.ProgramDecoder = (*Decoder)(nil)
