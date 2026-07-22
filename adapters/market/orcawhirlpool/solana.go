package orcawhirlpool

import (
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

// AccountReader is the optional read capability required by a protocol
// decoder. The feed itself remains limited to slots and logs.
type AccountReader interface {
	ReadAccount(context.Context, string) (solana.Account, error)
}

type MultipleAccountReader interface {
	ReadMultipleAccounts(context.Context, []string) ([]solana.Account, error)
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
	ticks, minTick, maxTick, subscriptions, err := d.readTicks(ctx, reader, currentTick, tickSpacing)
	if err != nil {
		return StateUpdate{}, err
	}
	d.mu.Lock()
	d.tickSpacing = tickSpacing
	d.subscriptions = append([]string{d.Pool}, subscriptions...)
	d.mu.Unlock()
	return NewCoveredStateUpdateWithFeeRate(readU128(65), currentTick, readU128(49), feeRate, tickSpacing, ticks, false, minTick, maxTick)
}

func (d *Decoder) readTicks(ctx context.Context, reader AccountReader, currentTick, spacing int32) ([]Tick, int32, int32, []string, error) {
	readerMany, ok := reader.(MultipleAccountReader)
	if !ok {
		// A reduced test reader may expose only the pool account. It cannot
		// establish tick coverage, so keep the state bounded to the current tick.
		return nil, currentTick, currentTick, nil, nil
	}
	if spacing <= 0 {
		return nil, 0, 0, nil, fmt.Errorf("invalid whirlpool tick spacing")
	}
	arrayWidth := int64(fixedTickArraySize) * int64(spacing)
	start := int32(floorDiv(int64(currentTick), arrayWidth) * arrayWidth)
	pool, err := solana.DecodePublicKey(d.Pool)
	if err != nil {
		return nil, 0, 0, nil, err
	}
	program, err := solana.DecodePublicKey(whirlpoolProgram)
	if err != nil {
		return nil, 0, 0, nil, err
	}
	starts := []int32{start - int32(arrayWidth), start, start + int32(arrayWidth), start - 2*int32(arrayWidth), start + 2*int32(arrayWidth)}
	addresses := make([]string, len(starts))
	for i, value := range starts {
		address, _, err := solana.FindProgramAddress([][]byte{[]byte("tick_array"), pool[:], []byte(strconv.FormatInt(int64(value), 10))}, program)
		if err != nil {
			return nil, 0, 0, nil, err
		}
		addresses[i] = solana.EncodePublicKey(address)
	}
	accounts, err := readerMany.ReadMultipleAccounts(ctx, addresses)
	if err != nil {
		return nil, 0, 0, nil, err
	}
	var ticks []Tick
	min, max := int32(1<<31-1), int32(-1<<31)
	for _, account := range accounts {
		parsed, startTick, ok, err := parseFixedTickArray(account.Data, spacing)
		if err != nil {
			return nil, 0, 0, nil, err
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
		return nil, currentTick, currentTick, addresses, nil
	}
	if currentTick < min || currentTick > max {
		return nil, currentTick, currentTick, addresses, nil
	}
	sort.Slice(ticks, func(i, j int) bool { return ticks[i].index < ticks[j].index })
	return ticks, min, max, addresses, nil
}

func (d *Decoder) decodePool(data []byte) (PoolUpdate, error) {
	if len(data) < 653 || string(data[:8]) != whirlpoolDiscriminator {
		return PoolUpdate{}, fmt.Errorf("invalid whirlpool account data")
	}
	feeRate := uint32(binary.LittleEndian.Uint16(data[45:47]))
	tick := int32(binary.LittleEndian.Uint32(data[81:85]))
	return newPoolUpdate(littleEndianInt(data[65:81]), tick, littleEndianInt(data[49:65]), feeRate)
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
var _ solanalogs.AccountDecoder = (*Decoder)(nil)
