package orcawhirlpool

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/VarozXYZ/vernier/adapters/chain/solana"
	"github.com/VarozXYZ/vernier/adapters/feed/solanalogs"
	"github.com/VarozXYZ/vernier/domain/market"
)

const whirlpoolDiscriminator = "\x3f\x95\xd1\x0c\xe1\x80\x63\x09"

// AccountReader is the optional read capability required by a protocol
// decoder. The feed itself remains limited to slots and logs.
type AccountReader interface {
	ReadAccount(context.Context, string) (solana.Account, error)
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
	return d.decode(account.Data)
}

func (d *Decoder) Decode(ctx context.Context, network solanalogs.Network, _ solana.LogNotification) ([]market.EventData, error) {
	data, err := d.Bootstrap(ctx, network, 0)
	if err != nil {
		return nil, err
	}
	return []market.EventData{data}, nil
}

func (d *Decoder) decode(data []byte) (StateUpdate, error) {
	if len(data) < 653 || string(data[:8]) != whirlpoolDiscriminator {
		return StateUpdate{}, fmt.Errorf("invalid whirlpool account data")
	}
	readU16 := func(offset int) uint16 { return binary.LittleEndian.Uint16(data[offset : offset+2]) }
	readI32 := func(offset int) int32 { return int32(binary.LittleEndian.Uint32(data[offset : offset+4])) }
	readU128 := func(offset int) *big.Int { return littleEndianInt(data[offset : offset+16]) }
	feeRate := readU16(45)
	feeBPS := uint16(0)
	if feeRate > 0 {
		feeBPS = (feeRate + 99) / 100
	}
	return NewCoveredStateUpdate(readU128(65), readI32(81), readU128(49), feeBPS, int32(readU16(41)), nil, true, 0, 0)
}

func littleEndianInt(value []byte) *big.Int {
	copyValue := append([]byte(nil), value...)
	for left, right := 0, len(copyValue)-1; left < right; left, right = left+1, right-1 {
		copyValue[left], copyValue[right] = copyValue[right], copyValue[left]
	}
	return new(big.Int).SetBytes(copyValue)
}

var _ solanalogs.Decoder = (*Decoder)(nil)
