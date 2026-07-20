// Package constantproduct provides a deterministic constant-product market adapter.
package constantproduct

import (
	"fmt"
	"math/big"
)

const snapshotSchemaVersion uint16 = 1

type ReserveUpdate struct {
	baseReserve  *big.Int
	quoteReserve *big.Int
	feeBPS       uint16
}

func NewReserveUpdate(baseReserve, quoteReserve *big.Int, feeBPS uint16) (ReserveUpdate, error) {
	if baseReserve == nil || quoteReserve == nil || baseReserve.Sign() <= 0 || quoteReserve.Sign() <= 0 {
		return ReserveUpdate{}, fmt.Errorf("reserves must be positive")
	}
	if feeBPS >= 10_000 {
		return ReserveUpdate{}, fmt.Errorf("fee must be below 10000 basis points")
	}
	return ReserveUpdate{
		baseReserve: new(big.Int).Set(baseReserve), quoteReserve: new(big.Int).Set(quoteReserve), feeBPS: feeBPS,
	}, nil
}

func (ReserveUpdate) EventKind() string { return "constant_product/reserves/v1" }
func (u ReserveUpdate) BaseReserve() *big.Int {
	if u.baseReserve == nil {
		return new(big.Int)
	}
	return new(big.Int).Set(u.baseReserve)
}
func (u ReserveUpdate) QuoteReserve() *big.Int {
	if u.quoteReserve == nil {
		return new(big.Int)
	}
	return new(big.Int).Set(u.quoteReserve)
}
func (u ReserveUpdate) FeeBPS() uint16 { return u.feeBPS }

func (u ReserveUpdate) validate() error {
	if u.BaseReserve().Sign() <= 0 || u.QuoteReserve().Sign() <= 0 || u.feeBPS >= 10_000 {
		return fmt.Errorf("invalid reserve update")
	}
	return nil
}

type Snapshot struct {
	schemaVersion uint16
	baseReserve   *big.Int
	quoteReserve  *big.Int
	feeBPS        uint16
}

func (Snapshot) SnapshotKind() string { return "constant_product/v1" }
func (s Snapshot) BaseReserve() *big.Int {
	if s.baseReserve == nil {
		return new(big.Int)
	}
	return new(big.Int).Set(s.baseReserve)
}
func (s Snapshot) QuoteReserve() *big.Int {
	if s.quoteReserve == nil {
		return new(big.Int)
	}
	return new(big.Int).Set(s.quoteReserve)
}
func (s Snapshot) FeeBPS() uint16 { return s.feeBPS }
