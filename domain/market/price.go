package market

import (
	"fmt"
	"math/big"
	"time"
)

// PriceObservation is immutable evidence for one external asset ratio.
type PriceObservation struct {
	source          SourceID
	base            AssetID
	quote           AssetID
	value           *big.Rat
	reference       string
	sourceUpdatedAt time.Time
	observedAt      time.Time
}

func NewPriceObservation(source SourceID, base, quote AssetID, value *big.Rat, reference string, sourceUpdatedAt, observedAt time.Time) (PriceObservation, error) {
	if source == "" || base == "" || quote == "" || base == quote || value == nil || value.Sign() <= 0 ||
		reference == "" || sourceUpdatedAt.IsZero() || observedAt.IsZero() {
		return PriceObservation{}, fmt.Errorf("price observation requires source, pair, positive value, reference, and times")
	}
	return PriceObservation{
		source: source, base: base, quote: quote, value: new(big.Rat).Set(value), reference: reference,
		sourceUpdatedAt: sourceUpdatedAt.UTC(), observedAt: observedAt.UTC(),
	}, nil
}

func (o PriceObservation) Source() SourceID { return o.source }
func (o PriceObservation) Base() AssetID    { return o.base }
func (o PriceObservation) Quote() AssetID   { return o.quote }
func (o PriceObservation) Value() *big.Rat {
	if o.value == nil {
		return new(big.Rat)
	}
	return new(big.Rat).Set(o.value)
}
func (o PriceObservation) Reference() string          { return o.reference }
func (o PriceObservation) SourceUpdatedAt() time.Time { return o.sourceUpdatedAt }
func (o PriceObservation) ObservedAt() time.Time      { return o.observedAt }
