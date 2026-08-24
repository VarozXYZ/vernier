package market

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"time"
)

// QuoteConversionSnapshot is one immutable, direction-specific exact-input
// normalization observation. It is an economic conversion, never a negative
// cost component.
type QuoteConversionSnapshot struct {
	Provider   SourceID
	Input      TokenAmount
	Output     TokenAmount
	ReceivedAt time.Time
	ExpiresAt  time.Time
	Signature  [sha256.Size]byte
}

func NewQuoteConversionSnapshot(provider SourceID, input, output TokenAmount,
	receivedAt, expiresAt time.Time) (QuoteConversionSnapshot, error) {
	if provider == "" || input.IsZero() || output.IsZero() || input.Token() == output.Token() ||
		receivedAt.IsZero() || !expiresAt.After(receivedAt) {
		return QuoteConversionSnapshot{}, fmt.Errorf("quote conversion snapshot is incomplete")
	}
	receivedAt, expiresAt = receivedAt.UTC(), expiresAt.UTC()
	hasher := sha256.New()
	hasher.Write([]byte(provider))
	hasher.Write([]byte(input.Token()))
	hasher.Write(input.Units().Bytes())
	hasher.Write([]byte(output.Token()))
	hasher.Write(output.Units().Bytes())
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(receivedAt.UnixNano()))
	hasher.Write(encoded[:])
	var signature [sha256.Size]byte
	copy(signature[:], hasher.Sum(nil))
	return QuoteConversionSnapshot{Provider: provider, Input: input, Output: output,
		ReceivedAt: receivedAt, ExpiresAt: expiresAt, Signature: signature}, nil
}

func (s QuoteConversionSnapshot) ValidAt(at time.Time) bool {
	return s.Provider != "" && !s.Input.IsZero() && !s.Output.IsZero() &&
		!at.UTC().After(s.ExpiresAt)
}
