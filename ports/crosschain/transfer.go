// Package crosschain defines protocol-neutral cross-chain transfer
// capabilities. Concrete transaction and attestation formats remain in
// adapters.
package crosschain

import (
	"context"
	"time"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

// Intent fixes the economic movement before any source-chain transaction is
// built. Recipient is a protocol-neutral 32-byte destination address. Each
// adapter owns the conversion to its wire format.
type Intent struct {
	ID               string
	SourceChain      market.ChainID
	DestinationChain market.ChainID
	SourceToken      market.TokenID
	DestinationToken market.TokenID
	Amount           market.TokenAmount
	Recipient        [32]byte
	CreatedAt        time.Time
}

// MessageID is the canonical identity used by Wormhole Guardian RPCs.
type MessageID struct {
	EmitterChain   uint16
	EmitterAddress [32]byte
	Sequence       uint64
}

// Attestation is deliberately opaque to the core. Destination-chain adapters
// validate and consume Raw; it must never be logged in full.
type Attestation struct {
	Message    MessageID
	Raw        []byte
	ObservedAt time.Time
}

// AttestationSource waits for independently signed cross-chain evidence.
type AttestationSource interface {
	Await(context.Context, MessageID) (Attestation, error)
}

// LiveTransferResult is the chain-neutral result of a fully settled bridge.
// Implementations may use multiple durable transactions between source and
// destination, but must record every prepared identity through the supplied
// journal before broadcasting it.
type LiveTransferResult struct {
	ActualInput         market.TokenAmount
	ActualOutput        market.TokenAmount
	Costs               []execution.CostComponent
	SourceIdentity      execution.TransactionIdentity
	DestinationIdentity execution.TransactionIdentity
	ObservedAt          time.Time
	Evidence            string
}

type LiveTransferService interface {
	Transfer(
		context.Context,
		execution.SequentialStageRequest,
		executionport.SequentialJournal,
	) (LiveTransferResult, error)
}
