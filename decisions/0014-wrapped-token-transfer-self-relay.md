# ADR 0014: Wrapped Token Transfer self-relay

## Context

Some assets are native on one EVM chain and represented by a Wormhole-wrapped
token on another. This is Wrapped Token Transfers (Token Bridge), not Native
Token Transfers, even though both use Guardian-signed VAAs.

## Decision

- Implement WTT as its own cross-chain adapter.
- Build the source Token Bridge transfer, identify the Core message by emitter
  chain/address and sequence, retrieve a signed VAA from redundant sources,
  check whether the VAA was already consumed, and build the destination redeem.
- Keep signer, nonce, fees, persistence, broadcast, and reconciliation in the
  chain transaction manager and saga.
- Normalize transfers to at most eight decimals and retain source dust.
- Never resend a source or redeem transaction while its outcome is unknown.

## Consequences

Recovery can resume from durable transaction and VAA identities without
confusing WTT custody/mint-burn semantics with NTT manager/transceiver semantics.
The adapter can be reused by any EVM pair with configured official deployments.

## Alternatives

Reusing the NTT adapter was rejected because its contracts and payloads are
incompatible. Depending on an optional relayer was rejected for the initial
flow because manual redemption gives explicit, recoverable transaction state.
