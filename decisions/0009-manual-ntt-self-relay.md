# ADR 0009: Manual Wormhole NTT self-relay

## Context

Wormhole Native Token Transfers can include an Executor request that pays a
relayer to deliver the attestation and destination transaction. Low-latency
systems with funded accounts on both chains can instead submit directly to the
NTT manager, obtain the signed VAA, and redeem it themselves.

The two paths have different cost shapes. An EVM destination normally needs one
redemption call. A legacy Solana Wormhole Core destination can require several
transactions: guardian-signature verification batches, VAA publication, and
NTT redemption. Counting only the final transaction materially understates the
manual path.

## Decision

- Model NTT as a protocol-neutral transfer intent plus chain-specific
  transaction builders.
- Use the manager's manual transceiver instruction, with no Executor request.
- Query the manager's delivery price for the exact encoded instruction; do not
  assume the payable value is zero.
- Identify the source message by Wormhole emitter chain, emitter address, and
  sequence, then fetch its signed VAA from independently configured Guardian
  RPC endpoints.
- Validate the VAA message identity and destination manager before building a
  redemption.
- On Solana, represent guardian verification, VAA publication, and NTT
  redemption as separate observable batches. Skip only work proven complete
  on-chain.
- Builders never own signers, nonces, blockhashes, fee selection, persistence,
  broadcast, or retries. Those remain TxManager and saga responsibilities.
- Never resend a source transfer or redemption while its outcome is unknown.
- Keep the manual canary read-only by default. Armed use requires the fixed
  integer amount to be repeated exactly, funded source and destination
  accounts, sufficient allowance, and a durable FULL/WAL journal.
- Persist transaction identity and nonce or blockhash before every canary
  broadcast. Do not persist private keys or signed transaction payloads.

## Consequences

Manual relay removes the Executor fee and gives the runtime direct control over
priority and timing, but it does not make delivery free. Destination gas,
Solana rent, guardian verification, VAA publication, and operational account
balances must all be included in cost and readiness checks.

The bridge can be reused by different setups without embedding deployment
addresses in public code. The armed canary proves the direct protocol path, but
it is not the production arbitrage saga or an automatic recovery mechanism.

## Alternatives

- Always using the Executor was rejected because its convenience fee and
  scheduling are unnecessary when the runtime already operates funded
  destination accounts.
- Treating the observed destination call as a standalone bridge was rejected
  because it omits attestation acquisition and, on Solana, prerequisite
  verification and publication transactions.
- Putting keys and broadcasting inside the NTT adapter was rejected because it
  would duplicate TxManager ownership and weaken recovery guarantees.
