# ADR 0003: Canonical chain and market adapters

## Context

EVM-compatible networks can differ in transport, ordering, finality, and RPC
behavior. Likewise, Uniswap V3 forks may change events, state, or quote
semantics. Treating every network or fork as interchangeable would hide
operational assumptions, while creating a separate implementation for every
deployment would duplicate canonical behavior.

## Decision

- Small EVM capabilities are shared internally; EVM is not a configured
  network implementation.
- Ethereum is the first canonical chain implementation.
- Uniswap V3 is the canonical implementation of the original protocol and
  depends only on the EVM capabilities it requires.
- Compatible networks and deployments may reuse canonical behavior. A named
  implementation is added only when behavior actually differs.
- Configuration selects compiled implementations explicitly. There is no
  automatic fallback from an unknown network or market to a generic EVM or V3
  implementation.
- The EVM log feed subscribes only to the configured market address and
  protocol event topics. It never subscribes to new heads and never infers a
  gap from non-contiguous event block numbers.

## Consequences

Chain transport and market protocol behavior can evolve independently without
leaking EVM types into the economic domain. Blocks without market events cost
no subscription work. Future deviations remain visible and testable instead
of becoming configuration flags inside canonical implementations.

The first live slice supports only Ethereum mainnet and canonical Uniswap V3.
A second real network or fork is required before extracting more shared
behavior.

## Alternatives

- A universal selectable EVM adapter was rejected because it would silently
  claim behavioral compatibility across networks.
- A deployment-specific market adapter for every chain was rejected because
  identical Uniswap V3 deployments should reuse the canonical implementation.
- A new-head subscription was rejected because pool-filtered log subscriptions
  are both more precise and less expensive for this feed.
