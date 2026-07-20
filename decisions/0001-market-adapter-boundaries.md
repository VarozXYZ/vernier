# ADR 0001: Market adapter boundaries

## Context

Strategies must evaluate markets without branching on liquidity model or feed
transport. Constant-product reserves, concentrated-liquidity ticks, and future
order books require different state transitions and quote algorithms, while
snapshot lifecycle, health, provenance, and quote evidence are shared.

## Decision

- `MarketEvent`, `MarketSnapshot`, and `Quote` are canonical domain envelopes.
- The generic mirror owns health, versioning, ordering outcomes, timestamps,
  and immutable snapshot publication.
- Feed-selected ordering policies decide only whether source evidence proves an
  event stale. Missing or incomparable evidence falls back to arrival order.
- A market adapter supplies a small state `Reducer` and, when supported, a
  `quote.Source`. Adapter state remains opaque to the core.
- Quote fees are typed components and may use different tokens or represent a
  cost or credit. A strategy must reject a quote when a component is not
  already reflected in its amounts unless that strategy explicitly models it.
- Runtime composition selects and wires feed, ordering, reducer, and quoter
  capabilities. Strategies depend only on canonical domain and port types.

The first two implementations are constant product and a deterministic local
Uniswap V3 adapter with initialized-tick traversal.

## Consequences

Adding a liquidity model does not change strategy code or the mirror lifecycle.
Each adapter remains responsible for immutable state, deterministic hashing,
state validation, and quote correctness. Composition must reject incompatible
or incomplete capabilities at startup.

The generic snapshot cannot inspect protocol state. Contract tests and a second
real adapter are therefore required to demonstrate the boundary.

## Alternatives

- A mirror per protocol was rejected because it duplicates health, ordering,
  versioning, and snapshot invariants.
- A domain union containing every protocol state was rejected because every new
  market shape would modify the core.
- A monolithic market adapter interface was rejected because decoding, quoting,
  state reduction, and execution are independently useful capabilities.
