# Roadmap

Vernier is developed through small, demonstrable vertical slices. Milestone
dates are intentionally omitted until implementation evidence supports them.

## 1. Deterministic Research kernel

Normalize synthetic market events, publish immutable snapshots, quote locally,
evaluate two-market strategies across constant-product and Uniswap V3 markets,
and produce auditable opportunity reports.

Status: implemented as an offline synthetic vertical slice.

## 2. Real market observation

Observe canonical and compatible pools through pool-filtered WebSocket logs,
maintain local state, and compare local quotes exactly with venue references at
the same block hash.

Status: implemented experimentally for canonical Uniswap V3, canonical
Uniswap V2, and Aerodrome Slipstream on configured EVM profiles. Two configured
markets can also be compared read-only at explicit point-in-time snapshots.
VIRTUAL across Robinhood Chain and Base is the public reference setup, with
local quotes checked exactly against both venue contracts. An experimental
continuous mode now shares the same mirrors and strategy, evaluates after
both-pool bootstrap and accepted log updates, and exposes disconnect
degradation explicitly.

## 3. Durable Research

Persist runs and market history, reconstruct opportunity windows, compare
settings, and make source health and data-quality incidents explicit.

## 4. Generality

Add another market shape and conformance suites to prove that the core does not
branch on chain or protocol names.

Status: constant-product and concentrated-liquidity shapes are represented.
Slipstream proves that a venue variant can reuse canonical V3 state and local
quoting while adapting metadata, dynamic fees, and its reference quoter. An
order-book shape remains future work.

## 5. Modeled execution

Model inventory, execution plans, latency, fills, and partial failures without
signing or broadcasting transactions.

## 6. Live and Shadow modes

Introduce durable execution and recovery using test wallets first, then compare
live decisions against Research without allowing Shadow mode to intervene.

No release is considered production-ready until its documented safety and
recovery guarantees are implemented and tested.
