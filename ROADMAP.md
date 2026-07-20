# Roadmap

Vernier is developed through small, demonstrable vertical slices. Milestone
dates are intentionally omitted until implementation evidence supports them.

## 1. Deterministic Research kernel

Normalize synthetic market events, publish immutable snapshots, quote locally,
evaluate two-market strategies across constant-product and Uniswap V3 markets,
and produce auditable opportunity reports.

Status: implemented as an offline synthetic vertical slice.

## 2. Durable Research

Persist runs and market history, reconstruct opportunity windows, compare
settings, and make source health and data-quality incidents explicit.

## 3. Generality

Add another market shape and conformance suites to prove that the core does not
branch on chain or protocol names.

Status: concentrated liquidity is represented by the local Uniswap V3 adapter;
an order-book shape remains future work.

## 4. Modeled execution

Model inventory, execution plans, latency, fills, and partial failures without
signing or broadcasting transactions.

## 5. Live and Shadow modes

Introduce durable execution and recovery using test wallets first, then compare
live decisions against Research without allowing Shadow mode to intervene.

No release is considered production-ready until its documented safety and
recovery guarantees are implemented and tested.
