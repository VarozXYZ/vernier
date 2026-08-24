# ADR 0016: Directional observed complete-flow costs

## Context

A prefunded parallel strategy can close market exposure with simultaneous
swaps while still owing two inventory-restoration transfers. A configured
constant cannot represent direction-dependent network fees, bridge spread, or
different source and destination transaction costs.

## Decision

- Make the model opt-in per execution policy so existing setups retain their
  current behavior.
- Maintain one background-refreshed immutable cost snapshot per direction.
- Keep background refresh independent from trading-capacity and rebalance gates;
  those gates may block admission but must not age the economic evidence.
- Refresh only on the configured background cadence. Market observations may
  replace the in-memory evidence used by a future refresh, but never schedule
  provider work. Cache age alone does not alert: start the alert timer only
  when missing or stale evidence blocks an evaluation, emit one durable alert
  if that admission block persists for 30 seconds, and emit one recovery event
  after a complete fresh snapshot is restored.
- Combine current transaction-fee and native-price caches with confirmed
  receipt high-water gas, the current base-transfer message fee, and a fresh
  read-only quote-return approval.
- Cache receipt calibration separately from the faster price/fee refresh.
- Freeze the selected directional snapshot across discovery, executable build,
  final local requote, durable admission, and pre-broadcast checks.
- Treat missing or stale evidence as an admission failure by default,
  including forced executions. A hybrid profile may explicitly opt into a
  positive, quote-denominated fixed fallback bounded by its maximum execution
  cost. Such a fallback is provisional economic evidence, is identified as a
  stale fallback in snapshots and reports, and never masquerades as an
  observed cost.
- Use an opted-in fallback only for evaluation and preflight. Confirmed receipt
  costs remain the source of truth for settlement and future calibration.
- Keep exchange fees and price impact in executable outputs and exclude them
  from the external-cost sum to prevent double counting.

## Consequences

The hot path performs only in-memory reads, while provider and receipt I/O run
out of band on a periodic cadence that market events cannot accelerate. A newly
started profile remains disarmed until both directions have complete evidence.
Confirmed settlements can improve later high-water calibration without
changing the cost frozen for an already admitted operation.
Executing or restoring inventory does not pause cost refresh. Stale evidence
still fails closed unless the active profile explicitly supplies the bounded
fallback. Because that fallback preserves admission, it does not start the
stale-block alert timer. The alert describes a sustained operational admission
block, not an unused cache becoming old; alert and recovery events remain
observational and do not modify admission decisions.

## Alternatives

A fixed quote-denominated primary model was rejected because it cannot
distinguish directions or current network conditions. Synchronous estimation
in the decision path was rejected because provider latency would make the
economic snapshot internally inconsistent. A silent or unbounded fallback was
rejected because operators must opt in and downstream evidence must preserve
the distinction between observed and provisional costs.
