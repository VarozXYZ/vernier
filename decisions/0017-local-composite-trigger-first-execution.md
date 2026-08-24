# ADR 0017: Local composite trigger-first execution

## Context

A prefunded two-market strategy may have deterministic local state on both
networks while one side needs a direct route and a two-hop route to compete for
the same input. Treating that side as a provider quote adds avoidable latency
and loses the exact allocation needed for execution.

## Decision

Introduce an opt-in `prefunded_trigger_first` policy without changing existing
parallel policies. A composed market publishes one immutable snapshot after all
watched logs from the same transaction have been applied in log-index order.
Local sizing returns both its quote and an integer `RouteAllocation` covering
the direct and two-stage branches.

Bootstrap and reconnect use an explicit synchronization phase. The feed first
loads every child at one immutable block, remains degraded while applying the
subscribed logs through a post-bootstrap watermark, and publishes no economic
trigger for those historical updates. Only the first transaction beyond that
watermark restores health and becomes eligible to trigger evaluation.

Execution-sensitive transaction assembly closes only when the ordered stream
exposes the next transaction hash. It does not guess completion from an idle
timer. The trailing hash of a pause waits for the next watched transaction. A
distinct late log for an already published hash means ordering was violated and
degrades and resynchronizes the feed instead of publishing another trigger.

The trigger network executes first. Its global output floor may consume at most
75% of expected complete-flow net profit, preserving 25% for the unexecuted
leg. After confirmed settlement, the other market is requoted from its newest
local snapshot and executes with fixed slippage as exposure reduction.

Different quote tokens representing one economic asset use immutable,
directional conversion snapshots refreshed outside the decision path. Missing
or stale conversion evidence blocks only the dependent direction and is never
replaced by a cost fallback.

An atomic executor accepts only typed route groups and immutable protocol
adapters. It applies one global minimum output, uses real parent output for a
child group, assigns integer remainder to the final branch, and rejects replay.

## Consequences

- The decision path performs no RPC or HTTP before its first broadcast.
- A slow composite bootstrap cannot expose intermediate or historical state as
  a trade signal; admission remains closed until subscribed state catches up.
- The trailing transaction of a quiet period waits for the next watched hash in
  exchange for never authorizing a timer-derived partial multihop snapshot.
- Partial execution is explicit: economic thresholds apply to admission, while
  the second leg prioritizes exposure reduction.
- Adapter and executor deployment becomes a separately reviewed operational
  step with bytecode, pool, router, factory, fee, token and allowance checks.
- Stable-token conversion and bridge restoration remain slower background
  workflows and require their own durable recovery evidence.
- A demonstrated second-leg revert selects the better fresh local exit and
  permits one retry; another revert requires manual intervention. Unknown
  outcomes remain reconciliation barriers.
- Background cost refresh simulates exact local executor calldata and combines
  it with confirmed receipt high-water gas. This I/O never enters admission.

## Alternatives

- Keep an external aggregator as the market source: rejected for the local
  execution profile because response and build latency dominates reaction.
- Broadcast both legs concurrently: retained by the existing parallel policy,
  but not selected where trigger-first ordering is required.
- Protect every internal hop independently: rejected because it can prevent
  optimal split execution; protection belongs to the aggregate terminal output.
