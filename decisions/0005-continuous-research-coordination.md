# ADR 0005: Continuous Research coordination

## Context

Point-in-time comparison is useful for parity checks, but a live research
process must keep both market mirrors current without putting RPC reference
quoting on the event path or treating block-number gaps as failures.

## Decision

- Each configured pool uses the generic filtered log feed and its adapter-owned
  reducer, while the coordinator owns no protocol-specific state.
- The coordinator evaluates only after every mirror has an immutable snapshot;
  each accepted event or explicit degraded-health update produces a new report.
- Cost evidence is acquired once per stream startup and reused for evaluations.
- Reconnection and full bootstrap remain feed responsibilities. The coordinator
  observes the resulting healthy/degraded snapshots and never infers gaps,
  staleness, or finality from block numbers.
- Stream output is text or newline-delimited JSON (`--format jsonl`) and is
  read-only; parity checks remain an explicit point-in-time operation.

## Consequences

Continuous research has deterministic trigger and snapshot semantics while
avoiding duplicate mirrors or venue calls in its hot path. Reports can be
consumed incrementally, but durable history, backpressure policy, and
opportunity windows remain future work.

## Alternatives

- Polling each venue was rejected for the initial stream because it duplicates
  state acquisition and adds avoidable RPC load.
- New-head subscriptions were rejected because pool-filtered logs are the
  relevant event boundary.
- Running parity RPC calls on every event was rejected because it makes the
  research loop rate-limited and nondeterministic.
