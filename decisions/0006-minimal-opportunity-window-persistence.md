# ADR 0006: Minimal opportunity-window persistence

Status: accepted

## Context

Continuous Research needs a durable record of economically meaningful periods:
the trigger, best observed trade, timing until profitability disappears, and
whether the period ended normally or because the feed degraded. Persisting
every normalized event, snapshot, quote, or reconnect attempt would duplicate
the live mirror and create a replay system that Research does not need.

## Decision

- Persist only `OpportunityWindow` records and a bounded set of best-so-far
  observations.
- Open a window only for `economic` or `policy_qualified` opportunities.
  `observed_spread`, `no_spread`, and `unclassifiable` close an active window
  but do not create one.
- A confirmed WebSocket disconnect fails the active window. Reconnection does
  a normal full bootstrap; later opportunities belong to a new continuity.
  There is no recovery table or recovery history.
- On startup, any dangling open window is finalized as
  `process_interrupted`; this is cleanup of the window lifecycle, not recovery
  persistence.
- SQLite is the first store, configured for WAL, foreign keys, and full
  synchronous writes. Window and best-observation writes are idempotent by
  stable identifiers.

## Consequences

Research can query opportunity history without replaying market data, and the
live hot path remains local. Exact quantities and trigger references remain
auditable, while every quote curve and event payload remains ephemeral.

## Alternatives

An event-sourced SQLite schema was rejected for this phase because it would
introduce replay, retention, backpressure, and recovery semantics before the
window lifecycle itself is validated.
