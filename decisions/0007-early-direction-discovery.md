# ADR 0007: Early direction discovery for two-market sizing

## Context

Evaluating a complete sizing curve in both directions repeats local pool
quoting before the strategy knows which market is cheaper. For live research,
that avoidable work adds latency to every accepted market event.

## Decision

When enabled, the two-market strategy performs a small deterministic probe
before exhaustive sizing. It converts evenly spaced quote-asset budgets from
the configured range into local exact-input quotes on both markets, compares
their base-asset outputs, and selects the market with the higher output as the
buy leg when it wins a strict majority of valid probes. Three probes provide
minimum/middle/maximum coverage by default. Discovery uses the same immutable
snapshot and local quote cache as the full evaluation.

Any unsupported sizing basis, missing snapshot, equal output, failed quote, or
incomplete probe makes the decision uncertain and preserves the safe fallback:
evaluate both directions. External reference providers are not called during
discovery; they remain asynchronous validation of the selected local result.

## Consequences

The normal live path evaluates one complete curve instead of two, while probe
quotes at points reused by that curve are cache hits. Direction evidence and
timings are included in reports, making the shortcut auditable. A genuinely
ambiguous market still pays the full two-direction cost rather than risking a
missed opportunity.

## Alternatives

- One probe was rejected because a single size can be distorted by fees,
  curvature, or a local reserve boundary.
- A remote quote was rejected because discovery must remain deterministic and
  outside the hot path.
- An automatic threshold or adaptive probe count was deferred until measured
  event-time distributions justify the added policy surface.
