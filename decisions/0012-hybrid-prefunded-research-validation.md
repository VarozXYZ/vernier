# ADR 0012: Hybrid prefunded Research validation

## Context

A Research setup may combine one locally mirrored market with one remotely
quoted market. Prefunded inventory means the buy and sell legs do not depend on
one another's output, while executable evidence from the remote provider is
only available after an unsigned build.

## Decision

Research may use a `prefunded_parallel` evaluation with a fixed quote notional
and a separately fixed base amount derived from one immutable cached valuation.
Both directions are quoted concurrently. Each remote direction retains its
exact discovery route in a bounded in-memory journal; only the best
policy-qualified direction proceeds to unsigned build and read-only
simulation.

After the build, Research captures the latest local snapshot, requotes only the
local leg with the original input, and recomputes PnL from the built remote
output. Economic-window state and executable-validation state remain separate.
The durable validation journal contains normalized hashes, quantities,
timestamps, transport metrics, and error classes, but no calldata or raw HTTP
payloads.

## Consequences

- Discovery uses at most one remote route request per direction and fixed size.
- A stale build may perform one fresh route-and-build retry; other failures do
  not retry automatically.
- Validation failure cannot erase or falsely close an observed economic window.
- Research composition still has no signer, nonce manager, or broadcast path.
- Feed readiness, health, overflow, and transaction-trigger deduplication remain
  explicit runtime gates.

## Alternatives

- Chaining the sell input from the buy output was rejected because it models
  transported inventory rather than prefunded parallel legs.
- Requesting a fresh route before every build was rejected because it adds
  latency and breaks traceability to the discovery result.
- Persisting provider payloads was rejected because normalized evidence is
  sufficient for analysis and avoids retaining executable calldata.
