# ADR 0004: Modular composition and behavioral reuse

## Context

The first live research slice proved real adapters but coupled configuration to
two named networks, repeated network wrappers, listed every sizing point, and
wired one price provider directly into the runtime. Extending that shape would
multiply code and configuration for data that differs without changing
behavior.

## Decision

- A small YAML manifest composes topology and policy documents by stable IDs.
- Secrets remain external; startup resolves and validates a typed immutable
  configuration before the decision path runs.
- Compatible EVM networks share one implementation and differ through explicit
  profiles. Specialized code is introduced only for semantic differences.
- Price providers implement one neutral capability. CoinGecko is primary and
  Chainlink is fallback; observations retain provider and time evidence.
- Sizing configuration describes a range. Native local exact-output quoting is
  preferred, with generic exact-input search as a compatibility fallback.

## Consequences

New setups mostly compose existing pieces instead of adding runners or wrappers.
Configuration remains declarative, while protocol behavior stays in tested Go
code. Provider failures and quote provenance remain observable.

## Alternatives

- One configuration file per setup was rejected because it duplicates topology.
- One package per compatible EVM network was rejected because identity alone is
  data, not behavior.
- A universal adapter controlled by arbitrary flags was rejected because it
  would hide real semantic differences.
- Remote exact-output calls on every sizing point were rejected from the local
  decision path because they add latency, rate usage, and nondeterminism.
