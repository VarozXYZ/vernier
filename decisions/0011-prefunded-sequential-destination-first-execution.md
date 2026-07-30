# ADR 0011: Prefunded sequential destination-first execution

## Context

Some cross-chain strategies hold both economic assets on both chains but
still choose to sequence their market legs. They buy first, wait for the real
economic output, and only then sell the equivalent destination inventory.
This differs from both transported inventory and prefunded parallel
execution.

The destination sale must remain the first liquidation attempt. An origin
sale is a risk-reduction circuit breaker, not an optimizer that continuously
compares two exits.

## Decision

- Register three compiled policy kinds: `transported_sequential`,
  `prefunded_sequential`, and `prefunded_parallel`.
- Select execution and inventory profiles by ID from schema-v2 YAML. YAML
  cannot define arbitrary steps or conditions.
- Represent dependent plans with typed stages, dependencies, settlement input
  references, a main branch, and one terminal circuit-breaker branch.
- In prefunded sequential execution, use this main branch: buy, destination
  sale, base transfer, quote transfer. The transfers consume the actual buy
  and destination-sale outputs respectively.
- Prepare the destination sale only after the buy has a verified economic
  settlement. Apply the dynamic `MinimumOutput` derived from the immutable
  admission cost snapshot.
- Authorize the origin sale only after a typed dynamic-slippage rejection or a
  failure that proves no destination effect. Possible broadcast, missing
  receipt, timeout, and unknown outcome remain recovery-required.
- The origin sale uses the bounded recovery slippage and fee policy and ends
  the operation without transfers.
- Persist policy kind, stage dependencies, input references, branch decisions,
  admission cost evidence, transaction identities, and settlements.
- Treat cached economic cost freshness as an admission condition. Once the
  operation starts, snapshot age does not block liquidation. Transaction fee
  data may be refreshed only when the transaction manager cannot prepare a
  valid transaction, and never between durable commit and broadcast.
- Compute effective inventory from wallet observations, allocation caps,
  buffers, reservations, and outbound in-flight value. Inbound in-flight value
  is not spendable.

## Consequences

Changing only `active_live` can select a different compiled lifecycle and
inventory profile. Transported execution retains its economic ordering.
Prefunded sequential recovery can prove that a destination transaction did
not execute before constructing the origin sale, preventing double sales.

Restoration adds latency after market risk has been closed, but it restores
both prefunded sides in a deterministic order and from real outputs.

## Alternatives

- A YAML workflow interpreter was rejected because it would make economic and
  recovery invariants configurable data.
- Reclassifying with a new cost snapshot after the buy was rejected because it
  would change the evidence under which the operation was admitted.
- A second `profit_drop` threshold was rejected because dynamic slippage is the
  single economic definition of a considerable drop.
- Falling back on timeout or unknown outcome was rejected because it could
  produce two sales.
