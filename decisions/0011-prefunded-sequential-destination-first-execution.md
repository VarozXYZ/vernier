# ADR 0011: Prefunded sequential destination-first execution

## Context

Some cross-chain strategies hold both economic assets on both chains but
still choose to sequence their market legs. They buy first, wait for the real
economic output, and only then sell the equivalent destination inventory.
This differs from both transported inventory and prefunded parallel
execution.

The destination sale must remain the first profit-preserving liquidation
attempt. An origin sale is a risk-reduction circuit breaker, not an optimizer
that continuously compares two exits during the normal path. Once a safe
recovery trigger exists, however, choosing origin without rebuilding and
comparing both sales can realize an avoidable loss.

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
- Apply dynamic slippage only to the buy, where it can safely reject an
  opportunity before inventory exposure begins.
- Prepare the destination sale only after the buy has a verified economic
  settlement. Use the configured fixed sell slippage and do not apply a
  profit-derived `MinimumOutput`; liquidation takes priority after exposure.
- Exhausted preparation failures before broadcast, or execution failures that
  prove no destination effect, open recovery selection. Economic deterioration
  by itself does not.
- Recovery independently obtains a fresh quote/build and simulation for both
  destination and origin. Each preparation retries with a completely fresh
  artifact. A single provider or HTTP error does not make a branch unavailable;
  retry exhaustion or terminal evidence does.
- Select the best valid net recovery, or the only executable branch. Recovery
  does not re-impose the original profit requirement, but both branches retain
  bounded slippage and hard fee caps. An origin sale ends without transfers.
- Possible broadcast, missing receipt, timeout, and unknown outcome remain
  recovery-required and never permit the other sale to be broadcast.
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
not execute before selecting another sale, preventing double sales. The
bounded comparison adds quote/build latency only after a recovery trigger and
avoids equating a stale or rejected profit-preserving artifact with an
intrinsically worse destination market.

Restoration adds latency after market risk has been closed, but it restores
both prefunded sides in a deterministic order and from real outputs.

## Alternatives

- A YAML workflow interpreter was rejected because it would make economic and
  recovery invariants configurable data.
- Reclassifying with a new cost snapshot after the buy was rejected because it
  would change the evidence under which the operation was admitted.
- A post-buy `profit_drop` or dynamic sell threshold was rejected because it
  delays liquidation and can divert execution from the normally superior
  destination market after inventory exposure already exists.
- Falling back on timeout or unknown outcome was rejected because it could
  produce two sales.
- Selecting origin after one failed destination build was rejected because the
  failure may be transient and the freshly rebuilt destination recovery may
  still be economically superior.
