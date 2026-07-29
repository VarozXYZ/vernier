# ADR 0010: Sequential inventory-carrying execution

## Context

Some cross-chain opportunities cannot execute both market legs against
prefunded inventory. They must buy an asset, transfer the confirmed output to
another chain, sell the amount that actually arrived, and optionally return
the quote asset.

That lifecycle is economically different from the existing prefunded parallel
plan. Encoding it in one setup runtime would couple inventory movement,
settlement, recovery, and provider details.

## Decision

- Model the lifecycle as a provider-neutral `SequentialPlan` with four ordered
  stages: buy, base-asset transfer, sell, and quote-asset return.
- The confirmed output of each stage is the only valid input for the next
  stage. Expected outputs never advance inventory.
- Stage drivers implement external effects behind execution ports. The saga
  does not know which chains, exchanges, bridges, wallets, or transports they
  use.
- Persist the operation before the first external effect and persist every
  prepared transaction and confirmed settlement in an operational journal.
- Treat a possible or unknown broadcast as recovery-required. Never infer
  non-execution and never resend automatically.
- After the base asset reaches the destination, an exit selector may compare a
  destination sale with one terminal return-to-origin route. It cannot create
  an unbounded bridge loop.
- Record network and provider costs as structured components and value them in
  the setup quote asset outside the domain.

## Consequences

New sequential setups can reuse the same planner, executor, state machine,
settlement rules, persistence contract, and recovery behavior. They provide
only chain, swap, bridge, confirmation, cost, and notification adapters.

Sequential execution is intentionally slower than prefunded parallel
execution because every stage depends on confirmed economic settlement from
the previous stage. A setup that can prefund both legs should continue to use
the parallel plan.

## Alternatives

- A setup-specific workflow was rejected because it would duplicate economic
  state transitions and recovery rules.
- Advancing on transaction acceptance was rejected because acceptance does not
  prove the amount available to the next stage.
- Automatically repeating an uncertain stage was rejected because it can
  duplicate exposure.
- Allowing arbitrary return loops was rejected because it makes termination
  and maximum exposure difficult to prove.
