# ADR 0013: Bounded asynchronous quote restoration

## Context

A prefunded parallel strategy closes market risk with two local swap effects,
then restores the two inventories over independent cross-chain routes. Waiting
for both deliveries needlessly prevents another trade when only the quote-token
return is slow, while treating inbound funds as spendable would overstate
inventory.

## Decision

- Base-token restoration remains a single-flight admission gate.
- Quote-token restoration is an independent durable job with capacity two.
- Inbound quote inventory is unavailable until delivery is confirmed.
- When either gate or inventory blocks admission, retain only the latest trigger
  metadata. Once capacity returns, request a fresh evaluation from current
  snapshots, costs, valuation, and balances; never queue an opportunity, quote,
  build, or signed artifact.
- Unknown transaction outcomes and recovery-required jobs close admission.
- A confirmed quote return may extend its already completed parent operation
  only while its durable restoration job remains active. Its confirmed bridge
  spread and source cost adjust the stored realized economics exactly once.

## Consequences

Two quote returns may overlap without permitting overlapping base restoration.
The bounded queue cannot grow with feed volume, and every delayed execution is
based on fresh economic evidence. Persistence must distinguish the critical
base gate, asynchronous quote jobs, and the coalesced reevaluation marker.

## Alternatives

A FIFO of candidates was rejected because economic artifacts expire while
waiting. Waiting for both restorations was rejected because it discards safe
capacity. Counting inbound transfers as spendable was rejected because their
delivery is not yet proven.
