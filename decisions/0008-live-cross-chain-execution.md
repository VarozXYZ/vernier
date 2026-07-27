# ADR 0008: Durable low-latency cross-chain Live execution

## Context

Research can identify an economic opportunity without possessing signers or a
broadcast path. Live execution must reuse the same immutable market evidence
while adding executable validation, inventory ownership, durable intent,
parallel broadcast, confirmation, and safe recovery. The hot path must avoid
avoidable network calls and dependent quote calculations.

## Decision

Live is a separate composition root. Research continues to have no structural
access to signers or broadcasters. Both legs use independently fixed inputs:
the buy leg uses a configured quote-asset notional and the sell leg uses that
notional divided by a shared cached base-asset valuation. Discovery quotes run
concurrently.

Provider discovery and executable validation are distinct effects. An
opportunity is validated with a fresh executable artifact while its local leg
is recalculated concurrently. Validation outputs replace discovery outputs but
cannot resize either input. An invalid, rate-limited, or locally stale
validation artifact aborts or restarts validation before persistence.

Each chain and account has one transaction manager that owns signer state,
nonce or blockhash metadata, fee inputs, broadcast, and reconciliation. Those
inputs are warmed before the hot path. Signed payloads exist only in memory.

The operational store uses SQLite WAL with configurable `FULL` or `NORMAL`
synchronous mode; production Live defaults to `FULL`. It commits the economic
intent, inventory reservation, and transaction identities before emission.
After a successful commit there is deliberately no market, profitability,
timeout, block-height, or RPC guard. Both already-signed transactions are
released concurrently. A partial or unknown outcome never causes an automatic
resend and opens the manual circuit breaker while reservations are retained.

On-chain status, block-height, and blockhash validity belong exclusively to
post-broadcast reconciliation. Confirmed economic effects settle inventory
using observed amounts rather than expected amounts.

## Consequences

The durable commit adds measurable latency but prevents an emitted operation
from lacking recovery evidence. Removing all post-commit decision work keeps
the interval between commit return and parallel broadcast minimal and
testable. Independent inputs reduce reaction latency but intentionally create
base-asset inventory variation, which is recorded and valued separately from
quote-asset proceeds.

Only one operation per setup may be active in the initial version. Partial
failure stops new operations and requires reconciliation followed by manual
action. Automatic exposure reduction remains a later, separately reviewed
capability.

## Alternatives

- Making the sell input depend on expected buy output was rejected because it
  serializes discovery and validation.
- Persisting signed payloads was rejected because recovery must rebuild from
  economic intent and current chain state.
- Checking block height before broadcast was rejected because it adds a
  network dependency to the critical path without proving expiry.
- Revalidating after the durable commit was rejected because it creates an
  unbounded committed-but-not-emitted interval.
- Automatic resend or hedging after an unknown result was rejected because it
  can duplicate exposure before non-execution is proven.
