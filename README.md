# Vernier

Vernier is an open-source Go framework for researching cross-chain arbitrage
strategies with reproducible market state, exact arithmetic, and explicit data
quality.

The project is in its initial implementation phase. It does not execute trades,
manage wallets, or promise profitability. Its deterministic Research kernel can
explain every evaluated opportunity from immutable market snapshots.

## Status

- Architecture baseline: complete.
- Deterministic Research kernel: available with constant-product and Uniswap
  V3 local market adapters.
- Read-only Ethereum observation: experimental, with filtered Uniswap V3 logs
  and exact local/QuoterV2 parity checks.
- Live execution: not implemented.

See [ROADMAP.md](ROADMAP.md) for the public delivery sequence.

## Demonstration

Run the versioned synthetic scenario without network access or external state:

```console
go run ./cmd/research --fixture examples/synthetic/two-market.json --format text
```

Use `--format json` for the deterministic audit report. The fixture format is
deliberately experimental and is not the future public configuration contract.
The report records its fixture hash, strategy and direction, exact quantities,
snapshot versions and hashes, local quotes, costs, classifications, and times.

The demonstration processes normalized events through one constant-product and
one Uniswap V3 adapter, publishes both through the same generic mirror, and
evaluates both directions for two strategies that share the infrastructure. The
V3 adapter performs exact-input integer quoting over active liquidity and
initialized ticks; it does not use a spot-price approximation.

Arrival order updates a mirror unless a feed-selected policy has comparable
source evidence (the fixture uses block number and known timestamp) proving an
event is older. Older events are ignored and audited without degrading the
mirror. Explicit feed-liveness failures, such as a WebSocket disconnect, degrade
the mirror and produce `unclassifiable` results until fresh data arrives.

The current V3 slice is deliberately local: it reduces normalized full-state,
swap, and liquidity events and quotes from immutable snapshots. ABI decoding,
RPC state loading, reorg handling, and transaction construction are future
adapter capabilities, not hidden dependencies of the quote path.

The adapter boundary is recorded in
[ADR 0001](decisions/0001-market-adapter-boundaries.md).

## Experimental Ethereum observation

The read-only observer uses the canonical Ethereum and Uniswap V3 adapters. Its
WebSocket subscription is restricted to one pool address and the Initialize,
Swap, Mint, and Burn topics. It does not subscribe to new heads, poll inactive
blocks, infer gaps from non-contiguous event blocks, sign, or broadcast.

Create a private ignored file under config/local/:

~~~json
{
  "schema_version": 1,
  "network_adapter": "ethereum",
  "venue_adapter": "uniswap-v3",
  "market_id": "local-market",
  "pool_address": "0x...",
  "quoter_v2_address": "0x...",
  "http_url_env": "VERNIER_ETHEREUM_HTTP_URL",
  "ws_url_env": "VERNIER_ETHEREUM_WS_URL",
  "token0_id": "token-0",
  "token1_id": "token-1",
  "quote_inputs": [
    {"token_in": "token-0", "amount": "1000000"},
    {"token_in": "token-1", "amount": "1000000"}
  ],
  "max_tick_words": 64
}
~~~

Set the named environment variables locally, then run:

~~~console
go run ./cmd/research observe-v3 --config config/local/pool.local.json --format jsonl --updates 1
~~~

The value updates=1 emits the bootstrap snapshot and waits for one block
containing a matching pool event. The value updates=0 runs until canceled.
Configuration, endpoints, and addresses are not included in output records.

The observer loads complete state only at startup and after a confirmed
WebSocket disconnection. Each active block is fetched once by exact block hash
to order its pool logs. Tick coverage is explicit; the local quoter fails
closed and loads additional bitmap words when a configured probe needs them.
Every emitted quote is compared exactly with QuoterV2 at the same block hash.

Canonical adapter reuse and network/fork boundaries are recorded in
[ADR 0003](decisions/0003-canonical-chain-and-market-adapters.md).

## Development

Vernier requires Go 1.25 or newer. The repository exposes one verification
entrypoint:

```console
go run ./tools/verify
```

Code contributions are closed by default. See [CONTRIBUTING.md](CONTRIBUTING.md)
and [SECURITY.md](SECURITY.md) before opening an issue.

Tests and test-only data are centralized under `tests/`; production package
directories contain implementation only.

## License

Licensed under the [Apache License 2.0](LICENSE).
