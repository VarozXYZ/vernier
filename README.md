# Vernier

Vernier is an open-source Go framework for researching cross-chain arbitrage
strategies with reproducible market state, exact arithmetic, and explicit data
quality.

The project is in its initial implementation phase. It does not execute trades,
manage wallets, or promise profitability. Its deterministic Research kernel can
explain every evaluated opportunity from immutable market snapshots.

## Status

- Architecture baseline: complete.
- Deterministic Research kernel: available with constant-product and
  concentrated-liquidity local market adapters.
- Read-only market observation: experimental, with configured EVM profiles,
  pool-filtered logs, and exact on-chain parity checks.
- Point-in-time cross-chain comparison: experimental for canonical Uniswap V2
  and the Aerodrome Slipstream V3 variant.
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
mirror. Explicit feed-liveness failures, such as a WebSocket disconnect,
degrade the mirror and produce `unclassifiable` results until reconnection
performs a full current-state bootstrap. A healthy WebSocket keeps the mirror
valid indefinitely: snapshots have no age expiry or finality gate.

The canonical V3 slice reduces normalized full-state, swap, liquidity, and
dynamic-fee events and quotes from immutable snapshots. On-chain ABI decoding
and bounded tick loading live in adapters; transaction construction is not
part of Research.

The adapter boundary is recorded in
[ADR 0001](decisions/0001-market-adapter-boundaries.md).

## Experimental Uniswap V3 observation

The read-only observer selects a canonical `uniswap_v3` market from the same
modular YAML used by cross-chain Research. Its WebSocket subscription is
restricted to one pool address and the Initialize, Swap, Mint, and Burn topics.
It does not subscribe to new heads, poll inactive blocks, infer gaps from
non-contiguous event blocks, sign, or broadcast.

Configure the chain, tokens, venue, and market in the private topology file;
the maximum sizing bound supplies the base-token coverage probe. Set the named
endpoint environment variable locally, then run:

~~~console
go run ./cmd/research observe-v3 --config config/local/vernier.yaml --market market_id --format jsonl --updates 1
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

## Experimental live cross-chain comparison

The private `compare-live` composition reads two configured markets at explicit
block hashes. It sizes in the base asset, models prepositioned inventory,
converts a fixed external cost through CoinGecko with Chainlink fallback,
evaluates both directions, and checks every local leg against the venue
reference quoter. Provider request pacing belongs to the EVM network layer,
not to either market adapter.

Configuration is modular YAML: a manifest selects topology and policy files,
while endpoint values and API keys remain in an ignored `.env` file. The
synthetic schema example under [examples/configuration](examples/configuration/)
contains no working networks or operational addresses. Private files belong
under ignored `config/local/`:

~~~console
go run ./cmd/research compare-live --config config/local/vernier.yaml --env-file .env --format text
~~~

The report contains configuration and snapshot hashes, exact quantities, cost
evidence, the complete sizing curve, and parity results. It never includes
configured addresses or endpoint values. The command is read-only and has no
signer or broadcast capability.

Compatible EVM networks share one implementation and differ through configured
identity, chain ID, and endpoint profiles. Protocol or network-specific code is
added only when behavior actually diverges. See
[ADR 0004](decisions/0004-modular-composition.md).

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
