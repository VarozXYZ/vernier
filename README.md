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
- Cross-chain route composition: experimental, with multi-hop immutable
  snapshots, Solana `logsSubscribe` feeds, Meteora DLMM and Orca Whirlpool
  local integer quoters, and optional Jupiter v2 reference validation.
- Point-in-time cross-chain comparison: experimental for canonical Uniswap V2
  and the Aerodrome Slipstream V3 variant.
- Live execution: not implemented.

See [ROADMAP.md](ROADMAP.md) for the public delivery sequence.

## Demonstration

Run the versioned synthetic scenario without network access or external state:

```console
go run ./cmd/research --fixture examples/synthetic/two-market.yaml --format text
```

Use `--format json` for the deterministic audit report. All maintained input
files are YAML; JSON remains an output and external-protocol format. The fixture
schema is deliberately experimental and is not the stable configuration contract.
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
the maximum sizing bound supplies the coverage probes. Set the named
endpoint environment variable locally, then run:

~~~console
go run ./cmd/research observe-v3 --config config/local/vernier.yaml --market market_id --format jsonl --updates 1
~~~

The value updates=1 emits the bootstrap snapshot and waits for one block
containing a matching pool event. The value updates=0 runs until canceled.
Configuration, endpoints, and addresses are not included in output records.

The observer loads complete state only at startup and after a confirmed
WebSocket disconnection. Venue adapters decode filtered logs individually in
subscription arrival order; same-block events are therefore applied one by
one, while provably older blocks are ignored. Venues that require block-level
correlation may use the exact-block fallback. Tick coverage is explicit; the
local quoter fails closed and loads additional bitmap words when a configured
probe needs them.
Every emitted quote is compared exactly with QuoterV2 at the same block hash.

Canonical adapter reuse and network/fork boundaries are recorded in
[ADR 0003](decisions/0003-canonical-chain-and-market-adapters.md).

## Experimental live cross-chain comparison

The `compare-live` composition reads two configured markets at explicit
block hashes. It sizes in the quote asset by default, models prepositioned inventory,
converts a fixed external cost through CoinGecko with Chainlink fallback,
probes both markets locally before sizing, and checks selected local legs
against the venue reference quoter. Provider request pacing belongs to the EVM
network layer, not to either market adapter.

The live strategy first runs three local quote-asset probes at evenly spaced
points across the configured sizing range (minimum, middle, and maximum for
the default). The market returning more base asset for the same quote budget
wins a strict majority decision and becomes the buy leg; the other market is
the sell leg. The full sizing curve is then evaluated only in that direction,
so probe quotes can be reused from the local cache. A tie, equal output, or
failed/incomplete probe safely falls back to evaluating both directions.
The policy is recorded in [ADR 0007](decisions/0007-early-direction-discovery.md).

Configuration is modular YAML: a manifest selects topology and policy files,
while endpoint values and API keys remain in an ignored setup profile such as
`.env.virtual` or `.env.private`. Environment keys are deliberately short and
describe the provider directly (`ROBINHOOD_WS_URL`, `SOLANA_HTTP_URL`,
`JUPITER_TAKER`); the selected filename provides setup isolation. VIRTUAL
across Robinhood Chain and Base is the deliberately public reference setup in
[examples/setups/virtual](examples/setups/virtual/). Its Base market uses the
canonical Aerodrome volatile pool adapter; changing the pool, factory, router,
fee, or adapter kind is a topology-only change. Run it with local endpoint
variables:

The sizing policy uses `asset: quote` by default, so the configured bounds are
WETH budgets for this VIRTUAL/WETH setup rather than VIRTUAL or ETH quantities.

~~~console
go run ./cmd/research compare-live --config examples/setups/virtual/vernier.yaml --env-file .env.virtual --format text
~~~

For continuous read-only observation, use the same setup with pool-filtered
WebSocket logs. The first report is emitted after both pools complete their
current-state bootstrap; subsequent reports are triggered by accepted events
or an explicit disconnect health change:

~~~console
go run ./cmd/research compare-live --config examples/setups/virtual/vernier.yaml --env-file .env.virtual --stream --updates 1 --format jsonl
~~~

Stream mode persists only economically meaningful opportunity windows. The
default local store is `.vernier/opportunities.sqlite`; override it with
`--opportunity-store` or pass an empty value to disable persistence. Events,
snapshots, quote curves, and reconnect attempts are not stored. A window opens
on `economic` or `policy_qualified`, records its best observed trade, and
closes when profitability disappears. A confirmed WebSocket disconnect marks
the active window failed; the reconnect bootstrap starts a new continuity.

Inspect the persisted history without starting a network feed:

~~~console
go run ./cmd/research windows --store .vernier/opportunities.sqlite --format text
go run ./cmd/research windows --store .vernier/opportunities.sqlite --status failed --format json
~~~

Diagnostics are written to stderr so they never alter the report on stdout.
Their timestamp prefix is compact and local: `YYYY-MM-DD/HH:MM:SS/milliseconds`;
the `time=` label, timezone suffix, and extra precision are intentionally
omitted.
Use `--log-level debug` when investigating startup or feed behavior; the
default `info` level reports configuration, network readiness, bootstrap
duration, accepted events, evaluation triggers, reconnects, and failures. To
inspect a JSONL run without interleaving diagnostics in the terminal, redirect
the two streams separately, for example:

~~~console
go run ./cmd/research compare-live --config examples/setups/virtual/vernier.yaml --env-file .env.virtual --stream --updates 1 --format jsonl > report.jsonl 2> stream.log
~~~

The default report is an evaluation summary: it shows the selected size and
net result for each direction without repeating run/config metadata or every
sizing sample. Add `--calculations full` when auditing the complete curve,
quotes, costs, snapshots, and parity evidence. This output switch is separate
from `--log-level debug`, which is reserved for runtime diagnostics.

### Running a configured route

The same command starts Research for any private modular manifest. Keep that
manifest and its topology/policy files outside the public tree, and provide
only endpoint, taker, and optional provider-key values through a setup-specific
profile such as `.env.private`:

~~~console
go run ./cmd/research compare-live --config <private-manifest.yaml> --env-file .env.private --format text
~~~

For a live, read-only run use the pool-filtered subscriptions and emit one
report per accepted route update:

~~~console
go run ./cmd/research compare-live --config <private-manifest.yaml> --env-file .env.private --stream --updates 1 --format jsonl --log-level info
~~~

Startup validates every configured endpoint, bootstraps every hop in both
markets, and waits for complete route snapshots before the first local
evaluation. The point-in-time command exits after that evaluation. Stream mode
then starts one feed per hop; an accepted event identifies its triggering hop,
updates only that mirror, and reevaluates the composed route. A configured
external quote source is called only after local sizing has selected the best
candidate, on a background path, so its latency cannot delay or change the
local Research result. No signer, broadcast, bridge, or raw-event persistence
is reachable from this command.

Stream mode caches the external cost evidence and never performs venue parity
RPC calls on the event loop. A healthy WebSocket has no age expiry. Events
proven older by block evidence are ignored; a confirmed disconnect degrades
the affected mirror, and reconnect performs a full bootstrap before healthy
reports resume. Use `--updates 0` to run until canceled.

The strategy also caches local quote results per market state, direction, and
sizing amount. An event on one pool invalidates only that pool's quote curve;
the unchanged market is reused and only the cross-market profitability is
recomputed. The cache retains the latest state per market, and reused quotes
are rebound to the current snapshot version for auditability.

With `--calculations full`, the report contains configuration and snapshot
hashes, exact quantities, cost evidence, the complete sizing curve, and parity
results. The default summary never includes configured addresses or endpoint
values. The command is read-only and has no signer or broadcast capability.

Quote evidence states whether each leg is `exact_input` or `exact_output`.
Uniswap V2 exact-output purchases are checked against router `getAmountsIn`;
exact-input legs are checked against the corresponding venue output function.
Results and classifications depend on the market snapshot and are not a claim
that an opportunity exists.

Compatible EVM networks share one implementation and differ through configured
identity, chain ID, and endpoint profiles. Protocol or network-specific code is
added only when behavior actually diverges. See
[ADR 0004](decisions/0004-modular-composition.md).

Solana topology uses separate `http_url_env` and `websocket_url_env` values.
Pools are independent from venues, and a `path` lists ordered hops. A healthy
pool-log subscription has no TTL or slot-gap rule; only a confirmed WebSocket
disconnect degrades it, and reconnect bootstraps current state without
backfill. A market may optionally set `reference_quote` to a modular
`quote_sources` entry of kind `jupiter`. For each accepted event, Research
first emits the complete local curve and selected size; only then does a
background validation request the selected leg. The follow-up record contains
the local output, external output, signed raw delta, local quote duration,
HTTP latency, total validation time, context slot, and any unavailability
reason. The external result never changes the local
classification and never signs or broadcasts a transaction.

The topology wiring is intentionally small and reusable:

```yaml
markets:
  market_external:
    path: configured_path
    reference_quote: external_quote
quote_sources:
  external_quote:
    kind: jupiter
    taker_env: PUBLIC_TAKER
    slippage_bps: 50
    max_accounts: 64
```

The taker and optional API key are environment values; no credentials belong
in YAML, fixtures, reports, or commits.

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
