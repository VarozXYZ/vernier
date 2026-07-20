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

## Development

Vernier requires Go 1.25 or newer. The repository exposes one verification
entrypoint:

```console
go run ./tools/verify
```

Code contributions are closed by default. See [CONTRIBUTING.md](CONTRIBUTING.md)
and [SECURITY.md](SECURITY.md) before opening an issue.

## License

Licensed under the [Apache License 2.0](LICENSE).
