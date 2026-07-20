# Vernier

Vernier is an open-source Go framework for researching cross-chain arbitrage
strategies with reproducible market state, exact arithmetic, and explicit data
quality.

The project is in its initial implementation phase. It does not execute trades,
manage wallets, or promise profitability. Its deterministic Research kernel can
explain every evaluated opportunity from immutable market snapshots.

## Status

- Architecture baseline: complete.
- Deterministic Research kernel: available.
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

The demonstration processes normalized events into constant-product mirrors,
takes immutable snapshots, and evaluates both directions for two strategies
that share the same market infrastructure. Arrival order updates a mirror unless
comparable source metadata (currently a block number or known timestamp) proves
an event is older, in which case it is ignored and audited without degrading the
mirror. Explicit feed-liveness failures, such as a WebSocket disconnect, degrade
the mirror and produce `unclassifiable` results until fresh data arrives.

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
