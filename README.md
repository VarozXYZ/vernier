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
that share the same market infrastructure. Sequence gaps are reported as
explicit degradation and produce `unclassifiable` results; they are not hidden
as an absence of market opportunity.

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
