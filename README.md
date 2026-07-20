# Vernier

Vernier is an open-source Go framework for researching cross-chain arbitrage
strategies with reproducible market state, exact arithmetic, and explicit data
quality.

The project is in its initial implementation phase. It does not execute trades,
manage wallets, or promise profitability. The first milestone is a deterministic
Research kernel that can explain every evaluated opportunity from immutable
market snapshots.

## Status

- Architecture baseline: complete.
- Research kernel: in development.
- Live execution: not implemented.

See [ROADMAP.md](ROADMAP.md) for the public delivery sequence.

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
