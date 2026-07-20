# Contributing

Vernier is currently maintained as a personal engineering project. Unsolicited
pull requests are not accepted by default.

For a non-sensitive bug or proposal, open an issue with the problem, expected
behavior, reproduction evidence, scope, and relevant constraints. Wait for an
explicit invitation before preparing code. The maintainer may request a patch or
external commit and reproduce the change on a project-owned branch after review.

Never report vulnerabilities, leaked credentials, or operational details in a
public issue. Follow [SECURITY.md](SECURITY.md) instead.

Project-owned changes use short branches, English Conventional Commit titles,
tests proportional to risk, and pull requests that explain problem, approach,
scope, validation, risks, and documentation.

All Go tests and test-only data live under the root `tests/` tree, mirroring the
production package layout. Production directories must not contain `_test.go`
files or `testdata/`. Tests exercise public behavior; production internals are
not exported solely to make them testable. `go run ./tools/verify` enforces this
layout.
