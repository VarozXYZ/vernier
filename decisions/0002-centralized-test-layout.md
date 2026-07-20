# ADR 0002: Centralized test layout

## Context

Go convention colocates tests with production packages. Vernier instead wants
production directories to contain implementation only and one visible root for
all test code and test-only data.

## Decision

All `_test.go` files and `testdata/` live under `tests/`, mirroring the package
layout where useful. Tests consume public package APIs or execute public
commands. Production symbols are not exported solely to support tests. The
repository verifier rejects test files outside `tests/`.

## Consequences

The layout makes the production/test boundary immediately visible and forces
tests toward externally observable contracts. It also prevents same-package
tests from accessing unexported implementation details, so very small internal
algorithms may receive less direct coverage unless their behavior is reachable
through a public capability.

## Alternatives

Colocated external-package tests were rejected because they leave tests spread
throughout the production tree. Same-package tests were rejected because they
couple verification to internals and cannot be centralized in Go.
