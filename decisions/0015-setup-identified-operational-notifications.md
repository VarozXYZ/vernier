# Setup-identified operational notifications

## Context

Several strategy runtimes can deliver alerts to the same operator channel. A
generic title such as `LIVE · COMPLETE` does not identify which setup completed
or failed, making incident triage unnecessarily ambiguous.

## Decision

Runtime composition resolves a human-readable setup label from the configured
base-token symbol and passes it once to the notification adapter. The adapter
includes that label in every message title it renders, covering Research,
tracking, configuration warnings, lifecycle, execution, recovery, canary, gas,
and inventory alerts.

Provider-neutral event payloads remain free of presentation concerns. Existing
callers that instantiate an adapter directly retain a compatibility fallback,
while all repository-owned runtime compositions provide the resolved label.

## Consequences

- Every operational alert emitted by a composed runtime is attributable to one
  setup without inspecting transaction details.
- New event kinds inherit setup identification automatically at the rendering
  boundary.
- Channel adapters remain responsible for escaping and formatting the label.

## Alternatives

- Adding the setup label independently to every event was rejected because a
  producer could omit it and durable outbox payloads would duplicate immutable
  runtime configuration.
- Encoding market addresses or topology in titles was rejected because it is
  noisy and can disclose private configuration.
