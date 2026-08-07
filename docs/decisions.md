# Decisions

One paragraph per non-obvious choice. Newest at the bottom.

## 2026-08-07 - Go, and the real Kafka wire protocol

Chose Go over Java/Rust/C++ for this project. Neither of us had used Go before; we picked it for goroutines
mapping naturally onto "one goroutine per client connection," and for the infra-ecosystem resume signal.
We chose to implement the real Kafka wire protocol rather than a custom one so that official clients
(`kafka-console-producer`, `kafka-python`, `franz-go`) connect without modification - that's the project's
core differentiator versus most "Kafka clone" repos, which stop at an in-memory queue behind a JSON API.

## 2026-08-07 - Building both tracks together, not split by person

The build plan's default is to split work into Track A (protocol/networking: `internal/protocol`,
`internal/broker`) and Track B (storage/state: `internal/storage`, `internal/group`), one person per track,
working in parallel. We're deviating from that: both of us are building both tracks together in the same
session. The track labels still matter for two reasons - they mark the seam where the `Log` interface
decouples protocol code from storage code, and they're still useful for resume framing (being able to speak
to "the protocol half" and "the storage half" separately). Every piece of work will still be identified as
belonging to Track A or Track B as it's built, even though there's no per-person ownership split.
