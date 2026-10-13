# Testing architecture and acceptance

## Placement

Unit/API tests live next to their SDK, using native tools. The shared repository owns `conformance/` data and worker contracts plus `tests/interop/` orchestration. There is no separate tests repository initially; a protocol change and the test proving it belong in one PR.

## Layers

| Layer | Purpose | Environment |
|---|---|---|
| Unit/state machine | transitions, duration math, cancellation, ownership races, errors | no backend; controlled time/randomness |
| Golden protocol | identical identity/value/error semantics | shared JSON vectors, independent SDK implementations |
| Adapter integration | actual atomic operations and server/client behavior | disposable Redis/etcd instances |
| Cross-language | A acquires; B contends; B succeeds after release; stale A cannot affect B | separately built/installed SDK worker processes |
| Fault integration | process pauses, dropped replies, partitions, failover, expiry | isolated Linux fault environment; real clustered topologies where claimed |
| Package consumer | artifact installs and documented examples actually work | clean consumer folder, no source-tree references |
| Compatibility | current SDK with supported older versions, API snapshots | released artifacts + mixed-version workers |

## Shared harness

Use Python to orchestrate worker executables, not as the production lock implementation. Each SDK provides a thin JSONL adapter under its test tree that calls its public API. The harness launches independently installed/built workers and speaks `conformance/WORKER-PROTOCOL.md`. It must not implement acquisition centrally on behalf of every SDK.

Worker stdout is protocol only; logs go to stderr with secrets redacted. Request IDs and concurrent command handling allow a blocked acquire and another command to coexist. Test names, seeds, config, server/client versions and source commit are captured.

The case catalog in `conformance/cases.json` is the initial acceptance matrix. Every required SDK/backend pair must execute every applicable case or explicitly explain why the case is not a claimed capability. Unsupported-capability tests still run. “Not applicable” is not “passed.”

## Safety oracles

Do not compare wall clocks from different hosts to assert mutual exclusion. Record invocation/response intervals and causal barriers; use a sequential-spec history checker or another justified oracle. A counter reaching the expected number is not proof of exclusion: the resource's own atomic increments could hide overlapping clients.

Include a protected-resource test sink that can atomically register/check fences and record operation IDs. In a stale-client scenario deliberately allow the paused worker to resume after expiry; verify that a newer registered fence rejects its write. For unfenced profiles, demonstrate the limitation instead of asserting that arbitrary stale work cannot run.

Worker events, backend traces and resource observations have different meanings. A green finite test run supports a documented profile; it is not a mathematical proof under all network schedules. Model-based tests and a small TLA+/PlusCal model can be added for lifecycle reasoning after the vertical slice, but must not replace real integration or become a prerequisite that prevents useful implementation.

## Failure coverage

Cancellation before send, during a queued request and after remote commit; timeout/lost ack; process kill; process pause longer than lease; event-loop starvation; slow GC-like pauses; old-owner renewal/release; provider shutdown races; stale reads; backend auth errors; queue/retry behavior; coordinator restart and supported leader failover; quorum loss; resource name Unicode/length boundaries; version mismatch; cleanup unknown; fencing epoch reset guard.

Use synchronization barriers rather than arbitrary sleep wherever possible. Real TTL tests still need bounded waits, tolerances and diagnostic deadlines. Repeat selected stress tests with published seeds. A flaky safety test is a defect: no automatic rerun-until-green, no blanket quarantining of safety cases.

## Matrix and cost

Fast PR: unit/API/golden/package metadata tests for changed SDKs. Backend smoke and affected pairwise interop on Linux. Changes in `spec/`, canonical Lua, conformance or common lifecycle rules trigger **all implemented SDK/backend pairs**. Release: complete matrix and package-consumer tests, no skipped required safety tests. Nightly: longer fault/stress tests and multi-node topologies. Windows/macOS cover SDK builds and supported operations; Linux is the first failure-injection platform, explicitly documented.

Use isolated ports/key namespaces per run. Never point destructive tests at user/production endpoints. CI tests own only their disposable processes/volumes. Test fault profiles are opt-in locally and clearly named.

## Evidence and gates

Evidence includes command, source commit, environment, start/end time, exit code, counts (pass/fail/skip), machine-readable report and artifact hashes. Store reports as CI artifacts and concise summaries in the repo, not huge logs or secrets. Target high core branch coverage, but coverage numbers are not the acceptance oracle. Every critical ownership branch needs a meaningful assertion.

Before declaring v0.1: all fourteen core SDK/backend combinations, old-owner tests, unknown outcomes, cancellation cleanup, real etcd three-member behavior for claims, package installation and executed docs examples must be evidenced. Any blocked test keeps that support claim unverified, not quietly supported.

## 2026-10-03 scope extension

The initial SDK scope covers .NET, C++, Python, TypeScript, Go, Rust and Java. SDK and backend support statements must remain specific to the tested profile. Tests stay in this monorepo. Native language examples in `examples/style/` are not distributed SDK tests and never qualify a release.
