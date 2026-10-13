# Contributing to Dilock

Dilock is experimental and under active development. Start with the SDK you know; a contributor should not need every language toolchain. Use the [human documentation index](docs/humans/README.md) to find project contracts and contributor-facing technical references.

Before changing behavior, read `spec/SEMANTICS.md`, the relevant backend profile and the SDK's README. Protocol changes, guarantees or public API changes need an issue/ADR explaining the intended behavior and compatibility impact. Small bug fixes should include a reproducer and a regression test.

Keep PRs focused. Include actual tests run, unavailable prerequisites, API/docs updates, package-consumer impact and any protocol/backend-version consequences. Do not claim that an unavailable integration test passed. Never commit credentials, production endpoints or private user data.

Use the language's formatter and documentation conventions. Safety-critical code needs an explanation of invariants, not just line coverage. Do not copy another project's implementation without checking its license and preserving required attribution.

Commit messages should be understandable and may use conventional prefixes (`feat`, `fix`, `docs`, `test`, `build`). No mandatory DCO/CLA is imposed by this blueprint; adding one is a governance decision that must be documented.

Report suspected vulnerabilities privately through the configured repository security reporting route once the repository is published; do not put exploit details or secrets in a public issue. See `SECURITY.md`.
