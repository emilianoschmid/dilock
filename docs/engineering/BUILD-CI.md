# Build and CI strategy

## Native builds first

The local release command is `tools/release.py`. Its delivery catalog is
`tools/release-catalog.json`. It delegates to native tools in pinned Docker
images, retains failures, and runs exact-package consumers separately. The manual
`.github/workflows/release-candidate.yml` stages candidates without publishing;
hosted execution and runner provisioning must be verified before relying on it.

.NET uses normal restore/build/test/pack; Python uses normal build/test/type/lint commands; C++ uses configure/build/CTest/install. A contributor to Python should not install .NET and a C++ compiler. Root `tools/dev.py` will be a thin dispatcher, not a new build system. Do not introduce Bazel, Nx or a complex meta-build merely because the repository is polyglot.

The local dispatcher is implemented as `tools/dev.py`: `doctor`, `lint --sdk`, `test --sdk`, `integration --sdk --backend`, `interop --backend`, `faults --backend`, `pack --sdk`, `docs`, and `verify-release`. `faults --backend` runs native-worker n-way contention, killed-owner expiry, and renewed-owner process-kill checks; it expects the selected real backend to be running and accepts credentialed endpoints through `--endpoint` without echoing them in the command log. `doctor` checks selected executables and reports missing prerequisites; it does not install tools or change Docker settings. The wrappers print their native commands and preserve command exit codes. This interface is local developer tooling, not an active CI gate; some SDK/backend combinations intentionally return an explicit unsupported-operation error until a native integration command is configured. Use each SDK README for its native build and test commands when a wrapper's general command does not match that SDK's setup.

The active pull-request workflow is `.github/workflows/pr.yml`. It runs on every pull request, merge-queue group and push to `main`; it has a source/contract job, a seven-SDK native test matrix, and an always-running aggregate gate that fails unless both jobs succeed. It grants only `contents: read`, disables persisted checkout credentials and does not use secrets, OIDC, publishing or remote provisioning. Actions are pinned to full SHAs verified on their official upstream release pages.

The PR workflow is designed to run language unit/native builds without Redis or etcd services. Backend integration, cross-SDK conformance, installed-package consumers and public API examples require separate service-backed jobs. Static workflow validation does not demonstrate that GitHub Actions executed successfully.

The active PR workflow does not enable publishing. Release preparation and publishing remain separate owner-reviewed steps.

## Toolchain pinning

M0 creates a machine-readable toolchain/dependency manifest: exact SDK/CLI/build-tool versions, minimum supported runtime/client/server versions, Docker image tags+digests and source URLs checked on that date. Use stable releases; no floating `latest`, invented SHA or unchecked scheduled release.

Examples of files to generate: `global.json`, central .NET package/build configuration, Python development lockfiles and `requires-python`, CMake presets plus a pinned dependency baseline, a CI toolchain manifest. Public library dependency ranges should reflect supported compatibility, not blindly freeze all transitive consumer dependencies. Test minimum and current resolved dependency sets.

## GitHub Actions

1. PR workflow: format/lint, unit/API/golden checks, affected integration and package-consumer smoke. Default `contents: read`; no publishing secrets or OIDC write permission.
2. Always-running aggregate gate: interpret results of selected jobs and fail when an expected test job failed, was cancelled, or was unexpectedly skipped. Avoid required status checks that remain pending because an entire path-filtered workflow never started. If merge queue is enabled, cover its event and ref semantics.
3. Main workflow: complete integration and cross-language matrix appropriate to cost; docs preview/build.
4. Scheduled/manual fault workflow: clustered backends, pauses/partitions and reproducible stress. Record failures; do not make a safety failure informational merely to keep main green.
5. Release workflow: validate immutable version/tag/commit, build in trusted clean jobs, test exact artifacts, stage, require approval, publish, read back version/package metadata and record hashes.

Path planning should conservatively include all SDKs for common-protocol changes. Add unit tests for the change planner. Documentation code samples trigger their compile/example tests even when only Markdown changed.

## Security

Pin actions to full verified commit SHAs, with comments identifying upstream releases; update through reviewed dependency automation. Avoid privileged `pull_request_target` execution of fork code. Never execute an untrusted checkout with release secrets, elevated GITHUB_TOKEN or id-token permissions. No unreviewed PR code on persistent self-hosted runners.

Separate dependency/build execution from publishing credentials. Use job-level least privilege, protected release environments and short-lived registry credentials/OIDC where supported. Do not interpolate issue titles, PR text or arbitrary version strings directly into shell scripts; validate inputs and pass safe arguments/environment variables. Keep credentials out of caches/artifacts. Treat restored caches and downloaded artifacts as untrusted unless bound to the expected workflow/commit and verified.

Generate provenance/attestations where supported and an SBOM for releases; include source commit, dependencies, license metadata and artifact checksums. Do not claim reproducible binary builds unless tested; pinned/repeatable inputs alone are not proof of bit-for-bit reproducibility.

Before changing CI security or publisher configuration, verify the current primary documentation from GitHub and each target package registry.

## 2026-10-03 scope extension

The initial SDK scope covers .NET, C++, Python, TypeScript, Go, Rust and Java. SDK and backend support statements must remain specific to the tested profile. Tests stay in this monorepo. Native language examples in `examples/style/` are not distributed SDK tests and never qualify a release.
