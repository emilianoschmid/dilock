# Native packages and publication

Keep a monorepo, not one repo for each language/backend pair. Every backend is an optional dependency. Sharing the repository does not force a shared build tool or require every compiler for local development. Package names here are candidates until verified.

| SDK | Units to build | Local release checks | Publication route |
|---|---|---|---|
| .NET | Dilock.Core / Redis / Etcd | pack nupkg + symbols; SourceLink; XML docs; clean restore | NuGet, configured trusted publishing |
| Python | `dilock` wheel with optional Redis/etcd extras | wheel + sdist; py.typed; clean venv | PyPI, configured trusted publisher |
| C++ | CMake component targets | install/export + consumer; Conan test_package; vcpkg overlay | release source/recipes; central registry review is separate |
| TypeScript | private local candidates `@dilock/core`, `@dilock/redis`, `@dilock/etcd`; verify npm scope ownership before release | declarations + ESM; independent npm packs; core-only consumer with no backend clients; exact Redis/etcd tarball consumers with isolated dependency closures | npm, configured trusted publisher where supported |
| Go | local candidates `example.invalid/dilock/core`, `/redis`, `/etcd` (all owner identity unverified); conformance worker is a fourth local tool module | workspace tests for core/Redis/etcd; vet; separate core-only, Redis-only, etcd-only and combined consumer dependency-closure checks; verify module path/subdirectory tags before release | signed/reviewed Git tags on public source, module proxies index it; publish only after replacing local paths with verified owner-qualified modules |
| Rust | dilock / dilock-redis / dilock-etcd crates | cargo package/dry-run; feature and MSRV tests | crates.io, current supported identity/credential setup |
| Java | `org.dilock:dilock-core`, `org.dilock:dilock-redis`, `org.dilock:dilock-etcd` (local candidate coordinates; group ownership unverified) | Reactor tests/package; main + sources + Javadoc JARs; isolated core-only, Redis-only and etcd-only Maven consumers; verify each adapter excludes the other backend client | Maven Central only after groupId ownership and release identity are verified; no publication configured |
| Kotlin/Scala | idiomatic facades | prove same Java engine; coroutine/effect tests | Maven Central, Scala cross-version artifacts where needed |
| Ruby | core/adapter gems | gem build; package consumer | RubyGems |
| PHP | Composer packages | composer validate/test; dist consumer | Packagist + VCS tags |
| Swift | SPM products | package resolve/build/test; tagged repository consumer | Git tags / SPM manifest |
| Dart | core/adapter pub packages | analyze/test; pub publish --dry-run | pub.dev |
| Elixir | Mix projects | mix test/docs/hex.build | Hex.pm |
| C | C ABI library/headers | install/pkg-config; ABI and allocator consumer | CMake/Conan/vcpkg/source release |

## Identity and release configuration

The machine-readable delivery catalog is in
[`tools/release-catalog.json`](../../tools/release-catalog.json). Each SDK's
checked-in consumer driver documents its exact-artifact checks. Local package
versions remain unchanged until final coordinates and ecosystem prerelease
mappings are reviewed.

A GitHub repository owner, npm scope, Composer vendor and Maven groupId cannot be invented as if already owned. Record them in project metadata and produce local packages without publishing. Domain ownership is not automatically conferred by choosing a display name. No empty registry reservations.

Use coordinated release versions for the first supported set, with explicit mappings for NuGet/npm prereleases, PEP 440, Go semver tags, Cargo and Maven. Track wire protocol version separately. Unimplemented packages are excluded from a release rather than stamped with the same version and uploaded empty. Extended SDKs may enter a later coordinated release after review.

For Go, choose the final module paths before publication; a module in a repository subdirectory needs its corresponding subdirectory-prefixed version tag. Do not claim the repo root tag versions every nested module. Incompatible major versions need Go's module-path/version treatment. Do not depend on a local `replace` in published consumers.

For Swift, SPM normally resolves a manifest at a repository root: design and test either an appropriate root Package.swift exposing sdk/swift paths or a generated publication mirror. Do not claim pointing SPM at an arbitrary monorepo subfolder works. A mirror is a publication artifact, not another independently edited source of truth.

## Secure automation

Build/test automatically on PRs and pushes. Publish only from an approved release workflow after owner configuration. Resolve actions to verified full commit SHAs, lock toolchain versions/images/dependencies, set minimum permissions and avoid privileged execution of untrusted PR code. Separate build/test from credentialed publish. No automatic installation of arbitrary unreviewed toolchains by sample runners.

Prefer configured trusted publishing/OIDC where the target registry currently supports it; do not invent OIDC support for every registry. Current npm/runner minimums, NuGet first-publication setup, Maven namespace/signing requirements and all ownership actions must be verified from registry docs during M0/M6. No token in repo or prompt. Never reuse credentials from another project.

Build once, record hashes, scan/SBOM where applicable, test those staged artifacts, approve, then publish the same artifacts. Registries are not an atomic transaction: track each published package; retries must not rebuild or overwrite immutable versions. Leave a precise recovery plan for partial publication.

Before implementing a publisher, verify its current supported authentication, namespace ownership, signing, and trusted-publishing requirements in the registry's official documentation.
