# Versions, artifacts and publishing

## Initial policy

Use one coordinated product version for implemented packages in 0.x, plus independent protocol version 1. This simplifies the fourteen core SDK/backend combinations while one maintainer is building the project. New independent release trains require an ADR once there is a reason. Do not publish empty planned packages to imitate version alignment.

Pre-release example: root tag `v0.1.0-alpha.1`, NuGet/C++ version `0.1.0-alpha.1`, Python metadata `0.1.0a1` under PEP 440. Stable `v0.1.0` maps to `0.1.0` everywhere. Generate/validate these mappings centrally, with tests; never rewrite an existing release tag or registry version. Product version does not promise all experimental backends are stable.

Maintain a changelog with added/changed/fixed/security/breaking sections and per-SDK/backend compatibility notes. A fix changing distributed behavior deserves a protocol-impact review even if a compiler sees no API change. Document coexistence of old/current releases and coordinated migration for incompatible storage protocols.

## Distribution matrix

| Ecosystem | Deliverables | First consumption | Public distribution |
|---|---|---|---|
| .NET | nupkg + symbols/source metadata + XML docs + README/license | local folder feed, clean consumer | NuGet.org after owner setup and tested trusted publishing |
| Python | wheel + sdist + metadata/typing/license/readme | clean virtualenv installing built artifacts | TestPyPI where useful, then PyPI trusted publishing |
| C++ | versioned source archive, CMake config/targets, Conan recipe/test_package, vcpkg overlay port | installed CMake project; local Conan cache; overlay registry | release source assets; submit recipes/ports to their registries through normal review |
| TypeScript | tarball, declarations, exports, README/license | `npm pack` + clean install | owned npm scope, current trusted/staged publishing policy verified |
| Go | module source + correct module-path tags | clean module importing tagged source | source tags/proxy discovery, not a generic package-upload endpoint |
| Java | jars, sources, javadoc, POM and required signatures/metadata | clean Maven consumer | verified Central namespace and publisher credentials |
| Rust | crate archives, rustdoc, feature/MSRV matrix | cargo package + clean archive consumer | crates.io after package ownership and publishing setup |

ConanCenter/vcpkg inclusion is **not** equivalent to pushing a NuGet package. Recipes can be generated/tested locally without upstream acceptance. Registry review and namespace ownership remain external actions. Initial C++ support can be useful through GitHub source releases and local recipes before central inclusion.

## Release flow

A version manifest and release PR select exactly the packages, source commit, protocol compatibility and server/client test matrix. All required tests pass on that commit. Build once in an appropriate trusted clean environment; test the exact generated packages outside source trees; hash and stage those artifacts. Publish those same verified bytes, not a fresh untested build with a new dependency resolution.

Prefer GitHub-hosted isolated build jobs. Only the minimal publish job receives OIDC/credentials and approved environment permission. Publication is explicit, never on every push. Maintain a dry-run path that validates and stages without remote writes. No publish command should bypass a failed or incomplete required gate.

Use current official OIDC/trusted publisher support for NuGet/PyPI/npm where current registry and runner requirements are satisfied. Some first-package/namespace/bootstrap operations are owner-specific; document the exact current registry procedure rather than presuming every registry can create an unclaimed package in the same way.

## Partial failure and rollback

Publishing packages across ecosystems is not an atomic transaction across registries. Publish in dependency order, record per-artifact outcomes, and verify registry versions/metadata. A retry uploads only missing identical artifacts or stops on a mismatch. Never “repair” a version by replacing bytes or moving a tag. Do not mark a GitHub release complete while required registry artifacts are missing.

If a correctness/security flaw ships, follow registry-specific yank/unlist/deprecation capabilities where appropriate, disclose affected versions accurately and publish a new fixed version. Do not delete arbitrary user-accessible releases automatically. Owner approval is required for destructive remote actions.

## Readiness

Name/license ownership, repository settings, publisher identity, protected environment, required test evidence, package install tests, docs links, license notices, release notes and artifact checksums must be verified. `tools/release_readiness.py` initially enforces only evidence-state completeness and intentionally fails; Codex must expand real CI gates rather than treating JSON booleans as proof.

## 2026-10-03 scope extension

The initial SDK scope covers .NET, C++, Python, TypeScript, Go, Rust and Java. SDK and backend support statements must remain specific to the tested profile. Tests stay in this monorepo. Native language examples in `examples/style/` are not distributed SDK tests and never qualify a release.
