# Observability, configuration and operational security

No telemetry leaves the process by default. Integrate with native logging/metrics facilities; optional OpenTelemetry adapters/exporters belong outside Core. Emit acquisition duration/outcome, renewal result/latency, live lease count, lease loss reasons and release outcomes. Do not use raw resource IDs, owner tokens or arbitrary names as metric labels: cardinality and information leakage matter. Detailed debug logging is opt-in and redacted.

Configuration is typed and validated. Backend connection config stays backend-specific rather than a universal string dictionary. Namespace and coordinator identity are explicit; connection aliases pointing to different servers do not create a shared domain. No automatic fallback to another coordinator, local mutex or in-memory provider on network failure. A fake backend is test-only and cannot be selected as silent production degradation.

Support authenticated/TLS connections through established clients. Do not log passwords, full credential-bearing URIs, ownership proofs or secret configuration files. Describe least-privilege backend ACL requirements; administrators provision namespace metadata/buckets/epochs. Runtime identity does not need broad administrative permissions merely to acquire a lock. No automatic destruction/recreation of an existing bucket, database or stream to match desired config.

Store dependency licenses and generated-source notices honestly. Include automated secret scanning and ecosystem vulnerability checks in CI, with an explicit triage process. A missing vulnerability result is not a clean bill of health. A lock library coordinates cooperative clients; it does not isolate mutually malicious applications holding the same backend credentials.

Examples and fault environments bind loopback/private test networks by default, use disposable data and refuse production-looking endpoints for destructive modes. Test credentials are labelled as non-production. Unknown cleanup may leave an orphan until TTL; report it without offering an unsafe force-unlock shortcut.
