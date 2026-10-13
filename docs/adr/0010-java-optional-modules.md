# ADR 0010: Split Java into optional Maven backend artifacts

Status: accepted for the local SDK build; Maven coordinates remain publication candidates.
Date: 2026-10-04

## Context

The Java distribution previously placed the Redis and etcd clients in one JAR and one mandatory dependency closure. This made either backend require both client stacks and contradicted the repository package matrix.

## Decision

Build three independent artifacts in the Java Maven reactor: `org.dilock:dilock-core`, `org.dilock:dilock-redis`, and `org.dilock:dilock-etcd`. The adapter artifacts depend on core and their own backend client only. Shared `Identity` and `DilockException` retain their existing fully qualified class names in core to avoid changing the current API. Each artifact attaches source and Javadoc JARs during `package`.

The JSONL interoperability worker remains a local test utility. Maven includes its module only with the `conformance-worker` profile; it depends on both adapters and is not part of the default SDK reactor.

These group and artifact identifiers are local candidates. This decision does not establish namespace ownership or authorize publication. No publication configuration or remote action is added.

## Consequences

Consumers can depend on core, Redis, or etcd independently. The worker harness needs the opt-in reactor profile. Cross-language wire protocol, resource identity, ownership and backend behavior are unchanged. Consumers and local build evidence must verify that each adapter's dependency closure excludes the other backend client.
