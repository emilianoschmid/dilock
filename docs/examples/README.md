# Distributed backend examples

These examples exercise the SDKs against Redis or etcd. They complement the
small [`examples/style`](../../examples/style/README.md) samples, which only
illustrate idiomatic language APIs and do not implement distributed locking.

Each backend quickstart demonstrates common defaults, full configuration, and
resource reuse where the SDK supports those patterns. The advanced consumers
show explicit lifecycle and error handling. Follow the SDK README for backend
requirements, metadata provisioning, cancellation, ownership, and fencing
limits. A successful local example does not establish production readiness.

| SDK | Redis | etcd |
|---|---|---|
| .NET | [Quickstart](dotnet-redis/quickstart/README.md), [consumer](dotnet-redis/README.md) | [Quickstart](dotnet-etcd/quickstart/README.md), [consumer](dotnet-etcd/README.md) |
| C++ | [Quickstart](cpp-redis/quickstart/README.md), [consumer](cpp-redis/README.md) | [Quickstart](cpp-etcd/quickstart/README.md), [consumer](cpp-etcd/README.md) |
| Python | [Quickstart](python-redis/quickstart/README.md), [consumer](python-redis/ADVANCED-CONSUMER.md) | [Quickstart](python-etcd/quickstart/README.md), [consumer](python-etcd/ADVANCED-CONSUMER.md) |
| TypeScript | [Quickstart](typescript-redis/quickstart/README.md), [consumer](typescript-redis/ADVANCED-CONSUMER.md) | [Quickstart](typescript-etcd/quickstart/README.md), [consumer](typescript-etcd/ADVANCED-CONSUMER.md) |
| Go | [Quickstart](go-redis/quickstart/README.md), [consumer](go-redis/README.md) | [Quickstart](go-etcd/quickstart/README.md), [consumer](go-etcd/README.md) |
| Rust | [Quickstart](rust-redis/quickstart/README.md), [consumer](rust-redis/README.md) | [Quickstart](rust-etcd/quickstart/README.md), [consumer](rust-etcd/README.md) |
| Java | [Quickstart](java-redis/quickstart/README.md), [consumer](java-redis/README.md) | [Quickstart](java-etcd/quickstart/README.md), [consumer](java-etcd/README.md) |

Quickstarts are runnable against local or disposable backend instances after
the required namespace metadata has been provisioned. They do not provision or
repair metadata, and they do not claim exactly-once effects or production
readiness.
