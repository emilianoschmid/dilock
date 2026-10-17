# Dilock for Ruby

Dilock implements the draft 1 Redis single-authority lease, etcd native-lock, and Consul KV/session profiles. It is a client library, not a coordinator service. The Ruby SDK uses the maintained `redis` Ruby client and the official gRPC Ruby transport with wire-compatible bindings generated from the etcd v3.7.2 API subset. The Consul adapter uses Ruby's standard-library HTTP client and requires no additional gem. No coordinator-side script installation is required: Redis runs the bundled owner-checked Lua scripts as ordinary `EVAL` requests.

This candidate requires Ruby 3.2 or later. Add `dilock` to a Bundler dependency set; backend clients are declared runtime dependencies. The candidate gem version is `0.1.0`. The RubyGems name and owner have not been verified, and no gem is configured for automatic publication.

## Redis

Redis namespace metadata must be provisioned once before clients connect. The
normal application path uses the finite defaults (5-second acquisition wait,
30-second lease, and 2-second RPC and cleanup budgets) and never provisions
metadata as a connection side effect:

```ruby
require "dilock"

provider = Dilock::RedisProvider.connect(
  "redis://127.0.0.1:6379/0",
  namespace: "billing",
  epoch: ENV.fetch("DILOCK_EPOCH")
)
begin
  result = provider.lock("invoice-reconciliation").with_lease do |lease|
    # Replace with a short, idempotent operation; expiry cannot stop stale work.
    puts "Redis lease held: #{lease.held?}"
  end
ensure
  provider.close
end
```

Only for an explicitly disposable development keyspace, an operator may use
the create-if-absent helper once before starting the application:

```ruby
Dilock::RedisProvider.provision_namespace!(
  "redis://127.0.0.1:6379/0",
  namespace: "billing",
  epoch: "local-dev-redis"
)
```

For a fully explicit Redis policy, pass a complete acquisition policy and the
provider RPC/cleanup budgets. Durations are milliseconds:

```ruby
policy = Dilock::AcquireOptions.new(wait_ms: 3_000, lease_ms: 20_000)
provider = Dilock::RedisProvider.connect(
  "rediss://cache.example:6380/0",
  namespace: "billing",
  epoch: ENV.fetch("DILOCK_EPOCH"),
  rpc_timeout_ms: 1_000,
  cleanup_timeout_ms: 1_000,
  default_acquisition: policy
)
begin
  provider.lock("invoice-reconciliation").with_lease(policy) do |lease|
    puts "Redis lease held: #{lease.held?}"
  end
ensure
  provider.close
end
```

For deployment, provision `/dilock`-style metadata according to [`spec/NAMESPACE-METADATA.md`](../../spec/NAMESPACE-METADATA.md) using the organization's approved Redis administration process; applications must not create or overwrite it. Use a protected non-evicting keyspace. This profile does not promise strong fencing or mutual exclusion across unsafe failover, eviction, manual deletion or state loss.

The provider owns two Redis clients: one for ordinary bounded requests and one with an independent cleanup timeout. `close` releases those client resources; do not close it concurrently with active operations. A lock handle is reusable. Each lease is one acquisition generation and lifecycle mutations are serialized. `try_acquire` returns `nil` only for ordinary contention. `acquire(wait_ms, options)` raises `AcquisitionTimeout` when its contention wait budget expires; transport errors and ambiguous post-send results raise typed `Dilock::Error` subclasses. `renew` is explicit. `with_lease` uses `ensure`; if normal work finishes but release confirms the owner is gone, it raises `LeaseLost`. Unknown cleanup is reported without replacing an exception raised by the block.

## etcd

The etcd adapter uses native v3 `Lock`/`Unlock`, one server lease per attempt, keepalive while waiting, an exact returned-key `Range` verification, and the attached key's `create_revision` as an epoch-scoped fence. Namespace metadata must already exist and match. For example, the operator can provision the exact JSON value from `spec/NAMESPACE-METADATA.md` at `/dilock/namespaces/billing/meta`; the SDK never creates it. `try_acquire` raises `UnsupportedCapability` because the native API cannot safely distinguish contention from an RPC deadline.

The common path uses the finite defaults (5-second wait, 30-second lease,
2-second RPC and cleanup budgets). The `default` namespace and matching epoch
must be provisioned before connecting:

For etcd, `rpc_timeout_ms` bounds the lease grant, keepalive and ownership
verification RPCs. The blocking native `Lock` RPC instead receives the
remaining acquisition wait budget, so a queued acquisition can wait longer
than the generic RPC timeout but never beyond its requested wait deadline.
Release first linearly ranges the exact opaque lock key and verifies its lease
ID before calling native `Unlock`. It reports `:released` only after that
ownership proof and positive acknowledgements from both `Unlock` and
`LeaseRevoke`. A confirmed absent or mismatched key, a `LeaseRevoke`
`NOT_FOUND`, or a Revoke acknowledgement without the full release evidence
reports `:already_not_owned`; unresolved ownership or cleanup reports
`:unknown`. Range, Unlock, and Revoke share one monotonic cleanup budget. This
also handles expiry between Range and Unlock, which etcd may acknowledge as a
no-op.

```ruby
require "dilock"

provider = Dilock::EtcdProvider.connect(
  "http://127.0.0.1:2379",
  epoch: ENV.fetch("DILOCK_ETCD_EPOCH")
)
begin
  provider.lock("invoice-reconciliation").with_lease do |lease|
    fence = lease.fence
    puts "etcd fence: #{fence.epoch}/#{fence.revision}"
  end
ensure
  provider.close
end
```

For the fully explicit mode, configure TLS, connection deadlines and the
provider's default acquisition policy:

```ruby
require "dilock"

provider = Dilock::EtcdProvider.connect(
  "https://etcd.example:2379",
  namespace: "billing",
  epoch: "etcd-cluster-incarnation-7",
  rpc_timeout_ms: 1_000,
  cleanup_timeout_ms: 1_000,
  default_acquisition: Dilock::AcquireOptions.new(wait_ms: 3_000, lease_ms: 20_000),
  root_certificates: File.read("ca.pem"),
  private_key: File.read("client-key.pem"),
  certificate_chain: File.read("client-cert.pem")
)
begin
  provider.lock("invoice-reconciliation").with_lease do |lease|
    # The protected resource must enforce the fence atomically.
    puts "etcd fence: #{lease.fence.epoch}/#{lease.fence.revision}"
  end
ensure
  provider.close
end
```

The provider owns its gRPC channels. `EtcdFence` contains an approved epoch and the server creation revision; applications must have the protected resource atomically enforce it. A revision is not an eternal sequence after snapshot restore. No lease expiry stops a paused Ruby process. TLS credentials are optional but required when the endpoint is HTTPS and the server is configured for TLS.

## Consul

Consul namespace metadata must be provisioned explicitly before connecting. Consul sessions require a TTL from 10 seconds through 24 hours; the SDK rounds the configured lease up to whole seconds and includes a conservative local safety margin. Every attempt creates its own session. The KV lock entry remains in place after release so Consul's `LockIndex` cannot reset.

```ruby
require 'dilock'

Dilock::ConsulProvider.provision_namespace!(
  'http://127.0.0.1:8500', namespace: 'billing', epoch: 'consul-cluster-incarnation-1'
)
provider = Dilock::ConsulProvider.connect(
  'http://127.0.0.1:8500', namespace: 'billing', epoch: 'consul-cluster-incarnation-1',
  token: ENV['CONSUL_HTTP_TOKEN']
)
begin
  provider.lock('invoice-reconciliation').with_lease do |lease|
    fence = lease.fence
    puts "Consul fence: #{fence.epoch}/#{fence.lock_index}"
  end
ensure
  provider.close
end
```

`ConsulFence` is the pair `(epoch, LockIndex)` and must be enforced atomically by the protected resource. Session release behavior does not stop a paused process from resuming stale work. The adapter does not delete lock keys, and applications must not remove them during normal operation.

## Errors and concurrency

`Dilock::Error#category` is stable for programmatic classification (`:backend_unavailable`, `:protocol_mismatch`, `:acquisition_unknown`, `:acquisition_timeout`, `:unsupported_capability`, `:lease_lost`, and `:configuration_error`). `try_acquire` uses `nil` only for Redis contention. Backends and handles may be shared among Ruby threads, but a lease serializes its own renew/release operations; a caller must not concurrently close the provider while work is active. Fiber scheduling does not make blocking Redis or gRPC calls non-blocking. No finalizer performs network I/O.

For etcd, malformed Lock ownership data is reported as `ProtocolMismatch` only
after the attempt lease revoke is acknowledged. If cleanup is not confirmed,
the acquisition result is `AcquisitionUnknown` because a server-side lock may
still exist until that lease expires.

If an asynchronous `Interrupt` arrives after etcd grants the attempt lease,
the SDK stops its keepalive worker and attempts bounded lease revocation before
returning. It preserves `Interrupt` when revocation is acknowledged; if cleanup
is unconfirmed, it raises `AcquisitionUnknown` with the interruption as its
cause. A lease grant may have committed even when its response or later lock
wait is interrupted.

Redis acquisition does not return a lease after its caller wait deadline; it
attempts owner-checked cleanup and reports `AcquisitionUnknown` if that cleanup
is not acknowledged. Redis and etcd renewal responses cannot extend a lease
after the previously confirmed local deadline. A `LeaseLost` response moves
the lease to its terminal lost state.

## Local checks

```sh
cd sdk/ruby
bundle check
bundle exec rake test
bundle exec rubocop lib test examples bin
gem build dilock.gemspec
```

The integration tests require a real Redis or etcd endpoint. Set `DILOCK_TEST_REDIS_URL` or `DILOCK_TEST_ETCD_ENDPOINT`, plus `DILOCK_TEST_NAMESPACE` and `DILOCK_TEST_EPOCH`, to opt in. The Redis tests explicitly provision their unique test namespace through `provision_namespace!`; the etcd test requires its metadata record to be provisioned out of band. Example programs are real SDK consumers, not the isolated style demonstrations under `examples/style`.

Redis has a single-authority guarantee profile and no strong fence. etcd uses linearizable native locks and exposes an epoch-scoped revision fence. Neither adapter promises exactly-once external effects, automatic protection against stale processes, quorum/failover behavior beyond etcd's documented assumptions, or compatibility with unverified server/client configurations. See [`spec/SEMANTICS.md`](../../spec/SEMANTICS.md), [`spec/backends/REDIS.md`](../../spec/backends/REDIS.md), and [`spec/backends/ETCD.md`](../../spec/backends/ETCD.md).
