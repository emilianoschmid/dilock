# Dilock for Elixir

The Elixir SDK contains Redis and etcd backend source, protocol identity and a
supervised provider/lease API. Redis uses Redix. The etcd adapter calls stubs
generated from the bundled etcd v3.7.2 protocol definitions. The adapter source
uses native etcd Lock/Lease RPCs. Run the SDK tests against disposable
backends before relying on a particular deployment profile.

## Redis use

Start a provider beneath your application's supervisor. Namespace metadata must
already be provisioned by an administrator or the explicit development helper;
ordinary startup never creates or repairs metadata.

```elixir
children = [
  {Dilock.Redis,
   endpoint: System.fetch_env!("REDIS_URL"), namespace: "orders",
   epoch: System.fetch_env!("DILOCK_COORDINATION_EPOCH"), name: Dilock.RedisProvider}
]
{:ok, _supervisor} = Supervisor.start_link(children, strategy: :one_for_one)

provider = Process.whereis(Dilock.RedisProvider)
{:ok, lock} = Dilock.lock(provider, "orders", "invoice/123")
case Dilock.with_lease(lock, fn _lease -> IO.puts("Redis lease acquired; run a short idempotent operation here") end) do
  {:ok, result} -> result
  {:error, reason} -> raise "lock operation failed: #{inspect(reason)}"
end
```

`Dilock.with_lease/2` uses the 5-second wait and 30-second lease defaults.
For explicit budgets, pass a complete map in milliseconds; the values replace
the defaults for that acquisition. The provider's RPC and cleanup budgets are
separate connection settings:

For two resources under the same Redis authority, start one provider and reuse
it to create both lock handles before acquiring either one. The executable
example first uses `with_lease/2` defaults, then a per-acquisition policy with
`with_lease/3`; provider RPC and cleanup budgets are set explicitly. Each
handle is only a resource identity. Both use the provider's existing command
client and cleanup client, so two handles do not create two more Redis
connections:

```sh
mix run examples/redis_shared_provider.exs
```

An optional `cleanup_observer` provider option receives a map with `:backend`,
`:outcome`, and `:reason` when scoped or orphaned lease cleanup is not
acknowledged. This includes an acquisition that succeeds after its caller has
terminated and a lease process that exits before confirmed release. The
callback also reports an unacknowledged release when its caller terminates
before receiving the result. It runs asynchronously; callback failures are
ignored. If the work function raises,
`with_lease` preserves that exception and reports uncertain cleanup through the
observer. On normal return, it returns `{:error, {:cleanup_not_acknowledged,
outcome, value}}` when cleanup is not confirmed.
If release confirms that the acquisition is already no longer owned, it
returns `{:error, {:lease_lost, :already_not_owned, value}}` rather than
reporting scoped work as successful.

```elixir
policy = %{wait_ms: 3_000, lease_ms: 20_000}
{:ok, provider} = Dilock.Redis.start_link(
  endpoint: System.fetch_env!("REDIS_URL"),
  namespace: "orders",
  epoch: System.fetch_env!("DILOCK_COORDINATION_EPOCH"),
  rpc_timeout_ms: 1_000,
  cleanup_timeout_ms: 1_000,
  name: Dilock.RedisConfigured
)
try do
  {:ok, lock} = Dilock.lock(provider, "orders", "invoice/124")
  case Dilock.with_lease(lock, policy, fn _lease -> IO.puts("Redis lease acquired under explicit policy") end) do
    {:ok, result} -> result
    {:error, reason} -> raise "Dilock failure: #{inspect(reason)}"
  end
after
  GenServer.stop(provider)
end
```

## etcd use

The etcd adapter uses the native queuing Lock RPC and an epoch-scoped
`create_revision` fence. Unlike Redis contention, a native Lock wait deadline
is not reported as `:busy`; backend and timeout outcomes stay tagged errors.
Namespace metadata must be provisioned out of band before startup. The adapter
currently accepts plaintext HTTP endpoints only.

```elixir
{:ok, provider} = Dilock.Etcd.start_link(
  endpoint: "http://127.0.0.1:2379",
  namespace: "orders",
  epoch: System.fetch_env!("DILOCK_COORDINATION_EPOCH"),
  name: Dilock.EtcdProvider
)
try do
  {:ok, lock} = Dilock.lock(provider, "orders", "invoice/123")
  policy = %{wait_ms: 5_000, lease_ms: 30_000}
  case Dilock.with_lease(lock, policy, fn lease ->
         {:ok, %Dilock.Etcd.Fence{} = fence} = Dilock.Etcd.fence(Dilock.Lease.ownership(lease))
         IO.inspect(fence, label: "verified etcd fence; enforce it atomically at the resource")
       end) do
    {:ok, result} -> result
    {:error, reason} -> raise "Dilock failure: #{inspect(reason)}"
  end
after
  GenServer.stop(provider)
end
```

The shared-provider etcd example follows the same two-resource flow: both
handles are created from one provider before either lock is acquired, the first
uses `with_lease/2` defaults, and the second supplies explicit acquisition
budgets. Both reuse the provider's existing gRPC channel. The adapter prints
the verified epoch-scoped creation-revision fence for each resource.

```sh
mix run examples/etcd_shared_provider.exs
```

An explicit acquisition returns `{:ok, lease}`; release it with
`Dilock.Lease.release/1` and then `Dilock.Lease.close/1`. The lease process
monitors the acquiring process and requests bounded cleanup if that process
terminates. A provider should be started by OTP supervision and reused for
multiple resource handles. `try_acquire/2` returns `:busy` only for ordinary
contention; backend and uncertain outcomes are tagged errors. Acquisition waits
run in supervised tasks, so one blocked resource does not block provider
renewals or releases. The provider call waits for the adapter's bounded RPC
operation instead of timing out a local future and leaving a stale acquisition
response; terminating the acquiring process before a response triggers
owner-specific cleanup.

The Redis backend uses `SET key value NX PX ttl` and the canonical owner-token
Lua scripts bundled from `spec/backends/`. Redis runs these scripts itself; no
Lua installation or manually loaded script is required. Use a protected,
non-evicting single-authority keyspace. Redis replication failover/state loss
does not provide strict mutual exclusion, and the adapter exposes no strong
fencing token. Lease expiry does not stop an old process or fence its external
effects.

## Build and verification

Dependencies are Redix, gRPC Elixir, Mint, Protobuf and Jason. The etcd protocol
sources are copied from upstream etcd v3.7.2 definitions and keep both upstream
license files. Generated Elixir messages and gRPC stubs are included in `lib/`,
so building or consuming the package does not require `protoc`. To regenerate
them from the vendored protocol definitions, run
`pwsh -File scripts/generate_protos.ps1`. The script builds the pinned Protobuf
dependency's `protoc-gen-elixir` escript when missing, then runs the local
definitions through the `plugins=grpc` generator. Regeneration needs Mix
dependencies and pinned protoc 31.1. The adapter currently requires HTTP
plaintext endpoints; `https://` returns `{:error, :unsupported_tls}` during
validation. Broader failure/concurrency checks and an installed package
consumer remain open. `mix hex.build` also requires an owner-confirmed
`package.links` URL, which is not recorded in this checkout. No package has
been published.

```shell
pwsh -File scripts/generate_protos.ps1
mix format --check-formatted
mix test
mix docs
mix hex.build
```

The `examples/etcd.exs` consumer requires a pre-provisioned etcd namespace.
`test/etcd_integration_test.exs` is enabled only when `DILOCK_ETCD_URL` and
`DILOCK_COORDINATION_EPOCH` are set, and optionally reads `DILOCK_NAMESPACE`;
it does not provision metadata. Run it only against a disposable etcd cluster
with a matching pre-provisioned metadata record:

```shell
DILOCK_ETCD_URL=http://localhost:2379 DILOCK_NAMESPACE=default \
  DILOCK_COORDINATION_EPOCH=local-test MIX_ENV=test mix test
```

`test/redis_integration_test.exs` is enabled when `DILOCK_REDIS_URL` is set.
It creates unique test namespaces, so point it at a disposable Redis instance:

```shell
DILOCK_REDIS_URL=redis://localhost:6379 MIX_ENV=test mix test
```

The Redis integration exercises two independent providers, contention, release,
`with_lease/3` result propagation, renewal, expiry and stale-release safety. It
does not validate Redis replication failover, partitions or external fencing.

`GRPC.Client.Adapters.Mint` is selected explicitly for etcd. This avoids
depending on gRPC's optional default Gun adapter; the adapter remains a separate
testable transport choice, not a coordinator fallback. The etcd API does not stop paused processes after lease expiry; applications
that protect external effects may need resource-enforced fencing.
