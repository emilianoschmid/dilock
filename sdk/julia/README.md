# Dilock Julia SDK preview

This experimental package requires Julia 1.10 or newer and provides Redis,
etcd, Consul and PostgreSQL advisory-session adapters. The etcd adapter uses gRPCClient 1.1; its predecessor supports
the Julia 1.6 resolver but lacks the service-client API this adapter needs.
Neither backend cell is release-ready: cross-SDK conformance, clustered
failure tests, Julia task-safety, and registry installation are still open. A
lease does not stop a process after expiry; protect external effects with a
resource-side fence or idempotency check.

`PostgresProvider` uses LibPQ.jl and retains one private `LibPQ.Connection` per
successful acquisition until same-session `pg_advisory_unlock`. PostgreSQL has
no TTL or generic fencing token; `renew!` is a liveness check only.

## etcd

`EtcdProvider(endpoint, namespace, epoch)` checks pre-provisioned namespace
metadata with a read-only etcd v3 Range call. The metadata key is
`/dilock/namespaces/<namespace>/meta`; ordinary clients never create or repair
it. Supply an endpoint such as `http://127.0.0.1:2379`, or pass `secure=true`
for gRPC over TLS and `token` for bearer authentication. The provider uses the
shared gRPCClient transport. Its construction performs network I/O and can
throw `BackendUnavailable` or `ProtocolMismatch`.

```julia
using Dilock

provider = EtcdProvider(ENV["DILOCK_ETCD_ENDPOINT"], "billing",
                        ENV["DILOCK_COORDINATION_EPOCH"];
                        token = get(ENV, "DILOCK_ETCD_TOKEN", nothing))
handle = lock(provider, "invoice/42")

with_lease(handle; wait_ms=5_000, lease_ms=30_000, auto_renew=true) do lease
    epoch, revision = fence(lease)
    # Send this fence to the protected resource and compare it atomically there.
end
```

Each acquisition grants a fresh etcd lease, keeps it alive while queued on the
native Lock RPC, verifies the exact returned lock key is attached to that lease,
and exposes its `create_revision` with the configured epoch through `fence`.
`wait_ms` is one total monotonic budget for lease grant, lock waiting, and exact
key verification; if it expires after a lease ID is known, Dilock attempts
bounded revocation and returns `WaitTimeout` only when that cleanup is
acknowledged. `lease_ms` requests the server lease TTL and must be 1,000 through
86,400,000 milliseconds. `try_acquire` is explicitly
unsupported because etcd's native Lock RPC queues instead of returning an
immediate busy result; it throws the exported `UnsupportedCapability` exception
before making a network call. Invalid endpoint or duration arguments continue
to use `ArgumentError`. `auto_renew` is off by default; when enabled, a Julia
background task maintains a gRPC keepalive stream while the lease is held.
`renew!` is not a manual etcd operation in this preview.

`acquire` returns an `EtcdLease` that the caller must release with `release!`
or scope with `with_lease`. `release!` first ranges the exact opaque key and
checks its lease ID, then sends native Unlock only for that verified owner and
revokes only that acquisition's lease. A single two-second cleanup budget
covers the preflight and both RPCs. `:released` requires exact ownership plus
positive Unlock and LeaseRevoke acknowledgements. Confirmed key absence or
mismatch, a Revoke gRPC `NOT_FOUND`, and revoke-only cleanup return
`:already_not_owned`; uncertain ownership or cleanup returns `:unknown`.
Julia task interruption during acquisition triggers best-effort
lease revocation and then raises `OperationCancelled`, which includes whether
acquisition is known not to have happened or its outcome is unknown. Interruption
during release also reports its remote outcome; cleanup still attempts lease
revocation after an interrupted Unlock. There is no cancellation token parameter.
On normal scope completion, `with_lease` returns the body result only after
`:released`; it raises `LeaseLost` for `:already_not_owned` and
`ReleaseUnknown` for `:unknown`. If the body throws, that original exception is
preserved while `on_release` can observe the cleanup outcome.
Treat a lease as task-owned: do not concurrently release it from multiple
tasks. A lease lost by timeout or transport failure cannot be revived.

## Redis

The Redis provider borrows a caller-owned Redis.jl connection; use one
connection per task and disconnect it yourself. The easy defaults are a five
second wait and thirty second lease. `with_lease` uses Julia's `do` syntax, and
`renew!` is explicit. Redis executes the packaged owner-checked Lua scripts;
infrastructure does not need to install scripts separately. Redis failover
does not provide strong fencing. `acquire` raises `WaitTimeout` when its wait
budget expires; `LeaseBusy` is not used as a timeout result. A SET response
that arrives late is never returned as a lease: Dilock attempts owner-checked
cleanup and raises `AcquisitionUnknown` if cleanup is not acknowledged. The
borrowed Redis.jl connection exposes no per-command deadline, so a stalled
command can delay when `acquire` returns even though it cannot produce a late
successful lease.

For a Redis policy override, keep the borrowed connection task-confined and
pass both duration values explicitly. The provider validates already
provisioned metadata; it does not create it.

```julia
using Dilock
using Redis

connection = RedisConnection("redis.internal", 6380)
try
    provider = Provider(connection, "reports", ENV["DILOCK_COORDINATION_EPOCH"])
    daily = lock(provider, "daily")
    with_lease(daily; wait_ms=10_000, lease_ms=60_000,
               on_release=outcome -> println("Dilock cleanup: ", outcome)) do lease
        # Run short protected work here.
    end
finally
    disconnect(connection)
end
```

`with_lease` returns the body result only after confirmed `:released` cleanup.
It raises `LeaseLost` if ownership was already gone and `ReleaseUnknown` if the
release acknowledgement is uncertain. If the body throws, its exception stays
primary and `on_release` can observe cleanup. Scoping does not extend a Redis
lease; use explicit `renew!` only when the application deliberately manages renewal.
`OperationCancelled` distinguishes an interrupted acquire, release or renewal
from contention and backend failure. Its `outcome` is `:not_acquired` only
after acknowledged owner-specific cleanup; otherwise it is `:unknown`.

Run the local suite with `julia --project=sdk/julia -e 'using Pkg; Pkg.test()'`. The etcd live
suite is opt-in via `DILOCK_ETCD_SERVER`, `DILOCK_ETCD_NAMESPACE`, and
`DILOCK_ETCD_EPOCH`; it expects a disposable etcd server with matching metadata
provisioned by an administrator. Nothing is published by these commands.

For a fully configured etcd call, the provider exposes TLS selection, a bearer
token and one RPC deadline; acquisition exposes the wait, lease TTL and
auto-renew switch. The application still needs to provide trust roots to the
underlying gRPC environment where required.

```julia
provider = EtcdProvider("https://etcd.internal:2379", "reports",
                        ENV["DILOCK_COORDINATION_EPOCH"];
                        secure=true, token=ENV["DILOCK_ETCD_TOKEN"], deadline_s=2)
daily = lock(provider, "daily")
with_lease(daily; wait_ms=10_000, lease_ms=60_000, auto_renew=true) do lease
    epoch, revision = fence(lease)
    # Pass the fence to the protected resource and compare it atomically.
end
```

The etcd native Lock API has no non-blocking try operation. Auto-renew maintains
the etcd lease while the scope is active; lease loss still requires the caller
to stop or fence protected work.

## Consul

The optional Consul adapter uses HTTP.jl and JSON3. It requires explicit
metadata provisioning, then a linearizable metadata read during connect. Use
the same namespace and epoch across participants. Consul session TTLs round up
to whole seconds and must be 10 seconds to 24 hours. The returned fence is the
configured epoch and the key's `LockIndex`; retain `dilock/locks/*` KV entries
so that the index cannot reset. The protected resource must atomically enforce
that fence.

```julia
using Dilock

provider = ConsulProvider(endpoint=get(ENV, "CONSUL_HTTP_ADDR", "http://127.0.0.1:8500"),
                          namespace="reports", epoch=ENV["DILOCK_COORDINATION_EPOCH"],
                          token=get(ENV, "CONSUL_HTTP_TOKEN", nothing))
provision_consul_namespace(provider) # explicit operator-controlled setup
connect_consul(provider)
daily = lock(provider, "daily")
with_lease(daily; wait_ms=5_000, lease_ms=30_000) do lease
    token = fence(lease)
    # Atomically compare/register token at the protected resource.
    renew!(lease) # renewal is explicit
end
close(provider)
```

Set `DILOCK_TEST_CONSUL_ENDPOINT` to enable the single-agent lifecycle test;
optional variables are `DILOCK_TEST_CONSUL_NAMESPACE`,
`DILOCK_TEST_CONSUL_EPOCH`, and `DILOCK_TEST_CONSUL_TOKEN`. See
`examples/consul.jl`.
