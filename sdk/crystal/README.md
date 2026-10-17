# Dilock for Crystal

The SDK provides a synchronous Redis single-authority adapter using the
`jgaskins/redis` shard. Its optional etcd API is an **experimental facade** over
Dilock's shared C ABI and native C++/gRPC engine; it is not a standalone Crystal
gRPC client. C ABI v1 and this facade are draft/experimental and are not ready
for release.

```crystal
require "dilock"

provider = Dilock::RedisProvider.new(ENV["REDIS_URL"], "billing", "prod-east-1")
begin
  invoice = provider.lock("invoice:42")
  reconciliation = provider.lock("accounting:reconcile")

  result = invoice.with_lease do |_lease|
    # Protect a short, idempotent operation. Redis leases do not stop stale work.
    "invoice processed"
  end
  puts(result || "invoice is busy")

  options = Dilock::Options.new(ttl_ms: 60_000, require_fencing: false)
  configured_result = reconciliation.with_lease(options) do |_lease|
    "reconciliation complete"
  end
  puts(configured_result || "reconciliation is busy")
ensure
  provider.close
end
```

Provision `dilock:namespace:<namespace>:meta` with the exact draft-1 Redis
metadata before use. A busy result is `nil`; backend and metadata errors raise.
An exception after an acquisition command was dispatched raises
`AcquisitionUnknown`, never ordinary contention. The provider owns a pooled
client; handles are reusable and leases are single-acquisition objects. Calls
are synchronous and may block the calling fiber/thread. `Handle#with_lease`
uses one nonblocking attempt and returns `nil` only on acknowledged contention;
omit options for the 30-second TTL or pass a complete `Options` value. It
releases exactly once before returning callback work. `AlreadyNotOwned` and
`Unknown` cleanup raise `ScopeReleaseError`; if work and cleanup both fail,
`ScopeCallbackError` keeps the work exception as its `cause` and exposes the
cleanup error as `release_error`. Configure no client retries for
state-changing commands (the provider sets `retry_attempts=0`). Acquisition
latency is measured with Crystal's monotonic clock, so wall-clock corrections
cannot extend the conservative local validity budget for a lease.
Provider lifecycle transitions are synchronized across threads: `close`
raises `ProviderBusy` while an acquisition is in flight and leaves the client
open, so the caller can retry closing after that call finishes.

Run `crystal spec` after installing Crystal and Shards.
Set `DILOCK_TEST_REDIS_URL=redis://127.0.0.1:6379/0` to opt into the real-Redis
integration spec. It provisions unique test namespace metadata and exercises
two-provider contention, release handoff, expiry, and stale-owner safety; use a
disposable Redis instance for this test.

## Experimental etcd facade

The facade calls the C ABI functions for native etcd `Lock`/`Unlock`, per-attempt
leases, keepalive, verified `create_revision` fences and bounded cleanup. It
does not implement gRPC in Crystal. Build and install the C and C++ SDKs with
`DILOCK_BUILD_ETCD=ON`, `DILOCK_BUILD_C_API=ON` and `BUILD_SHARED_LIBS=ON` (see
[`sdk/c/README.md`](../c/README.md)); keep `libdilock_c`, the C++ SDK and their
gRPC/protobuf/OpenSSL runtime dependencies available at link and load time.
The coordination epoch must be valid UTF-8, contain no NUL, and fit in 1,024
bytes. Endpoint and PEM strings cross the C ABI as NUL-terminated strings, so
they must also be valid UTF-8 and contain no NUL.
Compile with the install prefix on the linker search path, for example:

```sh
crystal build examples/etcd_basic.cr \
  --link-flags "-L/path/to/dilock-c/lib" -o etcd_basic
```

The loader must also find that prefix's shared libraries and the C ABI's
transitive native dependencies. Provision namespace metadata out of band with
profile `etcd-native-lock-v1` and the same stable coordination epoch as every
client. Set `DILOCK_ETCD_ENDPOINT`, `DILOCK_NAMESPACE`, and `DILOCK_EPOCH` before
running the example. The API is synchronous: `acquire` blocks until the native
lock succeeds, is cancelled, fails, or reaches its wait deadline. A
`Dilock::EtcdCancellation` can be requested from another thread and must remain
alive until `acquire` returns. Errors remain typed; timeout, cancellation,
backend failure, and uncertain acquisition are not contention. `try_acquire`
raises `EtcdUnsupportedCapability`, because etcd's native Lock RPC has no safe
nonblocking result. Wait and lease durations must be between 1 ms and 24 hours;
the lease must also exceed the configured RPC timeout plus safety margin.
Invalid duration options raise `ArgumentError` before a native call is made.
Passing a cancellation token after `close` raises `Dilock::Error` before its
destroyed native pointer can reach the C ABI.
Provider `lock` creation is serialized with `close` so the native provider
pointer cannot be destroyed during handle creation. This guard does not make
concurrent acquisition, release, cancellation-token use, or handle disposal
generally thread-safe; callers should still close leases and handles first.

`EtcdHandle#with_lease` is the block-scoped form. With no duration arguments
it uses the provider's configured wait and lease defaults; pass `wait_ms:`,
`lease_ms:`, and optionally `cancellation:` to configure one acquisition. It
releases once and closes the local lease wrapper before returning. A
non-`Released` outcome raises `ScopeReleaseError`, with `AlreadyNotOwned`
reported as lost ownership and `Unknown` retained as uncertain cleanup. If the
callback raises too, `ScopeCallbackError` retains that exception as its cause
and exposes cleanup as `release_error`. Providers and lock handles remain
reusable across scopes; close each handle, then its provider, when finished.

Always call `lease.release` when its acknowledgement matters, then `lease.close`;
`close` on an unreleased lease makes only bounded best-effort cleanup. Call
`handle.close` and `provider.close` after their children. The lease's fence
contains the approved epoch and verified etcd `create_revision`; the protected
resource must atomically enforce it. A lease does not stop stale work. Auto
renewal is opt-in and does not provide that guarantee.

The etcd specs require the matching shared ABI on the linker path. Set
`DILOCK_TEST_ETCD_ENDPOINT`, `DILOCK_TEST_ETCD_NAMESPACE`, and
`DILOCK_TEST_ETCD_EPOCH` to use a pre-provisioned disposable etcd namespace, then
run `crystal spec etcd_spec/dilock_etcd_spec.cr --link-flags "-L<abi-prefix>/lib"`.
The Redis-only suite is `crystal spec spec/dilock_spec.cr` and does not load or
link the optional ABI.

## Experimental Consul facade

Consul also uses the shared C ABI and C++ engine; it does not add another
Crystal HTTP stack. Build the C/C++ install with
`DILOCK_BUILD_CONSUL=ON`, `DILOCK_BUILD_C_API=ON`, and
`BUILD_SHARED_LIBS=ON`. Keep `libdilock_c`, the C++ engine, libcurl, and their
runtime libraries available at link and load time. Import the facade with
`require "dilock/consul"` (the facade loads the shared C ABI declarations).

Provision the namespace metadata explicitly with
`ConsulProvider#provision_namespace` before workers acquire locks. The
coordination epoch must be valid UTF-8, non-empty, NUL-free, and at most 1,024
bytes. ACL tokens are NUL-free strings; source them from the deployment secret
manager. Session TTLs must be 10 seconds through 24 hours; Consul rounds them
up to whole seconds. `try_acquire` returns `nil` only for confirmed contention.
Fences contain `(coordination_epoch, LockIndex)` and protected writes must
enforce them atomically. Retain lock KV entries after release so `LockIndex`
cannot reset.

```crystal
require "dilock"
require "dilock/consul"

provider = Dilock::ConsulProvider.new(
  "production-incarnation-1",
  "http://consul.service:8500",
  "billing",
  ENV["DILOCK_CONSUL_ACL_TOKEN"]? || "",
)
provider.provision_namespace
handle = provider.lock("invoice/42/reconcile")
lease = handle.acquire_default
fence = lease.fence
# Atomically apply fence.coordination_epoch and fence.lock_index at the resource.
raise Dilock::Error.new("release not confirmed") unless
  lease.release == Dilock::ConsulReleaseOutcome::Released
lease.close
handle.close
provider.close
```

Set `DILOCK_TEST_CONSUL_ENDPOINT`, `DILOCK_TEST_CONSUL_NAMESPACE`,
`DILOCK_TEST_CONSUL_EPOCH`, and optionally `DILOCK_TEST_CONSUL_ACL_TOKEN` to
run the live Crystal spec against a pre-provisioned disposable Consul agent.
The test provisions compatible metadata, then acquires, checks the TTL and
fence, and releases once. Compile it with the C ABI library on the linker path:

```sh
crystal spec consul_spec/dilock_consul_spec.cr --link-flags "-L<abi-prefix>/lib"
```

The short example uses the default namespace and requested TTL; the epoch is
still required. To configure every acquisition option exposed by Redis, pass
an `Options` value. `require_fencing: true` is rejected because this Redis
profile cannot provide strong fencing. Redis exposes a one-shot nonblocking
acquisition; a busy result is `nil`.

```crystal
result = handle.with_lease(Dilock::Options.new(ttl_ms: 60_000)) do |lease|
  # The provider and handle can be reused for later scopes.
  "daily report complete"
end
puts(result || "daily report is busy")
```
