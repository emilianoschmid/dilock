# Dilock for Perl

The Perl SDK provides two independent backend adapters. `Dilock` uses Redis
single-authority leases synchronously. `Dilock::Etcd` uses etcd's native
asynchronous Lock and Lease APIs. Neither backend turns a lease into a local
mutex or stops application work when the lease expires.

## Redis

```perl
use Dilock;

my $provider = Dilock->new(
    server => $ENV{REDIS_SERVER} // '127.0.0.1:6379',
    namespace => 'billing', epoch => 'prod-east-1',
);
my $handle = $provider->lock('invoice:42');
$handle->with_lease(sub {
    my ($lease) = @_;
    # Keep the protected work short and make it idempotent where possible.
});
$provider->close;
```

`try_acquire` returns `undef` only for acknowledged contention. `acquire`
waits by polling with bounded 20–50 ms jitter until its positive deadline; a
successful Redis response arriving after that deadline is owner-cleaned and is
not returned as a lease.
`with_lease` explicitly releases the lease on success or exception; in list
context it preserves every callback value and appends the release outcome, in
scalar context it preserves the callback's scalar result, and in void context
it invokes the callback in void context. If cleanup is `already_not_owned` or
`unknown`, list context returns that explicit outcome; scalar and void contexts
throw `Dilock::LeaseLost` or `Dilock::ReleaseUnknown` so cleanup failure is not
hidden by the callback's return context. If both the callback and cleanup
fail, `Dilock::ScopeError` exposes the original callback exception through
`cause` and the cleanup result through `release_outcome`; with acknowledged
cleanup the original callback exception is rethrown unchanged. A lease's
`renew(ttl_ms => ...)`
uses an owner-checked Redis script and returns true only after an acknowledged
renewal that still fits the previous local validity deadline. A rejected
renewal returns false and moves the lease to `lost`; an uncertain backend
response raises `Dilock::BackendUnavailable` and also moves it to `lost`.
`state` reports `held`, `lost`, `releasing`, `released`, or `unknown`, using a
monotonic deadline with a conservative safety margin. Renewal after that
deadline raises `Dilock::LeaseLost`. Redis provides no strong fencing in this
profile. Namespace metadata must be provisioned before use. The client
disables automatic reconnect and reports uncertain acquisition delivery as
`Dilock::AcquisitionUnknown`.

The configured form accepts Redis client timeouts and the complete acquisition
wait/TTL policy. Reconnect is disabled for state-changing commands; TLS options
are not exposed by this constructor.

```perl
use Dilock;

my $provider = Dilock->new(
    server => $ENV{REDIS_SERVER},
    namespace => 'reports', epoch => $ENV{DILOCK_COORDINATION_EPOCH},
    connect_timeout => 1.5, read_timeout => 2, write_timeout => 2,
);
my $daily = $provider->lock('daily');
my ($value, $release_outcome) = $daily->with_lease(
    sub { return 1 }, wait_seconds => 10, ttl_ms => 60_000,
);
die "release outcome unknown" if $release_outcome eq 'unknown';
$provider->close;
```

## etcd

`Dilock::Etcd` uses the EV event loop. Construct the provider and handle once,
then keep the loop running while acquisition and release callbacks are pending:

```perl
use EV;
use Dilock::Etcd;

my $provider = Dilock::Etcd->new(
    endpoint => '127.0.0.1:2379', namespace => 'billing', epoch => 'prod-east-1',
);
my $handle = $provider->lock('invoice:42');
$handle->acquire({ ttl_seconds => 30, wait_seconds => 10 }, sub {
    my ($lease, $error) = @_;
    die "$error" if $error;
    # The lease remains valid only until its server TTL expires.
    $lease->release(sub {
        my ($result, $release_error) = @_;
        die "$release_error" if $release_error;
        EV::break;
    });
});
EV::run;
$provider->close;
```

The returned acquisition handle supports `cancel`; cancellation revokes its
unique lease with bounded cleanup. A successful LeaseGrant must return its
effective positive TTL; the adapter does not substitute the requested TTL for
a malformed response. Ownership verification checks the exact
opaque key returned by Lock; an absent or different Range key is a protocol
mismatch after confirmed cleanup and an unknown acquisition if cleanup is not
acknowledged. A delayed keepalive acknowledgement cannot restore a locally
expired lease. `try_acquire` reports
`Dilock::Etcd::UnsupportedCapability`, because etcd's native Lock API has no
non-blocking try operation. Acquisition is asynchronous; callbacks run on the
EV loop. Do not block that loop while holding or waiting for a lease. A lease
exposes `{ epoch, create_revision }` from the exact returned lock key. The
application's protected resource must atomically compare/register this fence.
This adapter does not automatically renew an acquired lease; `auto_renew` is
explicitly unsupported. Release linearly ranges the exact opaque lock key and
checks that its lease ID matches before calling native Unlock. It reports
`released` only after that ownership proof and positive acknowledgements from
both Unlock and LeaseRevoke. A confirmed absent or mismatched key, a
LeaseRevoke `NOT_FOUND`, or a Revoke acknowledgement without all required
release evidence reports `already_not_owned`; uncertain ownership or cleanup
reports `unknown`. Range, Unlock, and Revoke share one bounded cleanup window.

The provider requires a stable non-empty `epoch`; `cleanup_seconds` defaults
to 2 and bounds best-effort cleanup acknowledgements. `acquire` accepts
`ttl_seconds` (integer 1–86400, default 30) and `wait_seconds` (positive finite
deadline, default 5); the deadline includes metadata lookup and lease creation.
Its callback receives `($lease, undef)` or `(undef, $error)`. Backend and
protocol failures are typed error objects; malformed local options throw with
`die`. `on_loss`, when supplied, receives `($lease, 'lease_expired')` on the EV
loop. `lease->state` reports `held`, `lost`, `releasing`, `released` or
`unknown`. `lease->release($callback)` reports `released`,
`already_not_owned` or `unknown`; an uncertain response also supplies a typed
error. Repeated calls return the cached terminal outcome and do not send another
Unlock or LeaseRevoke request.

After the LeaseGrant RPC is sent, a transport error, timeout, or cancellation
without a confirmed response is `Dilock::Etcd::AcquisitionUnknown`: etcd may
have created a lease, but the client has no lease ID with which to revoke it.
If a late successful response supplies the ID, the SDK best-effort revokes it;
that later cleanup cannot change the already-delivered result. Cancellation or
timeout before LeaseGrant is dispatched retains its ordinary typed outcome.
If Lock ownership verification fails, the result remains `AcquisitionUnknown`
when the attempt lease revoke is not acknowledged; `ProtocolMismatch` is
reported only after cleanup is confirmed.

`close` prevents new acquisitions and cancels pending attempts with bounded
cleanup. Held leases remain caller-owned: explicitly release them or let their
server TTL expire. Callbacks and lease methods belong to the same EV loop.

Provision `/dilock/namespaces/<namespace>/meta` out of band with compatible
draft-1 metadata before use. Runtime clients only read and validate metadata;
they do not create or repair it. `coordination_epoch` must be stable across all
clients and recognized by the protected resource.

For an explicit etcd policy, configure the provider cleanup budget and set the
queue wait and server lease TTL. This adapter does not expose TLS/auth options
through its current constructor and does not support acquired-lease auto-renew.

```perl
use EV;
use Dilock::Etcd;

my $provider = Dilock::Etcd->new(
    endpoint => $ENV{DILOCK_ETCD_ENDPOINT},
    namespace => 'reports', epoch => $ENV{DILOCK_COORDINATION_EPOCH},
    cleanup_seconds => 3,
);
my $daily = $provider->lock('daily');
$daily->acquire({ ttl_seconds => 60, wait_seconds => 10 }, sub {
    my ($lease, $error) = @_;
    die "$error" if $error;
    my $fence = $lease->fence;
    # Pass this fence to the protected resource and compare it atomically.
    $lease->release(sub {
        my ($outcome, $release_error) = @_;
        die "$release_error" if $release_error;
        die "release outcome unknown" if $outcome eq 'unknown';
        EV::break;
    });
});
EV::run;
$provider->close;
```

## Dependencies and tests

Install only the optional backend dependencies used by the application. The
core module has no backend-client dependency. For Redis, install
`Redis::Fast` 0.39 or newer and `Crypt::URandom` 0.55 or newer. For etcd,
install `EV::Etcd` 0.10 or newer (which supplies EV); callers injecting a
custom etcd client still need EV. The distribution recommends these adapters'
dependencies but does not force Redis-only consumers to install gRPC/etcd
client modules or etcd-only consumers to install the Redis client.

The deterministic test suite requires `Test::More` and `EV`; endpoint-gated
integration tests require the corresponding Redis or etcd backend dependency.
After installing the test requirements, run:

On Debian or Ubuntu, compiling `EV::Etcd` also requires the native headers and
`pkg-config` (`libev-dev`, `libgrpc-dev`, `libgrpc++-dev`,
`libprotobuf-c-dev`, and `pkg-config`).

```sh
prove -Ilib -It/lib t
```

Live Redis integration is opt-in through `DILOCK_REDIS_SERVER`. The Redis provider is synchronous and should not be shared concurrently unless
the application serializes it. The etcd provider and handles are EV-loop
affine; use them on one event loop. No lock operation promises exactly-once
effects or prevents a paused former owner from resuming.
