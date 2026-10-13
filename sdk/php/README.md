# Dilock PHP SDK

The PHP source tree contains synchronous Redis and etcd adapters. Both backends are experimental. Package identity and distribution are not configured; do not treat this source tree as a supported release package.

## Runtime and backends

- PHP 8.1 or later.
- Redis: phpredis (`ext-redis`); one configured standalone authority, no transparent coordinator switch or strong fencing.
- etcd: `ext-grpc`, `grpc/grpc` and `google/protobuf`; native etcd v3.7.2 Lock and Lease RPCs, with an epoch-scoped `create_revision` fence.

The etcd protobuf messages were generated locally from the pinned, reduced etcd v3.7.2 protocol files under `proto/`. Their upstream notices and licenses are included. Generated service clients call the native gRPC services directly; the SDK does not use etcd's HTTP gateway, which does not expose the native Lock service.

## Quick start with defaults

`RedisProvider::connect()` and `EtcdProvider::connect()` own one authority/channel and validate existing namespace metadata; they do not create or repair it. A provider can make reusable resource handles with `lock($name)`. `tryAcquire()` returns a lease or `null` only for confirmed contention; `acquire()` waits for the supplied finite budget and throws `WaitTimeout` when that budget expires after safe cleanup. Backend failures, protocol mismatch, unsupported capability and uncertain acquisition have distinct exception types.

Always use explicit release or `withLease()`:

```php
<?php
declare(strict_types=1);

use Dilock\RedisProvider;

$provider = RedisProvider::connect(
    getenv('DILOCK_REDIS_URL') ?: 'redis://127.0.0.1:6379',
    getenv('DILOCK_EPOCH') ?: throw new RuntimeException('DILOCK_EPOCH is required'),
);
try {
    $provider->lock('reports/rebuild')->withLease(static function ($lease): void {
        $lease->ensureHeld();
        // Perform a short, idempotent operation. A lease cannot stop stale work.
    });
} finally {
    $provider->close();
}
```

## Fully explicit Redis policy

The connection call accepts the default acquisition policy, and each
`withLease` call can instead receive a complete immutable
`AcquireOptions(waitSeconds, Options(leaseSeconds, rpcSeconds, cleanupSeconds,
requireFencing))`. Durations are seconds; omitted fields in `Options` take the
constructor defaults. This example sets every field and uses a separate resource
name so the two examples do not contend with one another:

```php
<?php
declare(strict_types=1);

use Dilock\AcquireOptions;
use Dilock\Options;
use Dilock\RedisProvider;

$policy = new AcquireOptions(3.0, new Options(
    leaseSeconds: 20.0,
    rpcSeconds: 1.0,
    cleanupSeconds: 1.0,
    requireFencing: false,
));
$provider = RedisProvider::connect(
    getenv('DILOCK_REDIS_URL') ?: 'redis://127.0.0.1:6379',
    getenv('DILOCK_EPOCH') ?: throw new RuntimeException('DILOCK_EPOCH is required'),
    getenv('DILOCK_NAMESPACE') ?: 'default',
    $policy,
);
try {
    $provider->lock('reports/rebuild')->withLease(static function ($lease): void {
        $lease->ensureHeld();
        // Redis does not provide a strong fencing token; protect writes with application-level controls.
    }, $policy);
} finally {
    $provider->close();
}
```

PHP has no portable cancellation signal or async scheduler in this API;
blocking calls are bounded by client deadlines. Renewal is explicit
(`$lease->renew()`), not a hidden background thread. Handles and providers are
not safe for sharing across PHP threads. No network operation is performed
from a destructor.

`withLease()` always attempts exact-owner cleanup on scope exit, including after
the local lease has been marked lost. If work returns normally but release says
the owner was already gone, it throws `LeaseLost`; if cleanup is unacknowledged,
it throws `CleanupError`. If work itself throws, that exception stays primary
and the cleanup observer (or `error_log`) records uncertainty. Observer failures
are suppressed. Call `release()` directly when the remote outcome must be
inspected. Process termination bypasses cleanup; server lease expiry is the
fallback.

## Namespace provisioning

Provision metadata out of band and use the same approved epoch in every SDK. Redis key: `dilock:namespace:<namespace>:meta`; etcd key: `/dilock/namespaces/<namespace>/meta`. Its UTF-8 JSON value must contain:

```json
{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"redis-single-authority-v1","coordination_epoch":"<approved-epoch>","fixed_lease_ms":null}
```

For etcd, use profile `etcd-native-lock-v1`. The SDK reads and checks the record; it never provisions it. See [`spec/NAMESPACE-METADATA.md`](../../spec/NAMESPACE-METADATA.md), [`spec/IDENTITY.md`](../../spec/IDENTITY.md) and [`spec/backends/REDIS.md`](../../spec/backends/REDIS.md).

## Local validation

With PHP, Composer and the optional native extensions available:

```shell
composer validate --strict
composer install
composer test
composer analyse
composer style
php examples/redis.php
php examples/etcd.php
```

The unit tests use deterministic command doubles for owner checks and ambiguous outcomes. An opt-in live Redis test exercises two independent providers, contention, release/handoff, TTL expiry, and stale-owner release protection against a real Redis server. Start an isolated Redis instance and set `DILOCK_TEST_REDIS_ENDPOINT` to its URI to enable it:

```shell
DILOCK_TEST_REDIS_ENDPOINT=redis://127.0.0.1:6379 composer test
```

The test creates metadata under a random namespace and epoch, with a 60-second TTL. It removes only the exact metadata value it created, using an atomic owner-value comparison; lock cleanup uses the SDK's owner-checked release. If the test process stops unexpectedly, the metadata expires automatically. The live test requires `ext-redis`; when the endpoint variable is absent, PHPUnit marks it skipped.

An opt-in etcd live test uses `DILOCK_TEST_ETCD_ENDPOINT` and pre-provisioned namespace metadata. No registry publication or remote resource creation has occurred.

For etcd, the caller wait deadline includes LeaseGrant. A successful grant arriving after that deadline is revoked before returning `WaitTimeout`; an unacknowledged revoke is `AcquisitionUnknown`. Invalid ownership verification is `ProtocolMismatch` only after attempt-lease cleanup is acknowledged. Changed-source regression tests are in `EtcdProtocolTest`.

After etcd's native `Lock` call, Dilock ranges the returned opaque key and checks that the response contains that exact key, the attempt's lease ID, and a positive signed 64-bit `create_revision`. A mismatched or malformed response fails closed and the attempt lease is revoked before acquisition is reported as unsuccessful. The exposed revision is a decimal string; consumers that use it for fencing must register and compare it atomically with their protected resource.
