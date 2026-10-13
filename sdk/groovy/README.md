# Dilock Groovy facade

This artifact is a small Groovy closure facade over the Java SDK. It reuses the
Java Redis and native etcd adapters and does not duplicate protocol logic.
`withRedis` and `withEtcd` own and close a provider. `withRedisLock` and
`withEtcdLock` own only the acquired lease, so a provider can serve multiple
resource scopes. These helpers call the lease's explicit bounded `release()`
and require the `RELEASED` outcome after otherwise successful work;
`ALREADY_NOT_OWNED` is reported as a scope failure so a lost lease cannot look
like completed work. Only the Java Redis or etcd adapter's typed `RELEASED`
outcome is accepted; an unrelated enum with that name is not cleanup evidence.
If work already failed, that exception stays primary and
a simultaneous release failure or non-`RELEASED` outcome is suppressed. This
avoids `AutoCloseable.close()`'s best-effort behavior,
which reports unknown cleanup only through an optional observer.
The callback must be non-null: provider and lock helpers validate it before
opening a provider or acquiring a lease. The lower-level `withCloseable` helper
still closes a transferred resource if its callback is null or throws.

The simple form uses Java's defaults (5 seconds acquisition wait, 30 seconds
lease):

```groovy
import org.dilock.groovy.Dilock

Dilock.withRedis('redis://127.0.0.1:6379', 'billing', 'redis-epoch-1') { provider ->
    Dilock.withRedisLock(provider, 'invoice/42/reconcile') { lease ->
        lease.assertValid()
        // Protect the actual resource with idempotency or resource-side fencing.
    }
}
```

Use an `org.dilock.redis.AcquireOptions(waitMillis, leaseMillis)` argument for
an explicit policy. For fully explicit Redis connection and default acquisition
settings, use the Java provider options constructor; the facade scopes still
own and close the provider and one lease separately:

```groovy
import java.time.Duration
import org.dilock.groovy.Dilock
import org.dilock.redis.AcquireOptions
import org.dilock.redis.RedisProvider
import org.dilock.redis.RedisProviderOptions

def policy = new AcquireOptions(12_000L, 45_000L)
def options = new RedisProviderOptions(
    'redis://127.0.0.1:6379', 'billing', 'prod-redis-epoch',
    Duration.ofSeconds(2), Duration.ofSeconds(3), policy, null)
Dilock.withCloseable(RedisProvider.connect(options)) { provider ->
    Dilock.withRedisLock(provider, 'invoice/42/reconcile', policy) { lease ->
        lease.assertValid()
        // Perform bounded, idempotent work here.
        // The scope awaits release and propagates an unacknowledged result.
    }
}
```

etcd lease renewal is opt-in. The facade's overload accepts either default
timings or an `AcquireOptions` value plus `autoRenew`; renewal keeps the same
server lease alive while this process can service keepalive, but it cannot stop
paused work or replace resource-side fence enforcement:

```groovy
Dilock.withEtcd('http://127.0.0.1:2379', 'billing', 'prod-etcd-epoch') { provider ->
    Dilock.withEtcdLock(provider, 'invoice/42/reconcile',
            new AcquireOptions(12_000L, 45_000L), true) { lease ->
        lease.assertValid()
        def fence = lease.fence()
        // The protected resource must atomically enforce this epoch/revision.
    }
}
```

`lease.close()` performs bounded best-effort cleanup; call `lease.release()`
when the caller needs the Java adapter's release result. Provider close cleans up
leases created by that provider. Neither a distributed lease nor local validity
checks stop a paused process after expiry. etcd's
`fence()` returns epoch plus verified `create_revision`; the protected resource
must atomically reject older fences. Redis does not provide strong fencing.

The module depends on both optional Java adapters so both backend examples share
one facade. The core and each backend remain separate Java artifacts. No Java
backend algorithm is copied into this project.

## Local build and examples

Install the Java reactor into the same local Maven repository, then run
`mvn test` here. Execute `groovy examples/RedisExample.groovy` or
`groovy examples/EtcdExample.groovy` only against explicitly provisioned local
authorities with matching namespace metadata. The examples default to
`127.0.0.1`, namespace `billing`, and local development epochs; override these
with `DILOCK_REDIS_URL`, `DILOCK_REDIS_NAMESPACE`, `DILOCK_REDIS_EPOCH`,
`DILOCK_ETCD_URL`, `DILOCK_ETCD_NAMESPACE`, and `DILOCK_ETCD_EPOCH` for
disposable test authorities. They never provision metadata or select a
production epoch. No package was published.
