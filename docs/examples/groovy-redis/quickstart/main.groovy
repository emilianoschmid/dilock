import org.dilock.groovy.Dilock

String endpoint = System.getenv().getOrDefault('DILOCK_REDIS_URL', 'redis://127.0.0.1:6379')
String namespace = System.getenv().getOrDefault('DILOCK_NAMESPACE', 'default')
String epoch = required('DILOCK_COORDINATION_EPOCH')

Dilock.withRedis(endpoint, namespace, epoch) { provider ->
    Dilock.withRedisLock(provider, 'reports/daily') { lease ->
        lease.assertValid()
        rebuildDailyReport()
    }
    // Second resource on the same provider connection with explicit policy.
    Dilock.withRedisLock(provider, 'reports/monthly', new org.dilock.redis.AcquireOptions(12_000L, 45_000L)) { lease ->
        lease.assertValid()
        rebuildMonthlyReport()
    }
}

def rebuildDailyReport() {
    // Replace with one bounded, idempotent unit of work.
}

def rebuildMonthlyReport() {}

String required(String name) {
    String value = System.getenv(name)
    if (!value) throw new IllegalStateException("${name} must match provisioned namespace metadata")
    value
}
