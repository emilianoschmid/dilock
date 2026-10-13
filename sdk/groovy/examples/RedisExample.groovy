import org.dilock.groovy.Dilock
import org.dilock.redis.AcquireOptions

String endpoint = System.getenv().getOrDefault('DILOCK_REDIS_URL', 'redis://127.0.0.1:6379')
String namespace = System.getenv().getOrDefault('DILOCK_REDIS_NAMESPACE', 'billing')
String epoch = System.getenv().getOrDefault('DILOCK_REDIS_EPOCH', 'local-redis-epoch')

// Convenient defaults: wait up to 5 seconds and request a 30-second lease.
Dilock.withRedis(endpoint, namespace, epoch) { provider ->
    Dilock.withRedisLock(provider, 'invoice/42/reconcile') { lease ->
        lease.assertValid()
        println "Redis lease ${lease.id()} acquired"
    }
}

// Explicit policy when the workload needs a different bounded wait and TTL.
Dilock.withRedis(endpoint, namespace, epoch) { provider ->
    Dilock.withRedisLock(provider, 'invoice/42/reconcile', new AcquireOptions(12_000L, 45_000L)) { lease ->
        lease.assertValid()
        println "Configured Redis lease ${lease.id()} acquired"
    }
}
