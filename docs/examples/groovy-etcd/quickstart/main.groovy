import org.dilock.groovy.Dilock

String endpoint = System.getenv().getOrDefault('DILOCK_ETCD_ENDPOINT', 'http://127.0.0.1:2379')
String namespace = System.getenv().getOrDefault('DILOCK_NAMESPACE', 'default')
String epoch = required('DILOCK_COORDINATION_EPOCH')

Dilock.withEtcd(endpoint, namespace, epoch) { provider ->
    Dilock.withEtcdLock(provider, 'reports/daily') { lease ->
        lease.assertValid()
        def fence = lease.fence()
        rebuildDailyReport(fence.epoch(), fence.value())
    }
    // Same provider/channel, another independent resource acquisition.
    Dilock.withEtcdLock(provider, 'reports/monthly', new org.dilock.redis.AcquireOptions(12_000L, 45_000L), true) { lease ->
        lease.assertValid()
        def fence = lease.fence()
        rebuildMonthlyReport(fence.epoch(), fence.value())
    }
}

def rebuildDailyReport(String epoch, long revision) {
    // Atomically reject stale epoch/revision fences at the protected resource.
    println "Applying work under fence ${epoch}/${revision}"
}

def rebuildMonthlyReport(String epoch, long revision) {
    println "Applying work under fence ${epoch}/${revision}"
}

String required(String name) {
    String value = System.getenv(name)
    if (!value) throw new IllegalStateException("${name} must match provisioned namespace metadata")
    value
}
