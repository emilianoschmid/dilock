import org.dilock.groovy.Dilock

String endpoint = System.getenv().getOrDefault('DILOCK_ETCD_URL', 'http://127.0.0.1:2379')
String namespace = System.getenv().getOrDefault('DILOCK_ETCD_NAMESPACE', 'billing')
String epoch = System.getenv().getOrDefault('DILOCK_ETCD_EPOCH', 'local-etcd-epoch')

Dilock.withEtcd(endpoint, namespace, epoch) { provider ->
    Dilock.withEtcdLock(provider, 'invoice/42/reconcile') { lease ->
        lease.assertValid()
        def fence = lease.fence()
        // The protected resource must atomically reject older epoch/revision pairs.
        println "etcd fence ${fence.epoch()}:${fence.value()}"
    }
}
