package org.dilock.groovy

import org.dilock.etcd.EtcdLease
import org.dilock.redis.AcquireOptions
import org.dilock.redis.RedisProvider
import org.junit.jupiter.api.Test
import redis.clients.jedis.RedisClient

import java.util.concurrent.Executors
import java.util.concurrent.Callable
import java.util.concurrent.FutureTask
import java.util.concurrent.TimeUnit

import static org.junit.jupiter.api.Assertions.*
import static org.junit.jupiter.api.Assumptions.assumeTrue

/** Opt-in real authority checks. Set DILOCK_* variables only for disposable local services. */
final class DilockLiveTest {
    @Test
    void redisFacadeContendsAndHandsOffAcrossIndependentProviders() {
        String endpoint = System.getenv('DILOCK_REDIS_URL')
        assumeTrue(endpoint != null && !endpoint.isBlank(), 'DILOCK_REDIS_URL is unset')
        String namespace = "groovy-live-redis-${System.currentTimeMillis()}"
        String epoch = "groovy-live-epoch-${System.currentTimeMillis()}"
        String resource = "resource/${namespace}"

        RedisClient provisioner = RedisClient.create(endpoint)
        try {
            RedisProvider.provisionNamespace(provisioner, namespace, epoch)
        } finally {
            provisioner.close()
        }

        Dilock.withRedis(endpoint, namespace, epoch) { first ->
            Dilock.withRedis(endpoint, namespace, epoch) { second ->
                def firstLease = first.lock(resource).acquire(new AcquireOptions(1_000, 5_000))
                try {
                    firstLease.assertValid()
                    assertTrue(second.lock(resource).tryAcquire(5_000).isEmpty(),
                            'independent provider must observe ordinary contention')
                    assertEquals('RELEASED', firstLease.release().toString())
                    Dilock.withRedisLock(second, resource) { successor ->
                        successor.assertValid()
                        assertTrue(first.lock(resource).tryAcquire(5_000).isEmpty(),
                                'successor remains held until the closure exits')
                    }
                } finally {
                    firstLease.close()
                }
            }
        }
    }

    @Test
    void etcdFacadeQueuesAndReturnsIncreasingRevisionFences() {
        String endpoint = System.getenv('DILOCK_ETCD_URL')
        assumeTrue(endpoint != null && !endpoint.isBlank(), 'DILOCK_ETCD_URL is unset')
        String namespace = System.getenv('DILOCK_NAMESPACE')
        String epoch = System.getenv('DILOCK_COORDINATION_EPOCH')
        assumeTrue(namespace != null && !namespace.isBlank()
                && epoch != null && !epoch.isBlank(),
                'nonblank etcd metadata must be pre-provisioned')
        String resource = "resource/groovy-${System.currentTimeMillis()}"

        Dilock.withEtcd(endpoint, namespace, epoch) { first ->
            Dilock.withEtcd(endpoint, namespace, epoch) { second ->
                def options = new AcquireOptions(5_000, 5_000)
                def firstLease = first.lock(resource).acquire(options)
                def executor = Executors.newSingleThreadExecutor()
                try {
                    def firstFence = firstLease.fence()
                    Callable<EtcdLease> acquisition = {
                        second.lock(resource).acquire(options)
                    } as Callable<EtcdLease>
                    FutureTask<EtcdLease> queued = new FutureTask<>(acquisition)
                    executor.execute(queued)
                    Thread.sleep(250)
                    assertFalse(queued.isDone(), 'second provider should queue behind the owner')
                    assertEquals('RELEASED', firstLease.release().toString())
                    EtcdLease successor = queued.get(7, TimeUnit.SECONDS)
                    assertNotNull(successor)
                    Dilock.withLease(successor) { lease ->
                        def nextFence = lease.fence()
                        assertEquals(epoch, nextFence.epoch())
                        assertTrue(nextFence.value() > firstFence.value())
                    }
                } finally {
                    firstLease.close()
                    executor.shutdownNow()
                }
            }
        }
    }

    @Test
    void consulFacadeContendsAndReturnsIncreasingLockIndexFences() {
        String endpoint = System.getenv('DILOCK_CONSUL_ENDPOINT')
        String namespace = System.getenv('DILOCK_CONSUL_NAMESPACE')
        String epoch = System.getenv('DILOCK_CONSUL_EPOCH')
        assumeTrue(endpoint != null && !endpoint.isBlank(), 'DILOCK_CONSUL_ENDPOINT is unset')
        assumeTrue(namespace != null && !namespace.isBlank(), 'DILOCK_CONSUL_NAMESPACE is unset')
        assumeTrue(epoch != null && !epoch.isBlank(), 'DILOCK_CONSUL_EPOCH is unset')
        String resource = "groovy-consul/${System.nanoTime()}"

        Dilock.withConsul(endpoint, namespace, epoch) { first ->
            Dilock.withConsul(endpoint, namespace, epoch) { second ->
                def options = new AcquireOptions(2_000, 10_000)
                def firstLease = first.lock(resource).acquire(options)
                try {
                    def firstFence = firstLease.fence()
                    assertEquals(epoch, firstFence.epoch())
                    assertTrue(firstFence.lockIndex() > 0)
                    assertTrue(second.lock(resource).tryAcquire().isEmpty(),
                            'an independent provider must observe ordinary contention')
                    assertEquals('RELEASED', firstLease.release().toString())

                    Dilock.withConsulLock(second, resource) { successor ->
                        def nextFence = successor.fence()
                        assertEquals(epoch, nextFence.epoch())
                        assertTrue(nextFence.lockIndex() > firstFence.lockIndex(),
                                'retained KV LockIndex must advance on handoff')
                    }
                } finally {
                    firstLease.close()
                }
            }
        }
    }
}
