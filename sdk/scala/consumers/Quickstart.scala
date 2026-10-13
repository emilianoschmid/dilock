import java.time.Duration
import org.dilock.scala3.{AcquirePolicy, DilockEtcd, DilockRedis, EtcdAcquirePolicy}
import org.dilock.redis.RedisProviderOptions
import org.dilock.etcd.EtcdProviderOptions

/** Compile-only public API consumer; running it requires provisioned backend namespaces. */
object Quickstart:
  def redisDefault(endpoint: String, epoch: String): Unit =
    val provider = DilockRedis.connect(endpoint, epoch)
    try
      provider.lock("invoice/42/reconcile").withLease() { lease =>
        lease.assertValid()
      }
    finally provider.close()

  def redisConfigured(endpoint: String, epoch: String): Unit =
    val policy = AcquirePolicy(Duration.ofSeconds(2), Duration.ofSeconds(20))
    val options = new RedisProviderOptions(
      endpoint, "billing", epoch,
      Duration.ofSeconds(2), Duration.ofSeconds(2), policy.toJava, null
    )
    val provider = DilockRedis.connect(options)
    try
      val result = provider.lock("invoice/42/reconcile").withLease(policy) { lease =>
        lease.assertValid()
      }
      result.fold(throw _, identity)
    finally provider.close()

  def etcdDefault(endpoint: String, epoch: String): Unit =
    val provider = DilockEtcd.connect(endpoint, epoch)
    try provider.lock("invoice/42/reconcile").withLease() { lease =>
      lease.assertValid()
      lease.fence
    }
    finally provider.close()

  def etcdConfigured(endpoint: String, epoch: String): Unit =
    val policy = EtcdAcquirePolicy(Duration.ofSeconds(2), Duration.ofSeconds(20), autoRenew = true)
    val options = new EtcdProviderOptions(
      endpoint, "billing", epoch,
      Duration.ofSeconds(2), Duration.ofSeconds(2),
      new org.dilock.redis.AcquireOptions(policy.waitTimeout, policy.lease), null
    )
    val provider = DilockEtcd.connect(options)
    try
      val result = provider.lock("invoice/42/reconcile").withLease(policy) { lease =>
        lease.assertValid()
        lease.fence
      }
      result.fold(throw _, identity)
    finally provider.close()
