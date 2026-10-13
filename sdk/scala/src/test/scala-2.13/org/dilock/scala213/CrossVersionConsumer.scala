package consumers

import java.time.Duration

import org.dilock.scala213._
import org.dilock.redis.DilockException

/** Compile-time consumer contract for Scala 2.13 Redis and etcd facade calls. */
object CrossVersionConsumer {
  def verifyPolicies(): Unit = {
    val redis = AcquirePolicy(Duration.ofMillis(500), Duration.ofSeconds(5)).toJava
    require(redis.waitMillis() == 500L && redis.leaseMillis() == 5000L)

    val etcd = EtcdAcquirePolicy(Duration.ofMillis(500), Duration.ofSeconds(5), autoRenew = true)
    require(etcd.waitTimeout == Duration.ofMillis(500) && etcd.lease == Duration.ofSeconds(5) && etcd.autoRenew)
  }

  def connectBoth(redisEndpoint: String, etcdEndpoint: String, epoch: String)
      : (RedisProvider, EtcdProvider) =
    (DilockRedis.connect(redisEndpoint, epoch), DilockEtcd.connect(etcdEndpoint, epoch))

  def useRedis(provider: RedisProvider, resource: String)
      : Either[DilockException, Int] = {
    val lock = provider.lock(resource)
    lock.withLease(AcquirePolicy()) { lease =>
      lease.assertValid()
      lease.renew(Duration.ofSeconds(30))
      1
    }
  }

  def useEtcd(provider: EtcdProvider, resource: String)
      : Either[DilockException, Long] = {
    val lock = provider.lock(resource)
    lock.withLease(EtcdAcquirePolicy(autoRenew = true)) { lease =>
      lease.assertValid()
      lease.fence.value()
    }
  }
}
