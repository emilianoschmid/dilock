import org.dilock.etcd.EtcdFence;
import org.dilock.etcd.EtcdLease;
import org.dilock.etcd.EtcdLock;
import org.dilock.etcd.EtcdProvider;
import org.dilock.redis.DilockException;
import org.dilock.redis.Identity;
import org.dilock.redis.RedisLease;
import org.dilock.redis.RedisLock;
import org.dilock.redis.RedisProvider;

/** Checkout-external consumer fixture linked only against the built Dilock JAR. */
public final class PackageConsumer {
    private PackageConsumer() { }

    public static void main(String[] args) throws ReflectiveOperationException {
        String digest = Identity.resourceId("consumer", "invoice/42");
        if (!digest.matches("[0-9a-f]{64}")) {
            throw new AssertionError("installed identity API returned an invalid SHA-256 digest");
        }

        require(RedisProvider.class, "connect", String.class, String.class, String.class);
        require(RedisProvider.class, "lock", String.class);
        require(RedisLock.class, "acquire");
        require(RedisLock.class, "tryAcquire", long.class);
        require(RedisLease.class, "renew", long.class);
        require(RedisLease.class, "release");
        require(EtcdProvider.class, "connect", String.class, String.class, String.class);
        require(EtcdProvider.class, "lock", String.class);
        require(EtcdLock.class, "acquire");
        require(EtcdLock.class, "tryAcquire", long.class);
        require(EtcdLease.class, "fence");
        require(EtcdFence.class, "epoch");
        require(DilockException.class, "kind");

        System.out.println("Exact Maven artifact consumer passed: identity, Redis, etcd, lease and fence APIs are available.");
    }

    private static void require(Class<?> type, String name, Class<?>... parameters) throws NoSuchMethodException {
        type.getMethod(name, parameters);
    }
}
