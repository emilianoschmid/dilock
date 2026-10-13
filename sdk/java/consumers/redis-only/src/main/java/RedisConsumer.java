import org.dilock.redis.RedisLease;
import org.dilock.redis.RedisLock;
import org.dilock.redis.RedisProvider;

public final class RedisConsumer {
    public static void main(String[] args) throws ReflectiveOperationException {
        RedisProvider.class.getMethod("connect", String.class, String.class, String.class);
        RedisLock.class.getMethod("tryAcquire", long.class);
        RedisLease.class.getMethod("release");
    }
}
