import org.dilock.redis.DilockException;
import org.dilock.redis.Identity;

public final class CoreConsumer {
    public static void main(String[] args) {
        if (Identity.resourceId("consumer", "core").length() != 64) throw new AssertionError("identity API unavailable");
        DilockException error = new DilockException(DilockException.Kind.LEASE_LOST, "expected");
        if (error.kind() != DilockException.Kind.LEASE_LOST) throw new AssertionError("typed error API unavailable");
    }
}
