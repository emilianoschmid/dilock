import org.dilock.postgres.PostgresLease;
import org.dilock.postgres.PostgresLock;
import org.dilock.postgres.PostgresProvider;
import org.dilock.postgres.PostgresProviderOptions;

public final class PostgresConsumer {
    public static void main(String[] args) throws ReflectiveOperationException {
        PostgresProvider.class.getMethod("connect", PostgresProviderOptions.class);
        PostgresProvider.class.getMethod("connect", String.class, String.class, String.class);
        PostgresProvider.class.getMethod("provisionNamespace", String.class, String.class, String.class);
        PostgresLock.class.getMethod("tryAcquire");
        PostgresLock.class.getMethod("acquire");
        PostgresLease.class.getMethod("renew");
        PostgresLease.class.getMethod("release");
        System.out.println("PostgreSQL artifact consumer OK: provider, lock, lease and provisioning APIs load");
    }
}
