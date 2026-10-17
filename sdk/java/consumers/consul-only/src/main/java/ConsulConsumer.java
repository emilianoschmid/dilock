import org.dilock.consul.ConsulFence;
import org.dilock.consul.ConsulLease;
import org.dilock.consul.ConsulLock;
import org.dilock.consul.ConsulProvider;
import org.dilock.consul.ConsulProviderOptions;

/** Exact-package consumer fixture for the optional Consul artifact. */
public final class ConsulConsumer {
    private ConsulConsumer() { }
    public static void main(String[] args) throws ReflectiveOperationException {
        require(ConsulProvider.class, "connect", ConsulProviderOptions.class);
        require(ConsulProvider.class, "lock", String.class);
        require(ConsulLock.class, "acquire");
        require(ConsulLease.class, "fence");
        require(ConsulFence.class, "lockIndex");
        System.out.println("Consul-only Maven artifact consumer passed.");
    }
    private static void require(Class<?> type, String name, Class<?>... parameters) throws NoSuchMethodException {
        type.getMethod(name, parameters);
    }
}
