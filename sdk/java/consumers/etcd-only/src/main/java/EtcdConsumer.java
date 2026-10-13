import org.dilock.etcd.EtcdFence;
import org.dilock.etcd.EtcdLease;
import org.dilock.etcd.EtcdLock;
import org.dilock.etcd.EtcdProvider;

public final class EtcdConsumer {
    public static void main(String[] args) throws ReflectiveOperationException {
        EtcdProvider.class.getMethod("connect", String.class, String.class, String.class);
        EtcdLock.class.getMethod("tryAcquire", long.class);
        EtcdLease.class.getMethod("fence");
        EtcdFence.class.getMethod("epoch");
    }
}
