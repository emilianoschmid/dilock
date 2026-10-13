import dilock;
import core.stdc.stdlib : getenv;
import std.exception : enforce;
import std.string : fromStringz, toStringz;
import std.stdio : writeln;

private string etcdEndpoint() @trusted {
    auto configured = getenv("DILOCK_ETCD_ENDPOINT".toStringz);
    return configured is null ? "127.0.0.1:2379" : configured.fromStringz.idup;
}

void main() {
    auto provider = EtcdProvider.connect(etcdEndpoint(), "billing", "local-etcd-epoch");
    scope(exit) provider.destroy();
    auto lock = provider.lock("invoice/42/reconcile");
    scope(exit) lock.destroy();
    auto lease = lock.acquireDefault();
    scope(exit) lease.destroy();

    enforce(lease.isValid(), "lease expired before protected work");
    auto fence = lease.fence();
    // The protected resource must atomically compare and record the fence.
    writeln("etcd fence: ", fence.epoch, ":", fence.createRevision);
    auto result = lease.release();
    enforce(result == ReleaseOutcome.released, "etcd release was not confirmed as released");
}
