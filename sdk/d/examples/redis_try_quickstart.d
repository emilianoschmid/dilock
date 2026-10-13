import dilock;
import std.exception : enforce;
import std.stdio : writeln;

void main() {
    auto provider = RedisProvider.connect("127.0.0.1", "billing", "redis-epoch-1");
    scope(exit) provider.destroy();

    auto lock = provider.lock("invoice/42/reconcile");
    scope(exit) lock.destroy();

    RedisLease lease;
    if (!lock.tryAcquire(30000, lease)) {
        writeln("resource is busy");
        return;
    }
    scope(exit) lease.destroy();

    enforce(lease.isValid(), "lease expired before protected work");
    enforce(lease.release() == ReleaseOutcome.released,
        "Redis release was not confirmed as released");
}
