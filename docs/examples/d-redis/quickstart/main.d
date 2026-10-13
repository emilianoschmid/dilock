import dilock;
import core.stdc.stdlib : getenv;
import std.exception : enforce;
import std.string : fromStringz, toStringz;
import std.stdio : writeln;

private string redisHost() @trusted {
    auto configured = getenv("DILOCK_REDIS_HOST".toStringz);
    return configured is null ? "127.0.0.1" : configured.fromStringz.idup;
}

void main() {
    auto provider = RedisProvider.connect(redisHost(), "billing", "local-redis-epoch");
    scope(exit) provider.destroy();
    auto lock = provider.lock("invoice/42/reconcile");
    scope(exit) lock.destroy();
    auto lease = lock.acquireDefault();
    scope(exit) lease.destroy();

    enforce(lease.isValid(), "lease expired before protected work");
    auto result = lease.release();
    enforce(result == ReleaseOutcome.released, "Redis release was not confirmed as released");
    writeln("Redis release: ", result);
}
