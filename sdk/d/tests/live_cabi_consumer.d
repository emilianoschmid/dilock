import dilock;
import core.stdc.stdlib : getenv;
import core.time : dur;
import core.thread : Thread;
import std.conv : to;
import std.exception : enforce;
import std.stdio : stderr, writeln;
import std.string : fromStringz, toStringz;

private void requireWaitTimeout(void delegate() operation, string message) {
    bool timedOut;
    Status observedStatus;
    string observedMessage;
    try {
        operation();
    } catch (DilockException error) {
        observedStatus = error.status;
        observedMessage = error.msg;
        timedOut = error.status == Status.waitTimeout;
    }
    enforce(timedOut, message ~ " (observed status: " ~ to!string(observedStatus) ~ ", message: " ~ observedMessage ~ ")");
}

private void requireInvalidArgument(void delegate() operation, string message) {
    bool invalid;
    try {
        operation();
    } catch (DilockException error) {
        invalid = error.status == Status.invalidArgument;
    }
    enforce(invalid, message);
}

private void runRedis() {
    RedisProviderOptions options;
    options.host = "redis";
    options.nameSpace = "d-live-cabi";
    options.coordinationEpoch = "d-live-epoch";
    options.rpcTimeout = dur!"msecs"(1200);
    options.cleanupTimeout = dur!"msecs"(800);
    options.safetyMargin = dur!"msecs"(20);
    options.pollInterval = dur!"msecs"(10);
    options.defaultWait = dur!"msecs"(200);
    options.defaultLease = dur!"msecs"(4000);
    auto provider = RedisProvider.connect(options);
    scope(exit) provider.destroy();
    auto ownerLock = provider.lock("lifecycle/redis");
    scope(exit) ownerLock.destroy();
    auto contenderLock = provider.lock("lifecycle/redis");
    scope(exit) contenderLock.destroy();

    auto owner = ownerLock.acquire(1000, 5000);
    scope(exit) owner.destroy();
    enforce(owner.isValid(), "Redis owner lease is not valid after acquisition");
    RedisLease busyLease;
    enforce(!contenderLock.tryAcquire(3000, busyLease),
        "Redis try-acquire must report ordinary contention without allocating a lease");
    requireWaitTimeout(delegate() { auto unexpected = contenderLock.acquire(150, 3000); unexpected.destroy(); },
        "Redis contention did not report waitTimeout");
    requireWaitTimeout(delegate() { auto unexpected = contenderLock.acquireDefault(); unexpected.destroy(); },
        "Redis configured default wait did not report waitTimeout");

    enforce(owner.release() == ReleaseOutcome.released, "Redis owner release was not acknowledged");
    enforce(contenderLock.tryAcquire(3000, busyLease), "Redis try-acquire did not acquire after release");
    scope(exit) busyLease.destroy();
    enforce(busyLease.isValid(), "Redis try-acquire returned an invalid lease");
    requireInvalidArgument(delegate() { contenderLock.tryAcquire(3000, busyLease); },
        "Redis try-acquire overwrote an already-owned output lease");
    enforce(busyLease.isValid(), "rejected Redis try-acquire invalidated the existing output lease");
    enforce(busyLease.release() == ReleaseOutcome.released, "Redis try-acquire lease release was not acknowledged");
    auto successor = contenderLock.acquire(1000, 3000);
    scope(exit) successor.destroy();
    enforce(successor.isValid(), "Redis successor lease is not valid");
    enforce(owner.release() == ReleaseOutcome.released, "Redis repeated release did not retain its terminal result");
    requireWaitTimeout(delegate() { auto unexpected = ownerLock.acquire(150, 3000); unexpected.destroy(); },
        "Redis stale release removed successor ownership");
    enforce(successor.release() == ReleaseOutcome.released, "Redis successor release was not acknowledged");
    writeln("PASS Redis: linked C ABI acquisition, bounded contention, handoff, stale repeat release, successor preserved");
}

private void runEtcd() {
    EtcdProviderOptions options;
    options.endpoint = "etcd:2379";
    options.nameSpace = "d-live-cabi";
    options.coordinationEpoch = "d-live-epoch";
    options.rpcTimeout = dur!"msecs"(1200);
    options.cleanupTimeout = dur!"msecs"(800);
    options.safetyMargin = dur!"msecs"(20);
    options.defaultWait = dur!"msecs"(200);
    options.defaultLease = dur!"seconds"(4);
    auto provider = EtcdProvider.connect(options);
    scope(exit) provider.destroy();
    auto ownerLock = provider.lock("lifecycle/etcd");
    scope(exit) ownerLock.destroy();
    auto contenderLock = provider.lock("lifecycle/etcd");
    scope(exit) contenderLock.destroy();

    auto owner = ownerLock.acquire(1000, 5000);
    scope(exit) owner.destroy();
    enforce(owner.isValid(), "etcd owner lease is not valid after acquisition");
    requireInvalidArgument(delegate() { ownerLock.tryAcquire(3000, owner); },
        "etcd try-acquire overwrote an already-owned output lease");
    enforce(owner.isValid(), "rejected etcd try-acquire invalidated the existing output lease");
    auto ownerFence = owner.fence();
    enforce(ownerFence.epoch == "d-live-epoch" && ownerFence.createRevision > 0,
        "etcd fence does not contain the configured epoch and a positive create_revision");
    requireWaitTimeout(delegate() { auto unexpected = contenderLock.acquire(150, 3000); unexpected.destroy(); },
        "etcd contention did not report waitTimeout");
    requireWaitTimeout(delegate() { auto unexpected = contenderLock.acquireDefault(); unexpected.destroy(); },
        "etcd configured default wait did not report waitTimeout");

    enforce(owner.release() == ReleaseOutcome.released, "etcd owner unlock was not acknowledged");
    auto successor = contenderLock.acquire(2000, 5000);
    scope(exit) successor.destroy();
    enforce(successor.isValid(), "etcd successor lease is not valid");
    auto successorFence = successor.fence();
    enforce(successorFence.epoch == ownerFence.epoch && successorFence.createRevision > ownerFence.createRevision,
        "etcd successor fence did not advance within the configured epoch");
    enforce(owner.release() == ReleaseOutcome.released, "etcd repeated unlock did not retain its terminal result");
    requireWaitTimeout(delegate() { auto unexpected = ownerLock.acquire(150, 3000); unexpected.destroy(); },
        "etcd stale unlock removed successor ownership");
    enforce(successor.release() == ReleaseOutcome.released, "etcd successor unlock was not acknowledged");

    // Natural lease expiry (auto-renew is disabled by default and specified
    // explicitly here) must not be reported as a successful Unlock when the
    // exact native key is already gone. The same resource must be acquirable
    // by a successor after expiry.
    options.autoRenew = false;
    auto expiryProvider = EtcdProvider.connect(options);
    scope(exit) expiryProvider.destroy();
    auto expiryLock = expiryProvider.lock("lifecycle/etcd-natural-expiry");
    scope(exit) expiryLock.destroy();
    auto expiredLease = expiryLock.acquire(1000, 2000);
    scope(exit) expiredLease.destroy();
    enforce(expiredLease.effectiveTtlMs() >= 2000,
        "etcd did not grant the expected minimum whole-second TTL");
    Thread.sleep(dur!"seconds"(5));
    enforce(!expiredLease.isValid(), "etcd lease unexpectedly remained locally valid after natural expiry");
    enforce(expiredLease.release() == ReleaseOutcome.alreadyNotOwned,
        "expired etcd lease must classify as alreadyNotOwned, not released or unknown");
    auto expirySuccessor = expiryLock.acquire(1000, 3000);
    scope(exit) expirySuccessor.destroy();
    enforce(expirySuccessor.isValid(), "successor could not acquire after natural etcd lease expiry");
    enforce(expirySuccessor.release() == ReleaseOutcome.released,
        "successor release after natural expiry was not acknowledged");

    writeln("PASS etcd: linked C ABI acquisition, bounded contention, handoff, increasing fence, stale unlock, natural expiry classified AlreadyNotOwned, successor acquired and released");
}

private string setting(string name, string fallback) {
    auto value = getenv(name.toStringz);
    return value is null ? fallback : value.fromStringz.idup;
}

private void runPostgres() {
    auto connectionString = setting("DILOCK_POSTGRES_DSN", "");
    enforce(connectionString.length, "set DILOCK_POSTGRES_DSN");
    PostgresProviderOptions options;
    options.connectionString = connectionString;
    options.nameSpace = "d-postgres-live";
    options.coordinationEpoch = "d-postgres-live-epoch";
    options.rpcTimeout = dur!"msecs"(2000);
    options.cleanupTimeout = dur!"msecs"(2000);
    options.pollInterval = dur!"msecs"(25);
    options.defaultWait = dur!"msecs"(5000);
    options.defaultLease = dur!"msecs"(30000);
    provisionPostgresNamespace(options);

    auto ownerProvider = PostgresProvider.connect(options);
    scope(exit) ownerProvider.destroy();
    auto successorProvider = PostgresProvider.connect(options);
    scope(exit) successorProvider.destroy();
    auto ownerLock = ownerProvider.lock("d/postgres-live");
    scope(exit) ownerLock.destroy();
    auto successorLock = successorProvider.lock("d/postgres-live");
    scope(exit) successorLock.destroy();

    PostgresLease owner;
    enforce(ownerLock.tryAcquire(owner), "PostgreSQL owner acquisition failed");
    scope(exit) owner.destroy();
    enforce(owner.isValid(), "PostgreSQL owner lease is not valid");
    PostgresLease busy;
    enforce(!successorLock.tryAcquire(busy), "PostgreSQL contender acquired a held key");
    enforce(owner.renew(), "PostgreSQL liveness check failed");
    enforce(owner.release() == ReleaseOutcome.released, "PostgreSQL owner release was not acknowledged");

    PostgresLease successor;
    enforce(successorLock.tryAcquire(successor), "PostgreSQL successor did not acquire after release");
    scope(exit) successor.destroy();
    enforce(successor.release() == ReleaseOutcome.released, "PostgreSQL successor release was not acknowledged");
    writeln("PASS PostgreSQL: D C ABI provision, contention, liveness, release, and handoff");
}

void main(string[] args) {
    enforce(args.length == 2, "usage: live_cabi_consumer.d <redis|etcd|postgres>");
    try {
        switch (args[1]) {
        case "redis": runRedis(); break;
        case "etcd": runEtcd(); break;
        case "postgres": runPostgres(); break;
        default: enforce(false, "backend must be redis, etcd or postgres");
        }
    } catch (Exception error) {
        stderr.writeln("FAIL: ", error.msg);
        throw error;
    }
}
