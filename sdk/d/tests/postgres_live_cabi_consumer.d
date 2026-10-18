import core.stdc.stdlib : getenv;
import core.time : dur;
import std.exception : enforce;
import std.stdio : writeln;
import std.string : fromStringz, toStringz;

import dilock.postgres;

private string setting(string name) {
    auto value = getenv(name.toStringz);
    enforce(value !is null, "set " ~ name);
    return value.fromStringz.idup;
}

void main() {
    PostgresProviderOptions options;
    options.connectionString = setting("DILOCK_POSTGRES_DSN");
    options.nameSpace = "d-postgres-live-dedicated";
    options.coordinationEpoch = "d-postgres-live-dedicated-epoch";
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
    auto ownerLock = ownerProvider.lock("d/postgres-dedicated-live");
    scope(exit) ownerLock.destroy();
    auto successorLock = successorProvider.lock("d/postgres-dedicated-live");
    scope(exit) successorLock.destroy();

    PostgresLease owner;
    enforce(ownerLock.tryAcquire(owner), "PostgreSQL owner acquisition failed");
    scope(exit) owner.destroy();
    enforce(owner.isValid(), "PostgreSQL owner lease is not valid");
    PostgresLease busy;
    enforce(!successorLock.tryAcquire(busy), "PostgreSQL contender acquired a held key");
    enforce(owner.renew(), "PostgreSQL liveness check failed");
    enforce(owner.release() == ReleaseOutcome.released,
        "PostgreSQL owner release was not acknowledged");

    PostgresLease successor;
    enforce(successorLock.tryAcquire(successor),
        "PostgreSQL successor did not acquire after release");
    scope(exit) successor.destroy();
    enforce(successor.release() == ReleaseOutcome.released,
        "PostgreSQL successor release was not acknowledged");
    writeln("PASS D PostgreSQL dedicated facade: provision, contention, liveness, release, and handoff");
}
