import core.stdc.stdlib : getenv;
import core.time : dur;
import std.conv : to;
import std.exception : enforce;
import std.string : fromStringz, toStringz;
import std.stdio : stderr, writeln;

import dilock;

private string setting(string name) {
    auto value = getenv(name.toStringz);
    return value is null ? "" : value.fromStringz.idup;
}

void main() {
    auto endpoint = setting("DILOCK_CONSUL_ENDPOINT");
    auto nameSpace = setting("DILOCK_CONSUL_NAMESPACE");
    auto epoch = setting("DILOCK_CONSUL_EPOCH");
    enforce(endpoint.length && nameSpace.length && epoch.length,
        "set DILOCK_CONSUL_ENDPOINT, DILOCK_CONSUL_NAMESPACE, and DILOCK_CONSUL_EPOCH");

    try {
        ConsulProviderOptions options;
        options.endpoint = endpoint;
        options.nameSpace = nameSpace;
        options.coordinationEpoch = epoch;
        options.rpcTimeout = dur!"msecs"(1000);
        options.cleanupTimeout = dur!"msecs"(1000);
        options.pollInterval = dur!"msecs"(25);
        auto firstProvider = ConsulProvider.connect(options);
        scope(exit) firstProvider.destroy();
        auto secondProvider = ConsulProvider.connect(options);
        scope(exit) secondProvider.destroy();

        auto firstLock = firstProvider.lock("d/live-consul-lifecycle");
        scope(exit) firstLock.destroy();
        auto secondLock = secondProvider.lock("d/live-consul-lifecycle");
        scope(exit) secondLock.destroy();

        ConsulLease first;
        enforce(firstLock.tryAcquire(10000, first), "first Consul acquisition failed");
        scope(exit) first.destroy();
        auto firstFence = first.fence();
        enforce(firstFence.epoch == epoch && firstFence.lockIndex > 0,
            "first Consul fence did not match the configured epoch");
        ConsulLease busy;
        enforce(!secondLock.tryAcquire(10000, busy), "contending D provider acquired the held key");
        enforce(first.renew(), "Consul session renewal was not confirmed");
        enforce(first.release() == ReleaseOutcome.released, "Consul release was not confirmed");

        ConsulLease successor;
        enforce(secondLock.tryAcquire(10000, successor), "Consul successor did not acquire after release");
        scope(exit) successor.destroy();
        auto successorFence = successor.fence();
        enforce(successorFence.epoch == epoch && successorFence.lockIndex > firstFence.lockIndex,
            "Consul LockIndex did not increase across handoff");
        enforce(successor.release() == ReleaseOutcome.released, "Consul successor release was not confirmed");
        writeln("PASS D Consul: contention, renewal, release, handoff, and LockIndex fencing");
    } catch (Exception error) {
        stderr.writeln("FAIL D Consul: ", error.msg);
        throw error;
    }
}
