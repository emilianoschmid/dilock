import dilock;
import std.stdio : writeln;

void main() {
    ConsulProviderOptions options;
    options.endpoint = "http://127.0.0.1:8500";
    options.nameSpace = "billing";
    options.coordinationEpoch = "local-consul-epoch";
    auto provider = ConsulProvider.connect(options);
    scope(exit) provider.destroy();

    auto lock = provider.lock("invoice/42/reconcile");
    scope(exit) lock.destroy();
    auto lease = lock.acquireDefault();
    scope(exit) lease.destroy();
    auto fence = lease.fence();
    writeln("Consul fence: ", fence.epoch, ":", fence.lockIndex);
    if (lease.release() != ReleaseOutcome.released)
        throw new Exception("Consul release was not confirmed as released");
}
