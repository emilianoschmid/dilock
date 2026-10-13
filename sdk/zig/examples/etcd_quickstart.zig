const std = @import("std");
const dilock = @import("dilock");

pub fn main() !void {
    var provider = try dilock.EtcdProvider.connect("127.0.0.1:2379", "billing", "local-etcd-epoch");
    defer provider.deinit();
    var lock = try provider.lock("invoice/42/reconcile");
    defer lock.deinit();
    var lease = try lock.acquireDefault();
    defer lease.deinit();
    if (!lease.isValid()) return error.LeaseLost;
    const effective_ttl_ms = try lease.effectiveTtlMs();
    var fence = try lease.fence(std.heap.page_allocator);
    defer fence.deinit();
    std.debug.print("etcd fence: {s}:{d}, effective TTL: {d} ms\n", .{
        fence.epoch,
        fence.create_revision,
        effective_ttl_ms,
    });
    const released = try lease.release();
    if (released != .released) return error.ReleaseNotConfirmed;
}
