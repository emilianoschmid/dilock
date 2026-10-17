const std = @import("std");
const dilock = @import("dilock");

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const endpoint_env = init.environ_map.get("DILOCK_CONSUL_ENDPOINT") orelse "http://127.0.0.1:8500";
    const token_env = init.environ_map.get("DILOCK_CONSUL_ACL_TOKEN") orelse "";
    const endpoint = try allocator.dupeSentinel(u8, endpoint_env, 0);
    defer allocator.free(endpoint);
    const token = try allocator.dupeSentinel(u8, token_env, 0);
    defer allocator.free(token);

    var provider = try dilock.ConsulProvider.connectWithOptions(.{
        .endpoint = endpoint,
        .namespace = "billing",
        .coordination_epoch = "local-consul-epoch",
        .acl_token = token,
    });
    defer provider.deinit();
    var lock = try provider.lock("invoice/42/reconcile");
    defer lock.deinit();
    var lease = try lock.acquireDefault();
    defer lease.deinit();
    if (!lease.isValid()) return error.LeaseLost;
    var fence = try lease.fence(allocator);
    defer fence.deinit();
    std.debug.print("Consul fence: {s}:{d}, effective TTL: {d} ms\n", .{
        fence.epoch,
        fence.lock_index,
        try lease.effectiveTtlMs(),
    });
    const released = try lease.release();
    if (released != .released) return error.ReleaseNotConfirmed;
}
