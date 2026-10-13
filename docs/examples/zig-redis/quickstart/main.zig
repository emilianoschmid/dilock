const std = @import("std");
const dilock = @import("dilock");

pub fn main() !void {
    var provider = try dilock.RedisProvider.connect("127.0.0.1", 6379, "billing", "local-redis-epoch");
    defer provider.deinit();
    var lock = try provider.lock("invoice/42/reconcile");
    defer lock.deinit();
    var lease = try lock.acquireDefault();
    defer lease.deinit();
    if (!lease.isValid()) return error.LeaseLost;
    const released = try lease.release();
    if (released != .released) return error.ReleaseNotConfirmed;
    std.debug.print("Redis release: {s}\n", .{@tagName(released)});
}
