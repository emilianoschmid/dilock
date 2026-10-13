const std = @import("std");
const c = @import("c");
const dilock = @import("dilock");

test {
    _ = @import("dilock");
}

test "C ABI version and default policies match the public contract" {
    try std.testing.expectEqual(c.DILOCK_C_ABI_VERSION, c.dilock_c_abi_version());
    var redis: c.dilock_redis_options = undefined;
    var etcd: c.dilock_etcd_options = undefined;
    c.dilock_redis_options_init(&redis);
    c.dilock_etcd_options_init(&etcd);
    try std.testing.expectEqual(@as(i64, 5000), redis.default_wait_ms);
    try std.testing.expectEqual(@as(i64, 30000), redis.default_lease_ms);
    try std.testing.expectEqual(@as(i64, 5000), etcd.default_wait_ms);
    try std.testing.expectEqual(@as(i64, 30000), etcd.default_lease_ms);
}

test "ABI status values remain the checked local version 1 mapping" {
    try std.testing.expectEqual(@as(c_int, 0), c.DILOCK_STATUS_OK);
    try std.testing.expectEqual(@as(c_int, 1), c.DILOCK_STATUS_BUSY);
    try std.testing.expectEqual(@as(c_int, 9), c.DILOCK_STATUS_UNSUPPORTED_CAPABILITY);
}


test "facade methods reject null handles before crossing the C ABI" {
    var redis_provider = dilock.RedisProvider{ .raw = null };
    defer redis_provider.deinit();
    try std.testing.expectError(error.InvalidArgument, redis_provider.lock("resource"));

    var redis_lock = dilock.RedisLock{ .raw = null };
    try std.testing.expectError(error.InvalidArgument, redis_lock.acquireDefault());
    try std.testing.expectError(error.InvalidArgument, redis_lock.acquire(1, 1));
    var cancellation = dilock.Cancellation{ .raw = null };
    try std.testing.expectError(error.InvalidArgument, redis_lock.acquireCancellable(1, 1, &cancellation));
    try std.testing.expectError(error.InvalidArgument, redis_lock.acquireDefaultCancellable(&cancellation));
    try std.testing.expectError(error.InvalidArgument, redis_lock.tryAcquire(1));
    redis_lock.deinit();

    var redis_lease = dilock.RedisLease{ .raw = null };
    try std.testing.expect(!redis_lease.isValid());
    try std.testing.expectError(error.InvalidArgument, redis_lease.release());
    try std.testing.expectError(error.InvalidArgument, redis_lease.renew(1));
    redis_lease.deinit();

    var etcd_provider = dilock.EtcdProvider{ .raw = null };
    defer etcd_provider.deinit();
    try std.testing.expectError(error.InvalidArgument, etcd_provider.lock("resource"));

    var etcd_lock = dilock.EtcdLock{ .raw = null };
    try std.testing.expectError(error.InvalidArgument, etcd_lock.acquireDefault());
    try std.testing.expectError(error.InvalidArgument, etcd_lock.acquire(1, 1));
    try std.testing.expectError(error.InvalidArgument, etcd_lock.acquireCancellable(1, 1, &cancellation));
    try std.testing.expectError(error.InvalidArgument, etcd_lock.acquireDefaultCancellable(&cancellation));
    try std.testing.expectError(error.InvalidArgument, etcd_lock.tryAcquire(1));
    etcd_lock.deinit();

    var etcd_lease = dilock.EtcdLease{ .raw = null };
    try std.testing.expect(!etcd_lease.isValid());
    try std.testing.expectError(error.InvalidArgument, etcd_lease.effectiveTtlMs());
    try std.testing.expectError(error.InvalidArgument, etcd_lease.fence(std.testing.allocator));
    try std.testing.expectError(error.InvalidArgument, etcd_lease.release());
    etcd_lease.deinit();

    try std.testing.expectError(error.InvalidArgument, cancellation.request());
    cancellation.deinit();
}
