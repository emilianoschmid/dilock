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
    var consul: c.dilock_consul_options = undefined;
    c.dilock_redis_options_init(&redis);
    c.dilock_etcd_options_init(&etcd);
    c.dilock_consul_options_init(&consul);
    try std.testing.expectEqual(@as(i64, 5000), redis.default_wait_ms);
    try std.testing.expectEqual(@as(i64, 30000), redis.default_lease_ms);
    try std.testing.expectEqual(@as(i64, 5000), etcd.default_wait_ms);
    try std.testing.expectEqual(@as(i64, 30000), etcd.default_lease_ms);
    try std.testing.expectEqual(@as(i64, 5000), consul.default_wait_ms);
    try std.testing.expectEqual(@as(i64, 30000), consul.default_lease_ms);
    try std.testing.expectEqualStrings("http://127.0.0.1:8500", std.mem.span(consul.endpoint));
}

test "ABI status values remain the checked local version 1 mapping" {
    try std.testing.expectEqual(@as(c_int, 0), c.DILOCK_STATUS_OK);
    try std.testing.expectEqual(@as(c_int, 1), c.DILOCK_STATUS_BUSY);
    try std.testing.expectEqual(@as(c_int, 9), c.DILOCK_STATUS_UNSUPPORTED_CAPABILITY);
}

test "PostgreSQL facade preserves contention, liveness, release and handoff" {
    const dsn = std.posix.getenv("DILOCK_POSTGRES_DSN") orelse return;
    const testing = std.testing;
    const namespace = "zig-postgres-live";
    const epoch = "zig-postgres-live-epoch";
    const config = dilock.PostgresProviderOptions{
        .connection_string = dsn,
        .namespace = namespace,
        .coordination_epoch = epoch,
    };
    try dilock.provisionPostgresNamespace(config);

    var owner_provider = try dilock.PostgresProvider.connectWithOptions(config);
    defer owner_provider.deinit();
    var successor_provider = try dilock.PostgresProvider.connectWithOptions(config);
    defer successor_provider.deinit();
    var owner_lock = try owner_provider.lock("zig/postgres-live");
    defer owner_lock.deinit();
    var successor_lock = try successor_provider.lock("zig/postgres-live");
    defer successor_lock.deinit();

    var owner = (try owner_lock.tryAcquire()).?;
    defer owner.deinit();
    try testing.expect(owner.isValid());
    try testing.expect((try successor_lock.tryAcquire()) == null);
    try testing.expect(try owner.renew());
    try testing.expectEqual(dilock.ReleaseOutcome.released, try owner.release());

    var successor = (try successor_lock.tryAcquire()).?;
    defer successor.deinit();
    try testing.expect(successor.isValid());
    try testing.expectEqual(dilock.ReleaseOutcome.released, try successor.release());
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

    var consul_provider = dilock.ConsulProvider{ .raw = null };
    defer consul_provider.deinit();
    try std.testing.expectError(error.InvalidArgument, consul_provider.lock("resource"));

    var consul_lock = dilock.ConsulLock{ .raw = null };
    try std.testing.expectError(error.InvalidArgument, consul_lock.acquireDefault());
    try std.testing.expectError(error.InvalidArgument, consul_lock.acquire(1, 1));
    try std.testing.expectError(error.InvalidArgument, consul_lock.acquireCancellable(1, 1, &cancellation));
    try std.testing.expectError(error.InvalidArgument, consul_lock.acquireDefaultCancellable(&cancellation));
    try std.testing.expectError(error.InvalidArgument, consul_lock.tryAcquire(1));
    consul_lock.deinit();

    var consul_lease = dilock.ConsulLease{ .raw = null };
    try std.testing.expect(!consul_lease.isValid());
    try std.testing.expectError(error.InvalidArgument, consul_lease.effectiveTtlMs());
    try std.testing.expectError(error.InvalidArgument, consul_lease.fence(std.testing.allocator));
    try std.testing.expectError(error.InvalidArgument, consul_lease.renew());
    try std.testing.expectError(error.InvalidArgument, consul_lease.release());
    consul_lease.deinit();

    try std.testing.expectError(error.InvalidArgument, cancellation.request());
    cancellation.deinit();
}
