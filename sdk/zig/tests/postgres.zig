const std = @import("std");
const dilock = @import("dilock");

test "PostgreSQL facade preserves contention, liveness, release and handoff" {
    const dsn = std.c.getenv("DILOCK_POSTGRES_DSN") orelse return;
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
