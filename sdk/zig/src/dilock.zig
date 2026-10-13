const std = @import("std");
const c = @import("c");

pub const Failure = error{
    AbiMismatch,
    InvalidArgument,
    Busy,
    Cancelled,
    WaitTimeout,
    BackendUnavailable,
    AcquisitionUnknown,
    ProtocolMismatch,
    LeaseLost,
    UnsupportedCapability,
    Internal,
};

pub const ReleaseOutcome = enum { released, already_not_owned, unknown };

/// Redis connection and provider-wide timing settings. String values must be
/// NUL-terminated; defaults match `dilock_redis_options_init`.
pub const RedisProviderOptions = struct {
    host: [*:0]const u8 = "127.0.0.1",
    port: i32 = 6379,
    username: [*:0]const u8 = "",
    password: [*:0]const u8 = "",
    database: i32 = 0,
    namespace: [*:0]const u8 = "default",
    coordination_epoch: [*:0]const u8 = "",
    rpc_timeout_ms: i64 = 2000,
    cleanup_timeout_ms: i64 = 2000,
    safety_margin_ms: i64 = 25,
    poll_interval_ms: i64 = 25,
    default_wait_ms: i64 = 5000,
    default_lease_ms: i64 = 30000,
};

/// etcd connection, TLS and provider-wide timing settings. TLS fields contain
/// PEM content. Defaults match `dilock_etcd_options_init`.
pub const EtcdProviderOptions = struct {
    endpoint: [*:0]const u8 = "127.0.0.1:2379",
    namespace: [*:0]const u8 = "default",
    coordination_epoch: [*:0]const u8 = "",
    tls_root_certificates: ?[*:0]const u8 = null,
    tls_client_certificate: ?[*:0]const u8 = null,
    tls_client_private_key: ?[*:0]const u8 = null,
    rpc_timeout_ms: i64 = 2000,
    cleanup_timeout_ms: i64 = 2000,
    safety_margin_ms: i64 = 25,
    default_wait_ms: i64 = 5000,
    default_lease_ms: i64 = 30000,
    auto_renew: bool = false,
};

fn nativeRedisOptions(config: RedisProviderOptions) c.dilock_redis_options {
    var options: c.dilock_redis_options = undefined;
    c.dilock_redis_options_init(&options);
    options.host = config.host;
    options.port = config.port;
    options.username = config.username;
    options.password = config.password;
    options.database = config.database;
    options.name_space = config.namespace;
    options.coordination_epoch = config.coordination_epoch;
    options.rpc_timeout_ms = config.rpc_timeout_ms;
    options.cleanup_timeout_ms = config.cleanup_timeout_ms;
    options.safety_margin_ms = config.safety_margin_ms;
    options.poll_interval_ms = config.poll_interval_ms;
    options.default_wait_ms = config.default_wait_ms;
    options.default_lease_ms = config.default_lease_ms;
    return options;
}

fn nativeEtcdOptions(config: EtcdProviderOptions) c.dilock_etcd_options {
    var options: c.dilock_etcd_options = undefined;
    c.dilock_etcd_options_init(&options);
    options.endpoint = config.endpoint;
    options.name_space = config.namespace;
    options.coordination_epoch = config.coordination_epoch;
    options.tls_root_certificates = config.tls_root_certificates;
    options.tls_client_certificate = config.tls_client_certificate;
    options.tls_client_private_key = config.tls_client_private_key;
    options.rpc_timeout_ms = config.rpc_timeout_ms;
    options.cleanup_timeout_ms = config.cleanup_timeout_ms;
    options.safety_margin_ms = config.safety_margin_ms;
    options.default_wait_ms = config.default_wait_ms;
    options.default_lease_ms = config.default_lease_ms;
    options.auto_renew = @intFromBool(config.auto_renew);
    return options;
}

fn ensureAbi() Failure!void {
    if (c.dilock_c_abi_version() != c.DILOCK_C_ABI_VERSION) return error.AbiMismatch;
}

/// A successful C ABI call that promises an owning handle must never be
/// exposed as a deinitialized Zig wrapper. Treat a missing output as an ABI
/// contract violation instead of returning a value that fails much later.
fn requireHandle(comptime T: type, raw: ?*T) Failure!*T {
    return raw orelse error.Internal;
}

test "successful ABI calls must return non-null owning handles" {
    const testing = std.testing;
    try testing.expectError(error.Internal, requireHandle(u8, null));
    var value: u8 = 7;
    try testing.expectEqual(&value, try requireHandle(u8, &value));
}

test "every C ABI status keeps its typed Zig error mapping" {
    const testing = std.testing;
    var detail: c.dilock_error = undefined;

    try check(c.DILOCK_STATUS_OK, &detail);
    try testing.expectError(error.Busy, check(c.DILOCK_STATUS_BUSY, &detail));
    try testing.expectError(error.InvalidArgument, check(c.DILOCK_STATUS_INVALID_ARGUMENT, &detail));
    try testing.expectError(error.Cancelled, check(c.DILOCK_STATUS_CANCELLED, &detail));
    try testing.expectError(error.WaitTimeout, check(c.DILOCK_STATUS_WAIT_TIMEOUT, &detail));
    try testing.expectError(error.BackendUnavailable, check(c.DILOCK_STATUS_BACKEND_UNAVAILABLE, &detail));
    try testing.expectError(error.AcquisitionUnknown, check(c.DILOCK_STATUS_ACQUISITION_UNKNOWN, &detail));
    try testing.expectError(error.ProtocolMismatch, check(c.DILOCK_STATUS_PROTOCOL_MISMATCH, &detail));
    try testing.expectError(error.LeaseLost, check(c.DILOCK_STATUS_LEASE_LOST, &detail));
    try testing.expectError(error.UnsupportedCapability, check(c.DILOCK_STATUS_UNSUPPORTED_CAPABILITY, &detail));
    try testing.expectError(error.Internal, check(c.DILOCK_STATUS_INTERNAL, &detail));
}

test "every C ABI release outcome keeps its typed Zig mapping" {
    const testing = std.testing;

    try testing.expectEqual(ReleaseOutcome.released, try outcome(c.DILOCK_RELEASED));
    try testing.expectEqual(ReleaseOutcome.already_not_owned, try outcome(c.DILOCK_ALREADY_NOT_OWNED));
    try testing.expectEqual(ReleaseOutcome.unknown, try outcome(c.DILOCK_RELEASE_UNKNOWN));
}

fn check(status: c.dilock_status, detail: *c.dilock_error) Failure!void {
    if (status == c.DILOCK_STATUS_OK) return;
    _ = detail;
    return switch (status) {
        c.DILOCK_STATUS_BUSY => error.Busy,
        c.DILOCK_STATUS_INVALID_ARGUMENT => error.InvalidArgument,
        c.DILOCK_STATUS_CANCELLED => error.Cancelled,
        c.DILOCK_STATUS_WAIT_TIMEOUT => error.WaitTimeout,
        c.DILOCK_STATUS_BACKEND_UNAVAILABLE => error.BackendUnavailable,
        c.DILOCK_STATUS_ACQUISITION_UNKNOWN => error.AcquisitionUnknown,
        c.DILOCK_STATUS_PROTOCOL_MISMATCH => error.ProtocolMismatch,
        c.DILOCK_STATUS_LEASE_LOST => error.LeaseLost,
        c.DILOCK_STATUS_UNSUPPORTED_CAPABILITY => error.UnsupportedCapability,
        else => error.Internal,
    };
}

fn outcome(value: c.dilock_release_outcome) Failure!ReleaseOutcome {
    return switch (value) {
        c.DILOCK_RELEASED => .released,
        c.DILOCK_ALREADY_NOT_OWNED => .already_not_owned,
        c.DILOCK_RELEASE_UNKNOWN => .unknown,
        else => error.Internal,
    };
}

pub const Cancellation = struct {
    raw: ?*c.dilock_cancellation,

    pub fn init() Failure!Cancellation {
        try ensureAbi();
        var detail: c.dilock_error = undefined;
        const raw = c.dilock_cancellation_create(&detail);
        return .{ .raw = try requireHandle(c.dilock_cancellation, raw) };
    }

    /// Requests cancellation. A deinitialized token is a caller error; silently
    /// ignoring it could leave a blocking acquisition running unexpectedly.
    pub fn request(self: *Cancellation) Failure!void {
        const raw = self.raw orelse return error.InvalidArgument;
        c.dilock_cancellation_request(raw);
    }

    pub fn deinit(self: *Cancellation) void {
        if (self.raw) |raw| c.dilock_cancellation_destroy(raw);
        self.raw = null;
    }
};

pub const RedisProvider = struct {
    raw: ?*c.dilock_redis_provider,

    pub fn connect(host: [*:0]const u8, port: i32, namespace: [*:0]const u8, epoch: [*:0]const u8) Failure!RedisProvider {
        return connectWithOptions(.{
            .host = host,
            .port = port,
            .namespace = namespace,
            .coordination_epoch = epoch,
        });
    }

    pub fn connectWithOptions(config: RedisProviderOptions) Failure!RedisProvider {
        try ensureAbi();
        var options = nativeRedisOptions(config);
        var detail: c.dilock_error = undefined;
        var raw: ?*c.dilock_redis_provider = null;
        try check(c.dilock_redis_provider_create(&options, &raw, &detail), &detail);
        return .{ .raw = try requireHandle(c.dilock_redis_provider, raw) };
    }

    pub fn lock(self: *RedisProvider, resource: [*:0]const u8) Failure!RedisLock {
        var detail: c.dilock_error = undefined;
        var raw: ?*c.dilock_redis_lock = null;
        try check(c.dilock_redis_lock_create(self.raw orelse return error.InvalidArgument, resource, &raw, &detail), &detail);
        return .{ .raw = try requireHandle(c.dilock_redis_lock, raw) };
    }

    pub fn deinit(self: *RedisProvider) void {
        if (self.raw) |raw| c.dilock_redis_provider_destroy(raw);
        self.raw = null;
    }
};

pub const RedisLock = struct {
    raw: ?*c.dilock_redis_lock,

    pub fn acquireDefault(self: *RedisLock) Failure!RedisLease {
        var detail: c.dilock_error = undefined;
        var raw: ?*c.dilock_redis_lease = null;
        try check(c.dilock_redis_acquire_default(self.raw orelse return error.InvalidArgument, &raw, &detail), &detail);
        return .{ .raw = try requireHandle(c.dilock_redis_lease, raw) };
    }

    pub fn acquire(self: *RedisLock, wait_ms: i64, lease_ms: i64) Failure!RedisLease {
        var detail: c.dilock_error = undefined;
        var raw: ?*c.dilock_redis_lease = null;
        try check(c.dilock_redis_acquire(self.raw orelse return error.InvalidArgument, wait_ms, lease_ms, &raw, &detail), &detail);
        return .{ .raw = try requireHandle(c.dilock_redis_lease, raw) };
    }

    pub fn acquireCancellable(self: *RedisLock, wait_ms: i64, lease_ms: i64, cancellation: *Cancellation) Failure!RedisLease {
        var detail: c.dilock_error = undefined;
        var raw: ?*c.dilock_redis_lease = null;
        try check(c.dilock_redis_acquire_with_cancellation(self.raw orelse return error.InvalidArgument, wait_ms, lease_ms, cancellation.raw orelse return error.InvalidArgument, &raw, &detail), &detail);
        return .{ .raw = try requireHandle(c.dilock_redis_lease, raw) };
    }

    /// Acquires with the provider's default wait and lease durations while
    /// observing the supplied cancellation handle.
    pub fn acquireDefaultCancellable(self: *RedisLock, cancellation: *Cancellation) Failure!RedisLease {
        var detail: c.dilock_error = undefined;
        var raw: ?*c.dilock_redis_lease = null;
        try check(c.dilock_redis_acquire_default_with_cancellation(self.raw orelse return error.InvalidArgument, cancellation.raw orelse return error.InvalidArgument, &raw, &detail), &detail);
        return .{ .raw = try requireHandle(c.dilock_redis_lease, raw) };
    }

    pub fn tryAcquire(self: *RedisLock, lease_ms: i64) Failure!?RedisLease {
        var detail: c.dilock_error = undefined;
        var raw: ?*c.dilock_redis_lease = null;
        const status = c.dilock_redis_try_acquire(self.raw orelse return error.InvalidArgument, lease_ms, &raw, &detail);
        if (status == c.DILOCK_STATUS_BUSY) return null;
        try check(status, &detail);
        return .{ .raw = try requireHandle(c.dilock_redis_lease, raw) };
    }

    pub fn deinit(self: *RedisLock) void {
        if (self.raw) |raw| c.dilock_redis_lock_destroy(raw);
        self.raw = null;
    }
};

pub const RedisLease = struct {
    raw: ?*c.dilock_redis_lease,

    pub fn isValid(self: *const RedisLease) bool {
        return if (self.raw) |raw| c.dilock_redis_lease_is_valid(raw) != 0 else false;
    }

    pub fn release(self: *RedisLease) Failure!ReleaseOutcome {
        var detail: c.dilock_error = undefined;
        var result: c.dilock_release_outcome = undefined;
        try check(c.dilock_redis_lease_release(self.raw orelse return error.InvalidArgument, &result, &detail), &detail);
        return outcome(result);
    }

    pub fn renew(self: *RedisLease, lease_ms: i64) Failure!bool {
        var detail: c.dilock_error = undefined;
        var renewed: i32 = 0;
        try check(c.dilock_redis_lease_renew(self.raw orelse return error.InvalidArgument, lease_ms, &renewed, &detail), &detail);
        return renewed != 0;
    }

    pub fn deinit(self: *RedisLease) void {
        if (self.raw) |raw| c.dilock_redis_lease_destroy(raw);
        self.raw = null;
    }
};

pub const EtcdProvider = struct {
    raw: ?*c.dilock_etcd_provider,

    pub fn connect(endpoint: [*:0]const u8, namespace: [*:0]const u8, epoch: [*:0]const u8) Failure!EtcdProvider {
        return connectWithOptions(.{
            .endpoint = endpoint,
            .namespace = namespace,
            .coordination_epoch = epoch,
        });
    }

    pub fn connectWithOptions(config: EtcdProviderOptions) Failure!EtcdProvider {
        try ensureAbi();
        var options = nativeEtcdOptions(config);
        var detail: c.dilock_error = undefined;
        var raw: ?*c.dilock_etcd_provider = null;
        try check(c.dilock_etcd_provider_create(&options, &raw, &detail), &detail);
        return .{ .raw = try requireHandle(c.dilock_etcd_provider, raw) };
    }

    pub fn lock(self: *EtcdProvider, resource: [*:0]const u8) Failure!EtcdLock {
        var detail: c.dilock_error = undefined;
        var raw: ?*c.dilock_etcd_lock = null;
        try check(c.dilock_etcd_lock_create(self.raw orelse return error.InvalidArgument, resource, &raw, &detail), &detail);
        return .{ .raw = try requireHandle(c.dilock_etcd_lock, raw) };
    }

    pub fn deinit(self: *EtcdProvider) void {
        if (self.raw) |raw| c.dilock_etcd_provider_destroy(raw);
        self.raw = null;
    }
};

pub const EtcdLock = struct {
    raw: ?*c.dilock_etcd_lock,

    pub fn acquireDefault(self: *EtcdLock) Failure!EtcdLease {
        var detail: c.dilock_error = undefined;
        var raw: ?*c.dilock_etcd_lease = null;
        try check(c.dilock_etcd_acquire_default(self.raw orelse return error.InvalidArgument, &raw, &detail), &detail);
        return .{ .raw = try requireHandle(c.dilock_etcd_lease, raw) };
    }

    pub fn acquire(self: *EtcdLock, wait_ms: i64, lease_ms: i64) Failure!EtcdLease {
        var detail: c.dilock_error = undefined;
        var raw: ?*c.dilock_etcd_lease = null;
        try check(c.dilock_etcd_acquire(self.raw orelse return error.InvalidArgument, wait_ms, lease_ms, &raw, &detail), &detail);
        return .{ .raw = try requireHandle(c.dilock_etcd_lease, raw) };
    }

    pub fn acquireCancellable(self: *EtcdLock, wait_ms: i64, lease_ms: i64, cancellation: *Cancellation) Failure!EtcdLease {
        var detail: c.dilock_error = undefined;
        var raw: ?*c.dilock_etcd_lease = null;
        try check(c.dilock_etcd_acquire_with_cancellation(self.raw orelse return error.InvalidArgument, wait_ms, lease_ms, cancellation.raw orelse return error.InvalidArgument, &raw, &detail), &detail);
        return .{ .raw = try requireHandle(c.dilock_etcd_lease, raw) };
    }

    /// Acquires with the provider's default wait and lease durations while
    /// observing the supplied cancellation handle.
    pub fn acquireDefaultCancellable(self: *EtcdLock, cancellation: *Cancellation) Failure!EtcdLease {
        var detail: c.dilock_error = undefined;
        var raw: ?*c.dilock_etcd_lease = null;
        try check(c.dilock_etcd_acquire_default_with_cancellation(self.raw orelse return error.InvalidArgument, cancellation.raw orelse return error.InvalidArgument, &raw, &detail), &detail);
        return .{ .raw = try requireHandle(c.dilock_etcd_lease, raw) };
    }

    pub fn tryAcquire(self: *EtcdLock, lease_ms: i64) Failure!?EtcdLease {
        var detail: c.dilock_error = undefined;
        var raw: ?*c.dilock_etcd_lease = null;
        try check(c.dilock_etcd_try_acquire(self.raw orelse return error.InvalidArgument, lease_ms, &raw, &detail), &detail);
        return .{ .raw = try requireHandle(c.dilock_etcd_lease, raw) };
    }

    pub fn deinit(self: *EtcdLock) void {
        if (self.raw) |raw| c.dilock_etcd_lock_destroy(raw);
        self.raw = null;
    }
};

/// An etcd fence owns its epoch storage. Do not copy this value; call `deinit`
/// exactly once for its sole owner.
pub const EtcdFence = struct {
    epoch: []const u8,
    create_revision: i64,
    storage: []u8,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *EtcdFence) void {
        self.allocator.free(self.storage);
        self.epoch = &.{};
        self.storage = &.{};
    }
};

pub const EtcdLease = struct {
    raw: ?*c.dilock_etcd_lease,

    pub fn isValid(self: *const EtcdLease) bool {
        return if (self.raw) |raw| c.dilock_etcd_lease_is_valid(raw) != 0 else false;
    }

    /// Returns etcd's server-granted lease TTL in milliseconds. etcd grants
    /// whole-second TTLs, so this may be greater than the requested duration.
    pub fn effectiveTtlMs(self: *const EtcdLease) Failure!i64 {
        var detail: c.dilock_error = undefined;
        var ttl_ms: i64 = 0;
        try check(c.dilock_etcd_lease_effective_ttl_ms(
            self.raw orelse return error.InvalidArgument,
            &ttl_ms,
            &detail,
        ), &detail);
        return ttl_ms;
    }

    /// Reads the epoch and etcd creation revision. The caller supplies the
    /// allocator because deployment epochs have no fixed maximum length.
    pub fn fence(self: *const EtcdLease, allocator: std.mem.Allocator) (Failure || std.mem.Allocator.Error)!EtcdFence {
        var detail: c.dilock_error = undefined;
        var value: c.dilock_etcd_fence = .{ .create_revision = 0, .coordination_epoch_bytes = 0 };
        const raw = self.raw orelse return error.InvalidArgument;

        // The C ABI reports the required epoch byte count before validating
        // the output buffer, so a zero-capacity probe gives us a bounded exact
        // allocation size without imposing a Zig-only epoch limit.
        const probe = c.dilock_etcd_lease_fence(raw, &value, null, 0, &detail);
        if (probe != c.DILOCK_STATUS_INVALID_ARGUMENT) try check(probe, &detail);

        const capacity = std.math.add(usize, value.coordination_epoch_bytes, 1) catch return error.Internal;
        const storage = try allocator.alloc(u8, capacity);
        errdefer allocator.free(storage);
        try check(c.dilock_etcd_lease_fence(raw, &value, storage.ptr, storage.len, &detail), &detail);
        return .{
            .epoch = storage[0..value.coordination_epoch_bytes],
            .create_revision = value.create_revision,
            .storage = storage,
            .allocator = allocator,
        };
    }

    pub fn release(self: *EtcdLease) Failure!ReleaseOutcome {
        var detail: c.dilock_error = undefined;
        var result: c.dilock_release_outcome = undefined;
        try check(c.dilock_etcd_lease_release(self.raw orelse return error.InvalidArgument, &result, &detail), &detail);
        return outcome(result);
    }

    pub fn deinit(self: *EtcdLease) void {
        if (self.raw) |raw| c.dilock_etcd_lease_destroy(raw);
        self.raw = null;
    }
};

test "configured provider options preserve all C ABI settings" {
    const testing = std.testing;
    const redis = RedisProviderOptions{
        .host = "cache.internal",
        .port = 6380,
        .username = "dilock-client",
        .password = "local-test-secret",
        .database = 4,
        .namespace = "payments",
        .coordination_epoch = "epoch-7",
        .rpc_timeout_ms = 800,
        .cleanup_timeout_ms = 700,
        .safety_margin_ms = 30,
        .poll_interval_ms = 15,
        .default_wait_ms = 6000,
        .default_lease_ms = 45000,
    };
    const native_redis = nativeRedisOptions(redis);
    try testing.expectEqualStrings("cache.internal", std.mem.span(native_redis.host));
    try testing.expectEqualStrings("dilock-client", std.mem.span(native_redis.username));
    try testing.expectEqualStrings("local-test-secret", std.mem.span(native_redis.password));
    try testing.expectEqual(@as(i32, 6380), native_redis.port);
    try testing.expectEqual(@as(i32, 4), native_redis.database);
    try testing.expectEqualStrings("payments", std.mem.span(native_redis.name_space));
    try testing.expectEqualStrings("epoch-7", std.mem.span(native_redis.coordination_epoch));
    try testing.expectEqual(@as(i64, 800), native_redis.rpc_timeout_ms);
    try testing.expectEqual(@as(i64, 700), native_redis.cleanup_timeout_ms);
    try testing.expectEqual(@as(i64, 30), native_redis.safety_margin_ms);
    try testing.expectEqual(@as(i64, 15), native_redis.poll_interval_ms);
    try testing.expectEqual(@as(i64, 6000), native_redis.default_wait_ms);
    try testing.expectEqual(@as(i64, 45000), native_redis.default_lease_ms);

    const etcd = EtcdProviderOptions{
        .endpoint = "etcd.internal:2379",
        .namespace = "payments",
        .coordination_epoch = "epoch-8",
        .tls_root_certificates = "root PEM",
        .tls_client_certificate = "client PEM",
        .tls_client_private_key = "private key PEM",
        .rpc_timeout_ms = 900,
        .cleanup_timeout_ms = 650,
        .safety_margin_ms = 35,
        .default_wait_ms = 7000,
        .default_lease_ms = 60000,
        .auto_renew = true,
    };
    const native_etcd = nativeEtcdOptions(etcd);
    try testing.expectEqualStrings("etcd.internal:2379", std.mem.span(native_etcd.endpoint));
    try testing.expectEqualStrings("payments", std.mem.span(native_etcd.name_space));
    try testing.expectEqualStrings("epoch-8", std.mem.span(native_etcd.coordination_epoch));
    try testing.expectEqualStrings("root PEM", std.mem.span(native_etcd.tls_root_certificates.?));
    try testing.expectEqualStrings("client PEM", std.mem.span(native_etcd.tls_client_certificate.?));
    try testing.expectEqualStrings("private key PEM", std.mem.span(native_etcd.tls_client_private_key.?));
    try testing.expectEqual(@as(i64, 900), native_etcd.rpc_timeout_ms);
    try testing.expectEqual(@as(i64, 650), native_etcd.cleanup_timeout_ms);
    try testing.expectEqual(@as(i64, 35), native_etcd.safety_margin_ms);
    try testing.expectEqual(@as(i64, 7000), native_etcd.default_wait_ms);
    try testing.expectEqual(@as(i64, 60000), native_etcd.default_lease_ms);
    try testing.expectEqual(@as(i32, 1), native_etcd.auto_renew);
}

test "default provider options stay aligned with C ABI initializers" {
    const testing = std.testing;

    var expected_redis: c.dilock_redis_options = undefined;
    c.dilock_redis_options_init(&expected_redis);
    const actual_redis = nativeRedisOptions(.{});
    try testing.expectEqualStrings(std.mem.span(expected_redis.host), std.mem.span(actual_redis.host));
    try testing.expectEqual(expected_redis.port, actual_redis.port);
    try testing.expectEqualStrings(std.mem.span(expected_redis.username), std.mem.span(actual_redis.username));
    try testing.expectEqualStrings(std.mem.span(expected_redis.password), std.mem.span(actual_redis.password));
    try testing.expectEqual(expected_redis.database, actual_redis.database);
    try testing.expectEqualStrings(std.mem.span(expected_redis.name_space), std.mem.span(actual_redis.name_space));
    try testing.expectEqualStrings(std.mem.span(expected_redis.coordination_epoch), std.mem.span(actual_redis.coordination_epoch));
    try testing.expectEqual(expected_redis.cleanup_callback, actual_redis.cleanup_callback);
    try testing.expectEqual(expected_redis.cleanup_user_data, actual_redis.cleanup_user_data);
    try testing.expectEqual(expected_redis.rpc_timeout_ms, actual_redis.rpc_timeout_ms);
    try testing.expectEqual(expected_redis.cleanup_timeout_ms, actual_redis.cleanup_timeout_ms);
    try testing.expectEqual(expected_redis.safety_margin_ms, actual_redis.safety_margin_ms);
    try testing.expectEqual(expected_redis.poll_interval_ms, actual_redis.poll_interval_ms);
    try testing.expectEqual(expected_redis.default_wait_ms, actual_redis.default_wait_ms);
    try testing.expectEqual(expected_redis.default_lease_ms, actual_redis.default_lease_ms);

    var expected_etcd: c.dilock_etcd_options = undefined;
    c.dilock_etcd_options_init(&expected_etcd);
    const actual_etcd = nativeEtcdOptions(.{});
    try testing.expectEqualStrings(std.mem.span(expected_etcd.endpoint), std.mem.span(actual_etcd.endpoint));
    try testing.expectEqualStrings(std.mem.span(expected_etcd.name_space), std.mem.span(actual_etcd.name_space));
    try testing.expectEqualStrings(std.mem.span(expected_etcd.coordination_epoch), std.mem.span(actual_etcd.coordination_epoch));
    try testing.expectEqual(expected_etcd.tls_root_certificates, actual_etcd.tls_root_certificates);
    try testing.expectEqual(expected_etcd.tls_client_certificate, actual_etcd.tls_client_certificate);
    try testing.expectEqual(expected_etcd.tls_client_private_key, actual_etcd.tls_client_private_key);
    try testing.expectEqual(expected_etcd.cleanup_callback, actual_etcd.cleanup_callback);
    try testing.expectEqual(expected_etcd.cleanup_user_data, actual_etcd.cleanup_user_data);
    try testing.expectEqual(expected_etcd.rpc_timeout_ms, actual_etcd.rpc_timeout_ms);
    try testing.expectEqual(expected_etcd.cleanup_timeout_ms, actual_etcd.cleanup_timeout_ms);
    try testing.expectEqual(expected_etcd.safety_margin_ms, actual_etcd.safety_margin_ms);
    try testing.expectEqual(expected_etcd.default_wait_ms, actual_etcd.default_wait_ms);
    try testing.expectEqual(expected_etcd.default_lease_ms, actual_etcd.default_lease_ms);
    try testing.expectEqual(expected_etcd.auto_renew, actual_etcd.auto_renew);
}
