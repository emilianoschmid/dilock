module dilock.postgres;

import core.stdc.stdint : int32_t, int64_t, uint32_t;
import core.time : Duration, dur;
import std.algorithm.searching : canFind;
import std.string : toStringz;

/// PostgreSQL-only D facade over the versioned Dilock C ABI.
///
/// This module deliberately contains no declarations or references for the
/// Redis, etcd or Consul routes. It can therefore link against a PostgreSQL-
/// only C ABI installation without requiring unused optional symbols.

enum Status : int {
    ok = 0,
    busy = 1,
    invalidArgument = 2,
    cancelled = 3,
    waitTimeout = 4,
    backendUnavailable = 5,
    acquisitionUnknown = 6,
    protocolMismatch = 7,
    leaseLost = 8,
    unsupportedCapability = 9,
    internalError = 10,
}

enum ReleaseOutcome : int { released = 0, alreadyNotOwned = 1, unknown = 2 }

struct PostgresProviderOptions {
    string connectionString;
    string nameSpace = "default";
    string coordinationEpoch;
    Duration rpcTimeout = dur!"msecs"(2000);
    Duration cleanupTimeout = dur!"msecs"(2000);
    Duration pollInterval = dur!"msecs"(25);
    Duration defaultWait = dur!"msecs"(5000);
    Duration defaultLease = dur!"msecs"(30000);
}

extern(C) {
    struct CError { int32_t code; char[384] message; }
    struct CPostgresOptions {
        const(char)* connection_string;
        const(char)* name_space;
        const(char)* coordination_epoch;
        extern(C) void function(int, int, const(char)*, void*) cleanup_callback;
        void* cleanup_user_data;
        int64_t rpc_timeout_ms;
        int64_t cleanup_timeout_ms;
        int64_t poll_interval_ms;
        int64_t default_wait_ms;
        int64_t default_lease_ms;
    }
    struct CPostgresProvider { }
    struct CPostgresLock { }
    struct CPostgresLease { }

    uint32_t dilock_c_abi_version();
    void dilock_postgres_options_init(CPostgresOptions*);
    Status dilock_postgres_provider_create(CPostgresOptions*, CPostgresProvider**, CError*);
    void dilock_postgres_provider_destroy(CPostgresProvider*);
    Status dilock_postgres_provision_namespace(CPostgresOptions*, CError*);
    Status dilock_postgres_lock_create(CPostgresProvider*, const(char)*, CPostgresLock**, CError*);
    void dilock_postgres_lock_destroy(CPostgresLock*);
    Status dilock_postgres_acquire_default(CPostgresLock*, CPostgresLease**, CError*);
    Status dilock_postgres_acquire(CPostgresLock*, int64_t, int64_t, CPostgresLease**, CError*);
    Status dilock_postgres_try_acquire(CPostgresLock*, CPostgresLease**, CError*);
    int32_t dilock_postgres_lease_is_valid(const(CPostgresLease)*);
    Status dilock_postgres_lease_renew(CPostgresLease*, int32_t*, CError*);
    Status dilock_postgres_lease_release(CPostgresLease*, int*, CError*);
    void dilock_postgres_lease_destroy(CPostgresLease*);
}

class DilockException : Exception {
    private Status _status;
    @property Status status() const @safe { return _status; }
    this(Status value, string message) @safe { _status = value; super(message); }
}

private Status normalizeStatus(Status status) @safe {
    auto raw = cast(int) status;
    if (raw < cast(int) Status.ok || raw > cast(int) Status.internalError)
        return Status.internalError;
    return status;
}

private string cErrorMessage(const ref CError error) @safe {
    size_t end;
    while (end < error.message.length && error.message[end] != '\0') ++end;
    return error.message[0 .. end].idup;
}

private void check(Status status, ref CError error) @trusted {
    if (status == Status.ok) return;
    auto normalized = normalizeStatus(status);
    auto message = cErrorMessage(error);
    if (normalized != status)
        throw new DilockException(Status.internalError,
            "linked Dilock C ABI returned an unknown status code: " ~ message);
    throw new DilockException(normalized, message);
}

private ReleaseOutcome releaseOutcomeFromC(int value) @safe {
    switch (value) {
    case 0: return ReleaseOutcome.released;
    case 1: return ReleaseOutcome.alreadyNotOwned;
    case 2: return ReleaseOutcome.unknown;
    default:
        throw new DilockException(Status.internalError,
            "linked Dilock C ABI returned an unknown release outcome");
    }
}

private void verifyAbi() @trusted {
    if (dilock_c_abi_version() != 1)
        throw new DilockException(Status.unsupportedCapability,
            "linked Dilock C ABI must be version 1");
}

private void validateCStringInput(string value, string label) @safe {
    if (value.canFind('\0'))
        throw new DilockException(Status.invalidArgument,
            label ~ " cannot contain U+0000 because the C ABI uses NUL-terminated strings");
}

private long wholeMilliseconds(Duration value, string label) @safe {
    auto milliseconds = value.total!"msecs";
    if (dur!"msecs"(milliseconds) != value)
        throw new DilockException(Status.invalidArgument,
            label ~ " must be an exact whole-millisecond duration");
    return milliseconds;
}

private CPostgresOptions toNativeOptions(PostgresProviderOptions config) @trusted {
    validateCStringInput(config.connectionString, "PostgreSQL connection string");
    validateCStringInput(config.nameSpace, "namespace");
    validateCStringInput(config.coordinationEpoch, "coordination epoch");
    CPostgresOptions options;
    dilock_postgres_options_init(&options);
    options.connection_string = config.connectionString.toStringz;
    options.name_space = config.nameSpace.toStringz;
    options.coordination_epoch = config.coordinationEpoch.toStringz;
    options.rpc_timeout_ms = wholeMilliseconds(config.rpcTimeout, "PostgreSQL RPC timeout");
    options.cleanup_timeout_ms = wholeMilliseconds(config.cleanupTimeout, "PostgreSQL cleanup timeout");
    options.poll_interval_ms = wholeMilliseconds(config.pollInterval, "PostgreSQL poll interval");
    options.default_wait_ms = wholeMilliseconds(config.defaultWait, "PostgreSQL default wait");
    options.default_lease_ms = wholeMilliseconds(config.defaultLease, "PostgreSQL default lease");
    return options;
}

/// Provision metadata explicitly; provider construction never creates it.
void provisionPostgresNamespace(PostgresProviderOptions config) @trusted {
    verifyAbi();
    auto options = toNativeOptions(config);
    CError error;
    check(dilock_postgres_provision_namespace(&options, &error), error);
}

struct PostgresProvider {
    private CPostgresProvider* raw;
    @disable this(this);

    static PostgresProvider connect(string connectionString, string namespace, string epoch) @trusted {
        PostgresProviderOptions config;
        config.connectionString = connectionString;
        config.nameSpace = namespace;
        config.coordinationEpoch = epoch;
        return connect(config);
    }

    static PostgresProvider connect(PostgresProviderOptions config) @trusted {
        verifyAbi();
        auto options = toNativeOptions(config);
        CError error;
        CPostgresProvider* value;
        check(dilock_postgres_provider_create(&options, &value, &error), error);
        return PostgresProvider(value);
    }

    PostgresLock lock(string resource) @trusted {
        validateCStringInput(resource, "resource");
        CPostgresLock* value;
        CError error;
        check(dilock_postgres_lock_create(raw, resource.toStringz, &value, &error), error);
        return PostgresLock(value);
    }

    void destroy() @trusted {
        if (raw !is null) {
            dilock_postgres_provider_destroy(raw);
            raw = null;
        }
    }

    private this(CPostgresProvider* value) { raw = value; }
}

struct PostgresLock {
    private CPostgresLock* raw;
    @disable this(this);
    private this(CPostgresLock* value) { raw = value; }

    PostgresLease acquireDefault() @trusted {
        CPostgresLease* value;
        CError error;
        check(dilock_postgres_acquire_default(raw, &value, &error), error);
        return PostgresLease(value);
    }

    PostgresLease acquire(long waitMs, long leaseMs) @trusted {
        CPostgresLease* value;
        CError error;
        check(dilock_postgres_acquire(raw, waitMs, leaseMs, &value, &error), error);
        return PostgresLease(value);
    }

    /// Returns false only for acknowledged contention.
    bool tryAcquire(ref PostgresLease lease) @trusted {
        if (lease.raw !is null)
            throw new DilockException(Status.invalidArgument,
                "tryAcquire output lease must be empty");
        CPostgresLease* value;
        CError error;
        auto status = dilock_postgres_try_acquire(raw, &value, &error);
        if (status == Status.busy) return false;
        check(status, error);
        lease = PostgresLease(value);
        return true;
    }

    void destroy() @trusted {
        if (raw !is null) {
            dilock_postgres_lock_destroy(raw);
            raw = null;
        }
    }
}

struct PostgresLease {
    private CPostgresLease* raw;
    @disable this(this);
    private this(CPostgresLease* value) { raw = value; }

    bool isValid() const @trusted {
        return raw !is null && dilock_postgres_lease_is_valid(raw) != 0;
    }

    /// Liveness check only; PostgreSQL has no server-side lease TTL.
    bool renew() @trusted {
        if (raw is null)
            throw new DilockException(Status.invalidArgument, "lease has been destroyed");
        int32_t renewed;
        CError error;
        check(dilock_postgres_lease_renew(raw, &renewed, &error), error);
        return renewed != 0;
    }

    ReleaseOutcome release() @trusted {
        int nativeOutcome;
        CError error;
        check(dilock_postgres_lease_release(raw, &nativeOutcome, &error), error);
        return releaseOutcomeFromC(nativeOutcome);
    }

    void destroy() @trusted {
        if (raw !is null) {
            dilock_postgres_lease_destroy(raw);
            raw = null;
        }
    }
}
