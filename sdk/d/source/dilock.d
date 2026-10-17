module dilock;

import core.stdc.stdint : int32_t, int64_t, uint32_t;
import core.time : Duration, dur;
import std.algorithm.searching : canFind;
import std.exception : enforce;
import std.string : fromStringz, toStringz;

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

private enum CReleaseOutcome : int {
    released = 0,
    alreadyNotOwned = 1,
    unknown = 2,
}

/// Redis connection and provider-wide timing policy. Durations are converted
/// to whole milliseconds at the C ABI boundary. The coordination epoch is
/// deployment-owned and must be supplied before connecting.
struct RedisProviderOptions {
    string host = "127.0.0.1";
    int port = 6379;
    string username;
    string password;
    int database = 0;
    string nameSpace = "default";
    string coordinationEpoch;
    Duration rpcTimeout = dur!"msecs"(2000);
    Duration cleanupTimeout = dur!"msecs"(2000);
    Duration safetyMargin = dur!"msecs"(25);
    Duration pollInterval = dur!"msecs"(25);
    Duration defaultWait = dur!"msecs"(5000);
    Duration defaultLease = dur!"msecs"(30000);
}

/// etcd connection, TLS and lease policy. TLS strings contain PEM contents,
/// not file paths. A client certificate and its private key must be supplied
/// together; a custom root certificate can be used without client identity.
/// Keepalives continue after acquisition only when `autoRenew` is true; they
/// always run while queued for etcd's native Lock.
struct EtcdProviderOptions {
    string endpoint = "127.0.0.1:2379";
    string nameSpace = "default";
    string coordinationEpoch;
    string tlsRootCertificates;
    string tlsClientCertificate;
    string tlsClientPrivateKey;
    Duration rpcTimeout = dur!"msecs"(2000);
    Duration cleanupTimeout = dur!"msecs"(2000);
    Duration safetyMargin = dur!"msecs"(25);
    Duration defaultWait = dur!"msecs"(5000);
    Duration defaultLease = dur!"msecs"(30000);
    bool autoRenew = false;
}

/// Consul HTTP agent, ACL and timing configuration. Namespace metadata must
/// be provisioned out of band before provider construction.
struct ConsulProviderOptions {
    string endpoint = "http://127.0.0.1:8500";
    string nameSpace = "default";
    string coordinationEpoch;
    string aclToken;
    Duration rpcTimeout = dur!"msecs"(2000);
    Duration cleanupTimeout = dur!"msecs"(2000);
    Duration pollInterval = dur!"msecs"(50);
    Duration defaultWait = dur!"msecs"(5000);
    Duration defaultLease = dur!"msecs"(30000);
}

extern(C) {
    struct CError { int32_t code; char[384] message; }
    struct RedisOptions {
        const(char)* host; int32_t port; const(char)* username; const(char)* password;
        int32_t database; const(char)* name_space; const(char)* coordination_epoch;
        extern(C) void function(int, int, const(char)*, void*) cleanup_callback; void* cleanup_user_data;
        int64_t rpc_timeout_ms; int64_t cleanup_timeout_ms; int64_t safety_margin_ms;
        int64_t poll_interval_ms; int64_t default_wait_ms; int64_t default_lease_ms;
    }
    struct EtcdOptions {
        const(char)* endpoint; const(char)* name_space; const(char)* coordination_epoch;
        const(char)* tls_root_certificates; const(char)* tls_client_certificate; const(char)* tls_client_private_key;
        extern(C) void function(int, int, const(char)*, void*) cleanup_callback; void* cleanup_user_data;
        int64_t rpc_timeout_ms; int64_t cleanup_timeout_ms; int64_t safety_margin_ms;
        int64_t default_wait_ms; int64_t default_lease_ms; int32_t auto_renew;
    }
    struct CConsulOptions {
        const(char)* endpoint; const(char)* name_space; const(char)* coordination_epoch;
        const(char)* acl_token;
        extern(C) void function(int, int, const(char)*, void*) cleanup_callback; void* cleanup_user_data;
        int64_t rpc_timeout_ms; int64_t cleanup_timeout_ms; int64_t poll_interval_ms;
        int64_t default_wait_ms; int64_t default_lease_ms;
    }
    struct CEtcdFence { int64_t create_revision; size_t coordination_epoch_bytes; }
    struct CConsulFence { ulong lock_index; size_t coordination_epoch_bytes; }
    struct CRedisProvider { }
    struct CRedisLock { }
    struct CRedisLease { }
    struct CEtcdProvider { }
    struct CEtcdLock { }
    struct CEtcdLease { }
    struct CConsulProvider { }
    struct CConsulLock { }
    struct CConsulLease { }
    struct CCancellation { }

    uint32_t dilock_c_abi_version();
    void dilock_redis_options_init(RedisOptions*);
    void dilock_etcd_options_init(EtcdOptions*);
    void dilock_consul_options_init(CConsulOptions*);
    Status dilock_redis_provider_create(RedisOptions*, CRedisProvider**, CError*);
    void dilock_redis_provider_destroy(CRedisProvider*);
    Status dilock_etcd_provider_create(EtcdOptions*, CEtcdProvider**, CError*);
    void dilock_etcd_provider_destroy(CEtcdProvider*);
    Status dilock_consul_provider_create(CConsulOptions*, CConsulProvider**, CError*);
    void dilock_consul_provider_destroy(CConsulProvider*);
    Status dilock_redis_lock_create(CRedisProvider*, const(char)*, CRedisLock**, CError*);
    Status dilock_etcd_lock_create(CEtcdProvider*, const(char)*, CEtcdLock**, CError*);
    void dilock_redis_lock_destroy(CRedisLock*);
    void dilock_etcd_lock_destroy(CEtcdLock*);
    Status dilock_consul_lock_create(CConsulProvider*, const(char)*, CConsulLock**, CError*);
    void dilock_consul_lock_destroy(CConsulLock*);
    Status dilock_redis_acquire_default(CRedisLock*, CRedisLease**, CError*);
    Status dilock_redis_acquire_default_with_cancellation(CRedisLock*, CCancellation*, CRedisLease**, CError*);
    Status dilock_redis_acquire(CRedisLock*, int64_t, int64_t, CRedisLease**, CError*);
    Status dilock_etcd_acquire_default(CEtcdLock*, CEtcdLease**, CError*);
    Status dilock_etcd_acquire_default_with_cancellation(CEtcdLock*, CCancellation*, CEtcdLease**, CError*);
    Status dilock_etcd_acquire(CEtcdLock*, int64_t, int64_t, CEtcdLease**, CError*);
    Status dilock_consul_acquire_default(CConsulLock*, CConsulLease**, CError*);
    Status dilock_consul_acquire_default_with_cancellation(CConsulLock*, CCancellation*, CConsulLease**, CError*);
    Status dilock_consul_acquire(CConsulLock*, int64_t, int64_t, CConsulLease**, CError*);
    Status dilock_redis_try_acquire(CRedisLock*, int64_t, CRedisLease**, CError*);
    Status dilock_etcd_try_acquire(CEtcdLock*, int64_t, CEtcdLease**, CError*);
    Status dilock_consul_try_acquire(CConsulLock*, int64_t, CConsulLease**, CError*);
    int32_t dilock_redis_lease_is_valid(const(CRedisLease)*);
    int32_t dilock_etcd_lease_is_valid(const(CEtcdLease)*);
    int32_t dilock_consul_lease_is_valid(const(CConsulLease)*);
    Status dilock_redis_lease_release(CRedisLease*, CReleaseOutcome*, CError*);
    Status dilock_redis_lease_renew(CRedisLease*, int64_t, int32_t*, CError*);
    Status dilock_etcd_lease_release(CEtcdLease*, CReleaseOutcome*, CError*);
    Status dilock_consul_lease_release(CConsulLease*, CReleaseOutcome*, CError*);
    Status dilock_etcd_lease_effective_ttl_ms(const(CEtcdLease)*, int64_t*, CError*);
    Status dilock_consul_lease_effective_ttl_ms(const(CConsulLease)*, int64_t*, CError*);
    Status dilock_consul_lease_renew(CConsulLease*, int32_t*, CError*);
    void dilock_redis_lease_destroy(CRedisLease*);
    void dilock_etcd_lease_destroy(CEtcdLease*);
    void dilock_consul_lease_destroy(CConsulLease*);
    Status dilock_etcd_lease_fence(const(CEtcdLease)*, CEtcdFence*, char*, size_t, CError*);
    Status dilock_consul_lease_fence(const(CConsulLease)*, CConsulFence*, char*, size_t, CError*);
    Status dilock_consul_provision_namespace(CConsulOptions*, CError*);
    CCancellation* dilock_cancellation_create(CError*);
    void dilock_cancellation_request(CCancellation*);
    void dilock_cancellation_destroy(CCancellation*);
    Status dilock_redis_acquire_with_cancellation(CRedisLock*, int64_t, int64_t, CCancellation*, CRedisLease**, CError*);
    Status dilock_etcd_acquire_with_cancellation(CEtcdLock*, int64_t, int64_t, CCancellation*, CEtcdLease**, CError*);
    Status dilock_consul_acquire_with_cancellation(CConsulLock*, int64_t, int64_t, CCancellation*, CConsulLease**, CError*);
}

class DilockException : Exception {
    private Status _status;
    @property Status status() const @safe { return _status; }
    this(Status value, string message) @safe { _status = value; super(message); }
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

/// The C ABI message is a fixed-size byte array. Bound the scan to that array
/// so a malformed or older native library cannot make error translation read
/// beyond the value supplied by the caller.
private string cErrorMessage(const ref CError error) @safe {
    size_t end;
    while (end < error.message.length && error.message[end] != '\0') ++end;
    return error.message[0 .. end].idup;
}

private Status normalizeStatus(Status status) @safe {
    auto raw = cast(int)status;
    if (raw < cast(int)Status.ok || raw > cast(int)Status.internalError)
        return Status.internalError;
    return status;
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
    if (dilock_c_abi_version() != 1) throw new DilockException(Status.unsupportedCapability, "linked Dilock C ABI must be version 1");
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

private RedisOptions toNativeOptions(RedisProviderOptions config) @trusted {
    validateCStringInput(config.host, "Redis host");
    validateCStringInput(config.username, "Redis username");
    validateCStringInput(config.password, "Redis password");
    validateCStringInput(config.nameSpace, "namespace");
    validateCStringInput(config.coordinationEpoch, "coordination epoch");

    RedisOptions options;
    dilock_redis_options_init(&options);
    options.host = config.host.toStringz;
    options.port = cast(int32_t)config.port;
    options.username = config.username.toStringz;
    options.password = config.password.toStringz;
    options.database = cast(int32_t)config.database;
    options.name_space = config.nameSpace.toStringz;
    options.coordination_epoch = config.coordinationEpoch.toStringz;
    options.rpc_timeout_ms = wholeMilliseconds(config.rpcTimeout, "Redis RPC timeout");
    options.cleanup_timeout_ms = wholeMilliseconds(config.cleanupTimeout, "Redis cleanup timeout");
    options.safety_margin_ms = wholeMilliseconds(config.safetyMargin, "Redis safety margin");
    options.poll_interval_ms = wholeMilliseconds(config.pollInterval, "Redis poll interval");
    options.default_wait_ms = wholeMilliseconds(config.defaultWait, "Redis default wait");
    options.default_lease_ms = wholeMilliseconds(config.defaultLease, "Redis default lease");
    return options;
}

private EtcdOptions toNativeOptions(EtcdProviderOptions config) @trusted {
    validateCStringInput(config.endpoint, "etcd endpoint");
    validateCStringInput(config.nameSpace, "namespace");
    validateCStringInput(config.coordinationEpoch, "coordination epoch");
    validateCStringInput(config.tlsRootCertificates, "etcd root certificates");
    validateCStringInput(config.tlsClientCertificate, "etcd client certificate");
    validateCStringInput(config.tlsClientPrivateKey, "etcd client private key");
    if ((config.tlsClientCertificate.length == 0) != (config.tlsClientPrivateKey.length == 0))
        throw new DilockException(Status.invalidArgument,
            "etcd client certificate and private key must be provided together");

    EtcdOptions options;
    dilock_etcd_options_init(&options);
    options.endpoint = config.endpoint.toStringz;
    options.name_space = config.nameSpace.toStringz;
    options.coordination_epoch = config.coordinationEpoch.toStringz;
    options.tls_root_certificates = config.tlsRootCertificates.toStringz;
    options.tls_client_certificate = config.tlsClientCertificate.toStringz;
    options.tls_client_private_key = config.tlsClientPrivateKey.toStringz;
    options.rpc_timeout_ms = wholeMilliseconds(config.rpcTimeout, "etcd RPC timeout");
    options.cleanup_timeout_ms = wholeMilliseconds(config.cleanupTimeout, "etcd cleanup timeout");
    options.safety_margin_ms = wholeMilliseconds(config.safetyMargin, "etcd safety margin");
    options.default_wait_ms = wholeMilliseconds(config.defaultWait, "etcd default wait");
    options.default_lease_ms = wholeMilliseconds(config.defaultLease, "etcd default lease");
    options.auto_renew = config.autoRenew ? 1 : 0;
    return options;
}

private CConsulOptions toNativeOptions(ConsulProviderOptions config) @trusted {
    validateCStringInput(config.endpoint, "Consul endpoint");
    validateCStringInput(config.nameSpace, "namespace");
    validateCStringInput(config.coordinationEpoch, "coordination epoch");
    validateCStringInput(config.aclToken, "Consul ACL token");

    CConsulOptions options;
    dilock_consul_options_init(&options);
    options.endpoint = config.endpoint.toStringz;
    options.name_space = config.nameSpace.toStringz;
    options.coordination_epoch = config.coordinationEpoch.toStringz;
    options.acl_token = config.aclToken.toStringz;
    options.rpc_timeout_ms = wholeMilliseconds(config.rpcTimeout, "Consul RPC timeout");
    options.cleanup_timeout_ms = wholeMilliseconds(config.cleanupTimeout, "Consul cleanup timeout");
    options.poll_interval_ms = wholeMilliseconds(config.pollInterval, "Consul poll interval");
    options.default_wait_ms = wholeMilliseconds(config.defaultWait, "Consul default wait");
    options.default_lease_ms = wholeMilliseconds(config.defaultLease, "Consul default lease");
    return options;
}

/// Creates metadata with CAS=0. Application provider creation never provisions it.
void provisionConsulNamespace(ConsulProviderOptions config) @trusted {
    verifyAbi();
    auto options = toNativeOptions(config);
    CError error;
    check(dilock_consul_provision_namespace(&options, &error), error);
}

struct RedisProvider {
    private CRedisProvider* raw;
    @disable this(this);

    static RedisProvider connect(string host, string namespace, string epoch, int port = 6379) @trusted {
        RedisProviderOptions config;
        config.host = host;
        config.port = port;
        config.nameSpace = namespace;
        config.coordinationEpoch = epoch;
        return connect(config);
    }

    /// Connect with Redis authentication, database, and timing options. Empty
    /// username/password select the Redis default user without AUTH.
    static RedisProvider connect(RedisProviderOptions config) @trusted {
        verifyAbi();
        auto options = toNativeOptions(config);
        CError error;
        CRedisProvider* raw;
        check(dilock_redis_provider_create(&options, &raw, &error), error);
        return RedisProvider(raw);
    }

    RedisLock lock(string resource) @trusted {
        validateCStringInput(resource, "resource");
        CRedisLock* value;
        CError error;
        check(dilock_redis_lock_create(raw, resource.toStringz, &value, &error), error);
        return RedisLock(value);
    }

    void destroy() @trusted { if (raw !is null) { dilock_redis_provider_destroy(raw); raw = null; } }
    private this(CRedisProvider* value) { raw = value; }
}

struct RedisLock {
    private CRedisLock* raw;
    @disable this(this);
    private this(CRedisLock* value) { raw = value; }

    RedisLease acquireDefault() @trusted {
        CRedisLease* value; CError error;
        check(dilock_redis_acquire_default(raw, &value, &error), error);
        return RedisLease(value);
    }
    RedisLease acquireDefault(ref Cancellation cancellation) @trusted {
        if (cancellation.raw is null)
            throw new DilockException(Status.invalidArgument, "cancellation handle has been destroyed");
        CRedisLease* value; CError error;
        check(dilock_redis_acquire_default_with_cancellation(raw, cancellation.raw, &value, &error), error);
        return RedisLease(value);
    }
    RedisLease acquire(long waitMs, long leaseMs) @trusted {
        CRedisLease* value; CError error;
        check(dilock_redis_acquire(raw, waitMs, leaseMs, &value, &error), error);
        return RedisLease(value);
    }
    RedisLease acquire(long waitMs, long leaseMs, ref Cancellation cancellation) @trusted {
        if (cancellation.raw is null)
            throw new DilockException(Status.invalidArgument, "cancellation handle has been destroyed");
        CRedisLease* value; CError error;
        check(dilock_redis_acquire_with_cancellation(raw, waitMs, leaseMs, cancellation.raw, &value, &error), error);
        return RedisLease(value);
    }
    /// Attempts immediate acquisition. Returns false only for ordinary Redis
    /// contention; backend and protocol failures remain typed exceptions.
    /// The output must be empty before the call. This prevents D's `out`
    /// parameter initialization from discarding an existing native lease.
    /// When false is returned, `lease` remains empty. On success, the caller
    /// owns the lease and must call `destroy` (normally with `scope(exit)`).
    bool tryAcquire(long leaseMs, ref RedisLease lease) @trusted {
        if (lease.raw !is null)
            throw new DilockException(Status.invalidArgument,
                "tryAcquire output lease must be empty; release or destroy its current lease first");
        CRedisLease* value; CError error;
        auto status = dilock_redis_try_acquire(raw, leaseMs, &value, &error);
        if (status == Status.busy) return false;
        check(status, error);
        lease = RedisLease(value);
        return true;
    }
    void destroy() @trusted { if (raw !is null) { dilock_redis_lock_destroy(raw); raw = null; } }
}

struct RedisLease {
    private CRedisLease* raw;
    @disable this(this);
    private this(CRedisLease* value) { raw = value; }
    bool isValid() const @trusted { return raw !is null && dilock_redis_lease_is_valid(raw) != 0; }
    ReleaseOutcome release() @trusted {
        CReleaseOutcome value; CError error;
        check(dilock_redis_lease_release(raw, &value, &error), error);
        return releaseOutcomeFromC(cast(int)value);
    }
    /// Extends a Redis lease when it is still owned. False means the backend
    /// acknowledged that this acquisition is no longer current.
    bool renew(long leaseMs) @trusted {
        if (raw is null)
            throw new DilockException(Status.invalidArgument, "lease has been destroyed");
        int32_t renewed;
        CError error;
        check(dilock_redis_lease_renew(raw, leaseMs, &renewed, &error), error);
        return renewed != 0;
    }
    void destroy() @trusted { if (raw !is null) { dilock_redis_lease_destroy(raw); raw = null; } }
}

struct EtcdProvider {
    private CEtcdProvider* raw;
    @disable this(this);
    static EtcdProvider connect(string endpoint, string namespace, string epoch) @trusted {
        EtcdProviderOptions config;
        config.endpoint = endpoint;
        config.nameSpace = namespace;
        config.coordinationEpoch = epoch;
        return connect(config);
    }

    /// Connect using optional PEM contents and provider-wide lease policy.
    static EtcdProvider connect(EtcdProviderOptions config) @trusted {
        verifyAbi();
        auto options = toNativeOptions(config);
        CError error;
        CEtcdProvider* raw;
        check(dilock_etcd_provider_create(&options, &raw, &error), error);
        return EtcdProvider(raw);
    }
    EtcdLock lock(string resource) @trusted {
        validateCStringInput(resource, "resource");
        CEtcdLock* value; CError error;
        check(dilock_etcd_lock_create(raw, resource.toStringz, &value, &error), error);
        return EtcdLock(value);
    }
    void destroy() @trusted { if (raw !is null) { dilock_etcd_provider_destroy(raw); raw = null; } }
    private this(CEtcdProvider* value) { raw = value; }
}

struct ConsulProvider {
    private CConsulProvider* raw;
    @disable this(this);

    static ConsulProvider connect(string endpoint, string namespace, string epoch,
                                  string aclToken = "") @trusted {
        ConsulProviderOptions config;
        config.endpoint = endpoint;
        config.nameSpace = namespace;
        config.coordinationEpoch = epoch;
        config.aclToken = aclToken;
        return connect(config);
    }

    static ConsulProvider connect(ConsulProviderOptions config) @trusted {
        verifyAbi();
        auto options = toNativeOptions(config);
        CError error;
        CConsulProvider* value;
        check(dilock_consul_provider_create(&options, &value, &error), error);
        return ConsulProvider(value);
    }

    ConsulLock lock(string resource) @trusted {
        validateCStringInput(resource, "resource");
        CConsulLock* value;
        CError error;
        check(dilock_consul_lock_create(raw, resource.toStringz, &value, &error), error);
        return ConsulLock(value);
    }

    void destroy() @trusted { if (raw !is null) { dilock_consul_provider_destroy(raw); raw = null; } }
    private this(CConsulProvider* value) { raw = value; }
}

struct ConsulLock {
    private CConsulLock* raw;
    @disable this(this);
    private this(CConsulLock* value) { raw = value; }

    ConsulLease acquireDefault() @trusted {
        CConsulLease* value; CError error;
        check(dilock_consul_acquire_default(raw, &value, &error), error);
        return ConsulLease(value);
    }
    ConsulLease acquireDefault(ref Cancellation cancellation) @trusted {
        if (cancellation.raw is null)
            throw new DilockException(Status.invalidArgument, "cancellation handle has been destroyed");
        CConsulLease* value; CError error;
        check(dilock_consul_acquire_default_with_cancellation(raw, cancellation.raw, &value, &error), error);
        return ConsulLease(value);
    }
    ConsulLease acquire(long waitMs, long leaseMs) @trusted {
        CConsulLease* value; CError error;
        check(dilock_consul_acquire(raw, waitMs, leaseMs, &value, &error), error);
        return ConsulLease(value);
    }
    ConsulLease acquire(long waitMs, long leaseMs, ref Cancellation cancellation) @trusted {
        if (cancellation.raw is null)
            throw new DilockException(Status.invalidArgument, "cancellation handle has been destroyed");
        CConsulLease* value; CError error;
        check(dilock_consul_acquire_with_cancellation(raw, waitMs, leaseMs, cancellation.raw, &value, &error), error);
        return ConsulLease(value);
    }
    /// The output lease must be empty; false means only acknowledged contention.
    bool tryAcquire(long leaseMs, ref ConsulLease lease) @trusted {
        if (lease.raw !is null)
            throw new DilockException(Status.invalidArgument,
                "tryAcquire output lease must be empty; release or destroy its current lease first");
        CConsulLease* value; CError error;
        auto status = dilock_consul_try_acquire(raw, leaseMs, &value, &error);
        if (status == Status.busy) return false;
        check(status, error);
        lease = ConsulLease(value);
        return true;
    }
    void destroy() @trusted { if (raw !is null) { dilock_consul_lock_destroy(raw); raw = null; } }
}

struct EtcdLock {
    private CEtcdLock* raw;
    @disable this(this);
    private this(CEtcdLock* value) { raw = value; }
    EtcdLease acquireDefault() @trusted {
        CEtcdLease* value; CError error;
        check(dilock_etcd_acquire_default(raw, &value, &error), error);
        return EtcdLease(value);
    }
    EtcdLease acquireDefault(ref Cancellation cancellation) @trusted {
        if (cancellation.raw is null)
            throw new DilockException(Status.invalidArgument, "cancellation handle has been destroyed");
        CEtcdLease* value; CError error;
        check(dilock_etcd_acquire_default_with_cancellation(raw, cancellation.raw, &value, &error), error);
        return EtcdLease(value);
    }
    EtcdLease acquire(long waitMs, long leaseMs) @trusted {
        CEtcdLease* value; CError error;
        check(dilock_etcd_acquire(raw, waitMs, leaseMs, &value, &error), error);
        return EtcdLease(value);
    }
    EtcdLease acquire(long waitMs, long leaseMs, ref Cancellation cancellation) @trusted {
        if (cancellation.raw is null)
            throw new DilockException(Status.invalidArgument, "cancellation handle has been destroyed");
        CEtcdLease* value; CError error;
        check(dilock_etcd_acquire_with_cancellation(raw, waitMs, leaseMs, cancellation.raw, &value, &error), error);
        return EtcdLease(value);
    }
    /// etcd's native Lock profile does not support non-blocking acquisition.
    /// The ABI reports that capability explicitly; this method never maps it
    /// to contention.
    /// The output must be empty before the call, matching Redis tryAcquire's
    /// ownership rule. etcd's native Lock profile then reports its explicit
    /// unsupported-capability status.
    bool tryAcquire(long leaseMs, ref EtcdLease lease) @trusted {
        if (lease.raw !is null)
            throw new DilockException(Status.invalidArgument,
                "tryAcquire output lease must be empty; release or destroy its current lease first");
        CEtcdLease* value; CError error;
        auto status = dilock_etcd_try_acquire(raw, leaseMs, &value, &error);
        if (status == Status.busy) return false;
        check(status, error);
        lease = EtcdLease(value);
        return true;
    }
    void destroy() @trusted { if (raw !is null) { dilock_etcd_lock_destroy(raw); raw = null; } }
}

struct EtcdFence { string epoch; long createRevision; }

private size_t fenceEpochBufferCapacity(size_t epochBytes) @safe {
    return epochBytes + 1;
}

struct EtcdLease {
    private CEtcdLease* raw;
    @disable this(this);
    private this(CEtcdLease* value) { raw = value; }
    bool isValid() const @trusted { return raw !is null && dilock_etcd_lease_is_valid(raw) != 0; }
    /// Returns the server-granted etcd TTL, which uses whole-second units.
    long effectiveTtlMs() const @trusted {
        if (raw is null)
            throw new DilockException(Status.invalidArgument, "lease has been destroyed");
        int64_t value;
        CError error;
        check(dilock_etcd_lease_effective_ttl_ms(raw, &value, &error), error);
        return value;
    }
    EtcdFence fence() @trusted {
        if (raw is null)
            throw new DilockException(Status.invalidArgument, "lease has been destroyed");
        EtcdFence value;
        CEtcdFence rawFence;
        CError error;
        auto probe = dilock_etcd_lease_fence(raw, &rawFence, null, 0, &error);
        if (probe != Status.invalidArgument) check(probe, error);
        auto epochBytes = rawFence.coordination_epoch_bytes;
        auto epoch = new char[fenceEpochBufferCapacity(epochBytes)];
        check(dilock_etcd_lease_fence(raw, &rawFence, epoch.ptr, epoch.length, &error), error);
        if (rawFence.coordination_epoch_bytes != epochBytes)
            throw new DilockException(Status.protocolMismatch, "C ABI fence epoch length changed during retrieval");
        value.epoch = epoch[0 .. epochBytes].idup;
        value.createRevision = rawFence.create_revision;
        return value;
    }
    ReleaseOutcome release() @trusted {
        CReleaseOutcome value; CError error;
        check(dilock_etcd_lease_release(raw, &value, &error), error);
        return releaseOutcomeFromC(cast(int)value);
    }
    void destroy() @trusted { if (raw !is null) { dilock_etcd_lease_destroy(raw); raw = null; } }
}

struct ConsulFence { string epoch; ulong lockIndex; }

struct ConsulLease {
    private CConsulLease* raw;
    @disable this(this);
    private this(CConsulLease* value) { raw = value; }
    bool isValid() const @trusted { return raw !is null && dilock_consul_lease_is_valid(raw) != 0; }
    long effectiveTtlMs() const @trusted {
        if (raw is null)
            throw new DilockException(Status.invalidArgument, "lease has been destroyed");
        int64_t value;
        CError error;
        check(dilock_consul_lease_effective_ttl_ms(raw, &value, &error), error);
        return value;
    }
    bool renew() @trusted {
        if (raw is null)
            throw new DilockException(Status.invalidArgument, "lease has been destroyed");
        int32_t renewed;
        CError error;
        check(dilock_consul_lease_renew(raw, &renewed, &error), error);
        return renewed != 0;
    }
    ConsulFence fence() @trusted {
        if (raw is null)
            throw new DilockException(Status.invalidArgument, "lease has been destroyed");
        ConsulFence value;
        CConsulFence rawFence;
        CError error;
        auto probe = dilock_consul_lease_fence(raw, &rawFence, null, 0, &error);
        if (probe != Status.invalidArgument) check(probe, error);
        auto epochBytes = rawFence.coordination_epoch_bytes;
        auto epoch = new char[epochBytes + 1];
        check(dilock_consul_lease_fence(raw, &rawFence, epoch.ptr, epoch.length, &error), error);
        if (rawFence.coordination_epoch_bytes != epochBytes)
            throw new DilockException(Status.protocolMismatch, "C ABI fence epoch length changed during retrieval");
        value.epoch = epoch[0 .. epochBytes].idup;
        value.lockIndex = rawFence.lock_index;
        return value;
    }
    ReleaseOutcome release() @trusted {
        CReleaseOutcome value; CError error;
        check(dilock_consul_lease_release(raw, &value, &error), error);
        return releaseOutcomeFromC(cast(int)value);
    }
    void destroy() @trusted { if (raw !is null) { dilock_consul_lease_destroy(raw); raw = null; } }
}

struct Cancellation {
    private CCancellation* raw;
    @disable this(this);
    static Cancellation create() @trusted {
        verifyAbi(); CError error;
        auto raw = dilock_cancellation_create(&error);
        if (raw is null) check(cast(Status)error.code, error);
        return Cancellation(raw);
    }
    /// Request cancellation. A nil or destroyed handle is a caller error and
    /// must not silently turn a cancellation request into a no-op.
    void request() @trusted {
        if (raw is null)
            throw new DilockException(Status.invalidArgument, "cancellation handle has been destroyed");
        dilock_cancellation_request(raw);
    }
    void destroy() @trusted { if (raw !is null) { dilock_cancellation_destroy(raw); raw = null; } }
    private this(CCancellation* value) { raw = value; }
}

unittest {
    assert(normalizeStatus(cast(Status)0) == Status.ok);
    assert(normalizeStatus(cast(Status)9) == Status.unsupportedCapability);
    assert(normalizeStatus(cast(Status)99) == Status.internalError);
    CError error;
    bool rejectedUnknownStatus;
    try {
        check(cast(Status)99, error);
    } catch (DilockException failure) {
        rejectedUnknownStatus = failure.status == Status.internalError;
    }
    assert(rejectedUnknownStatus, "unknown C statuses must map to an internal error");
    assert(releaseOutcomeFromC(0) == ReleaseOutcome.released);
    assert(releaseOutcomeFromC(1) == ReleaseOutcome.alreadyNotOwned);
    assert(releaseOutcomeFromC(2) == ReleaseOutcome.unknown);
    bool rejected;
    try {
        releaseOutcomeFromC(99);
    } catch (DilockException error) {
        rejected = error.status == Status.internalError;
    }
    assert(rejected, "unknown C release outcomes must remain an internal error");
}

unittest {
    CError error;
    error.message[] = 'x';
    auto message = cErrorMessage(error);
    assert(message.length == error.message.length,
        "unterminated fixed-size ABI messages must be read only to the array boundary");
    assert(message[$ - 1] == 'x');
    error.message[3] = '\0';
    assert(cErrorMessage(error) == "xxx");
}


unittest {
    assert(fenceEpochBufferCapacity(0) == 1);
    assert(fenceEpochBufferCapacity(1025) == 1026);
    assert(fenceEpochBufferCapacity(4096) == 4097);
}

unittest {
    auto lock = RedisLock(cast(CRedisLock*) 1);
    auto lease = RedisLease(cast(CRedisLease*) 2);
    bool rejected;
    try {
        lock.tryAcquire(3000, lease);
    } catch (DilockException error) {
        rejected = error.status == Status.invalidArgument;
    }
    assert(rejected, "Redis tryAcquire must reject a non-empty output lease");
    assert(lease.raw == cast(CRedisLease*) 2, "rejected Redis tryAcquire must preserve existing ownership");
}

unittest {
    auto lock = EtcdLock(cast(CEtcdLock*) 1);
    auto lease = EtcdLease(cast(CEtcdLease*) 2);
    bool rejected;
    try {
        lock.tryAcquire(3000, lease);
    } catch (DilockException error) {
        rejected = error.status == Status.invalidArgument;
    }
    assert(rejected, "etcd tryAcquire must reject a non-empty output lease before reporting capability");
    assert(lease.raw == cast(CEtcdLease*) 2, "rejected etcd tryAcquire must preserve existing ownership");
}

unittest {
    auto lock = ConsulLock(cast(CConsulLock*) 1);
    auto lease = ConsulLease(cast(CConsulLease*) 2);
    bool rejected;
    try {
        lock.tryAcquire(30000, lease);
    } catch (DilockException error) {
        rejected = error.status == Status.invalidArgument;
    }
    assert(rejected, "Consul tryAcquire must reject a non-empty output lease");
    assert(lease.raw == cast(CConsulLease*) 2, "rejected Consul tryAcquire must preserve existing ownership");
}

unittest {
    validateCStringInput("resource", "resource");
    bool rejected;
    try {
        validateCStringInput("visible\0truncated", "resource");
    } catch (DilockException error) {
        rejected = error.status == Status.invalidArgument;
    }
    assert(rejected, "C ABI inputs containing U+0000 must be rejected before conversion");
}

unittest {
    Cancellation cancellation;
    bool rejected;
    try {
        cancellation.request();
    } catch (DilockException error) {
        rejected = error.status == Status.invalidArgument;
    }
    assert(rejected, "requesting cancellation on a nil/destroyed handle must not silently do nothing");
}

unittest {
    auto redisLease = RedisLease(null);
    bool rejected;
    try {
        redisLease.renew(1000);
    } catch (DilockException error) {
        rejected = error.status == Status.invalidArgument;
    }
    assert(rejected, "renewing a destroyed Redis lease must fail as invalidArgument");
}

unittest {
    auto lease = EtcdLease(null);
    bool rejected;
    try {
        lease.effectiveTtlMs();
    } catch (DilockException error) {
        rejected = error.status == Status.invalidArgument;
    }
    assert(rejected, "reading effective TTL from a destroyed etcd lease must fail explicitly");
}

unittest {
    auto lease = ConsulLease(null);
    bool rejected;
    try {
        lease.fence();
    } catch (DilockException error) {
        rejected = error.status == Status.invalidArgument;
    }
    assert(rejected, "reading the fence from a destroyed Consul lease must fail explicitly");
}

unittest {
    auto redisLock = RedisLock(null);
    auto etcdLock = EtcdLock(null);
    auto cancellation = Cancellation(null);
    bool rejected;
    try {
        redisLock.acquireDefault(cancellation);
    } catch (DilockException error) {
        rejected = error.status == Status.invalidArgument;
    }
    assert(rejected, "default Redis acquisition with cancellation must validate its lock");
    rejected = false;
    try {
        etcdLock.acquireDefault(cancellation);
    } catch (DilockException error) {
        rejected = error.status == Status.invalidArgument;
    }
    assert(rejected, "default etcd acquisition with cancellation must validate its lock");
}

unittest {
    auto redisLock = RedisLock(cast(CRedisLock*) 1);
    auto etcdLock = EtcdLock(cast(CEtcdLock*) 1);
    auto cancellation = Cancellation(null);
    bool rejected;
    try {
        redisLock.acquire(1, 1, cancellation);
    } catch (DilockException error) {
        rejected = error.status == Status.invalidArgument;
    }
    assert(rejected, "Redis acquisition must not treat a destroyed cancellation as no cancellation");
    rejected = false;
    try {
        redisLock.acquireDefault(cancellation);
    } catch (DilockException error) {
        rejected = error.status == Status.invalidArgument;
    }
    assert(rejected, "default Redis acquisition must reject a destroyed cancellation before FFI");
    rejected = false;
    try {
        etcdLock.acquire(1, 1, cancellation);
    } catch (DilockException error) {
        rejected = error.status == Status.invalidArgument;
    }
    assert(rejected, "etcd acquisition must not treat a destroyed cancellation as no cancellation");
    rejected = false;
    try {
        etcdLock.acquireDefault(cancellation);
    } catch (DilockException error) {
        rejected = error.status == Status.invalidArgument;
    }
    assert(rejected, "default etcd acquisition must reject a destroyed cancellation before FFI");
}

unittest {
    assert(dilock_c_abi_version() == 1);
    RedisOptions redis;
    EtcdOptions etcd;
    CConsulOptions consul;
    dilock_redis_options_init(&redis);
    dilock_etcd_options_init(&etcd);
    dilock_consul_options_init(&consul);
    assert(redis.default_wait_ms == 5000 && redis.default_lease_ms == 30000);
    assert(etcd.default_wait_ms == 5000 && etcd.default_lease_ms == 30000);
    assert(consul.default_wait_ms == 5000 && consul.default_lease_ms == 30000);
    assert(consul.poll_interval_ms == 50);
}

unittest {
    auto redisDefaults = RedisProviderOptions.init;
    auto etcdDefaults = EtcdProviderOptions.init;
    auto consulDefaults = ConsulProviderOptions.init;
    assert(redisDefaults.host == "127.0.0.1" && redisDefaults.port == 6379);
    assert(redisDefaults.nameSpace == "default" && redisDefaults.database == 0);
    assert(redisDefaults.rpcTimeout.total!"msecs" == 2000);
    assert(redisDefaults.cleanupTimeout.total!"msecs" == 2000);
    assert(redisDefaults.safetyMargin.total!"msecs" == 25);
    assert(redisDefaults.pollInterval.total!"msecs" == 25);
    assert(redisDefaults.defaultWait.total!"msecs" == 5000);
    assert(redisDefaults.defaultLease.total!"msecs" == 30000);
    assert(etcdDefaults.endpoint == "127.0.0.1:2379");
    assert(etcdDefaults.nameSpace == "default" && !etcdDefaults.autoRenew);
    assert(etcdDefaults.rpcTimeout.total!"msecs" == 2000);
    assert(etcdDefaults.cleanupTimeout.total!"msecs" == 2000);
    assert(etcdDefaults.safetyMargin.total!"msecs" == 25);
    assert(etcdDefaults.defaultWait.total!"msecs" == 5000);
    assert(etcdDefaults.defaultLease.total!"msecs" == 30000);
    assert(consulDefaults.endpoint == "http://127.0.0.1:8500");
    assert(consulDefaults.nameSpace == "default");
    assert(consulDefaults.rpcTimeout.total!"msecs" == 2000);
    assert(consulDefaults.cleanupTimeout.total!"msecs" == 2000);
    assert(consulDefaults.pollInterval.total!"msecs" == 50);
    assert(consulDefaults.defaultWait.total!"msecs" == 5000);
    assert(consulDefaults.defaultLease.total!"msecs" == 30000);

    auto redisNative = toNativeOptions(redisDefaults);
    auto etcdNative = toNativeOptions(etcdDefaults);
    auto consulNative = toNativeOptions(consulDefaults);
    assert(redisNative.rpc_timeout_ms == 2000 && redisNative.cleanup_timeout_ms == 2000);
    assert(redisNative.safety_margin_ms == 25 && redisNative.poll_interval_ms == 25);
    assert(redisNative.default_wait_ms == 5000 && redisNative.default_lease_ms == 30000);
    assert(etcdNative.rpc_timeout_ms == 2000 && etcdNative.cleanup_timeout_ms == 2000);
    assert(etcdNative.safety_margin_ms == 25);
    assert(etcdNative.default_wait_ms == 5000 && etcdNative.default_lease_ms == 30000);
    assert(etcdNative.auto_renew == 0);
    assert(consulNative.endpoint.fromStringz == "http://127.0.0.1:8500");
    assert(consulNative.name_space.fromStringz == "default");
    assert(consulNative.rpc_timeout_ms == 2000 && consulNative.cleanup_timeout_ms == 2000);
    assert(consulNative.poll_interval_ms == 50);
    assert(consulNative.default_wait_ms == 5000 && consulNative.default_lease_ms == 30000);
}

unittest {
    RedisProviderOptions redisConfig;
    redisConfig.rpcTimeout = dur!"usecs"(1500);
    bool rejected;
    try {
        toNativeOptions(redisConfig);
    } catch (DilockException error) {
        rejected = error.status == Status.invalidArgument;
    }
    assert(rejected, "sub-millisecond durations must not be truncated at the C ABI");
}

unittest {
    RedisProviderOptions redisConfig;
    redisConfig.host = "cache.internal";
    redisConfig.port = 6380;
    redisConfig.username = "dilock-client";
    redisConfig.password = "local-test-secret";
    redisConfig.database = 4;
    redisConfig.nameSpace = "payments";
    redisConfig.coordinationEpoch = "epoch-7";
    redisConfig.rpcTimeout = dur!"msecs"(800);
    redisConfig.cleanupTimeout = dur!"msecs"(700);
    redisConfig.safetyMargin = dur!"msecs"(30);
    redisConfig.pollInterval = dur!"msecs"(15);
    redisConfig.defaultWait = dur!"msecs"(6000);
    redisConfig.defaultLease = dur!"msecs"(45000);
    auto redisNative = toNativeOptions(redisConfig);
    assert(redisNative.host.fromStringz == redisConfig.host);
    assert(redisNative.port == 6380 && redisNative.database == 4);
    assert(redisNative.username.fromStringz == redisConfig.username);
    assert(redisNative.password.fromStringz == redisConfig.password);
    assert(redisNative.name_space.fromStringz == redisConfig.nameSpace);
    assert(redisNative.coordination_epoch.fromStringz == redisConfig.coordinationEpoch);
    assert(redisNative.rpc_timeout_ms == 800 && redisNative.cleanup_timeout_ms == 700);
    assert(redisNative.safety_margin_ms == 30 && redisNative.poll_interval_ms == 15);
    assert(redisNative.default_wait_ms == 6000 && redisNative.default_lease_ms == 45000);

    EtcdProviderOptions etcdConfig;
    etcdConfig.endpoint = "etcd.internal:2379";
    etcdConfig.nameSpace = "payments";
    etcdConfig.coordinationEpoch = "epoch-8";
    etcdConfig.tlsRootCertificates = "root PEM";
    etcdConfig.tlsClientCertificate = "client PEM";
    etcdConfig.tlsClientPrivateKey = "private key PEM";
    etcdConfig.rpcTimeout = dur!"msecs"(900);
    etcdConfig.cleanupTimeout = dur!"msecs"(650);
    etcdConfig.safetyMargin = dur!"msecs"(35);
    etcdConfig.defaultWait = dur!"msecs"(7000);
    etcdConfig.defaultLease = dur!"msecs"(60000);
    etcdConfig.autoRenew = true;
    auto etcdNative = toNativeOptions(etcdConfig);
    assert(etcdNative.endpoint.fromStringz == etcdConfig.endpoint);
    assert(etcdNative.name_space.fromStringz == etcdConfig.nameSpace);
    assert(etcdNative.coordination_epoch.fromStringz == etcdConfig.coordinationEpoch);
    assert(etcdNative.tls_root_certificates.fromStringz == etcdConfig.tlsRootCertificates);
    assert(etcdNative.tls_client_certificate.fromStringz == etcdConfig.tlsClientCertificate);
    assert(etcdNative.tls_client_private_key.fromStringz == etcdConfig.tlsClientPrivateKey);
    assert(etcdNative.rpc_timeout_ms == 900 && etcdNative.cleanup_timeout_ms == 650);
    assert(etcdNative.safety_margin_ms == 35);
    assert(etcdNative.default_wait_ms == 7000 && etcdNative.default_lease_ms == 60000);
    assert(etcdNative.auto_renew == 1);
}

unittest {
    EtcdProviderOptions etcdConfig;
    etcdConfig.tlsClientCertificate = "client PEM";
    bool rejected;
    try {
        toNativeOptions(etcdConfig);
    } catch (DilockException error) {
        rejected = error.status == Status.invalidArgument;
    }
    assert(rejected, "an etcd client certificate without its private key must fail before the C ABI");

    etcdConfig.tlsClientCertificate = "";
    etcdConfig.tlsClientPrivateKey = "private key PEM";
    rejected = false;
    try {
        toNativeOptions(etcdConfig);
    } catch (DilockException error) {
        rejected = error.status == Status.invalidArgument;
    }
    assert(rejected, "an etcd private key without its client certificate must fail before the C ABI");

    // A custom trust root alone is valid TLS configuration and does not imply
    // client authentication.
    etcdConfig.tlsClientPrivateKey = "";
    etcdConfig.tlsRootCertificates = "root PEM";
    auto rootsOnly = toNativeOptions(etcdConfig);
    assert(rootsOnly.tls_root_certificates.fromStringz == "root PEM");
    assert(rootsOnly.tls_client_certificate.fromStringz == "");
    assert(rootsOnly.tls_client_private_key.fromStringz == "");
}

unittest {
    RedisProviderOptions redisConfig;
    redisConfig.password = "bad\0credential";
    bool rejected;
    try {
        toNativeOptions(redisConfig);
    } catch (DilockException error) {
        rejected = error.status == Status.invalidArgument;
    }
    assert(rejected, "Redis credentials with embedded NUL must fail before the C ABI");

    EtcdProviderOptions etcdConfig;
    etcdConfig.tlsClientPrivateKey = "bad\0key";
    rejected = false;
    try {
        toNativeOptions(etcdConfig);
    } catch (DilockException error) {
        rejected = error.status == Status.invalidArgument;
    }
    assert(rejected, "PEM values with embedded NUL must fail before the C ABI");

    ConsulProviderOptions consulConfig;
    consulConfig.aclToken = "bad\0token";
    rejected = false;
    try {
        toNativeOptions(consulConfig);
    } catch (DilockException error) {
        rejected = error.status == Status.invalidArgument;
    }
    assert(rejected, "Consul tokens with embedded NUL must fail before the C ABI");
}
