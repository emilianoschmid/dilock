const POSTGRES_PROFILE = "postgres-advisory-session-v1"
const POSTGRES_SCHEME = "sha256-framed-utf8-v1"

struct PostgresProvider
    connection_string::String
    namespace::String
    epoch::String
    rpc_timeout::Float64
    cleanup_timeout::Float64
    poll_ms::Int
    wait_ms::Int
    lease_ms::Int
    connected::Base.RefValue{Bool}
    closed::Base.RefValue{Bool}
    leases::Vector{Any}
end

struct PostgresHandle
    provider::PostgresProvider
    key::Int64
end

mutable struct PostgresLease
    provider::PostgresProvider
    connection::Union{LibPQ.Connection,Nothing}
    key::Int64
    state::Symbol
end

function PostgresProvider(; connection_string, namespace="default", epoch,
                          rpc_timeout=2.0, cleanup_timeout=2.0, poll_ms=25,
                          wait_ms=5_000, lease_ms=30_000)
    resource_id(namespace, "validation")
    isvalid(epoch) && 1 <= ncodeunits(epoch) <= 1024 && !occursin('\0', epoch) ||
        throw(ArgumentError("coordination epoch must be 1..1024 UTF-8 bytes and contain no NUL"))
    isfinite(rpc_timeout) && rpc_timeout > 0 || throw(ArgumentError("rpc_timeout must be positive"))
    isfinite(cleanup_timeout) && cleanup_timeout > 0 || throw(ArgumentError("cleanup_timeout must be positive"))
    poll_ms isa Integer && poll_ms > 0 || throw(ArgumentError("poll_ms must be positive"))
    wait_ms isa Integer && wait_ms > 0 || throw(ArgumentError("wait_ms must be positive"))
    lease_ms isa Integer && lease_ms >= 100 && lease_ms <= 86_400_000 || throw(ArgumentError("lease_ms must be 100..86400000"))
    provider = PostgresProvider(String(connection_string), String(namespace), String(epoch), Float64(rpc_timeout),
                                Float64(cleanup_timeout), Int(poll_ms), Int(wait_ms), Int(lease_ms),
                                Ref(false), Ref(false), Any[])
    connection = LibPQ.Connection(provider.connection_string)
    try
        _postgres_validate_metadata(provider, connection)
    finally
        Base.close(connection)
    end
    provider.connected[] = true
    provider
end

function _postgres_result(connection, sql, parameters=String[])
    isempty(parameters) ? LibPQ.execute(connection, sql) : LibPQ.execute(connection, sql, String.(parameters))
end

function _postgres_validate_metadata(provider::PostgresProvider, connection)
    result = _postgres_result(connection,
        "SELECT protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms FROM dilock_namespace_meta WHERE namespace = \$1",
        [provider.namespace])
    LibPQ.num_rows(result) == 1 && LibPQ.num_columns(result) == 5 || throw(ProtocolMismatch("PostgreSQL namespace metadata is missing or malformed"))
    result[1, 1] isa Integer && result[1, 1] == 1 && result[1, 2] == POSTGRES_SCHEME &&
        result[1, 3] == POSTGRES_PROFILE && result[1, 4] == provider.epoch && result[1, 5] === missing ||
        throw(ProtocolMismatch("PostgreSQL namespace metadata is incompatible"))
    true
end

function provision_postgres_namespace(connection_string, namespace, epoch)
    resource_id(namespace, "validation")
    connection = LibPQ.Connection(connection_string)
    insert_result = nothing
    try
        _postgres_result(connection, "BEGIN")
        try
            _postgres_result(connection, "SELECT pg_advisory_xact_lock(hashtextextended('dilock:namespace-meta:v1', 0))")
            _postgres_result(connection, "CREATE TABLE IF NOT EXISTS dilock_namespace_meta (namespace text PRIMARY KEY, protocol_major integer NOT NULL, identity_scheme text NOT NULL, profile text NOT NULL, coordination_epoch text NOT NULL, fixed_lease_ms bigint NULL)")
            insert_result = _postgres_result(connection,
                "INSERT INTO dilock_namespace_meta (namespace, protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms) VALUES (\$1, 1, \$2, \$3, \$4, NULL) ON CONFLICT (namespace) DO NOTHING RETURNING namespace",
                [namespace, POSTGRES_SCHEME, POSTGRES_PROFILE, epoch])
            _postgres_result(connection, "COMMIT")
        catch
            try _postgres_result(connection, "ROLLBACK") catch end
            rethrow()
        end
        _postgres_validate_metadata(PostgresProvider(connection_string=connection_string, namespace=namespace, epoch=epoch), connection)
        return LibPQ.num_rows(insert_result) == 1 ? :created : :compatible_existing
    finally
        Base.close(connection)
    end
end

function lock(provider::PostgresProvider, resource::AbstractString)
    provider.closed[] && throw(ArgumentError("provider is closed"))
    provider.connected[] || throw(ProtocolMismatch("PostgreSQL metadata was not validated"))
    PostgresHandle(provider, advisory_key(resource_id(provider.namespace, resource)))
end

function advisory_key(resource_id_value::AbstractString)
    length(resource_id_value) == 64 && occursin(r"\A[0-9a-f]{64}\z", resource_id_value) || throw(ArgumentError("resource id must be a lowercase SHA-256 digest"))
    reinterpret(Int64, [parse(UInt64, resource_id_value[1:16]; base=16)])[1]
end

function try_acquire(handle::PostgresHandle; lease_ms=handle.provider.lease_ms)
    lease_ms isa Integer && 100 <= lease_ms <= 86_400_000 || throw(ArgumentError("lease_ms must be 100..86400000"))
    provider = handle.provider
    provider.closed[] && throw(ArgumentError("provider is closed"))
    connection = LibPQ.Connection(provider.connection_string)
    try
        result = _postgres_result(connection, "SELECT pg_try_advisory_lock(\$1::bigint)", [string(handle.key)])
        LibPQ.num_rows(result) == 1 || throw(AcquisitionUnknown("PostgreSQL advisory-lock response is malformed"))
        if result[1, 1] !== true
            Base.close(connection)
            return nothing
        end
        lease = PostgresLease(provider, connection, handle.key, :held)
        push!(provider.leases, lease)
        return lease
    catch
        Base.close(connection)
        rethrow()
    end
end

function acquire(handle::PostgresHandle; wait_ms=handle.provider.wait_ms, lease_ms=handle.provider.lease_ms)
    wait_ms isa Integer && 1 <= wait_ms <= 86_400_000 || throw(ArgumentError("wait_ms must be 1..86400000"))
    finish = time_ns() + UInt64(wait_ms * 1_000_000)
    while time_ns() < finish
        lease = try_acquire(handle; lease_ms)
        lease === nothing || return lease
        sleep(min(handle.provider.poll_ms / 1000, max(0.0, (finish - time_ns()) / 1e9)))
    end
    throw(WaitTimeout())
end

valid(lease::PostgresLease) = lease.state === :held && lease.connection !== nothing
function renew!(lease::PostgresLease)
    valid(lease) || (lease.state = :lost; throw(LeaseLost("PostgreSQL advisory session is no longer live")))
    try
        _postgres_result(lease.connection, "SELECT 1")
        true
    catch err
        lease.state = :lost
        throw(BackendUnavailable("PostgreSQL liveness check failed: $(typeof(err))"))
    end
end

function release!(lease::PostgresLease)
    lease.state === :released && return :already_not_owned
    lease.state === :released_unknown && return :unknown
    outcome = :unknown
    try
        result = _postgres_result(lease.connection, "SELECT pg_advisory_unlock(\$1::bigint)", [string(lease.key)])
        LibPQ.num_rows(result) == 1 || throw(ArgumentError("malformed release response"))
        outcome = result[1, 1] === true ? :released : :already_not_owned
    catch
        outcome = :unknown
    finally
        lease.connection === nothing || Base.close(lease.connection)
        lease.connection = nothing
    end
    lease.state = outcome === :unknown ? :released_unknown : :released
    outcome
end

function close(provider::PostgresProvider)
    provider.closed[] && return nothing
    for lease in copy(provider.leases)
        release!(lease)
    end
    provider.closed[] = true
    nothing
end
