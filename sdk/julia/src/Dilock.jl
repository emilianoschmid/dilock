module Dilock

using Redis
using SHA
using Random
using JSON3
using HTTP
using Base64
using LibPQ

export Provider, Lease, lock, try_acquire, acquire, release!, renew!, with_lease,
       resource_id, LeaseBusy, WaitTimeout, AcquisitionUnknown, BackendUnavailable, ProtocolMismatch,
       UnsupportedCapability, OperationCancelled, LeaseLost, ReleaseUnknown,
       EtcdProvider, EtcdHandle, EtcdLease, fence
export ConsulProvider, ConsulHandle, ConsulLease, provision_consul_namespace, connect_consul
export valid, effective_ttl_ms
export PostgresProvider, PostgresHandle, PostgresLease, provision_postgres_namespace, advisory_key
include("Postgres.jl")

struct LeaseBusy <: Exception end
struct WaitTimeout <: Exception end
struct AcquisitionUnknown <: Exception
    message::String
end
struct BackendUnavailable <: Exception
    message::String
end
struct ProtocolMismatch <: Exception
    message::String
end
struct UnsupportedCapability <: Exception
    message::String
end

"""Cancellation of an in-flight backend operation, with its remote outcome."""
struct OperationCancelled <: Exception
    operation::Symbol
    outcome::Symbol
    cause::Any
end
"""A scoped operation completed after its lease was no longer owned."""
struct LeaseLost <: Exception
    message::String
end
"""A scoped operation completed but remote release could not be confirmed."""
struct ReleaseUnknown <: Exception
    message::String
end

Base.showerror(io::IO, err::OperationCancelled) = print(
    io, "Dilock ", err.operation, " was cancelled (remote outcome: ", err.outcome, ")")
Base.showerror(io::IO, err::LeaseLost) = print(io, err.message)
Base.showerror(io::IO, err::ReleaseUnknown) = print(io, err.message)

Base.showerror(io::IO, err::UnsupportedCapability) = print(io, err.message)

const RELEASE_SCRIPT = read(joinpath(@__DIR__, "redis-release.lua"), String)
const RENEW_SCRIPT = read(joinpath(@__DIR__, "redis-renew.lua"), String)
const DEFAULT_WAIT_MS = 5_000
const DEFAULT_LEASE_MS = 30_000

_u32be(n::Integer) = UInt8[(n >> 24) & 0xff, (n >> 16) & 0xff, (n >> 8) & 0xff, n & 0xff]

"""Hash strict UTF-8 identity framing; names are case-sensitive and unnormalized."""
function resource_id(namespace::AbstractString, resource::AbstractString)
    isvalid(namespace) && occursin(r"\A[a-z0-9][a-z0-9_.-]{0,63}\z", namespace) ||
        throw(ArgumentError("invalid namespace"))
    isvalid(resource) || throw(ArgumentError("resource must be valid UTF-8 Unicode scalar data"))
    bytes = codeunits(String(resource))
    1 <= length(bytes) <= 1024 || throw(ArgumentError("resource must encode to 1..1024 UTF-8 bytes"))
    any(==(0x00), bytes) && throw(ArgumentError("resource must not contain U+0000"))
    nsbytes = Vector{UInt8}(codeunits(namespace))
    frame = vcat(Vector{UInt8}(codeunits("dilock.lock.identity\0")),
                 _u32be(length(nsbytes)), nsbytes, _u32be(length(bytes)), Vector{UInt8}(bytes))
    bytes2hex(sha256(frame))
end

"""Redis provider around a caller-owned Redis.jl connection. Use one connection per task."""
struct Provider{C}
    connection::C
    namespace::String
    epoch::String
    function Provider(connection, namespace::AbstractString, epoch::AbstractString)
        resource_id(namespace, "validation")
        isempty(epoch) && throw(ArgumentError("epoch must be provisioned and nonempty"))
        raw = Redis.get(connection, "dilock:namespace:$(namespace):meta")
        raw === nothing && throw(ProtocolMismatch("namespace metadata is missing"))
        length(raw) <= 8192 || throw(ProtocolMismatch("namespace metadata is too large"))
        meta = try JSON3.read(String(raw)) catch; throw(ProtocolMismatch("namespace metadata is invalid JSON")) end
        expected = ((:protocol_major, 1), (:identity_scheme, "sha256-framed-utf8-v1"),
                    (:profile, "redis-single-authority-v1"), (:coordination_epoch, String(epoch)),
                    (:fixed_lease_ms, nothing))
        for (key, value) in expected
            hasproperty(meta, key) || throw(ProtocolMismatch("namespace metadata is incompatible"))
            actual = getproperty(meta, key)
            # Julia considers `true == 1`; JSON metadata must preserve the field's
            # contract type as well as its value (notably protocol_major).
            typeof(actual) === typeof(value) && isequal(actual, value) ||
                throw(ProtocolMismatch("namespace metadata is incompatible"))
        end
        new{typeof(connection)}(connection, String(namespace), String(epoch))
    end
end

struct Handle{P}
    provider::P
    key::String
end
lock(provider::Provider, resource::AbstractString) =
    Handle(provider, "dilock:{$(resource_id(provider.namespace, resource))}:lease")

mutable struct Lease{P}
    provider::P
    key::String
    owner::String
    lease_ms::Int
    deadline_ns::UInt64
    state::Symbol
end

function _cleanup!(lease::Lease)
    lease.state == :released && return :already_not_owned
    # A failed reply is terminal: retrying could replace the original unknown
    # outcome with a later absence observation and falsely claim certainty.
    lease.state == :released_unknown && return :unknown
    lease.state = :releasing
    try
        result = Redis.evalscript(lease.provider.connection, RELEASE_SCRIPT, 1, lease.key, lease.owner)
        lease.state = :released
        return result == 1 ? :released : :already_not_owned
    catch err
        lease.state = :released_unknown
        err isa InterruptException && throw(OperationCancelled(:release, :unknown, err))
        return :unknown
    end
end

function _cleanup_after_acquire_failure!(lease::Lease, err, message::String)
    cleanup_error = nothing
    cleanup = try _cleanup!(lease) catch caught; cleanup_error = caught; :unknown end
    cancellation = err isa InterruptException ? err :
                   cleanup_error isa InterruptException ? cleanup_error :
                   cleanup_error isa OperationCancelled ? cleanup_error.cause : nothing
    if cancellation !== nothing
        outcome = cleanup in (:released, :already_not_owned) ? :not_acquired : :unknown
        throw(OperationCancelled(:acquire, outcome, cancellation))
    end
    throw(AcquisitionUnknown(message))
end

"""One SET NX PX attempt. `nothing` means acknowledged contention only."""
function try_acquire(handle::Handle; lease_ms::Integer=DEFAULT_LEASE_MS)
    100 <= lease_ms <= 86_400_000 || throw(ArgumentError("lease must be 100..86400000 ms"))
    owner = "dilock/1/" * bytes2hex(rand(RandomDevice(), UInt8, 32))
    sent = time_ns()
    result = try
        # Redis.jl 3.x declares SET as Bool, but its decoder throws MethodError
        # for Redis' null reply when NX contends. Read the raw reply so `nothing`
        # remains ordinary, acknowledged contention rather than an unknown write.
        command = Redis.flatten_command("set", handle.key, owner, "nx", "px", lease_ms)
        Redis.execute_command(handle.provider.connection, command)
    catch err
        # The write may have committed even when its response was lost. Try owner-specific cleanup.
        provisional = Lease(handle.provider, handle.key, owner, Int(lease_ms), sent, :held)
        _cleanup_after_acquire_failure!(provisional, err,
            "SET outcome was not acknowledged: $(typeof(err))")
    end
    if result === false || result === nothing
        keytype = try
            Redis.keytype(handle.provider.connection, handle.key)
        catch err
            err isa InterruptException && throw(OperationCancelled(:acquire, :not_acquired, err))
            throw(BackendUnavailable("Redis TYPE check failed after contention: $(typeof(err))"))
        end
        (keytype == "string" || keytype == "none") && return nothing
        throw(ProtocolMismatch("lock key has an incompatible Redis type"))
    end
    (result === true || result == "OK") || begin
        provisional = Lease(handle.provider, handle.key, owner, Int(lease_ms), sent, :held)
        _cleanup_after_acquire_failure!(provisional, nothing,
            "Redis returned an unexpected SET reply")
    end
    margin_ms = max(10, ceil(Int, lease_ms * 0.01))
    deadline = sent + UInt64((lease_ms - margin_ms) * 1_000_000)
    time_ns() < deadline || begin
        provisional = Lease(handle.provider, handle.key, owner, Int(lease_ms), deadline, :held)
        _cleanup_after_acquire_failure!(provisional, nothing,
            "SET acknowledgement arrived after conservative deadline")
    end
    Lease(handle.provider, handle.key, owner, Int(lease_ms), deadline, :held)
end

"""Wait only on acknowledged contention; defaults are five-second wait and thirty-second lease."""
function acquire(handle::Handle; wait_ms::Integer=DEFAULT_WAIT_MS, lease_ms::Integer=DEFAULT_LEASE_MS)
    1 <= wait_ms <= 86_400_000 || throw(ArgumentError("wait must be 1..86400000 ms"))
    finish = time_ns() + UInt64(wait_ms * 1_000_000)
    while time_ns() < finish
        lease = try_acquire(handle; lease_ms)
        lease === nothing || return _accept_acquired(lease, finish)
        _contention_backoff(min(0.05, max(0.0, (finish - time_ns()) / 1e9)))
    end
    throw(WaitTimeout())
end

function _contention_backoff(duration::Real, sleeper::Function=sleep)
    try
        sleeper(duration)
    catch err
        err isa InterruptException && throw(OperationCancelled(:acquire, :not_acquired, err))
        rethrow()
    end
end

function renew!(lease::Lease; lease_ms::Integer=lease.lease_ms)
    100 <= lease_ms <= 86_400_000 || throw(ArgumentError("lease must be 100..86400000 ms"))
    lease.state == :held && time_ns() < lease.deadline_ns || (lease.state = :lost; return false)
    sent = time_ns()
    try
        # Redis.jl 3.x accepts one `keys` and one `args` value. Pass all Lua
        # ARGV values as a vector; positional owner/TTL arguments dispatch to
        # no evalscript method and turn every renewal into an unknown outcome.
        result = Redis.evalscript(lease.provider.connection, RENEW_SCRIPT, 1,
                                  lease.key, [lease.owner, lease_ms])
        deadline = sent + UInt64((lease_ms - max(10, ceil(Int, lease_ms * 0.01))) * 1_000_000)
        if result == 1 && time_ns() < deadline && time_ns() < lease.deadline_ns
            lease.lease_ms = Int(lease_ms)
            lease.deadline_ns = deadline
            return true
        end
        lease.state = :lost
        false
    catch err
        lease.state = :lost
        err isa InterruptException && throw(OperationCancelled(:renew, :unknown, err))
        throw(AcquisitionUnknown("renewal was not acknowledged: $(typeof(err))"))
    end
end

"""Explicit owner-checked release. Returns released/already_not_owned/unknown."""
release!(lease::Lease) = _cleanup!(lease)

function _accept_acquired(lease::Lease, finish::UInt64;
                          now::Function=time_ns, release::Function=release!)
    now() < finish && return lease
    # The backend call may finish after the caller's wait budget. Never surface
    # that late lease; owner-checked cleanup is the only safe next action.
    outcome = try release(lease) catch err
        err isa OperationCancelled && throw(OperationCancelled(:acquire, :unknown, err.cause))
        :unknown
    end
    outcome in (:released, :already_not_owned) && throw(WaitTimeout())
    throw(AcquisitionUnknown("acquisition completed after the wait deadline and owner cleanup was not acknowledged"))
end

function _release_and_observe!(lease, observer; suppress_observer_errors::Bool)
    outcome = try
        release!(lease)
    catch err
        observed = err isa OperationCancelled && err.operation == :release ? err.outcome : :unknown
        try observer === nothing || observer(observed) catch end
        rethrow()
    end
    observer === nothing && return outcome
    try
        observer(outcome)
    catch
        # A reporting hook must never replace an exception already raised by
        # the scoped operation (including InterruptException). If cleanup itself
        # is unconfirmed, the typed cleanup failure is more useful than a hook
        # failure and must remain visible to the caller.
        (suppress_observer_errors || outcome !== :released) || rethrow()
    end
    outcome
end

function _require_confirmed_scope_release(outcome::Symbol)
    outcome === :released && return outcome
    outcome === :already_not_owned && throw(LeaseLost(
        "Dilock lease was no longer owned when scoped work completed"))
    outcome === :unknown && throw(ReleaseUnknown(
        "Dilock scoped release acknowledgement is unknown"))
    throw(ProtocolMismatch("Dilock release returned an invalid outcome"))
end

"""Run `f(lease)`, release it, and optionally report the release outcome.

`on_release` is called synchronously with `:released`, `:already_not_owned`,
or `:unknown` after the release attempt. The body result is returned unchanged.
If the body throws (including task interruption), cleanup and observation are
attempted and the original exception is rethrown; observer errors are suppressed
in that case. If the body succeeds, an observer error is propagated. Cleanup is
best effort and is not hard-bounded because Redis.jl 3.x exposes no command
deadline for its borrowed connection. Use one Redis connection per task and let
the caller manage its lifetime.
"""
function with_lease(f::Function, handle::Handle; on_release=nothing, kwargs...)
    lease = acquire(handle; kwargs...)
    result = nothing
    try
        result = f(lease)
    catch
        try
            _release_and_observe!(lease, on_release; suppress_observer_errors=true)
        catch
            # Preserve the body exception even if cleanup unexpectedly throws.
        end
        rethrow()
    end
    outcome = _release_and_observe!(lease, on_release; suppress_observer_errors=false)
    _require_confirmed_scope_release(outcome)
    result
end

include("Etcd.jl")
using .Etcd: EtcdProvider, EtcdHandle, EtcdLease
import .Etcd: fence
include("Consul.jl")

end
