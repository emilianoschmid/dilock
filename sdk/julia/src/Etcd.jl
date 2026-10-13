module Etcd

using JSON3
using gRPCClient
using Random

import ..Dilock: AcquisitionUnknown, BackendUnavailable, ProtocolMismatch,
                 WaitTimeout, UnsupportedCapability, OperationCancelled,
                 resource_id, lock, acquire,
                 release!, with_lease, _release_and_observe!,
                 _require_confirmed_scope_release

export EtcdProvider, EtcdHandle, EtcdLease, lock, acquire, release!, with_lease,
       try_acquire, renew!, fence

const DEFAULT_WAIT_MS = 5_000
const DEFAULT_LEASE_MS = 30_000
const CLEANUP_MS = 2_000
const LEASE_PATH = "/etcdserverpb.Lease/"
const LOCK_PATH = "/v3lockpb.Lock/"
const KV_PATH = "/etcdserverpb.KV/"

struct _EtcdWaitDeadline <: Exception end
struct _EtcdLeaseNotFound <: Exception end

# These protobuf helpers encode the small, stable subset of etcd v3 messages
# used here. Unknown response fields are skipped according to the protobuf wire
# format, so newer etcd servers can add fields without breaking this client.
function _varint!(out::Vector{UInt8}, value::UInt64)
    while value >= 0x80
        push!(out, UInt8(value & 0x7f) | 0x80)
        value >>= 7
    end
    push!(out, UInt8(value))
    out
end

function _vfield(number::Integer, value::Integer)
    out = UInt8[]
    _varint!(out, UInt64((number << 3) | 0))
    _varint!(out, reinterpret(UInt64, Int64(value)))
end

function _bfield(number::Integer, value::AbstractVector{UInt8})
    out = UInt8[]
    _varint!(out, UInt64((number << 3) | 2))
    _varint!(out, UInt64(length(value)))
    append!(out, value)
    out
end
_bfield(number::Integer, value::AbstractString) = _bfield(number, Vector{UInt8}(codeunits(value)))

function _read_varint(data::AbstractVector{UInt8}, index::Int)
    value = UInt64(0)
    for byte_index in 0:9
        index <= length(data) || throw(ProtocolMismatch("malformed etcd protobuf varint"))
        byte = data[index]
        index += 1
        payload = UInt64(byte & 0x7f)
        # A uint64 varint is at most ten bytes; only bit zero is available in
        # the tenth byte. Julia's UInt64 shift otherwise truncates overflow,
        # which could turn malformed server data into a valid lease or revision.
        byte_index == 9 && payload > 1 &&
            throw(ProtocolMismatch("malformed etcd protobuf varint"))
        value |= payload << (7 * byte_index)
        byte < 0x80 && return value, index
    end
    throw(ProtocolMismatch("malformed etcd protobuf varint"))
end

function _fields(data::AbstractVector{UInt8})
    fields = Dict{Int,Vector{Any}}()
    index = 1
    while index <= length(data)
        tag, index = _read_varint(data, index)
        number, wire = Int(tag >> 3), Int(tag & 7)
        number > 0 || throw(ProtocolMismatch("invalid etcd protobuf field number"))
        value = if wire == 0
            v, index = _read_varint(data, index)
            v
        elseif wire == 1
            index + 7 <= length(data) || throw(ProtocolMismatch("truncated etcd protobuf field"))
            v = data[index:index + 7]; index += 8; v
        elseif wire == 2
            count, index = _read_varint(data, index)
            count <= UInt64(length(data) - index + 1) || throw(ProtocolMismatch("truncated etcd protobuf field"))
            finish = index + Int(count) - 1
            v = count == 0 ? UInt8[] : data[index:finish]
            index = finish + 1
            v
        elseif wire == 5
            index + 3 <= length(data) || throw(ProtocolMismatch("truncated etcd protobuf field"))
            v = data[index:index + 3]; index += 4; v
        else
            throw(ProtocolMismatch("unsupported etcd protobuf wire type"))
        end
        push!(get!(fields, number, Any[]), value)
    end
    fields
end

_one(fields, number, default=nothing) = get(fields, number, Any[default])[end]
function _signed(value)
    value === nothing && return nothing
    reinterpret(Int64, UInt64(value))
end
_string(value) = value === nothing ? nothing : String(Vector{UInt8}(value))

struct EtcdProvider
    host::String
    port::Int
    secure::Bool
    token::Union{Nothing,String}
    namespace::String
    epoch::String
    deadline_s::Float64
end

function _acquisition_rpc_timeout(provider::EtcdProvider, wait_deadline_ns::UInt64;
                                  now::Function=time_ns)
    current = now()
    current < wait_deadline_ns || throw(_EtcdWaitDeadline())
    remaining_s = Float64(wait_deadline_ns - current) / 1_000_000_000
    min(provider.deadline_s, remaining_s)
end

function _endpoint(endpoint::AbstractString)
    value = String(endpoint)
    if startswith(value, "http://") || startswith(value, "https://")
        secure = startswith(value, "https://")
        hostport = split(value, "://"; limit=2)[2]
    else
        secure = false
        hostport = value
    end
    occursin('/', hostport) && throw(ArgumentError("etcd endpoint must be host:port without a path"))
    host, port_text = if startswith(hostport, "[")
        closing = findfirst(==(']'), hostport)
        closing !== nothing && closing > 2 || throw(ArgumentError("etcd IPv6 endpoint must use [address]:port"))
        closing < lastindex(hostport) && hostport[nextind(hostport, closing)] == ':' ||
            throw(ArgumentError("etcd IPv6 endpoint must use [address]:port"))
        (hostport[nextind(hostport, firstindex(hostport)):prevind(hostport, closing)],
         hostport[nextind(hostport, nextind(hostport, closing)):end])
    else
        pieces = split(hostport, ':'; limit=2)
        length(pieces) == 2 && !isempty(pieces[1]) || throw(ArgumentError("etcd endpoint must be host:port"))
        pieces[1], pieces[2]
    end
    isempty(host) && throw(ArgumentError("etcd endpoint host is empty"))
    port = tryparse(Int, port_text)
    port !== nothing && 1 <= port <= 65535 || throw(ArgumentError("etcd endpoint port is invalid"))
    host, port, secure
end

function EtcdProvider(endpoint::AbstractString, namespace::AbstractString, epoch::AbstractString;
                      secure::Union{Nothing,Bool}=nothing, token::Union{Nothing,AbstractString}=nothing,
                      deadline_s::Real=10)
    # Reuse the canonical identity validator without making any backend call.
    resource_id(namespace, "validation")
    isempty(epoch) && throw(ArgumentError("epoch must be provisioned and nonempty"))
    isfinite(deadline_s) && deadline_s > 0 || throw(ArgumentError("deadline_s must be positive and finite"))
    host, port, endpoint_secure = _endpoint(endpoint)
    provider = EtcdProvider(host, port, something(secure, endpoint_secure),
                            token === nothing ? nothing : String(token), String(namespace),
                            String(epoch), Float64(deadline_s))
    _validate_metadata(provider)
    provider
end

struct EtcdHandle
    provider::EtcdProvider
    name::Vector{UInt8}
end

function lock(provider::EtcdProvider, resource::AbstractString)
    rid = resource_id(provider.namespace, resource)
    EtcdHandle(provider, Vector{UInt8}(codeunits("/dilock/locks/" * rid)))
end

mutable struct EtcdLease
    provider::EtcdProvider
    key::Vector{UInt8}
    lease_id::Int64
    epoch::String
    create_revision::Int64
    ttl_s::Int64
    deadline_ns::UInt64
    state::Symbol
    keepalive_req::Any
    keepalive_input::Any
    keepalive_sent_at::Any
    keepalive_sender::Any
    keepalive_task::Any
    renewal::Bool
end

function _client(provider::EtcdProvider, path::String, request_stream::Bool=false, response_stream::Bool=false;
                 deadline_s=provider.deadline_s)
    gRPCClient.gRPCServiceClient{Vector{UInt8},request_stream,Vector{UInt8},response_stream}(
        provider.host, provider.port, path; secure=provider.secure, token=provider.token,
        deadline=deadline_s)
end

function _translate_rpc_error(err, wait_deadline::Bool, lease_revoke::Bool=false)
    err isa InterruptException && throw(err)
    if lease_revoke && err isa gRPCClient.gRPCServiceCallException &&
       hasproperty(err, :grpc_status) && getproperty(err, :grpc_status) == 5
        # Preserve only LeaseRevoke NOT_FOUND: it confirms this unique lease
        # has expired or was already revoked. Other RPCs keep their usual error
        # mapping so unrelated missing resources are not treated as lease loss.
        throw(_EtcdLeaseNotFound())
    end
    if err isa gRPCClient.gRPCServiceCallException &&
       hasproperty(err, :grpc_status) && getproperty(err, :grpc_status) == 4
    wait_deadline && throw(_EtcdWaitDeadline())
        throw(BackendUnavailable("etcd gRPC deadline exceeded"))
    end
    throw(BackendUnavailable("etcd gRPC call failed ($(typeof(err)))"))
end

function _rpc(provider::EtcdProvider, path::String, request::Vector{UInt8};
              deadline_s=provider.deadline_s, wait_deadline::Bool=false,
              lease_revoke::Bool=false)
    client = _client(provider, path; deadline_s)
    try
        gRPCClient.grpc_sync_request(client, request; deadline=deadline_s)
    catch err
        _translate_rpc_error(err, wait_deadline, lease_revoke)
    end
end

function _range(provider::EtcdProvider, key::Vector{UInt8};
                deadline_s::Real=provider.deadline_s, wait_deadline::Bool=false)
    request = _bfield(1, key)
    response = _fields(_rpc(provider, KV_PATH * "Range", request;
                            deadline_s, wait_deadline))
    _range_record(response)
end

function _range_record(response::Dict{Int,Vector{Any}})
    nested = _one(response, 2, nothing)
    nested === nothing && return nothing
    kvs = _fields(nested)
    (key=_one(kvs, 1, UInt8[]), create_revision=_signed(_one(kvs, 2, 0)),
     lease=_signed(_one(kvs, 6, 0)))
end

function _verified_create_revision(record, key::Vector{UInt8}, lease_id::Int64)
    record !== nothing && record.key == key && record.lease == lease_id ||
        throw(ProtocolMismatch("etcd Lock key is not attached to the expected lease"))
    record.create_revision !== nothing && record.create_revision > 0 ||
        throw(ProtocolMismatch("etcd Lock key has no valid creation revision"))
    record.create_revision
end

function _validate_metadata(provider::EtcdProvider)
    key = Vector{UInt8}(codeunits("/dilock/namespaces/$(provider.namespace)/meta"))
    envelope = _fields(_rpc(provider, KV_PATH * "Range", _bfield(1, key)))
    nested = _one(envelope, 2, nothing)
    nested === nothing && throw(ProtocolMismatch("namespace metadata is missing"))
    kv = _fields(nested)
    _one(kv, 1, UInt8[]) == key || throw(ProtocolMismatch("etcd returned a different metadata key"))
    payload = _one(kv, 5, UInt8[])
    length(payload) <= 8192 || throw(ProtocolMismatch("namespace metadata is too large"))
    metadata = try JSON3.read(String(payload)) catch; throw(ProtocolMismatch("namespace metadata is invalid JSON")) end
    expected = ((:protocol_major, 1), (:identity_scheme, "sha256-framed-utf8-v1"),
                (:profile, "etcd-native-lock-v1"), (:coordination_epoch, provider.epoch),
                (:fixed_lease_ms, nothing))
    for (field, wanted) in expected
        hasproperty(metadata, field) || throw(ProtocolMismatch("namespace metadata is incompatible"))
        actual = getproperty(metadata, field)
        typeof(actual) === typeof(wanted) && isequal(actual, wanted) ||
            throw(ProtocolMismatch("namespace metadata is incompatible"))
    end
    nothing
end

function _grant(provider::EtcdProvider, ttl_s::Int64;
                deadline_s::Real=provider.deadline_s, wait_deadline::Bool=false)
    sent = time_ns()
    fields = _fields(_rpc(provider, LEASE_PATH * "LeaseGrant", _vfield(1, ttl_s);
                          deadline_s, wait_deadline))
    id = _signed(_one(fields, 2, 0))
    ttl = _signed(_one(fields, 3, 0))
    id > 0 && ttl >= ttl_s || throw(BackendUnavailable("etcd did not grant the requested lease"))
    id, ttl, sent
end

function _revoke(provider::EtcdProvider, id::Int64; deadline_s::Real=CLEANUP_MS / 1000)
    _rpc(provider, LEASE_PATH * "LeaseRevoke", _vfield(1, id); deadline_s, lease_revoke=true)
    nothing
end

function _start_keepalive!(lease::EtcdLease)
    client = _client(lease.provider, LEASE_PATH * "LeaseKeepAlive", true, true;
                     deadline_s=Inf)
    input = Channel{Vector{UInt8}}(1)
    output = Channel{Vector{UInt8}}(1)
    sent_times = Channel{UInt64}(1)
    lease.keepalive_input = input
    lease.keepalive_sent_at = sent_times
    request = gRPCClient.grpc_async_request(client, input, output)
    lease.keepalive_req = request
    interval = max(0.1, lease.ttl_s / 3)
    lease.keepalive_sender = @async begin
        try
            while lease.state in (:waiting, :held) && (lease.state == :waiting || lease.renewal)
                sent = time_ns()
                put!(sent_times, sent)
                put!(input, _vfield(1, lease.lease_id))
                sleep(interval)
            end
        catch
            # Closing/cancellation is normal here; the observer records transport loss.
        end
    end
    lease.keepalive_task = @async begin
        try
            for response in output
                sent = take!(sent_times)
                parsed = _fields(response)
                returned_id = _signed(_one(parsed, 2, 0))
                ttl = _signed(_one(parsed, 3, 0))
                if returned_id != lease.lease_id || ttl <= 0
                    lease.state = :lost
                    break
                end
                lease.deadline_ns = sent + UInt64(max(0, ttl * 1000 - 100) * 1_000_000)
            end
        catch
            lease.state in (:waiting, :held) && (lease.state = :lost)
        end
    end
    nothing
end

function _attempt_shutdown_steps(actions::Function...)
    interrupted = nothing
    for action in actions
        try
            action()
        catch err
            err isa InterruptException && interrupted === nothing && (interrupted = err)
        end
    end
    interrupted
end

function _stop_keepalive!(lease::EtcdLease)
    lease.keepalive_req === nothing && lease.keepalive_input === nothing &&
        lease.keepalive_task === nothing && lease.keepalive_sender === nothing && return
    prior_state = lease.state
    lease.state in (:waiting, :held) && (lease.state = :stopping)
    actions = Function[]
    lease.keepalive_input !== nothing && isopen(lease.keepalive_input) &&
        push!(actions, () -> close(lease.keepalive_input))
    lease.keepalive_req !== nothing &&
        push!(actions, () -> gRPCClient.grpc_cancel(lease.keepalive_req))
    lease.keepalive_task !== nothing && push!(actions, () -> wait(lease.keepalive_task))
    interrupted = _attempt_shutdown_steps(actions...)
    lease.keepalive_req = nothing
    lease.keepalive_input = nothing
    lease.keepalive_sent_at = nothing
    lease.keepalive_task = nothing
    lease.keepalive_sender = nothing
    lease.state == :stopping && (lease.state = prior_state)
    interrupted === nothing || throw(interrupted)
    nothing
end

_lease_not_found(err) = err isa _EtcdLeaseNotFound ||
                        (hasproperty(err, :grpc_status) && getproperty(err, :grpc_status) == 5)

function _unlock_then_revoke!(lease::EtcdLease, ownership::Symbol, unlock::Bool,
                              unlock_action::Function, revoke_action::Function;
                              preflight_error=nothing)
    ownership in (:owned, :not_owned, :unknown) ||
        throw(ArgumentError("invalid etcd cleanup ownership observation"))
    interrupted = preflight_error isa InterruptException ? preflight_error : nothing
    unlock_acknowledged = false
    # Unlock is safe only after the exact key was observed under this attempt's
    # lease. Absence, mismatch, or an unavailable preflight must skip it.
    if unlock && ownership === :owned && !isempty(lease.key)
        try
            unlock_action()
            unlock_acknowledged = true
        catch err
            err isa InterruptException && (interrupted = err)
            # Revoke is an independent cleanup path for this acquisition's
            # unique lease. Do not let an ambiguous Unlock acknowledgement
            # prevent cleanup of the waiter/lock attached to that lease.
        end
    end
    revoke_error = try
        revoke_action()
        nothing
    catch err
        err
    end
    revoke_acknowledged = revoke_error === nothing
    revoke_not_found = revoke_error !== nothing && _lease_not_found(revoke_error)
    outcome = if revoke_not_found || ownership === :not_owned
        # etcd confirmed the unique lease is gone, or the exact lock key was
        # already absent/mismatched. Neither is this call's acknowledged release.
        :already_not_owned
    elseif ownership === :owned && unlock_acknowledged && revoke_acknowledged
        # Only this full sequence proves that this call removed its own key and
        # received positive acknowledgements for both native cleanup RPCs.
        :released
    elseif revoke_acknowledged
        # Revoke-only cleanup is terminal, but does not prove that Unlock
        # acknowledged removal of the observed native key.
        :already_not_owned
    else
        :unknown
    end
    if outcome !== :unknown
        # The unique lease is terminal. `:released` is reserved for the full
        # exact-owner + Unlock ACK + positive Revoke ACK sequence above.
        lease.state = :released
        interrupted === nothing || throw(OperationCancelled(:release, outcome, interrupted))
        revoke_error isa InterruptException && throw(OperationCancelled(:release, outcome, revoke_error))
        return outcome
    end
    lease.state = :released_unknown
    if revoke_error isa InterruptException || interrupted !== nothing
        throw(OperationCancelled(:release, :unknown, something(interrupted, revoke_error)))
    end
    :unknown
end

function _lease_cleanup!(lease::EtcdLease; unlock::Bool)
    lease.state == :released && return :already_not_owned
    # Never retry an ambiguous Unlock/Revoke sequence. A second attempt cannot
    # recover which operation committed before the first response was lost.
    lease.state == :released_unknown && return :unknown
    lease.state = :releasing
    stop_error = try _stop_keepalive!(lease); nothing catch err; err end
    cleanup_deadline_ns = time_ns() + UInt64(CLEANUP_MS) * UInt64(1_000_000)
    remaining_cleanup_s() = begin
        now = time_ns()
        now < cleanup_deadline_ns || throw(BackendUnavailable("etcd cleanup deadline exceeded"))
        Float64(cleanup_deadline_ns - now) / 1_000_000_000
    end
    ownership = :unknown
    preflight_error = nothing
    if !isempty(lease.key)
        ownership = :unknown
        try
            record = _range(lease.provider, lease.key; deadline_s=remaining_cleanup_s())
            ownership = record !== nothing && record.key == lease.key &&
                        record.lease == lease.lease_id ? :owned : :not_owned
        catch err
            preflight_error = err
        end
    end
    unlock_action = () -> _rpc(lease.provider, LOCK_PATH * "Unlock", _bfield(1, lease.key);
                               deadline_s=remaining_cleanup_s())
    revoke_action = () -> _revoke(lease.provider, lease.lease_id;
                                  deadline_s=remaining_cleanup_s())
    outcome = _unlock_then_revoke!(lease, ownership, unlock, unlock_action, revoke_action;
                                   preflight_error)
    stop_error isa InterruptException && throw(OperationCancelled(:release, outcome, stop_error))
    outcome
end

function _make_lease(provider, key, id, ttl, renewal)
    EtcdLease(provider, key, id, provider.epoch, 0, ttl,
              time_ns() + UInt64(max(0, ttl * 1000 - 100) * 1_000_000),
              :waiting, nothing, nothing, nothing, nothing, nothing, renewal)
end

"""Acquire with a fresh etcd lease under one total `wait_ms` budget."""
function acquire(handle::EtcdHandle; wait_ms::Integer=DEFAULT_WAIT_MS,
                 lease_ms::Integer=DEFAULT_LEASE_MS, auto_renew::Bool=false)
    1 <= wait_ms <= 86_400_000 || throw(ArgumentError("wait must be 1..86400000 ms"))
    1_000 <= lease_ms <= 86_400_000 || throw(ArgumentError("etcd lease must be 1000..86400000 ms"))
    provider = handle.provider
    wait_deadline_ns = time_ns() + UInt64(wait_ms) * UInt64(1_000_000)
    grant_timeout = try
        _acquisition_rpc_timeout(provider, wait_deadline_ns)
    catch err
        err isa _EtcdWaitDeadline && throw(WaitTimeout())
        rethrow()
    end
    id, ttl, grant_sent = try
        _grant(provider, ceil(Int64, lease_ms / 1000);
               deadline_s=grant_timeout, wait_deadline=true)
    catch err
        err isa InterruptException && throw(OperationCancelled(:acquire, :unknown, err))
        err isa _EtcdWaitDeadline && throw(WaitTimeout())
        rethrow()
    end
    lease = _make_lease(provider, UInt8[], id, ttl, auto_renew)
    lease.deadline_ns = grant_sent + UInt64(max(0, ttl * 1000 - 100) * 1_000_000)
    try
        _start_keepalive!(lease)
        lock_timeout = _acquisition_rpc_timeout(provider, wait_deadline_ns)
        response = _rpc(provider, LOCK_PATH * "Lock", vcat(_bfield(1, handle.name), _vfield(2, id));
                        deadline_s=lock_timeout, wait_deadline=true)
        time_ns() < wait_deadline_ns || throw(_EtcdWaitDeadline())
        lock_response = _fields(response)
        key = _one(lock_response, 2, UInt8[])
        isempty(key) && throw(ProtocolMismatch("etcd Lock returned an empty lock key"))
        range_timeout = _acquisition_rpc_timeout(provider, wait_deadline_ns)
        verified = _range(provider, key; deadline_s=range_timeout, wait_deadline=true)
        create_revision = _verified_create_revision(verified, key, id)
        lease.state == :lost && throw(AcquisitionUnknown("etcd keepalive was lost before ownership verification"))
        now = time_ns()
        now < wait_deadline_ns || throw(_EtcdWaitDeadline())
        now < lease.deadline_ns || throw(AcquisitionUnknown("etcd ownership verification exceeded the conservative lease budget"))
        lease.key = key
        lease.create_revision = create_revision
        lease.state = :held
        if !auto_renew
            _stop_keepalive!(lease)
            lease.state == :stopping && (lease.state = :held)
        end
        time_ns() < wait_deadline_ns || throw(_EtcdWaitDeadline())
        time_ns() < lease.deadline_ns ||
            throw(AcquisitionUnknown("etcd lease validity elapsed during acquisition cleanup"))
        return lease
    catch err
        stop_error = try _stop_keepalive!(lease); nothing catch caught; caught end
        cleanup_error = try _revoke(provider, id); nothing catch caught; caught end
        cleaned = cleanup_error === nothing
        lease.state = cleaned ? :released : :released_unknown
        cancellation = err isa InterruptException ? err :
                       stop_error isa InterruptException ? stop_error :
                       cleanup_error isa InterruptException ? cleanup_error : nothing
        cancellation === nothing || throw(OperationCancelled(
            :acquire, cleaned ? :not_acquired : :unknown, cancellation))
        err isa _EtcdWaitDeadline && cleaned && throw(WaitTimeout())
        cleaned || throw(AcquisitionUnknown("etcd acquisition failed and lease cleanup was not acknowledged"))
        rethrow()
    end
end

"""etcd native Lock is a queued blocking RPC; nonblocking acquisition is unsupported."""
try_acquire(::EtcdHandle; kwargs...) = throw(UnsupportedCapability(
    "etcd native Lock has no nonblocking try-acquire operation"))

function renew!(lease::EtcdLease)
    lease.state == :held && lease.renewal || return false
    time_ns() < lease.deadline_ns || (lease.state = :lost; return false)
    # Automatic keepalive is maintained by the bidi stream. This reports whether
    # the local conservative budget still permits relying on the acquisition.
    lease.state == :held && time_ns() < lease.deadline_ns
end

fence(lease::EtcdLease) = (epoch=lease.epoch, revision=lease.create_revision)
release!(lease::EtcdLease) = _lease_cleanup!(lease; unlock=true)

function with_lease(f::Function, handle::EtcdHandle; on_release=nothing, kwargs...)
    lease = acquire(handle; kwargs...)
    result = try f(lease) catch
        try _release_and_observe!(lease, on_release; suppress_observer_errors=true) catch end
        rethrow()
    end
    outcome = _release_and_observe!(lease, on_release; suppress_observer_errors=false)
    _require_confirmed_scope_release(outcome)
    result
end

end
