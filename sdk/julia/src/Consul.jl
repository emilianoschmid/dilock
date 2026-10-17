const CONSUL_MAX_MS = 86_400_000
const CONSUL_MIN_TTL_MS = 10_000

struct ConsulProvider
    endpoint::String
    namespace::String
    epoch::String
    token::Union{Nothing,String}
    rpc_timeout::Float64
    cleanup_timeout::Float64
    poll_ms::Int
    wait_ms::Int
    lease_ms::Int
    connected::Base.RefValue{Bool}
    closed::Base.RefValue{Bool}
    leases::Vector{Any}
end

struct ConsulHandle
    provider::ConsulProvider
    key::String
end

mutable struct ConsulLease
    provider::ConsulProvider
    key::String
    session::String
    epoch::String
    lock_index::String
    ttl_ms::Int
    deadline_ns::UInt64
    state::Symbol
    gate::ReentrantLock
end

function ConsulProvider(; endpoint="http://127.0.0.1:8500", namespace="default",
                        epoch, token=nothing, rpc_timeout=2.0,
                        cleanup_timeout=2.0, poll_ms=50, wait_ms=5_000,
                        lease_ms=30_000)
    occursin(r"\Ahttps?://(?:\[[0-9A-Fa-f:]+\]|[A-Za-z0-9.-]+)(?::[0-9]{1,5})?/?\z", endpoint) ||
        throw(ArgumentError("Consul endpoint must be an HTTP(S) authority without credentials or path"))
    resource_id(namespace, "validation")
    isvalid(epoch) && 1 <= ncodeunits(epoch) <= 1024 && !occursin('\0', epoch) ||
        throw(ArgumentError("coordination epoch must be 1..1024 UTF-8 bytes and contain no NUL"))
    token === nothing || occursin(r"\A[!-~]+\z", token) || throw(ArgumentError("ACL token must be printable non-space ASCII"))
    isfinite(rpc_timeout) && 0 < rpc_timeout <= 86_400 || throw(ArgumentError("rpc_timeout must be in (0,86400] seconds"))
    isfinite(cleanup_timeout) && 0 < cleanup_timeout <= 86_400 || throw(ArgumentError("cleanup_timeout must be in (0,86400] seconds"))
    poll_ms isa Integer && 1 <= poll_ms <= CONSUL_MAX_MS || throw(ArgumentError("poll_ms must be an integer in 1..86400000"))
    wait_ms isa Integer && 1 <= wait_ms <= CONSUL_MAX_MS || throw(ArgumentError("wait_ms must be an integer in 1..86400000"))
    lease_ms isa Integer && CONSUL_MIN_TTL_MS <= lease_ms <= CONSUL_MAX_MS || throw(ArgumentError("Consul lease must be an integer in 10000..86400000 ms"))
    ConsulProvider(chop(endpoint; tail=endswith(endpoint, "/") ? 1 : 0), String(namespace), String(epoch),
                   token === nothing ? nothing : String(token), Float64(rpc_timeout), Float64(cleanup_timeout),
                   Int(poll_ms), Int(wait_ms), Int(lease_ms), Ref(false), Ref(false), Any[])
end

_consul_meta_key(provider::ConsulProvider) = "dilock/namespaces/$(provider.namespace)/meta"
_consul_expected(provider::ConsulProvider) = Dict(
    "protocol_major" => 1, "identity_scheme" => "sha256-framed-utf8-v1",
    "profile" => "consul-kv-session-v1", "coordination_epoch" => provider.epoch,
    "fixed_lease_ms" => nothing)

function _consul_request(provider::ConsulProvider, method::String, path::String;
                         body=nothing, timeout=provider.rpc_timeout, mutation=false)
    headers = Pair{String,String}["Accept" => "application/json"]
    body === nothing || push!(headers, "Content-Type" => "application/json")
    provider.token === nothing || push!(headers, "X-Consul-Token" => provider.token)
    timeout_seconds = floor(Int, timeout)
    timeout_seconds >= 1 || throw(BackendUnavailable("Consul timeout is below HTTP.jl's one-second resolution"))
    response = try
        HTTP.request(method, provider.endpoint * path; headers, body=body === nothing ? "" : body,
                     retry=false, redirect=false, status_exception=false, require_ssl_verification=true,
                     connect_timeout=timeout_seconds, readtimeout=timeout_seconds)
    catch err
        mutation && throw(AcquisitionUnknown("Consul mutation response is unknown: $(typeof(err))"))
        throw(BackendUnavailable("Consul request failed: $(typeof(err))"))
    end
    response.status == 404 && method == "GET" && return nothing
    200 <= response.status < 300 && return String(response.body)
    error_type = mutation && !(400 <= response.status < 500) ? AcquisitionUnknown : BackendUnavailable
    throw(error_type("Consul HTTP $(response.status) for $method $path"))
end

function _consul_read(provider::ConsulProvider, key::String; timeout=provider.rpc_timeout)
    raw = _consul_request(provider, "GET", "/v1/kv/$key?consistent"; timeout)
    raw === nothing && return nothing
    rows = try JSON3.read(raw) catch; throw(ProtocolMismatch("Consul KV response is malformed JSON")) end
    rows isa AbstractVector && length(rows) == 1 || throw(ProtocolMismatch("Consul KV response must contain one row"))
    row = only(rows)
    hasproperty(row, :Key) && row.Key == key && hasproperty(row, :LockIndex) ||
        throw(ProtocolMismatch("Consul KV row omitted its key or LockIndex"))
    index = string(row.LockIndex)
    occursin(r"\A[0-9]+\z", index) || throw(ProtocolMismatch("Consul LockIndex is malformed"))
    (key=String(row.Key), session=hasproperty(row, :Session) && row.Session !== nothing ? String(row.Session) : nothing,
     lock_index=index, value=hasproperty(row, :Value) && row.Value !== nothing ? String(row.Value) : nothing)
end

function _consul_check_metadata(provider::ConsulProvider)
    row = _consul_read(provider, _consul_meta_key(provider))
    row !== nothing && row.value !== nothing || throw(ProtocolMismatch("Consul namespace metadata is missing"))
    actual = try JSON3.read(String(base64decode(row.value))) catch; throw(ProtocolMismatch("Consul metadata is malformed")) end
    expected = _consul_expected(provider)
    Set(String.(collect(keys(actual)))) == Set(keys(expected)) || throw(ProtocolMismatch("Consul namespace metadata is incompatible"))
    for (key, value) in expected
        hasproperty(actual, Symbol(key)) || throw(ProtocolMismatch("Consul metadata field is missing"))
        found = getproperty(actual, Symbol(key))
        typeof(found) === typeof(value) && isequal(found, value) || throw(ProtocolMismatch("Consul namespace metadata is incompatible"))
    end
    true
end

function provision_consul_namespace(provider::ConsulProvider)
    body = JSON3.write(_consul_expected(provider))
    reply = _consul_request(provider, "PUT", "/v1/kv/$(_consul_meta_key(provider))?cas=0"; body, mutation=true)
    reply == "true" && return true
    _consul_check_metadata(provider)
end

function connect_consul(provider::ConsulProvider)
    provider.closed[] && throw(ArgumentError("provider is closed"))
    _consul_check_metadata(provider)
    provider.connected[] = true
    provider
end

function lock(provider::ConsulProvider, resource::AbstractString)
    provider.closed[] && throw(ArgumentError("provider is closed"))
    provider.connected[] || throw(ProtocolMismatch("connect_consul must validate provisioned metadata first"))
    ConsulHandle(provider, "dilock/locks/$(resource_id(provider.namespace, resource))")
end

_consul_bool(raw, value) = raw !== nothing && strip(raw) == value
_consul_effective_ttl(ms) = cld(ms, 1000) * 1000
_consul_margin(ms) = max(100, div(ms, 100))
_consul_deadline(sent, ttl) = sent + UInt64((ttl - _consul_margin(ttl)) * 1_000_000)
_consul_remaining(deadline::UInt64) = begin
    now = time_ns()
    deadline > now ? Float64(deadline - now) / 1e9 : 0.0
end

function _consul_session(provider::ConsulProvider, ttl::Int)
    sent = time_ns()
    body = JSON3.write(Dict("Name" => "dilock-attempt", "TTL" => "$(div(ttl, 1000))s",
        "Behavior" => "release", "LockDelay" => "0s", "Checks" => Any[]))
    raw = _consul_request(provider, "PUT", "/v1/session/create"; body, mutation=true)
    response = try JSON3.read(raw) catch; throw(AcquisitionUnknown("Consul session creation response is malformed")) end
    hasproperty(response, :ID) && !isempty(response.ID) && occursin(r"\A[A-Za-z0-9-]+\z", response.ID) ||
        throw(AcquisitionUnknown("Consul session creation returned an invalid ID"))
    String(response.ID), _consul_deadline(sent, ttl)
end

function _consul_renew_session(provider::ConsulProvider, session::String, ttl::Int)
    sent = time_ns()
    raw = _consul_request(provider, "PUT", "/v1/session/renew/$session"; mutation=true)
    rows = try JSON3.read(raw) catch; throw(LeaseLost("Consul session renewal response is malformed")) end
    rows isa AbstractVector && !isempty(rows) && hasproperty(first(rows), :ID) && first(rows).ID == session ||
        throw(LeaseLost("Consul session renewal was not confirmed"))
    _consul_deadline(sent, ttl)
end

function _consul_destroy(provider::ConsulProvider, session::String)
    try
        raw = _consul_request(provider, "PUT", "/v1/session/destroy/$session"; timeout=provider.cleanup_timeout, mutation=true)
        _consul_bool(raw, "true")
    catch
        false
    end
end

_consul_release_after_destroy(outcome::Symbol, destroyed::Bool) = destroyed ? outcome : :unknown

function acquire(handle::ConsulHandle; wait_ms=handle.provider.wait_ms, lease_ms=handle.provider.lease_ms)
    provider = handle.provider
    provider.closed[] && throw(ArgumentError("provider is closed"))
    wait_ms isa Integer && 1 <= wait_ms <= CONSUL_MAX_MS || throw(ArgumentError("wait_ms must be an integer in 1..86400000"))
    lease_ms isa Integer && CONSUL_MIN_TTL_MS <= lease_ms <= CONSUL_MAX_MS || throw(ArgumentError("lease_ms must be an integer in 10000..86400000"))
    ttl = _consul_effective_ttl(Int(lease_ms))
    provider.rpc_timeout * 1000 + _consul_margin(ttl) < ttl || throw(ArgumentError("RPC timeout and safety margin exceed the Consul TTL"))
    deadline = time_ns() + UInt64(wait_ms * 1_000_000)
    session, session_deadline = _consul_session(provider, ttl)
    next_renew = time_ns() + UInt64(div(ttl, 3) * 1_000_000)
    try
        while time_ns() < deadline
            remaining = max(0.001, _consul_remaining(deadline))
            raw = _consul_request(provider, "PUT", "/v1/kv/$(handle.key)?acquire=$session";
                                  body="", timeout=min(provider.rpc_timeout, remaining), mutation=true)
            if _consul_bool(raw, "true")
                row = _consul_read(provider, handle.key; timeout=min(provider.rpc_timeout, max(0.001, _consul_remaining(deadline))))
                row !== nothing && row.session == session && parse(BigInt, row.lock_index) > 0 ||
                    throw(AcquisitionUnknown("Consul acquire ownership could not be confirmed"))
                time_ns() < deadline && time_ns() < session_deadline ||
                    throw(AcquisitionUnknown("Consul acquire completed after its wait or lease deadline"))
                lease = ConsulLease(provider, handle.key, session, provider.epoch, row.lock_index,
                                    ttl, session_deadline, :held, ReentrantLock())
                push!(provider.leases, lease)
                return lease
            end
            _consul_bool(raw, "false") || throw(AcquisitionUnknown("Consul acquire acknowledgement is malformed"))
            if time_ns() >= next_renew
                session_deadline = _consul_renew_session(provider, session, ttl)
                time_ns() < session_deadline || throw(AcquisitionUnknown("Consul waiter renewal returned after its conservative deadline"))
                next_renew = time_ns() + UInt64(div(ttl, 3) * 1_000_000)
            end
            sleep(min(provider.poll_ms / 1000, max(0, _consul_remaining(deadline))))
        end
        throw(WaitTimeout())
    catch err
        cleaned = _consul_destroy(provider, session)
        err isa WaitTimeout && !cleaned && throw(AcquisitionUnknown("Consul wait expired; attempt cleanup is unconfirmed"))
        rethrow()
    end
end

try_acquire(handle::ConsulHandle; lease_ms=handle.provider.lease_ms) =
    try acquire(handle; wait_ms=1, lease_ms) catch err; err isa WaitTimeout ? nothing : rethrow() end

fence(lease::ConsulLease) = (epoch=lease.epoch, lock_index=lease.lock_index)
effective_ttl_ms(lease::ConsulLease) = lease.ttl_ms
valid(lease::ConsulLease) = lease.state === :held && time_ns() < lease.deadline_ns

function renew!(lease::ConsulLease)
    Base.lock(lease.gate) do
        valid(lease) || (lease.state = :lost; throw(LeaseLost("Consul lease is no longer locally valid")))
        try
            deadline = _consul_renew_session(lease.provider, lease.session, lease.ttl_ms)
            row = _consul_read(lease.provider, lease.key)
            if time_ns() >= lease.deadline_ns || row === nothing || row.session != lease.session || row.lock_index != lease.lock_index
                lease.state = :lost
                throw(LeaseLost("Consul session no longer owns the same LockIndex"))
            end
            lease.deadline_ns = deadline
            true
        catch
            lease.state = :lost
            rethrow()
        end
    end
end

function release!(lease::ConsulLease)
    Base.lock(lease.gate) do
        lease.state === :released && return :already_not_owned
        lease.state === :released_unknown && return :unknown
        outcome = try
            raw = _consul_request(lease.provider, "PUT", "/v1/kv/$(lease.key)?release=$(lease.session)";
                                  body="", timeout=lease.provider.cleanup_timeout, mutation=true)
            if _consul_bool(raw, "true")
                :released
            elseif _consul_bool(raw, "false")
                row = _consul_read(lease.provider, lease.key; timeout=lease.provider.cleanup_timeout)
                row !== nothing && row.session == lease.session ? :unknown : :already_not_owned
            else
                :unknown
            end
        catch
            :unknown
        end
        destroyed = _consul_destroy(lease.provider, lease.session)
        outcome = _consul_release_after_destroy(outcome, destroyed)
        lease.state = outcome === :unknown ? :released_unknown : :released
        outcome
    end
end

function with_lease(f::Function, handle::ConsulHandle; on_release=nothing, kwargs...)
    lease = acquire(handle; kwargs...)
    result = nothing
    try
        result = f(lease)
    catch
        try release!(lease) catch end
        rethrow()
    end
    outcome = release!(lease)
    on_release === nothing || on_release(outcome)
    outcome === :released || throw(outcome === :already_not_owned ? LeaseLost("Consul lease was lost") : ReleaseUnknown("Consul release acknowledgement is unknown"))
    result
end

function Base.close(provider::ConsulProvider)
    provider.closed[] = true
    foreach(release!, provider.leases)
    empty!(provider.leases)
    nothing
end
