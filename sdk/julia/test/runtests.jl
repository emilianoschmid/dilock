using Test, JSON3, Dilock, Redis, Random

struct MissingEtcdLease <: Exception
    grpc_status::Int
end

root = normpath(joinpath(@__DIR__, "..", "..", ".."))
vectors = JSON3.read(read(joinpath(root, "spec", "vectors", "identity.json"), String))
@testset "draft 1 identity vectors" begin
    for v in vectors.valid
    @test Dilock.resource_id(v.namespace, v.resource) == v.sha256
    end
    @test_throws ArgumentError Dilock.resource_id("Bad", "resource")
    @test_throws ArgumentError Dilock.resource_id("default\n", "resource")
    @test_throws ArgumentError Dilock.resource_id("default", "")
    @test_throws ArgumentError Dilock.resource_id("default", String(UInt8[0xff]))
end

@testset "Consul profile contract" begin
    provider = Dilock.ConsulProvider(epoch="consul-test", namespace="test")
    expected = Dilock._consul_expected(provider)
    @test expected["profile"] == "consul-kv-session-v1"
    @test expected["coordination_epoch"] == "consul-test"
    @test Dilock._consul_effective_ttl(10_001) == 11_000
    @test Dilock._consul_margin(10_000) == 100
    @test Dilock.resource_id("test", "resource") == Dilock.resource_id(provider.namespace, "resource")
    @test Dilock._consul_release_after_destroy(:released, false) == :unknown
    @test Dilock._consul_release_after_destroy(:already_not_owned, true) == :already_not_owned
    @test_throws ArgumentError Dilock.ConsulProvider(epoch="bad", endpoint="http://user:pass@localhost:8500")
    @test_throws ArgumentError Dilock.ConsulProvider(epoch="bad", endpoint="http://localhost:8500/v1")
    @test_throws ArgumentError Dilock.ConsulProvider(epoch="")
end

@testset "Redis scripts match the shared contract" begin
    @test read(joinpath(@__DIR__, "..", "src", "redis-release.lua"), String) ==
          read(joinpath(root, "spec", "backends", "redis-release.lua"), String)
    @test read(joinpath(@__DIR__, "..", "src", "redis-renew.lua"), String) ==
          read(joinpath(root, "spec", "backends", "redis-renew.lua"), String)
end

if haskey(ENV, "DILOCK_TEST_CONSUL_ENDPOINT")
    @testset "Consul session lifecycle" begin
        provider = Dilock.ConsulProvider(
            endpoint=ENV["DILOCK_TEST_CONSUL_ENDPOINT"],
            namespace=get(ENV, "DILOCK_TEST_CONSUL_NAMESPACE", "dilock-julia-live"),
            epoch=get(ENV, "DILOCK_TEST_CONSUL_EPOCH", "julia-live-test"),
            token=get(ENV, "DILOCK_TEST_CONSUL_TOKEN", nothing))
        Dilock.provision_consul_namespace(provider)
        Dilock.connect_consul(provider)
        resource = Dilock.lock(provider, "lifecycle")
        lease = Dilock.acquire(resource; wait_ms=10_000, lease_ms=30_000)
        @test Dilock.fence(lease).epoch == provider.epoch
        @test parse(BigInt, Dilock.fence(lease).lock_index) > 0
        @test Dilock.renew!(lease)
        @test Dilock.release!(lease) == :released
        Base.close(provider)
    end
end

@testset "renewal validates lease duration before backend access" begin
    lease = Dilock.Lease(nothing, "key", "owner", 30_000, typemax(UInt64), :held)
    @test_throws ArgumentError Dilock.renew!(lease; lease_ms=99)
    @test_throws ArgumentError Dilock.renew!(lease; lease_ms=86_400_001)
    @test lease.state == :held
end

mutable struct FakeRedis
    store::Dict{String,String}
    force_contention::Bool
    type_override::Union{Nothing,String}
    type_error::Bool
    type_exception::Union{Nothing,Exception}
    release_error::Bool
    set_exception::Union{Nothing,Exception}
    release_exception::Union{Nothing,Exception}
    release_calls::Int
    renew_calls::Int
end

@testset "etcd v3 protobuf wire encoding and parsing" begin
    # LeaseGrantRequest.ttl is field 1; LockRequest.name and .lease are fields 1/2.
    @test Dilock.Etcd._vfield(1, 30) == UInt8[0x08, 0x1e]
    encoded = vcat(Dilock.Etcd._bfield(1, "/dilock/locks/abc"), Dilock.Etcd._vfield(2, 7))
    fields = Dilock.Etcd._fields(encoded)
    @test Dilock.Etcd._string(Dilock.Etcd._one(fields, 1)) == "/dilock/locks/abc"
    @test Dilock.Etcd._signed(Dilock.Etcd._one(fields, 2)) == 7
    # Unknown fixed-width and length-delimited fields are skipped safely.
    future = vcat(UInt8[0x19], zeros(UInt8, 8), UInt8[0x22, 0x02, 0xaa, 0xbb], encoded)
    @test Dilock.Etcd._signed(Dilock.Etcd._one(Dilock.Etcd._fields(future), 2)) == 7
    @test_throws Dilock.ProtocolMismatch Dilock.Etcd._fields(UInt8[0x0a, 0x05, 0x01])
    # uint64 varints may use ten bytes, but only the low bit of byte ten is
    # representable. Never truncate malformed lease IDs or fence revisions.
    max_uint64 = vcat(fill(UInt8(0xff), 9), UInt8[0x01])
    @test Dilock.Etcd._read_varint(max_uint64, 1) == (typemax(UInt64), 11)
    @test_throws Dilock.ProtocolMismatch Dilock.Etcd._read_varint(vcat(fill(UInt8(0x80), 9), UInt8[0x02]), 1)
    @test_throws Dilock.ProtocolMismatch Dilock.Etcd._read_varint(vcat(fill(UInt8(0x80), 10)), 1)
    @test_throws Dilock.ProtocolMismatch Dilock.Etcd._read_varint(UInt8[0x80], 1)

    # RangeResponse.kvs is repeated: no field means no matching key, rather
    # than a fabricated zero-valued KeyValue record.
    empty_range = Dilock.Etcd._fields(Dilock.Etcd._bfield(1, Dilock.Etcd._vfield(3, 9)))
    @test Dilock.Etcd._range_record(empty_range) === nothing
    key = UInt8[0x2f, 0x6b]
    kv = vcat(Dilock.Etcd._bfield(1, key), Dilock.Etcd._vfield(2, 42),
              Dilock.Etcd._vfield(6, 7))
    record = Dilock.Etcd._range_record(Dilock.Etcd._fields(Dilock.Etcd._bfield(2, kv)))
    @test record == (key=key, create_revision=Int64(42), lease=Int64(7))
end

@testset "etcd lock verification requires a usable fencing revision" begin
    key = UInt8[0x2f, 0x6c, 0x6f, 0x63, 0x6b]
    record = (key=key, create_revision=Int64(42), lease=Int64(7))
    @test Dilock.Etcd._verified_create_revision(record, key, 7) == 42
    @test_throws Dilock.ProtocolMismatch Dilock.Etcd._verified_create_revision(
        nothing, key, 7)
    @test_throws Dilock.ProtocolMismatch Dilock.Etcd._verified_create_revision(
        merge(record, (create_revision=Int64(0),)), key, 7)
    @test_throws Dilock.ProtocolMismatch Dilock.Etcd._verified_create_revision(
        merge(record, (create_revision=nothing,)), key, 7)
    @test_throws Dilock.ProtocolMismatch Dilock.Etcd._verified_create_revision(
        merge(record, (create_revision=Int64(-1),)), key, 7)
    @test_throws Dilock.ProtocolMismatch Dilock.Etcd._verified_create_revision(
        merge(record, (lease=Int64(8),)), key, 7)
end

@testset "etcd endpoint parsing is explicit" begin
    @test Dilock.Etcd._endpoint("127.0.0.1:2379") == ("127.0.0.1", 2379, false)
    @test Dilock.Etcd._endpoint("https://etcd.example:2379") == ("etcd.example", 2379, true)
    @test Dilock.Etcd._endpoint("[::1]:2379") == ("::1", 2379, false)
    @test Dilock.Etcd._endpoint("https://[2001:db8::7]:2379") == ("2001:db8::7", 2379, true)
    @test_throws ArgumentError Dilock.Etcd._endpoint("etcd.example")
    @test_throws ArgumentError Dilock.Etcd._endpoint("http://etcd.example:2379/prefix")
    @test_throws ArgumentError Dilock.Etcd._endpoint("[::1]")
end

@testset "etcd acquisition RPCs share the caller wait budget" begin
    provider = Dilock.Etcd.EtcdProvider("etcd.example", 2379, false, nothing,
                                        "default", "test-epoch", 2.0)
    finish = UInt64(10_000_000_000)
    @test Dilock.Etcd._acquisition_rpc_timeout(provider, finish;
        now=() -> UInt64(9_000_000_000)) == 1.0
    @test Dilock.Etcd._acquisition_rpc_timeout(provider, finish;
        now=() -> UInt64(7_000_000_000)) == 2.0
    @test_throws Dilock.Etcd._EtcdWaitDeadline Dilock.Etcd._acquisition_rpc_timeout(
        provider, finish; now=() -> finish)
end

@testset "etcd RPC interruption is not backend unavailability" begin
    interrupt = InterruptException()
    caught = try
        Dilock.Etcd._translate_rpc_error(interrupt, false)
        nothing
    catch err
        err
    end
    @test caught === interrupt
end

@testset "etcd keepalive shutdown attempts every step and preserves interruption" begin
    calls = Ref(0)
    interrupt = InterruptException()
    caught = Dilock.Etcd._attempt_shutdown_steps(
        () -> (calls[] += 1; throw(interrupt)),
        () -> (calls[] += 1; error("secondary shutdown failure")),
        () -> (calls[] += 1; nothing))
    @test caught === interrupt
    @test calls[] == 3
end

@testset "etcd nonblocking try-acquire has a typed unsupported error" begin
    provider = Dilock.Etcd.EtcdProvider("etcd.example", 2379, false, nothing,
                                        "default", "test-epoch", 2.0)
    handle = Dilock.Etcd.EtcdHandle(provider, UInt8[])
    err = try
        Dilock.Etcd.try_acquire(handle)
        nothing
    catch caught
        caught
    end
    @test err isa Dilock.UnsupportedCapability
    @test occursin("nonblocking try-acquire", sprint(showerror, err))
end
FakeRedis() = FakeRedis(Dict{String,String}(), false, nothing, false, nothing, false, nothing, nothing, 0, 0)
function Redis.get(redis::FakeRedis, key::AbstractString)
    Base.get(redis.store, String(key), nothing)
end
function Redis.execute_command(redis::FakeRedis, command)
    command[1] == "set" || error("unexpected command")
    redis.set_exception === nothing || throw(redis.set_exception)
    key, value = String(command[2]), String(command[3])
    (redis.force_contention || haskey(redis.store, key)) && return nothing
    redis.store[key] = value
    "OK"
end
function Redis.keytype(redis::FakeRedis, key::AbstractString)
    redis.type_exception === nothing || throw(redis.type_exception)
    redis.type_error && error("TYPE failed")
    redis.type_override !== nothing && return redis.type_override
    haskey(redis.store, String(key)) ? "string" : "none"
end
function Redis.evalscript(redis::FakeRedis, script, numkeys, key, args)
    if occursin("PEXPIRE", script)
        redis.renew_calls += 1
        owner, ttl = args
        1 <= ttl <= 86_400_000 || error("invalid test TTL")
        return Base.get(redis.store, String(key), nothing) == String(owner) ? 1 : 0
    end
    redis.release_calls += 1
    redis.release_exception === nothing || throw(redis.release_exception)
    redis.release_error && error("release response unavailable")
    owner = args isa AbstractVector ? only(args) : args
    Base.get(redis.store, String(key), nothing) == String(owner) || return 0
    delete!(redis.store, String(key))
    1
end

function fake_handle(redis::FakeRedis)
    redis.store["dilock:namespace:default:meta"] = """{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"redis-single-authority-v1","coordination_epoch":"test-epoch","fixed_lease_ms":null}"""
    provider = Dilock.Provider(redis, "default", "test-epoch")
    Dilock.lock(provider, "scoped-resource")
end

@testset "Redis renewal passes owner and TTL through Redis.jl EVAL" begin
    redis = FakeRedis()
    lease = Dilock.try_acquire(fake_handle(redis); lease_ms=1_000)
    @test Dilock.renew!(lease; lease_ms=2_000)
    @test lease.lease_ms == 2_000
    @test lease.state == :held
    @test redis.renew_calls == 1

    redis.store[lease.key] = "successor-owner"
    @test !Dilock.renew!(lease; lease_ms=3_000)
    @test lease.state == :lost
    @test redis.renew_calls == 2
end

@testset "acquire never returns a lease after its wait deadline" begin
    redis = FakeRedis()
    handle = fake_handle(redis)
    lease = Dilock.Lease(handle.provider, handle.key, "owner", 1_000, UInt64(100), :held)
    @test Dilock._accept_acquired(lease, UInt64(10);
        now=() -> UInt64(9), release=_ -> :released) === lease

    late_lease = Dilock.Lease(handle.provider, handle.key, "late-owner", 1_000, UInt64(100), :held)
    calls = Ref(0)
    late_timeout = try
        Dilock._accept_acquired(late_lease, UInt64(10);
            now=() -> UInt64(10), release=_ -> (calls[] += 1; :released))
        nothing
    catch err
        err
    end
    @test late_timeout isa Dilock.WaitTimeout
    @test calls[] == 1

    unknown_lease = Dilock.Lease(handle.provider, handle.key, "unknown-owner", 1_000, UInt64(100), :held)
    late_unknown = try
        Dilock._accept_acquired(unknown_lease, UInt64(10);
            now=() -> UInt64(11), release=_ -> :unknown)
        nothing
    catch err
        err
    end
    @test late_unknown isa Dilock.AcquisitionUnknown
end

@testset "unknown release remains terminal and is not retried" begin
    redis = FakeRedis()
    lease = Dilock.try_acquire(fake_handle(redis); lease_ms=1_000)
    @test lease isa Dilock.Lease

    redis.release_error = true
    @test Dilock.release!(lease) == :unknown
    calls_after_unknown = redis.release_calls
    @test Dilock.release!(lease) == :unknown
    @test redis.release_calls == calls_after_unknown

    # The etcd lease uses the same terminal classification without requiring
    # an RPC once an earlier Unlock/Revoke outcome is already unknown.
    provider = Dilock.EtcdProvider("127.0.0.1", 2379, false, nothing,
                                   "default", "test-epoch", 1.0)
    etcd_lease = Dilock.EtcdLease(provider, UInt8[], 1, "test-epoch", 0, 1,
                                  typemax(UInt64), :released_unknown,
                                  nothing, nothing, nothing, nothing, nothing, false)
    @test Dilock.release!(etcd_lease) == :unknown
end

@testset "backend interruption remains distinct and reports remote outcome" begin
    redis = FakeRedis()
    handle = fake_handle(redis)
    redis.set_exception = InterruptException()
    cancelled_acquire = try
        Dilock.try_acquire(handle; lease_ms=1_000)
        nothing
    catch err
        err
    end
    @test cancelled_acquire isa Dilock.OperationCancelled
    @test cancelled_acquire.operation == :acquire
    @test cancelled_acquire.outcome == :not_acquired
    @test occursin("remote outcome: not_acquired", sprint(showerror, cancelled_acquire))

    redis.set_exception = InterruptException()
    redis.release_exception = InterruptException()
    cancelled_cleanup = try
        Dilock.try_acquire(handle; lease_ms=1_000)
        nothing
    catch err
        err
    end
    @test cancelled_cleanup isa Dilock.OperationCancelled
    @test cancelled_cleanup.operation == :acquire
    @test cancelled_cleanup.outcome == :unknown

    redis.set_exception = nothing
    redis.release_exception = nothing
    redis.force_contention = true
    redis.type_exception = InterruptException()
    cancelled_type_check = try
        Dilock.try_acquire(handle; lease_ms=1_000)
        nothing
    catch err
        err
    end
    @test cancelled_type_check isa Dilock.OperationCancelled
    @test cancelled_type_check.operation == :acquire
    @test cancelled_type_check.outcome == :not_acquired

    interrupted_backoff = try
        Dilock._contention_backoff(0, _ -> throw(InterruptException()))
        nothing
    catch err
        err
    end
    @test interrupted_backoff isa Dilock.OperationCancelled
    @test interrupted_backoff.operation == :acquire
    @test interrupted_backoff.outcome == :not_acquired

    redis.force_contention = false
    redis.type_exception = nothing
    lease = Dilock.try_acquire(handle; lease_ms=1_000)
    redis.release_exception = InterruptException()
    cancelled_release = try
        Dilock.release!(lease)
        nothing
    catch err
        err
    end
    @test cancelled_release isa Dilock.OperationCancelled
    @test cancelled_release.operation == :release
    @test cancelled_release.outcome == :unknown
    @test lease.state == :released_unknown
end

@testset "with_lease reports interrupted cleanup without hiding body outcome" begin
    redis = FakeRedis()
    handle = fake_handle(redis)
    redis.release_exception = InterruptException()
    observed = Symbol[]
    cancelled = try
        Dilock.with_lease(handle; lease_ms=1_000,
                          on_release=outcome -> push!(observed, outcome)) do _
            :body_result
        end
        nothing
    catch err
        err
    end
    @test cancelled isa Dilock.OperationCancelled
    @test cancelled.operation == :release
    @test observed == [:unknown]

    redis = FakeRedis()
    handle = fake_handle(redis)
    redis.release_exception = InterruptException()
    body_error = ErrorException("body failed")
    observed = Symbol[]
    caught = try
        Dilock.with_lease(handle; lease_ms=1_000,
                          on_release=outcome -> push!(observed, outcome)) do _
            throw(body_error)
        end
        nothing
    catch err
        err
    end
    @test caught === body_error
    @test observed == [:unknown]
end

@testset "with_lease requires confirmed release on successful work" begin
    redis = FakeRedis()
    handle = fake_handle(redis)
    redis.release_error = true
    observed = Symbol[]
    @test_throws Dilock.ReleaseUnknown Dilock.with_lease(handle; lease_ms=1_000,
        on_release=outcome -> push!(observed, outcome)) do _
        :body_result
    end
    @test observed == [:unknown]

    redis = FakeRedis()
    handle = fake_handle(redis)
    observed = Symbol[]
    @test_throws Dilock.LeaseLost Dilock.with_lease(handle; lease_ms=1_000,
        on_release=outcome -> push!(observed, outcome)) do lease
        redis.store[lease.key] = "successor-owner"
        :body_result
    end
    @test observed == [:already_not_owned]

    @test Dilock._require_confirmed_scope_release(:released) == :released
    @test_throws Dilock.LeaseLost Dilock._require_confirmed_scope_release(:already_not_owned)
    @test_throws Dilock.ReleaseUnknown Dilock._require_confirmed_scope_release(:unknown)

    provider = Dilock.EtcdProvider("127.0.0.1", 2379, false, nothing,
                                   "default", "test-epoch", 1.0)
    etcd_lease = Dilock.EtcdLease(provider, UInt8[], 1, "test-epoch", 0, 1,
                                  typemax(UInt64), :released_unknown,
                                  nothing, nothing, nothing, nothing, nothing, false)
    observed = Symbol[]
    outcome = Dilock._release_and_observe!(etcd_lease,
        value -> push!(observed, value); suppress_observer_errors=false)
    @test outcome == :unknown
    @test observed == [:unknown]
    @test_throws Dilock.ReleaseUnknown Dilock._require_confirmed_scope_release(outcome)
end

@testset "etcd release preserves the strongest acknowledged cleanup outcome" begin
    provider = Dilock.EtcdProvider("127.0.0.1", 2379, false, nothing,
                                   "default", "test-epoch", 1.0)
    lease() = Dilock.EtcdLease(provider, UInt8[0x6b], 1, "test-epoch", 7, 1,
                               typemax(UInt64), :releasing,
                               nothing, nothing, nothing, nothing, nothing, false)

    unlock_calls = Ref(0)
    revoke_calls = Ref(0)
    # An exact-key Range preflight is summarized as :owned, :not_owned or
    # :unknown before these cleanup RPCs are attempted.
    acknowledged = lease()
    outcome = Dilock.Etcd._unlock_then_revoke!(acknowledged, :owned, true,
        () -> (unlock_calls[] += 1; error("Unlock acknowledgement lost")),
        () -> (revoke_calls[] += 1; nothing))
    @test outcome == :already_not_owned
    @test acknowledged.state == :released
    @test unlock_calls[] == 1
    @test revoke_calls[] == 1

    unlock_calls[] = 0
    revoke_calls[] = 0
    unlocked = lease()
    outcome = Dilock.Etcd._unlock_then_revoke!(unlocked, :owned, true,
        () -> (unlock_calls[] += 1; nothing),
        () -> (revoke_calls[] += 1; error("auxiliary revoke failed")))
    @test outcome == :unknown
    @test unlocked.state == :released_unknown
    @test unlock_calls[] == 1
    @test revoke_calls[] == 1

    # Only exact ownership plus positive acknowledgements from both cleanup
    # RPCs earns the Released outcome.
    unlock_calls[] = 0
    revoke_calls[] = 0
    fully_acknowledged = lease()
    outcome = Dilock.Etcd._unlock_then_revoke!(fully_acknowledged, :owned, true,
        () -> (unlock_calls[] += 1; nothing),
        () -> (revoke_calls[] += 1; nothing))
    @test outcome == :released
    @test fully_acknowledged.state == :released
    @test unlock_calls[] == 1
    @test revoke_calls[] == 1

    unlock_calls[] = 0
    revoke_calls[] = 0
    unknown = lease()
    outcome = Dilock.Etcd._unlock_then_revoke!(unknown, :unknown, true,
        () -> (unlock_calls[] += 1; error("Unlock acknowledgement lost")),
        () -> (revoke_calls[] += 1; error("revoke acknowledgement lost")))
    @test outcome == :unknown
    @test unknown.state == :released_unknown
    @test unlock_calls[] == 0
    @test revoke_calls[] == 1

    unlock_calls[] = 0
    revoke_calls[] = 0
    interrupted = lease()
    cancelled = try
        Dilock.Etcd._unlock_then_revoke!(interrupted, :owned, true,
            () -> (unlock_calls[] += 1; throw(InterruptException())),
            () -> (revoke_calls[] += 1; nothing))
        nothing
    catch err
        err
    end
    @test cancelled isa Dilock.OperationCancelled
    @test cancelled.operation == :release
    @test cancelled.outcome == :already_not_owned
    @test interrupted.state == :released
    @test unlock_calls[] == 1
    @test revoke_calls[] == 1

    # A Range that proves absence or a different lease forbids Unlock. A
    # successful revoke is cleanup, but it did not remove this key.
    unlock_calls[] = 0
    revoke_calls[] = 0
    absent = lease()
    outcome = Dilock.Etcd._unlock_then_revoke!(absent, :not_owned, true,
        () -> (unlock_calls[] += 1; nothing),
        () -> (revoke_calls[] += 1; nothing))
    @test outcome == :already_not_owned
    @test absent.state == :released
    @test unlock_calls[] == 0
    @test revoke_calls[] == 1

    # A preflight failure skips Unlock. A positive Revoke confirms terminal
    # cleanup but is classified as AlreadyNotOwned, not Released.
    unlock_calls[] = 0
    revoke_calls[] = 0
    preflight_unknown = lease()
    outcome = Dilock.Etcd._unlock_then_revoke!(preflight_unknown, :unknown, true,
        () -> (unlock_calls[] += 1; error("Unlock acknowledgement lost")),
        () -> (revoke_calls[] += 1; nothing);
        preflight_error=ErrorException("Range unavailable"))
    @test outcome == :already_not_owned
    @test preflight_unknown.state == :released
    @test unlock_calls[] == 0
    @test revoke_calls[] == 1

    # An Unlock ACK cannot override NOT_FOUND from Revoke: natural expiry may
    # have happened between the exact-key Range and the two cleanup RPCs.
    unlock_calls[] = 0
    revoke_calls[] = 0
    expired_after_unlock = lease()
    outcome = Dilock.Etcd._unlock_then_revoke!(expired_after_unlock, :owned, true,
        () -> (unlock_calls[] += 1; nothing),
        () -> (revoke_calls[] += 1; throw(MissingEtcdLease(5))))
    @test outcome == :already_not_owned
    @test expired_after_unlock.state == :released
    @test unlock_calls[] == 1
    @test revoke_calls[] == 1

    # etcd returns gRPC NOT_FOUND when a lease expired before LeaseRevoke.
    # This confirms that nothing remains, not that release removed the key.
    unlock_calls[] = 0
    revoke_calls[] = 0
    expired = lease()
    outcome = Dilock.Etcd._unlock_then_revoke!(expired, :owned, true,
        () -> (unlock_calls[] += 1; error("Unlock acknowledgement lost")),
        () -> (revoke_calls[] += 1; throw(MissingEtcdLease(5))))
    @test outcome == :already_not_owned
    @test expired.state == :released
    @test unlock_calls[] == 1
    @test revoke_calls[] == 1
end

@testset "SET NX contention validates Redis key type without mutation" begin
    redis = FakeRedis()
    redis.store["dilock:namespace:default:meta"] = """{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"redis-single-authority-v1","coordination_epoch":"test-epoch","fixed_lease_ms":null}"""
    provider = Dilock.Provider(redis, "default", "test-epoch")
    handle = Dilock.lock(provider, "resource")
    lease = Dilock.try_acquire(handle; lease_ms=1000)
    @test lease isa Dilock.Lease
    @test Dilock.try_acquire(handle; lease_ms=1000) === nothing

    redis.force_contention = true
    redis.type_override = "none"
    @test Dilock.try_acquire(handle; lease_ms=1000) === nothing
    redis.type_override = "hash"
    original = redis.store[handle.key]
    @test_throws Dilock.ProtocolMismatch Dilock.try_acquire(handle; lease_ms=1000)
    @test redis.store[handle.key] == original

    redis.type_override = nothing
    redis.type_error = true
    @test_throws Dilock.BackendUnavailable Dilock.try_acquire(handle; lease_ms=1000)
end

@testset "namespace metadata validates exact JSON value types" begin
    redis = FakeRedis()
    metadata_key = "dilock:namespace:default:meta"
    valid = """{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"redis-single-authority-v1","coordination_epoch":"test-epoch","fixed_lease_ms":null,"future_optional":"allowed"}"""
    redis.store[metadata_key] = valid
    @test Dilock.Provider(redis, "default", "test-epoch") isa Dilock.Provider
    @test_throws Dilock.ProtocolMismatch Dilock.Provider(redis, "default", "wrong-epoch")

    # In Julia, `true == 1`; accepting this JSON would treat a boolean protocol
    # version as the required integer. The contract compares parsed values, and
    # JSON3 normalizes the JSON number 1.0 to the integer value 1.
    invalid = replace(valid, "\"protocol_major\":1" => "\"protocol_major\":true")
    redis.store[metadata_key] = invalid
    @test_throws Dilock.ProtocolMismatch Dilock.Provider(redis, "default", "test-epoch")
    @test redis.store[metadata_key] == invalid
end

@testset "with_lease reports cleanup outcomes" begin
    redis = FakeRedis()
    handle = fake_handle(redis)
    observed = Symbol[]
    result = Dilock.with_lease(handle; lease_ms=1000,
                               on_release=outcome -> push!(observed, outcome)) do lease
        :body_result
    end
    @test result == :body_result
    @test observed == [:released]

    # Simulate the lease being replaced while the scoped body is running.
    observed = Symbol[]
    @test_throws Dilock.LeaseLost Dilock.with_lease(handle; lease_ms=1000,
        on_release=outcome -> push!(observed, outcome)) do lease
        redis.store[lease.key] = "successor-owner"
        nothing
    end
    @test observed == [:already_not_owned]
    @test redis.store[handle.key] == "successor-owner"

    # A lost release response is observable without changing body success.
    delete!(redis.store, handle.key)
    redis.release_error = true
    observed = Symbol[]
    @test_throws Dilock.ReleaseUnknown Dilock.with_lease(handle; lease_ms=1000,
        on_release=outcome -> push!(observed, outcome)) do lease
        :body_result
    end
    @test observed == [:unknown]
end

@testset "with_lease preserves body exceptions and interruption" begin
    redis = FakeRedis()
    handle = fake_handle(redis)
    body_error = ErrorException("body failed")
    observed = Symbol[]
    caught = try
        Dilock.with_lease(handle; lease_ms=1000,
                          on_release=outcome -> begin
                              push!(observed, outcome)
                              error("observer failed")
                          end) do lease
            throw(body_error)
        end
        nothing
    catch err
        err
    end
    @test caught === body_error
    @test observed == [:released]

    interrupt = InterruptException()
    caught = try
        Dilock.with_lease(handle; lease_ms=1000,
                          on_release=_ -> error("observer failed")) do lease
            throw(interrupt)
        end
        nothing
    catch err
        err
    end
    @test caught === interrupt

    caught = try
        Dilock.with_lease(handle; lease_ms=1000,
                          on_release=_ -> error("observer failed")) do lease
            :body_result
        end
        nothing
    catch err
        err
    end
    @test caught isa ErrorException
    @test caught.msg == "observer failed"
end

# Opt-in integration test against a caller-provided disposable Redis endpoint.
# Run with DILOCK_REDIS_SERVER=127.0.0.1:6379 julia --project=. -e 'using Pkg; Pkg.test()'.
if haskey(ENV, "DILOCK_REDIS_SERVER")
    @testset "real Redis lifecycle and stale-owner safety" begin
        endpoint = split(ENV["DILOCK_REDIS_SERVER"], ":"; limit=2)
        @test length(endpoint) == 2
        host, port = endpoint
        first_connection = Redis.RedisConnection(host=host, port=parse(Int, port))
        second_connection = Redis.RedisConnection(host=host, port=parse(Int, port))
        namespace = "it" * bytes2hex(rand(RandomDevice(), UInt8, 6))
        epoch = "it-" * bytes2hex(rand(RandomDevice(), UInt8, 8))
        metadata_key = "dilock:namespace:$(namespace):meta"
        metadata = JSON3.write((protocol_major=1,
                                identity_scheme="sha256-framed-utf8-v1",
                                profile="redis-single-authority-v1",
                                coordination_epoch=epoch,
                                fixed_lease_ms=nothing))
        key = nothing
        try
            Redis.set(first_connection, metadata_key, metadata)
            first_provider = Dilock.Provider(first_connection, namespace, epoch)
            second_provider = Dilock.Provider(second_connection, namespace, epoch)
            first_handle = Dilock.lock(first_provider, "resource")
            second_handle = Dilock.lock(second_provider, "resource")
            key = first_handle.key

            first = Dilock.try_acquire(first_handle; lease_ms=500)
            @test first isa Dilock.Lease
            @test Dilock.try_acquire(second_handle; lease_ms=500) === nothing
            @test Dilock.renew!(first; lease_ms=800)
            @test Dilock.release!(first) == :released

            outcome = Ref{Union{Nothing,Symbol}}(nothing)
            scoped_result = Dilock.with_lease(first_handle; lease_ms=1000,
                                               on_release=value -> (outcome[] = value)) do lease
                :scoped_result
            end
            @test scoped_result == :scoped_result
            @test outcome[] == :released
            @test Redis.get(second_connection, key) === nothing

            expiring = Dilock.try_acquire(first_handle; lease_ms=200)
            @test expiring isa Dilock.Lease
            sleep(0.3)
            successor = Dilock.try_acquire(second_handle; lease_ms=1000)
            @test successor isa Dilock.Lease
            @test Dilock.release!(expiring) == :already_not_owned
            @test Redis.get(second_connection, key) == successor.owner
            @test Dilock.release!(successor) == :released
            @test Redis.get(second_connection, key) === nothing
        finally
            key === nothing || Redis.del(first_connection, key)
            Redis.del(first_connection, metadata_key)
            Redis.disconnect(first_connection)
            Redis.disconnect(second_connection)
        end
    end
end

# Opt-in integration against a disposable single-member etcd endpoint with
# administrator-provisioned namespace metadata. Runtime code never creates it.
if haskey(ENV, "DILOCK_ETCD_SERVER")
    @testset "native etcd lease, queue and fence lifecycle" begin
        namespace = ENV["DILOCK_ETCD_NAMESPACE"]
        epoch = ENV["DILOCK_ETCD_EPOCH"]
        first_provider = Dilock.EtcdProvider(ENV["DILOCK_ETCD_SERVER"], namespace, epoch)
        second_provider = Dilock.EtcdProvider(ENV["DILOCK_ETCD_SERVER"], namespace, epoch)
        first_handle = Dilock.lock(first_provider, "julia-etcd-lifecycle")
        second_handle = Dilock.lock(second_provider, "julia-etcd-lifecycle")

        first = Dilock.acquire(first_handle; wait_ms=2_000, lease_ms=1_000)
        first_fence = Dilock.fence(first)
        @test first_fence.epoch == epoch
        @test first_fence.revision > 0
        @test first.lease_id > 0

        waiting = @async Dilock.acquire(second_handle; wait_ms=6_000, lease_ms=1_000)
        sleep(1.5) # the queued native Lock must remain alive beyond its initial TTL
        @test !istaskdone(waiting)
        @test Dilock.release!(first) == :released

        second = fetch(waiting)
        second_fence = Dilock.fence(second)
        @test second.lease_id != first.lease_id
        @test second_fence.revision > first_fence.revision
        @test Dilock.release!(second) == :released
        @test Dilock.release!(second) == :already_not_owned

        holder = Dilock.acquire(first_handle; wait_ms=2_000, lease_ms=1_000)
        timeout_result = @async try
            Dilock.acquire(second_handle; wait_ms=250, lease_ms=1_000)
        catch err
            err
        end
        @test fetch(timeout_result) isa Dilock.WaitTimeout
        @test Dilock.release!(holder) == :released
        after_timeout = Dilock.acquire(second_handle; wait_ms=2_000, lease_ms=1_000)
        @test Dilock.release!(after_timeout) == :released

        renewed = Dilock.acquire(first_handle; wait_ms=2_000, lease_ms=1_000, auto_renew=true)
        sleep(1.5)
        @test renewed.state == :held
        @test time_ns() < renewed.deadline_ns
        @test Dilock.release!(renewed) == :released

        # Confirm etcd's actual gRPC NOT_FOUND response after a lease expires
        # without keepalive; fake status-code tests cannot detect error
        # translation that discards the original gRPC status.
        expired_lease_id, expired_ttl, _ = Dilock.Etcd._grant(first_provider, 1)
        @test expired_lease_id > 0
        sleep(expired_ttl + 1.0)
        revoke_error = try
            Dilock.Etcd._revoke(first_provider, expired_lease_id)
            nothing
        catch err
            err
        end
        @test revoke_error isa Dilock.Etcd._EtcdLeaseNotFound
        @test Dilock.Etcd._lease_not_found(revoke_error)
    end
end
