require "spec"
require "time"
require "../src/dilock"
require "../src/dilock/etcd"

private class FakeScopeLease
  property outcome : Dilock::ScopeCleanupOutcome
  property release_error : Exception?
  property close_error : Exception?
  getter release_count = 0
  getter close_count = 0

  def initialize(@outcome : Dilock::ScopeCleanupOutcome,
                 @release_error : Exception? = nil, @close_error : Exception? = nil); end

  def release : Dilock::ScopeCleanupOutcome
    @release_count += 1
    raise @release_error.not_nil! if @release_error
    @outcome
  end

  def close : Nil
    @close_count += 1
    raise @close_error.not_nil! if @close_error
  end
end

describe Dilock::RedisProvider do
  it "matches the shared identity vector" do
    Dilock.identity("default", "camera/123/reindex").should eq("e0cdd484a99b6aee4162335939d2a2d128a490c097069fd85b2fc1e540a940b6")
  end

  it "rejects invalid namespace and resource values before network I/O" do
    expect_raises(ArgumentError) { Dilock.identity("Bad", "resource") }
    expect_raises(ArgumentError) { Dilock.identity("default", "") }
    expect_raises(ArgumentError) { Dilock.identity("default", "x\0y") }
    expect_raises(ArgumentError) { Dilock.identity("default", String.new(Bytes[0xff])) }
  end

  it "rejects invalid lease durations" do
    expect_raises(ArgumentError) { Dilock::Options.new(0) }
    expect_raises(ArgumentError) { Dilock::Options.new(86_400_001) }
  end

  it "rejects close while an acquisition owns an in-flight provider reservation" do
    lifecycle = Dilock::ProviderLifecycle.new
    lifecycle.with_acquisition do
      expect_raises(Dilock::ProviderBusy) { lifecycle.close! }
    end

    # The acquisition guard is released even after the block exits, then close
    # is idempotent and future acquisition reservations are rejected.
    lifecycle.close!.should be_true
    lifecycle.close!.should be_false
    expect_raises(Dilock::Error) { lifecycle.with_acquisition { :unreachable } }
  end

  it "requires namespace metadata to be an object with an integer protocol major" do
    metadata = {
      "protocol_major"     => 1,
      "identity_scheme"    => "sha256-framed-utf8-v1",
      "profile"            => "redis-single-authority-v1",
      "coordination_epoch" => "test-epoch",
      "fixed_lease_ms"     => nil,
    }.to_json
    Dilock::Internal.validate_redis_metadata!(metadata, "test-epoch")

    fractional_version = metadata.sub("\"protocol_major\":1", "\"protocol_major\":1.0")
    fractional_version.should_not eq(metadata)
    expect_raises(Dilock::ProtocolMismatch) do
      Dilock::Internal.validate_redis_metadata!(fractional_version, "test-epoch")
    end
    expect_raises(Dilock::ProtocolMismatch) do
      Dilock::Internal.validate_redis_metadata!("[]", "test-epoch")
    end
  end

  it "requires a positive etcd acquisition wait budget" do
    Dilock::EtcdInternal.validate_positive_wait_ms(1_i64)
    expect_raises(ArgumentError) { Dilock::EtcdInternal.validate_positive_wait_ms(0_i64) }
    expect_raises(ArgumentError) { Dilock::EtcdInternal.validate_positive_wait_ms(-1_i64) }
  end

  it "requires a positive budget after applying the conservative TTL margin" do
    Dilock::Internal.remaining_lease_budget_ms(1_000, 989.0).should eq(1.0)
    Dilock::Internal.remaining_lease_budget_ms(1_000, 990.0).should eq(0.0)
    # The minimum margin equals this TTL, so even a zero-latency ACK has no
    # conservative budget left and must be rejected by the caller.
    Dilock::Internal.remaining_lease_budget_ms(10, 0.0).should eq(0.0)
  end

  it "measures acquisition latency from monotonic clock readings" do
    dispatched_at = Time.instant
    acknowledged_at = dispatched_at + 995.milliseconds

    elapsed_ms = Dilock::Internal.acquisition_elapsed_ms(dispatched_at, acknowledged_at)
    elapsed_ms.should eq(995.0)
    Dilock::Internal.remaining_lease_budget_ms(1_000, elapsed_ms).should be < 0.0
  end

  it "caches an unknown release acknowledgement instead of claiming not-owned" do
    release = Dilock::ReleaseOnce.new
    calls = 0

    first = release.run do
      calls += 1
      raise "connection lost after release dispatch"
    end
    second = release.run do
      calls += 1
      Dilock::ReleaseOutcome::AlreadyNotOwned
    end

    first.should eq(Dilock::ReleaseOutcome::Unknown)
    second.should eq(Dilock::ReleaseOutcome::Unknown)
    calls.should eq(1)
  end

  it "classifies SET NX misses with TYPE and preserves backend errors" do
    Dilock::Internal.classify_contention { "string" }
    Dilock::Internal.classify_contention { "none" }
    expect_raises(Dilock::ProtocolMismatch) do
      Dilock::Internal.classify_contention { "hash" }
    end
    expect_raises(Dilock::BackendUnavailable) do
      Dilock::Internal.classify_contention { raise "TYPE failed" }
    end
  end
end

describe Dilock::ScopeInternal do
  it "returns callback values only after exactly one acknowledged cleanup" do
    lease = FakeScopeLease.new(Dilock::ScopeCleanupOutcome::Released)
    result = Dilock::ScopeInternal.run(lease, "test", ->(value : FakeScopeLease) { value.release },
      ->(value : FakeScopeLease) { value.close }) { 42 }

    result.should eq(42)
    lease.release_count.should eq(1)
    lease.close_count.should eq(1)
  end

  it "surfaces lost and unknown outcomes after successful callback work" do
    lost = FakeScopeLease.new(Dilock::ScopeCleanupOutcome::AlreadyNotOwned)
    lost_error = expect_raises(Dilock::ScopeReleaseError) do
      Dilock::ScopeInternal.run(lost, "test", ->(value : FakeScopeLease) { value.release }) { :done }
    end
    lost_error.lost?.should be_true
    lost_error.outcome.should eq(Dilock::ScopeCleanupOutcome::AlreadyNotOwned)
    lost.release_count.should eq(1)

    unknown = FakeScopeLease.new(Dilock::ScopeCleanupOutcome::Unknown)
    unknown_error = expect_raises(Dilock::ScopeReleaseError) do
      Dilock::ScopeInternal.run(unknown, "test", ->(value : FakeScopeLease) { value.release }) { :done }
    end
    unknown_error.lost?.should be_false
    unknown_error.outcome.should eq(Dilock::ScopeCleanupOutcome::Unknown)
    unknown.release_count.should eq(1)
  end

  it "keeps the original callback exception when cleanup succeeds" do
    lease = FakeScopeLease.new(Dilock::ScopeCleanupOutcome::Released)
    work_error = Exception.new("work failed")
    caught = expect_raises(Exception) do
      Dilock::ScopeInternal.run(lease, "test", ->(value : FakeScopeLease) { value.release }) do
        raise work_error
      end
    end

    caught.should be(work_error)
    lease.release_count.should eq(1)
  end

  it "keeps callback failure primary while exposing failed cleanup" do
    lease = FakeScopeLease.new(Dilock::ScopeCleanupOutcome::Unknown)
    work_error = Exception.new("work failed")
    caught = expect_raises(Dilock::ScopeCallbackError) do
      Dilock::ScopeInternal.run(lease, "test", ->(value : FakeScopeLease) { value.release }) do
        raise work_error
      end
    end

    caught.callback_error.should be(work_error)
    caught.cause.should be(work_error)
    caught.release_error.outcome.should eq(Dilock::ScopeCleanupOutcome::Unknown)
    lease.release_count.should eq(1)
  end

  it "still disposes local state and retains both cleanup failures" do
    release_error = Exception.new("release transport failed")
    close_error = Exception.new("local close failed")
    lease = FakeScopeLease.new(Dilock::ScopeCleanupOutcome::Unknown, release_error, close_error)
    caught = expect_raises(Dilock::ScopeReleaseError) do
      Dilock::ScopeInternal.run(lease, "test", ->(value : FakeScopeLease) { value.release },
        ->(value : FakeScopeLease) { value.close }) { :done }
    end

    caught.outcome.should eq(Dilock::ScopeCleanupOutcome::Unknown)
    caught.cleanup_errors.should eq([release_error, close_error])
    lease.release_count.should eq(1)
    lease.close_count.should eq(1)
  end
end

if endpoint = ENV["DILOCK_TEST_REDIS_URL"]?
  describe "Dilock Redis integration" do
    it "keeps ownership across providers, expiry, and a delayed stale release" do
      namespace = "it#{Random::Secure.hex(6)}"
      epoch = "test-#{Random::Secure.hex(8)}"
      metadata_key = "dilock:namespace:#{namespace}:meta"
      admin = Redis::Client.new(URI.parse(endpoint))
      first_provider : Dilock::RedisProvider? = nil
      second_provider : Dilock::RedisProvider? = nil
      first_lease : Dilock::Lease? = nil
      handoff : Dilock::Lease? = nil
      stale : Dilock::Lease? = nil
      successor : Dilock::Lease? = nil

      begin
        metadata = {
          "protocol_major"     => 1,
          "identity_scheme"    => "sha256-framed-utf8-v1",
          "profile"            => "redis-single-authority-v1",
          "coordination_epoch" => epoch,
          "fixed_lease_ms"     => nil,
        }.to_json
        admin.set(metadata_key, metadata)

        first_provider = Dilock::RedisProvider.new(endpoint, namespace, epoch)
        second_provider = Dilock::RedisProvider.new(endpoint, namespace, epoch)
        first_handle = first_provider.lock("redis-integration-resource")
        second_handle = second_provider.lock("redis-integration-resource")

        default_scoped_result = first_handle.with_lease do |_lease|
          "default-scope"
        end
        default_scoped_result.should eq("default-scope")

        configured_scoped_result = first_handle.with_lease(Dilock::Options.new(ttl_ms: 2_000)) do |_lease|
          "configured-scope"
        end
        configured_scoped_result.should eq("configured-scope")

        separate_resource = first_provider.lock("redis-integration-second-resource")
        separate_resource.with_lease { |_lease| "same-provider" }.should eq("same-provider")

        first_lease = first_handle.try_acquire(Dilock::Options.new(2_000))
        first_lease.should_not be_nil
        second_handle.try_acquire(Dilock::Options.new(2_000)).should be_nil
        first_lease.not_nil!.release.should eq(Dilock::ReleaseOutcome::Released)

        handoff = second_handle.try_acquire(Dilock::Options.new(2_000))
        handoff.should_not be_nil
        handoff.not_nil!.release.should eq(Dilock::ReleaseOutcome::Released)

        stale = first_handle.try_acquire(Dilock::Options.new(100))
        stale.should_not be_nil
        sleep 150.milliseconds
        successor = second_handle.try_acquire(Dilock::Options.new(2_000))
        successor.should_not be_nil
        stale.not_nil!.release.should eq(Dilock::ReleaseOutcome::AlreadyNotOwned)
        first_handle.try_acquire(Dilock::Options.new(2_000)).should be_nil
        successor.not_nil!.release.should eq(Dilock::ReleaseOutcome::Released)
      ensure
        first_lease.try(&.release)
        handoff.try(&.release)
        stale.try(&.release)
        successor.try(&.release)
        first_provider.try(&.close)
        second_provider.try(&.close)
        admin.del(metadata_key)
        admin.close
      end
    end
  end
end
