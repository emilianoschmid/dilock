require "spec"
require "../src/dilock"
require "../src/dilock/etcd"

describe Dilock::EtcdProvider do
  it "serializes native lock creation with idempotent provider destruction" do
    lifecycle = Dilock::EtcdProviderLifecycle.new
    lifecycle.with_open_provider { :handle }.should eq(:handle)

    destroyed = 0
    lifecycle.close { destroyed += 1 }
    lifecycle.close { destroyed += 1 }
    destroyed.should eq(1)
    expect_raises(Dilock::Error, "provider is closed") do
      lifecycle.with_open_provider { :unreachable }
    end
  end

  it "validates local options before opening the native provider" do
    expect_raises(ArgumentError) do
      Dilock::EtcdProvider.new(coordination_epoch: "", endpoint: "127.0.0.1:2379")
    end
    expect_raises(ArgumentError) do
      Dilock::EtcdProvider.new(coordination_epoch: "epoch", endpoint: "", namespace: "default")
    end
    expect_raises(ArgumentError) do
      Dilock::EtcdProvider.new(coordination_epoch: "epoch", wait_ms: -1)
    end
    expect_raises(ArgumentError) do
      Dilock::EtcdProvider.new(coordination_epoch: "bad\0epoch")
    end
    expect_raises(ArgumentError) do
      Dilock::EtcdProvider.new(coordination_epoch: String.new(Bytes[0xff]))
    end
    expect_raises(ArgumentError) do
      Dilock::EtcdProvider.new(coordination_epoch: "epoch", endpoint: "etcd\0:2379")
    end
    expect_raises(ArgumentError) do
      Dilock::EtcdProvider.new(coordination_epoch: "epoch", tls_root_certificates: "root\0cert")
    end
    expect_raises(ArgumentError) do
      Dilock::EtcdProvider.new(coordination_epoch: "e" * 1_025)
    end
    Dilock::MAX_COORDINATION_EPOCH_BYTES.should eq(1_024)
  end

  it "validates per-acquisition waits and leases before crossing the C ABI" do
    Dilock::EtcdInternal.validate_positive_wait_ms(1_i64)
    Dilock::EtcdInternal.validate_positive_wait_ms(Dilock::MAX_ETCD_DURATION_MS)
    expect_raises(ArgumentError) { Dilock::EtcdInternal.validate_positive_wait_ms(0_i64) }
    expect_raises(ArgumentError) do
      Dilock::EtcdInternal.validate_positive_wait_ms(Dilock::MAX_ETCD_DURATION_MS + 1)
    end

    Dilock::EtcdInternal.validate_lease_ms(2_026_i64, 2_000_i64, 25_i64)
    expect_raises(ArgumentError) { Dilock::EtcdInternal.validate_lease_ms(2_025_i64, 2_000_i64, 26_i64) }
    expect_raises(ArgumentError) { Dilock::EtcdInternal.validate_lease_ms(0_i64, 2_000_i64, 25_i64) }
    expect_raises(ArgumentError) do
      Dilock::EtcdInternal.validate_lease_ms(2_025_i64, Int64::MAX, 25_i64)
    end
    expect_raises(ArgumentError) do
      Dilock::EtcdInternal.validate_lease_ms(Dilock::MAX_ETCD_DURATION_MS + 1, 1_i64, 0_i64)
    end

    # A null test handle makes this regression fail safely if try_acquire ever
    # reaches the C ABI before validating the lease duration locally.
    provider = Dilock::EtcdProvider.allocate
    handle = Dilock::EtcdHandle.new(provider, Pointer(Void).null,
      5_000_i64, 30_000_i64, 2_000_i64, 25_i64)
    expect_raises(ArgumentError) { handle.try_acquire(0_i64) }
    expect_raises(ArgumentError) { handle.try_acquire(2_025_i64) }
    expect_raises(ArgumentError) { handle.try_acquire(Dilock::MAX_ETCD_DURATION_MS + 1) }
  end

  it "rejects invalid timeout policy values before provider creation" do
    Dilock::EtcdInternal.validate_timeout_options(1_i64, 1_i64, 0_i64)
    expect_raises(ArgumentError) { Dilock::EtcdInternal.validate_timeout_options(0_i64, 1_i64, 0_i64) }
    expect_raises(ArgumentError) { Dilock::EtcdInternal.validate_timeout_options(1_i64, 0_i64, 0_i64) }
    expect_raises(ArgumentError) { Dilock::EtcdInternal.validate_timeout_options(1_i64, 1_i64, -1_i64) }
  end

  it "rejects a closed cancellation token before passing its native pointer" do
    Dilock::EtcdInternal.validate_cancellation_open(false).should be_nil
    expect_raises(Dilock::Error, "cancellation token is closed") do
      Dilock::EtcdInternal.validate_cancellation_open(true)
    end
  end
end

if endpoint = ENV["DILOCK_TEST_ETCD_ENDPOINT"]?
  namespace = ENV.fetch("DILOCK_TEST_ETCD_NAMESPACE")
  epoch = ENV.fetch("DILOCK_TEST_ETCD_EPOCH")

  describe "Dilock Crystal C ABI etcd integration" do
    it "queues, cancels before dispatch, verifies fences, and releases ownership" do
      first_provider = Dilock::EtcdProvider.new(epoch, endpoint, namespace, wait_ms: 1_000, lease_ms: 3_000)
      second_provider = Dilock::EtcdProvider.new(epoch, endpoint, namespace, wait_ms: 1_000, lease_ms: 3_000)
      first_handle = first_provider.lock("crystal-etcd-live")
      second_handle = second_provider.lock("crystal-etcd-live")
      first_lease : Dilock::EtcdLease? = nil
      second_lease : Dilock::EtcdLease? = nil
      cancellation : Dilock::EtcdCancellation? = nil

      begin
        cancellation = Dilock::EtcdCancellation.new
        cancellation.request
        expect_raises(Dilock::EtcdCancelled) do
          second_handle.acquire(1_000, 3_000, cancellation)
        end
        cancellation.close
        cancellation = nil

        first_lease = first_handle.acquire
        first_lease.not_nil!.effective_ttl.should be > 0.milliseconds
        first_fence = first_lease.not_nil!.fence
        first_fence.coordination_epoch.should eq(epoch)
        first_fence.create_revision.should be > 0

        expect_raises(Dilock::EtcdWaitTimeout) do
          second_handle.acquire(100, 3_000)
        end
        first_lease.not_nil!.release.should eq(Dilock::EtcdReleaseOutcome::Released)
        first_lease.not_nil!.release.should eq(Dilock::EtcdReleaseOutcome::Released)
        first_lease.not_nil!.close
        first_lease = nil

        second_lease = second_handle.acquire
        second_fence = second_lease.not_nil!.fence
        second_fence.coordination_epoch.should eq(epoch)
        second_fence.create_revision.should be > first_fence.create_revision
        second_lease.not_nil!.release.should eq(Dilock::EtcdReleaseOutcome::Released)
      ensure
        cancellation.try(&.close)
        first_lease.try(&.close)
        second_lease.try(&.release)
        second_lease.try(&.close)
        first_handle.close
        second_handle.close
        first_provider.close
        second_provider.close
      end
    end

    it "reports an expired native lease as already not owned and preserves its successor" do
      provider = Dilock::EtcdProvider.new(epoch, endpoint, namespace,
        wait_ms: 1_000, lease_ms: 3_000, auto_renew: false)
      handle = provider.lock("crystal-etcd-natural-expiry")
      expired : Dilock::EtcdLease? = nil
      successor : Dilock::EtcdLease? = nil

      begin
        expired = handle.acquire(1_000, 3_000)
        expired.not_nil!.effective_ttl.should be > 0.milliseconds
        sleep 6.seconds
        expired.not_nil!.valid?.should be_false
        expired.not_nil!.release.should eq(Dilock::EtcdReleaseOutcome::AlreadyNotOwned)

        successor = handle.acquire(1_000, 3_000)
        successor.not_nil!.fence.create_revision.should be > expired.not_nil!.fence.create_revision
        successor.not_nil!.release.should eq(Dilock::EtcdReleaseOutcome::Released)
      ensure
        expired.try(&.release)
        expired.try(&.close)
        successor.try(&.release)
        successor.try(&.close)
        handle.close
        provider.close
      end
    end

    it "rejects non-blocking acquisition as an unsupported etcd capability" do
      provider = Dilock::EtcdProvider.new(epoch, endpoint, namespace)
      handle = provider.lock("crystal-etcd-unsupported-try")
      begin
        expect_raises(Dilock::EtcdUnsupportedCapability) { handle.try_acquire }
      ensure
        handle.close
        provider.close
      end
    end

    it "races native lock creation against provider close without using a destroyed pointer" do
      24.times do |attempt|
        provider = Dilock::EtcdProvider.new(epoch, endpoint, namespace)
        start_mutex = Thread::Mutex.new
        start_condition = Thread::ConditionVariable.new
        ready_threads = 0
        start_race = false
        created_handle : Dilock::EtcdHandle? = nil
        lock_error : Exception? = nil
        close_error : Exception? = nil

        lock_thread = Thread.new do
          start_mutex.synchronize do
            ready_threads += 1
            # Wake every waiter: a single signal may select a peer worker, which then re-waits while the coordinator sleeps.
            start_condition.broadcast
            until start_race
              start_condition.wait(start_mutex)
            end
          end
          begin
            created_handle = provider.lock("crystal-etcd-provider-close-race-#{attempt}")
          rescue ex
            lock_error = ex
          end
        end
        close_thread = Thread.new do
          start_mutex.synchronize do
            ready_threads += 1
            # Wake every waiter: a single signal may select a peer worker, which then re-waits while the coordinator sleeps.
            start_condition.broadcast
            until start_race
              start_condition.wait(start_mutex)
            end
          end
          begin
            provider.close
          rescue ex
            close_error = ex
          end
        end

        start_mutex.synchronize do
          until ready_threads == 2
            start_condition.wait(start_mutex)
          end
          start_race = true
          start_condition.broadcast
        end
        lock_thread.join
        close_thread.join

        close_error.should be_nil
        if handle = created_handle
          lock_error.should be_nil
          handle.close
        else
          lock_error.should_not be_nil
          error = lock_error.not_nil!
          error.should be_a(Dilock::Error)
          error.message.should eq("provider is closed")
        end
      end
    end

    it "scopes default and configured acquisitions across reusable handles" do
      provider = Dilock::EtcdProvider.new(epoch, endpoint, namespace)
      daily = provider.lock("crystal-etcd-scope-daily")
      monthly = provider.lock("crystal-etcd-scope-monthly")

      begin
        default_result = daily.with_lease do |lease|
          fence = lease.fence
          fence.coordination_epoch.should eq(epoch)
          fence.create_revision.should be > 0
          "default"
        end
        default_result.should eq("default")

        configured_result = daily.with_lease(wait_ms: 1_000_i64, lease_ms: 5_000_i64) do |lease|
          lease.effective_ttl.should be > 0.milliseconds
          "configured"
        end
        configured_result.should eq("configured")

        monthly.with_lease { |_lease| "second-resource" }.should eq("second-resource")
      ensure
        daily.close
        monthly.close
        provider.close
      end
    end
  end
end
