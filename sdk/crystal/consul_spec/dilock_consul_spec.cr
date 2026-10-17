require "spec"
require "../src/dilock"
require "../src/dilock/etcd"
require "../src/dilock/consul"

describe Dilock::ConsulInternal do
  it "validates the shared endpoint, identity, token and policy bounds" do
    Dilock::ConsulInternal.validate_options!(
      "http://127.0.0.1:8500", "default", "epoch-1", "", 5_000_i64,
      30_000_i64, 2_000_i64, 2_000_i64, 25_i64,
    )
    expect_raises(ArgumentError) do
      Dilock::ConsulInternal.validate_options!(
        "http://127.0.0.1:8500", "default", "", "", 5_000_i64,
        30_000_i64, 2_000_i64, 2_000_i64, 25_i64,
      )
    end
    expect_raises(ArgumentError) do
      Dilock::ConsulInternal.validate_options!(
        "http://127.0.0.1:8500", "default", "epoch\0bad", "", 5_000_i64,
        30_000_i64, 2_000_i64, 2_000_i64, 25_i64,
      )
    end
    expect_raises(ArgumentError) do
      Dilock::ConsulInternal.validate_options!(
        "http://127.0.0.1:8500", "default", "epoch-1", "bad\0token", 5_000_i64,
        30_000_i64, 2_000_i64, 2_000_i64, 25_i64,
      )
    end
    expect_raises(ArgumentError) do
      Dilock::ConsulInternal.validate_options!(
        "http://127.0.0.1:8500", "default", "epoch-1", "", 0_i64,
        30_000_i64, 2_000_i64, 2_000_i64, 25_i64,
      )
    end
    Dilock::ConsulInternal.validate_lease_ms(10_000_i64)
    Dilock::ConsulInternal.validate_lease_ms(86_400_000_i64)
    expect_raises(ArgumentError) { Dilock::ConsulInternal.validate_lease_ms(9_999_i64) }
    expect_raises(ArgumentError) { Dilock::ConsulInternal.validate_lease_ms(86_400_001_i64) }
  end

  it "keeps release outcome values aligned with the C ABI" do
    Dilock::ConsulReleaseOutcome::Released.value.should eq(0)
    Dilock::ConsulReleaseOutcome::AlreadyNotOwned.value.should eq(1)
    Dilock::ConsulReleaseOutcome::Unknown.value.should eq(2)
  end
end

if endpoint = ENV["DILOCK_TEST_CONSUL_ENDPOINT"]?
  epoch = ENV.fetch("DILOCK_TEST_CONSUL_EPOCH")
  namespace = ENV.fetch("DILOCK_TEST_CONSUL_NAMESPACE")
  token = ENV["DILOCK_TEST_CONSUL_ACL_TOKEN"]? || ""

  describe "Dilock Crystal Consul C ABI integration" do
    it "provisions metadata, acquires, fences and releases the session lock" do
      provider = Dilock::ConsulProvider.new(epoch, endpoint, namespace, token,
        wait_ms: 2_000_i64, lease_ms: 10_000_i64)
      provider.provision_namespace
      handle = provider.lock("crystal-consul-live")
      lease = handle.acquire_default
      lease.valid?.should be_true
      lease.effective_ttl.should be >= 10.seconds
      fence = lease.fence
      fence.coordination_epoch.should eq(epoch)
      fence.lock_index.should be > 0
      lease.release.should eq(Dilock::ConsulReleaseOutcome::Released)
      lease.close
      handle.close
      provider.close
    end
  end
end
