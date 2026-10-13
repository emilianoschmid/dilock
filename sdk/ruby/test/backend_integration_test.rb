# frozen_string_literal: true

require 'minitest/autorun'
require_relative '../lib/dilock'

# Shared provider setup and lifecycle helpers for backend integration scenarios.
module RedisProviderTestHelpers
  private

  def with_redis_test_providers
    endpoint, namespace, epoch = redis_test_coordinates
    Dilock::RedisProvider.provision_namespace!(endpoint, namespace: namespace, epoch: epoch)
    providers = create_redis_providers(endpoint, namespace, epoch)
    yield providers, namespace, epoch
  ensure
    providers&.each(&:close)
  end

  def with_two_redis_providers
    with_redis_test_providers do |providers|
      resource = "ruby-two-provider-#{Process.pid}-#{SecureRandom.hex(8)}"
      handles = providers.map { |provider| provider.lock(resource) }
      yield(*handles, Dilock::AcquireOptions.new(wait_ms: 0, lease_ms: 5_000),
            Dilock::AcquireOptions.new(wait_ms: 0, lease_ms: 500))
    end
  end

  def redis_test_coordinates
    endpoint = ENV.fetch('DILOCK_TEST_REDIS_URL', nil)
    skip 'set DILOCK_TEST_REDIS_URL and provisioned namespace metadata to run live Redis integration' unless endpoint

    [endpoint, ENV.fetch('DILOCK_TEST_NAMESPACE', 'default'), ENV.fetch('DILOCK_TEST_EPOCH', 'local-test-redis')]
  end

  def create_redis_providers(endpoint, namespace, epoch)
    providers = []
    providers << Dilock::RedisProvider.connect(endpoint, namespace: namespace, epoch: epoch)
    providers << Dilock::RedisProvider.connect(endpoint, namespace: namespace, epoch: epoch)
    providers
  rescue StandardError
    providers&.each(&:close)
    raise
  end

  def assert_redis_release_contention(handle)
    lease = acquire_redis_lease(handle, 2_000)
    refute_nil lease
    assert_nil acquire_redis_lease(handle, 2_000)
    assert_equal :released, lease.release
    assert_equal :released, lease.release
  ensure
    lease&.release
  end

  def assert_redis_renewal(first_handle, second_handle)
    lease = acquire_redis_lease(first_handle, 500)
    refute_nil lease
    sleep 0.2
    assert lease.renew(2_000), 'the current owner must be able to extend its Redis TTL'
    sleep 0.5
    assert_redis_renewal_handoff(lease, second_handle)
  ensure
    lease&.release
  end

  def assert_redis_renewal_handoff(lease, second_handle)
    assert_nil acquire_redis_lease(second_handle, 1_000),
               'the other provider must remain blocked after the original TTL expires'
    assert_equal :released, lease.release
    successor = acquire_redis_lease(second_handle, 1_000)
    refute_nil successor, 'the other provider must acquire after the renewed owner releases'
    assert_equal :released, successor.release
  ensure
    successor&.release
  end

  def acquire_redis_lease(handle, lease_ms)
    handle.try_acquire(Dilock::AcquireOptions.new(wait_ms: 0, lease_ms: lease_ms))
  end

  def with_etcd_test_provider
    endpoint = ENV.fetch('DILOCK_TEST_ETCD_ENDPOINT', nil)
    skip 'set DILOCK_TEST_ETCD_ENDPOINT and provisioned namespace metadata to run live etcd integration' unless endpoint

    namespace = ENV.fetch('DILOCK_TEST_NAMESPACE', 'default')
    epoch = ENV.fetch('DILOCK_TEST_EPOCH', 'local-test-etcd')
    provider = Dilock::EtcdProvider.connect(endpoint, namespace: namespace, epoch: epoch)
    yield provider, epoch
  ensure
    provider&.close
  end

  def assert_etcd_lock_lifecycle(provider, epoch)
    handle = provider.lock("ruby-integration-#{Process.pid}-#{SecureRandom.hex(8)}")
    lease = handle.acquire(5_000, Dilock::AcquireOptions.new(wait_ms: 5_000, lease_ms: 3_000))
    refute_nil lease
    assert_equal epoch, lease.fence.epoch
    assert_operator lease.fence.revision, :>, 0
    assert_etcd_release_is_idempotent(lease)
  ensure
    lease&.release
  end

  def assert_etcd_release_is_idempotent(lease)
    assert_equal :released, lease.release
    assert_equal :released, lease.release
  end
end

# Shared ownership assertions for Redis provider integration scenarios.
module RedisIntegrationAssertions
  private

  def assert_redis_handoff(first_handle, second_handle, options)
    owner = first_handle.try_acquire(options)
    refute_nil owner
    assert_nil second_handle.try_acquire(options), 'a second provider must observe the live owner'

    assert_equal :released, owner.release
    handoff = second_handle.try_acquire(options)
    refute_nil handoff, 'the second provider must acquire after owner release'
    assert_equal :released, handoff.release
  ensure
    owner&.release
    handoff&.release
  end

  def assert_redis_expiry_handoff(first_handle, second_handle, options, expiring_options)
    stale = first_handle.try_acquire(expiring_options)
    refute_nil stale
    successor = acquire_redis_successor(second_handle, options)

    assert_equal :already_not_owned, stale.release, "an expired owner's cleanup must not delete the successor"
    assert_nil first_handle.try_acquire(options), 'the successor must remain held after stale release'
    assert_equal :released, successor.release
  ensure
    stale&.release
    successor&.release
  end

  def acquire_redis_successor(handle, options)
    deadline = monotonic_time + 5
    loop do
      successor = handle.try_acquire(options)
      return successor if successor
      raise 'expired Redis lease was not released within five seconds' if monotonic_time >= deadline

      sleep 0.025
    end
  end

  def monotonic_time
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end
end

class DilockBackendIntegrationTest < Minitest::Test
  include RedisProviderTestHelpers
  include RedisIntegrationAssertions

  def test_redis_two_providers_contention_handoff_expiry_and_stale_release
    with_two_redis_providers do |first_handle, second_handle, options, expiring_options|
      assert_redis_handoff(first_handle, second_handle, options)
      assert_redis_expiry_handoff(first_handle, second_handle, options, expiring_options)
    end
  end

  def test_redis_owner_checked_release_and_contention
    with_redis_test_providers do |providers|
      handle = providers.first.lock("ruby-integration-#{Process.pid}-#{SecureRandom.hex(8)}")
      assert_redis_release_contention(handle)
    end
  end

  def test_redis_renewal_extends_lease_for_other_provider
    with_two_redis_providers do |first_handle, second_handle|
      assert_redis_renewal(first_handle, second_handle)
    end
  end

  def test_etcd_native_lock_queues_and_releases
    with_etcd_test_provider do |provider, epoch|
      assert_etcd_lock_lifecycle(provider, epoch)
    end
  end
end
