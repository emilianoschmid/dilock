# frozen_string_literal: true

require 'minitest/autorun'
require 'securerandom'
require_relative '../lib/dilock/version'
require_relative '../lib/dilock/core'
require_relative '../lib/dilock/consul'

class ConsulLiveTest < Minitest::Test
  def test_contention_renewal_and_monotonic_lock_index
    endpoint = ENV['DILOCK_CONSUL_ENDPOINT']
    namespace = ENV['DILOCK_CONSUL_NAMESPACE']
    epoch = ENV['DILOCK_CONSUL_EPOCH']
    skip 'set DILOCK_CONSUL_ENDPOINT, DILOCK_CONSUL_NAMESPACE and DILOCK_CONSUL_EPOCH' unless endpoint && namespace && epoch

    token = ENV['CONSUL_HTTP_TOKEN']
    Dilock::ConsulProvider.provision_namespace!(endpoint, namespace:, epoch:, token:)
    provider = Dilock::ConsulProvider.connect(endpoint, namespace:, epoch:, token:)
    handle = provider.lock("ruby/live/#{SecureRandom.hex(12)}")
    options = Dilock::AcquireOptions.new(wait_ms: 1_000, lease_ms: 10_000)
    first = handle.try_acquire(options)
    refute_nil first
    assert_equal epoch, first.fence.epoch
    assert_operator first.fence.lock_index, :>, 0
    assert_nil handle.try_acquire(options)
    assert first.renew
    assert_equal :released, first.release

    next_lease = handle.try_acquire(options)
    refute_nil next_lease
    assert_operator next_lease.fence.lock_index, :>, first.fence.lock_index
    assert_equal :released, next_lease.release
  ensure
    provider&.close
  end
end
