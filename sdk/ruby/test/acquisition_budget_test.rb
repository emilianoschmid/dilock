# frozen_string_literal: true

require 'minitest/autorun'
require_relative '../lib/dilock'
require_relative 'acquisition_budget_support'

class DilockAcquisitionBudgetTest < Minitest::Test
  include DilockEtcdTestSupport
  include DilockEtcdGrantTestSupport
  include DilockEtcdReleaseTestSupport
  include DilockLeaseTestSupport
  include DilockRedisTestSupport

  def test_etcd_rpc_budget_is_capped_by_monotonic_acquisition_deadline
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 0.040
    bounded = Dilock::Internal.bounded_rpc_timeout_ms(deadline, 2_000)
    assert_operator bounded, :>, 0
    assert_operator bounded, :<=, 40
    assert_equal 0, Dilock::Internal.bounded_rpc_timeout_ms(
      Process.clock_gettime(Process::CLOCK_MONOTONIC) - 0.001, 2_000
    )
  end

  def test_etcd_lease_grant_transport_failure_is_an_ambiguous_acquisition
    provider = provider_with_failed_grant
    options = Dilock::AcquireOptions.new(wait_ms: 1_000, lease_ms: 3_000)
    state = provider.send(:new_acquisition_state, options)

    error = assert_raises(Dilock::AcquisitionUnknown) do
      provider.send(:grant_acquisition_lease, options, state)
    end
    assert_equal :acquisition_unknown, error.category
  end

  def test_interrupted_etcd_acquisition_reports_unknown_when_attempt_lease_cleanup_is_unconfirmed
    provider = provider_with_interrupted_etcd_lock(cleanup_acknowledged: false)
    options = Dilock::AcquireOptions.new(wait_ms: 1_000, lease_ms: 3_000)

    error = assert_raises(Dilock::AcquisitionUnknown) do
      provider.send(:__acquire, 'a' * 64, options, wait: true)
    end

    assert_equal :acquisition_unknown, error.category
    assert_instance_of Interrupt, error.cause
    assert_equal 1, provider.instance_variable_get(:@cleanup_calls)
  end

  def test_interrupted_etcd_acquisition_preserves_cancellation_after_acknowledged_cleanup
    provider = provider_with_interrupted_etcd_lock(cleanup_acknowledged: true)
    options = Dilock::AcquireOptions.new(wait_ms: 1_000, lease_ms: 3_000)

    error = assert_raises(Interrupt) do
      provider.send(:__acquire, 'a' * 64, options, wait: true)
    end

    assert_equal 'etcd lock wait interrupted', error.message
    assert_equal 1, provider.instance_variable_get(:@cleanup_calls)
  end

  def test_interrupted_etcd_revoke_reports_unknown_without_replaying_cleanup
    provider = provider_with_interrupted_etcd_revoke

    error = assert_raises(Dilock::AcquisitionUnknown) do
      provider.send(:__acquire, 'a' * 64, acquire_options, wait: true)
    end

    assert_unknown_cleanup(error, provider)
  end
end

class DilockEtcdGrantBudgetTest < Minitest::Test
  include DilockEtcdTestSupport
  include DilockEtcdGrantTestSupport
  include DilockEtcdReleaseTestSupport
  include DilockLeaseTestSupport
  include DilockRedisTestSupport

  def test_etcd_malformed_effective_grant_ttl_is_cleaned_before_protocol_mismatch
    provider, state, lease_stub = malformed_grant_fixture

    error = assert_raises(Dilock::ProtocolMismatch) do
      provider.send(:grant_acquisition_lease, acquire_options, state)
    end
    assert_equal :protocol_mismatch, error.category
    assert_equal 1, lease_stub.instance_variable_get(:@revoke_calls)
  end

  def test_etcd_keepalive_ttl_above_profile_limit_is_rejected
    provider = Dilock::EtcdProvider.allocate
    error = assert_raises(Dilock::LeaseLost) do
      provider.send(:confirmed_keepalive_ttl_ms, Etcdserverpb::LeaseKeepAliveResponse.new(TTL: 86_401))
    end
    assert_equal :lease_lost, error.category
  end

  def test_etcd_native_lock_uses_the_full_wait_budget_when_rpc_timeout_is_shorter
    result, deadline, limited = lock_deadline_with_wait(5.0)

    assert_equal :locked, result
    assert_operator deadline - Time.now, :>, 3.0
    assert limited
  end

  def test_etcd_native_lock_remains_bounded_by_a_wait_budget_shorter_than_rpc_timeout
    result, deadline, limited = lock_deadline_with_wait(1.0)

    assert_equal :locked, result
    assert_operator deadline - Time.now, :>, 0.5
    assert_operator deadline - Time.now, :<, 1.1
    assert limited
  end

  def test_etcd_invalid_lock_response_is_unknown_when_revoke_is_unacknowledged
    provider = provider_with_invalid_lock_response
    assert_unknown_invalid_lock_key(provider)
    assert_unknown_invalid_lock_entry(provider)
  end

  def test_etcd_invalid_lock_response_remains_protocol_mismatch_after_confirmed_revoke
    provider = provider_with_acknowledged_cleanup

    error = assert_raises(Dilock::ProtocolMismatch) do
      provider.send(:validate_lock_key!, nil, {})
    end
    assert_equal :protocol_mismatch, error.category
  end
end

class DilockRenewalBudgetTest < Minitest::Test
  include DilockEtcdTestSupport
  include DilockEtcdGrantTestSupport
  include DilockEtcdReleaseTestSupport
  include DilockLeaseTestSupport
  include DilockRedisTestSupport

  def test_redis_acquire_does_not_return_a_success_after_its_wait_deadline
    provider = provider_with_late_redis_acquisition

    error = assert_raises(Dilock::AcquisitionTimeout) do
      provider.send(:__acquire, 'a' * 64, Dilock::AcquireOptions.new(wait_ms: 10, lease_ms: 2_000), wait: true)
    end
    assert_equal :acquisition_timeout, error.category
  end

  def test_redis_and_etcd_renewal_cannot_revive_a_lease_after_its_previous_deadline
    providers = [
      provider_with_delayed_redis_renewal,
      provider_with_delayed_etcd_renewal
    ]
    providers.each do |provider|
      assert_lease_renewal_stays_lost(provider)
    end
  end

  def test_etcd_waiter_keepalive_cannot_extend_validity_after_previous_deadline
    provider = Dilock::EtcdProvider.allocate
    provider.define_singleton_method(:keepalive_once) do |_lease_id|
      sleep 0.04
      Etcdserverpb::LeaseKeepAliveResponse.new(TTL: 1)
    end
    old_deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 0.02
    state = { mutex: Mutex.new, deadline: old_deadline, ttl_ms: 1_000, stopped: false, error: nil }

    provider.send(:refresh_keepalive_state, state, 7)

    assert_instance_of Dilock::LeaseLost, state[:error]
    assert_equal old_deadline, state[:deadline]
  end
end

class DilockEtcdReleaseBudgetTest < Minitest::Test
  include DilockEtcdTestSupport
  include DilockEtcdGrantTestSupport
  include DilockEtcdReleaseTestSupport
  include DilockLeaseTestSupport
  include DilockRedisTestSupport

  def test_etcd_release_is_unknown_when_range_and_revoke_are_unconfirmed
    provider = etcd_release_provider(unlock: missing_lock_unlock, revoke: failed_revoke)
    lease = etcd_release_lease(provider)

    assert_equal :unknown, lease.release
    assert_equal :released_unknown, lease.state
  end

  def test_etcd_release_requires_owned_range_unlock_ack_and_positive_revoke_ack
    unlock_only = etcd_release_lease(etcd_release_provider(unlock: successful_unlock, revoke: failed_revoke))

    assert_equal :unknown, unlock_only.release
    assert_equal :released, etcd_release_lease(etcd_release_provider(unlock: successful_unlock)).release
  end

  def test_etcd_release_classifies_absent_or_mismatched_exact_key_without_unlock
    absent = etcd_release_lease(etcd_release_provider(unlock: successful_unlock, range: empty_range))
    mismatch = etcd_release_lease(etcd_release_provider(unlock: successful_unlock, range: mismatched_lease_range))

    assert_equal :already_not_owned, absent.release
    assert_equal :already_not_owned, mismatch.release
  end

  def test_etcd_release_revoke_only_ack_is_already_not_owned
    lease = etcd_release_lease(etcd_release_provider(unlock: failed_unlock))

    assert_equal :already_not_owned, lease.release
  end

  def test_etcd_release_not_found_revoke_confirms_expiry_after_unlock_ack
    lease = etcd_release_lease(etcd_release_provider(unlock: successful_unlock, revoke: missing_lease_revoke))

    assert_equal :already_not_owned, lease.release
  end

  def test_etcd_release_range_error_plus_unconfirmed_revoke_is_unknown
    lease = etcd_release_lease(
      etcd_release_provider(
        unlock: successful_unlock, range: malformed_release_range, revoke: failed_revoke
      )
    )

    assert_equal :unknown, lease.release
  end

  def test_etcd_release_range_error_plus_revoke_only_ack_is_already_not_owned
    lease = etcd_release_lease(
      etcd_release_provider(
        unlock: successful_unlock, range: malformed_release_range, revoke: successful_revoke
      )
    )

    assert_equal :already_not_owned, lease.release
  end

  def test_etcd_release_unlock_ack_then_expired_lease_not_found_is_already_not_owned
    lease = etcd_release_lease(etcd_release_provider(unlock: successful_unlock, revoke: missing_lease_revoke))

    assert_equal :already_not_owned, lease.release
  end

  def test_etcd_release_shares_one_cleanup_budget_across_unlock_and_revoke
    provider, now, range, unlock, revoke, budgets = etcd_release_budget_fixture(
      start_ms: 1_000, unlock_ms: 1_065, revoke_ms: 1_090,
      unlock_error: failed_unlock_error, revoke_result: :acknowledged
    )

    lease = etcd_release_lease(provider)
    outcome = provider.send(:release_with_budget, 100, lease:, now:, operations: { range:, unlock:, revoke: })

    assert_equal :already_not_owned, outcome
    assert_equal [100], budgets.fetch(:range)
    assert_equal [100], budgets.fetch(:unlock)
    assert_equal [35], budgets.fetch(:revoke)
  end

  def test_etcd_release_does_not_start_revoke_after_shared_cleanup_budget_expires
    provider, now, range, unlock, revoke, _budgets = etcd_release_budget_fixture(
      start_ms: 2_000, range_ms: 25, unlock_ms: 2_050, revoke_ms: nil,
      unlock_error: failed_unlock_error, expected_budget: 25,
      revoke_result: -> { flunk('revoke must not start after the shared cleanup deadline') }
    )

    lease = etcd_release_lease(provider)
    outcome = provider.send(:release_with_budget, 50, lease:, now:, operations: { range:, unlock:, revoke: })

    assert_equal :unknown, outcome
  end
end

class DilockRedisCancellationTest < Minitest::Test
  include DilockEtcdTestSupport
  include DilockEtcdGrantTestSupport
  include DilockEtcdReleaseTestSupport
  include DilockLeaseTestSupport
  include DilockRedisTestSupport

  def test_interrupted_redis_set_preserves_cancellation_when_owner_cleanup_is_acknowledged
    provider = provider_with_interrupted_set(cleanup_result: 1)

    error = assert_raises(Interrupt) do
      provider.send(:dispatch_set, 'lock-key', 'owner-token', 1_000)
    end

    assert_equal 'cancelled after dispatch', error.message
  end

  def test_interrupted_redis_set_reports_unknown_when_owner_cleanup_fails
    provider = provider_with_interrupted_set(cleanup_error: Redis::BaseError.new('cleanup transport failed'))

    error = assert_raises(Dilock::AcquisitionUnknown) do
      provider.send(:dispatch_set, 'lock-key', 'owner-token', 1_000)
    end

    assert_equal :acquisition_unknown, error.category
    assert_instance_of Interrupt, error.cause
  end

  def test_interrupted_redis_set_reports_unknown_when_cleanup_is_interrupted
    provider = provider_with_interrupted_set(cleanup_error: Interrupt.new('cleanup interrupted'))

    error = assert_raises(Dilock::AcquisitionUnknown) do
      provider.send(:dispatch_set, 'lock-key', 'owner-token', 1_000)
    end

    assert_equal :acquisition_unknown, error.category
    assert_instance_of Interrupt, error.cause
  end

  def test_interrupted_redis_release_is_observed_as_unknown_and_is_not_retried
    lease, observations = redis_lease_with_interrupted_release
    assert_interrupted_release_state(lease)
    assert_interrupted_release_observation(observations)
  end

  private

  def assert_interrupted_release_state(lease)
    error = assert_raises(Interrupt) { lease.release }
    assert_equal 'release acknowledgement interrupted', error.message
    assert_equal :released_unknown, lease.state
    assert_equal :unknown, lease.release
  end

  def assert_interrupted_release_observation(observations)
    assert_equal 1, observations.length
    assert_equal 'release-unknown', observations.first.fetch(:reason)
    assert_match(/interrupted:Interrupt/, observations.first.fetch(:outcome))
  end
end
