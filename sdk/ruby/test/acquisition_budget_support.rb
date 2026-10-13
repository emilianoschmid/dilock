# frozen_string_literal: true

require_relative '../lib/dilock'

module DilockEtcdTestSupport
  private

  def acquire_options
    Dilock::AcquireOptions.new(wait_ms: 1_000, lease_ms: 3_000)
  end

  def cleanup_observation(provider)
    provider.instance_variable_get(:@cleanup_observations).first
  end

  def assert_unknown_cleanup(error, provider)
    assert_equal :acquisition_unknown, error.category
    assert_instance_of Interrupt, error.cause
    assert_equal 'etcd revoke interrupted', error.cause.message
    assert_equal 1, provider.instance_variable_get(:@revoke_calls)
    assert_equal 'lease-revoke', cleanup_observation(provider).fetch(:reason)
  end

  def provider_with_failed_grant
    provider = Dilock::EtcdProvider.allocate
    provider.instance_variable_set(:@rpc_timeout_ms, 1_000)
    lease_stub = Object.new
    lease_stub.define_singleton_method(:lease_grant) do |*_arguments, **_options|
      raise GRPC::BadStatus.new(GRPC::Core::StatusCodes::UNAVAILABLE, 'connection lost after dispatch')
    end
    provider.instance_variable_set(:@lease, lease_stub)
    provider
  end

  def provider_with_interrupted_etcd_lock(cleanup_acknowledged:)
    provider = interrupted_etcd_lock_provider('etcd lock wait interrupted')
    provider.instance_variable_set(:@cleanup_calls, 0)
    provider.define_singleton_method(:cleanup_acquisition_lease) do |state|
      next false if state[:cleanup_attempted]

      @cleanup_calls += 1
      state[:cleanup_attempted] = true
      cleanup_acknowledged
    end
    provider
  end

  def interrupted_etcd_lock_provider(message)
    provider = Dilock::EtcdProvider.allocate
    provider.define_singleton_method(:ensure_open!) { true }
    provider.define_singleton_method(:grant_acquisition_lease) do |_options, state|
      state[:lease_id] = 7
      [7, Process.clock_gettime(Process::CLOCK_MONOTONIC), 3_000]
    end
    provider.define_singleton_method(:start_keepalive) { |*_arguments| { thread: nil } }
    provider.define_singleton_method(:stop_keepalive) { |_state| true }
    provider.define_singleton_method(:await_lock) { |_name, _lease_id, _state| raise Interrupt, message }
    provider
  end

  def provider_with_interrupted_etcd_revoke
    provider = interrupted_etcd_lock_provider('etcd lock interrupted')
    provider.instance_variable_set(:@cleanup_timeout_ms, 1_000)
    provider.instance_variable_set(:@revoke_calls, 0)
    provider.instance_variable_set(:@cleanup_observations, [])
    provider.instance_variable_set(:@cleanup_observer, ->(**event) { record_cleanup(provider, event) })
    provider.instance_variable_set(:@lease, interrupted_revoke_stub(provider))
    provider
  end

  def record_cleanup(provider, event)
    provider.instance_variable_get(:@cleanup_observations) << event
  end

  def interrupted_revoke_stub(provider)
    Object.new.tap do |stub|
      stub.define_singleton_method(:lease_revoke) do |*_arguments, **_options|
        calls = provider.instance_variable_get(:@revoke_calls) + 1
        provider.instance_variable_set(:@revoke_calls, calls)
        raise Interrupt, 'etcd revoke interrupted'
      end
    end
  end

  def lock_deadline_with_wait(wait_seconds)
    provider, lock_stub = provider_with_deadline_lock
    state = { lease_id: 7, wait_deadline: monotonic_time + wait_seconds }
    result = provider.send(:await_lock, '/resource', 7, state)
    [result, lock_stub.instance_variable_get(:@deadline), state[:lock_wait_limited]]
  end

  def provider_with_deadline_lock
    provider = Dilock::EtcdProvider.allocate
    provider.instance_variable_set(:@rpc_timeout_ms, 2_000)
    lock_stub = Object.new
    lock_stub.define_singleton_method(:lock) do |_request, deadline:|
      @deadline = deadline
      :locked
    end
    provider.instance_variable_set(:@lock, lock_stub)
    [provider, lock_stub]
  end

  def monotonic_time
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end
end

module DilockEtcdGrantTestSupport
  private

  def malformed_grant_fixture
    provider = Dilock::EtcdProvider.allocate
    provider.instance_variable_set(:@rpc_timeout_ms, 1_000)
    provider.instance_variable_set(:@cleanup_timeout_ms, 1_000)
    lease_stub = malformed_grant_stub
    provider.instance_variable_set(:@lease, lease_stub)
    [provider, malformed_grant_state, lease_stub]
  end

  def malformed_grant_stub
    Object.new.tap do |stub|
      stub.define_singleton_method(:lease_grant) { |*_args, **_kwargs| Etcdserverpb::LeaseGrantResponse.new(ID: 7, TTL: 0, error: '') }
      stub.define_singleton_method(:lease_revoke) { |*_args, **_kwargs| @revoke_calls = @revoke_calls.to_i + 1 }
    end
  end

  def malformed_grant_state
    { lease_id: nil, cleanup_attempted: false, wait_deadline: monotonic_time + 1 }
  end

  def provider_with_invalid_lock_response
    provider = provider_with_unacknowledged_cleanup
    provider.instance_variable_set(:@kv, empty_lock_range_client)
    provider
  end

  def provider_with_unacknowledged_cleanup
    provider = Dilock::EtcdProvider.allocate
    provider.define_singleton_method(:cleanup_acquisition_lease) { |_state| false }
    provider.define_singleton_method(:ensure_wait_budget_for_acquisition!) { |_state| true }
    provider.define_singleton_method(:rpc_deadline) { |_timeout| Time.now + 1 }
    provider.define_singleton_method(:valid_lock_entry?) { |_entry, _state| false }
    provider.instance_variable_set(:@rpc_timeout_ms, 1_000)
    provider
  end

  def empty_lock_range_client
    Object.new.tap do |client|
      client.define_singleton_method(:range) { |*_args, **_kwargs| Struct.new(:kvs).new([]) }
    end
  end

  def provider_with_acknowledged_cleanup
    Dilock::EtcdProvider.allocate.tap do |provider|
      provider.define_singleton_method(:cleanup_acquisition_lease) { |_state| true }
    end
  end

  def assert_unknown_invalid_lock_key(provider)
    error = assert_raises(Dilock::AcquisitionUnknown) do
      provider.send(:validate_lock_key!, nil, {})
    end
    assert_equal :acquisition_unknown, error.category
  end

  def assert_unknown_invalid_lock_entry(provider)
    error = assert_raises(Dilock::AcquisitionUnknown) do
      provider.send(:verified_lock_entry, '/locks/opaque', wait_deadline: monotonic_time + 1)
    end
    assert_equal :acquisition_unknown, error.category
  end
end

module DilockEtcdReleaseRangeTestSupport
  private

  def successful_owned_range
    lambda do |*_args, **_kwargs|
      entry = Struct.new(:key, :lease).new('/opaque-lock-key', 7)
      Struct.new(:kvs).new([entry])
    end
  end

  def empty_range
    ->(*_args, **_kwargs) { Struct.new(:kvs).new([]) }
  end

  def mismatched_lease_range
    lambda do |*_args, **_kwargs|
      entry = Struct.new(:key, :lease).new('/opaque-lock-key', 8)
      Struct.new(:kvs).new([entry])
    end
  end

  def malformed_release_range
    lambda do |*_args, **_kwargs|
      raise GRPC::BadStatus.new(GRPC::Core::StatusCodes::UNAVAILABLE, 'range unavailable')
    end
  end

  def missing_lease_revoke
    status_error(GRPC::Core::StatusCodes::NOT_FOUND, 'lease missing')
  end
end

module DilockEtcdReleaseTestSupport
  include DilockEtcdReleaseRangeTestSupport

  private

  def etcd_release_provider(unlock:, range: successful_owned_range, revoke: successful_revoke)
    provider = Dilock::EtcdProvider.allocate
    provider.instance_variable_set(:@cleanup_timeout_ms, 1_000)
    provider.instance_variable_set(:@lock, rpc_client(:unlock, unlock))
    provider.instance_variable_set(:@kv, rpc_client(:range, range))
    provider.instance_variable_set(:@lease, rpc_client(:lease_revoke, revoke))
    provider.define_singleton_method(:rpc_deadline) { |_timeout| Time.now + 1 }
    provider
  end

  def rpc_client(method_name, operation)
    Object.new.tap { |client| client.define_singleton_method(method_name, &operation) }
  end

  def etcd_release_lease(provider)
    ownership = Dilock::Lease::Ownership.new(key: '/opaque-lock-key', owner: 7)
    Dilock::Lease.new(provider, ownership, 1_000, monotonic_time + 1)
  end

  def successful_revoke
    ->(*_args, **_kwargs) { :acknowledged }
  end

  def successful_unlock
    ->(*_args, **_kwargs) { :acknowledged }
  end

  def failed_revoke
    status_error(GRPC::Core::StatusCodes::UNAVAILABLE, 'revoke acknowledgement lost')
  end

  def missing_lock_unlock
    status_error(GRPC::Core::StatusCodes::NOT_FOUND, 'lock key missing')
  end

  def failed_unlock
    status_error(GRPC::Core::StatusCodes::UNAVAILABLE, 'unlock acknowledgement lost')
  end

  def failed_unlock_error
    GRPC::BadStatus.new(GRPC::Core::StatusCodes::UNAVAILABLE, 'unlock acknowledgement lost')
  end

  def status_error(code, message)
    ->(*_args, **_kwargs) { raise GRPC::BadStatus.new(code, message) }
  end

  def etcd_release_budget_fixture(**settings)
    provider = Dilock::EtcdProvider.allocate
    clock = { now_ms: settings.fetch(:start_ms) }
    budgets = { range: [], unlock: [], revoke: [] }
    now = -> { clock[:now_ms] }
    range, unlock, revoke = release_budget_operations(settings, budgets, clock)
    [provider, now, range, unlock, revoke, budgets]
  end

  def release_budget_operations(settings, budgets, clock)
    range_settings = { after_ms: settings.fetch(:range_ms, 0) }
    unlock_settings = {
      after_ms: settings.fetch(:unlock_ms), error: settings.fetch(:unlock_error),
      expected_budget: settings[:expected_budget]
    }
    revoke_settings = { after_ms: settings.fetch(:revoke_ms), result: settings.fetch(:revoke_result) }
    [budget_range_operation(budgets[:range], clock, range_settings),
     budget_operation(budgets[:unlock], clock, unlock_settings),
     budget_operation(budgets[:revoke], clock, revoke_settings)]
  end

  def budget_range_operation(budgets, clock, settings)
    lambda do |budget|
      budgets << budget
      clock[:now_ms] += settings.fetch(:after_ms)
      entry = Struct.new(:key, :lease).new('/opaque-lock-key', 7)
      Struct.new(:kvs).new([entry])
    end
  end

  def budget_operation(budgets, clock, settings)
    lambda do |budget|
      record_budget_operation(budgets, clock, settings, budget)
    end
  end

  def record_budget_operation(budgets, clock, settings, budget)
    expected_budget = settings[:expected_budget]
    assert_equal expected_budget, budget if expected_budget
    budgets << budget
    clock[:now_ms] = settings[:after_ms]
    raise settings[:error] if settings[:error]

    result = settings[:result]
    result.call if result.respond_to?(:call)
    result
  end
end

module DilockLeaseTestSupport
  private

  def assert_lease_renewal_stays_lost(provider)
    ownership = Dilock::Lease::Ownership.new(key: '/lock', owner: 7)
    lease = Dilock::Lease.new(provider, ownership, 1_000, monotonic_time + 0.03)
    refute lease.renew(1_000)
    assert_equal :lost, lease.state
  end

  def provider_with_delayed_redis_renewal
    provider = Dilock::RedisProvider.allocate
    provider.instance_variable_set(:@redis, delayed_redis_client)
    provider
  end

  def delayed_redis_client
    Object.new.tap do |client|
      client.define_singleton_method(:eval) do |*_arguments, **_options|
        sleep 0.04
        1
      end
    end
  end

  def provider_with_delayed_etcd_renewal
    provider = Dilock::EtcdProvider.allocate
    provider.instance_variable_set(:@rpc_timeout_ms, 1_000)
    provider.instance_variable_set(:@lease, delayed_etcd_lease_client)
    provider
  end

  def delayed_etcd_lease_client
    stream = Object.new
    stream.define_singleton_method(:first) do
      sleep 0.04
      Etcdserverpb::LeaseKeepAliveResponse.new(TTL: 1)
    end
    Object.new.tap { |client| client.define_singleton_method(:lease_keep_alive) { |*_args, **_kwargs| stream } }
  end
end

module DilockRedisTestSupport
  private

  def provider_with_late_redis_acquisition
    provider = Dilock::RedisProvider.allocate
    provider.instance_variable_set(:@closed, false)
    provider.instance_variable_set(:@redis, delayed_successful_set_client)
    provider.instance_variable_set(:@cleanup_redis, cleanup_client(1, nil))
    provider
  end

  def delayed_successful_set_client
    Object.new.tap do |client|
      client.define_singleton_method(:set) do |*_args, **_kwargs|
        sleep 0.04
        'OK'
      end
    end
  end

  def redis_lease_with_interrupted_release
    observations = []
    provider = Dilock::RedisProvider.allocate
    provider.instance_variable_set(:@cleanup_redis, interrupted_release_client)
    provider.instance_variable_set(:@cleanup_observer, ->(**event) { observations << event })
    [redis_test_lease(provider), observations]
  end

  def redis_test_lease(provider)
    ownership = Dilock::Lease::Ownership.new(key: 'lock-key', owner: 'owner-token')
    Dilock::Lease.new(provider, ownership, 1_000, monotonic_time + 1)
  end

  def interrupted_release_client
    Object.new.tap do |client|
      client.define_singleton_method(:eval) do |*_args, **_kwargs|
        raise Interrupt, 'release acknowledgement interrupted'
      end
    end
  end

  def provider_with_interrupted_set(cleanup_result: nil, cleanup_error: nil)
    provider = Dilock::RedisProvider.allocate
    provider.instance_variable_set(:@redis, interrupted_redis_client)
    provider.instance_variable_set(:@cleanup_redis, cleanup_client(cleanup_result, cleanup_error))
    provider
  end

  def interrupted_redis_client
    Object.new.tap do |client|
      client.define_singleton_method(:set) { |*_args, **_kwargs| raise Interrupt, 'cancelled after dispatch' }
    end
  end

  def cleanup_client(result, error)
    Object.new.tap do |client|
      client.define_singleton_method(:eval) do |*_args, **_kwargs|
        raise error if error

        result
      end
    end
  end
end
