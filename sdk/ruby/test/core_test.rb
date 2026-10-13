# frozen_string_literal: true

require 'json'
require 'minitest/autorun'
require_relative '../lib/dilock/core'

module DilockCoreTestSupport
  VECTORS = File.expand_path('../../../spec/vectors/identity.json', __dir__)

  private

  def block_cleanup_provider
    provider = Object.new
    provider.define_singleton_method(:__acquire) do |_id, _options, wait:|
      next unless wait

      ownership = Dilock::Lease::Ownership.new(key: 'key', owner: 'token')
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 1
      Dilock::Lease.new(self, ownership, 1_000, deadline)
    end
    provider.define_singleton_method(:__release) { |_lease| :unknown }
    provider.define_singleton_method(:default_acquisition) { Dilock::AcquireOptions::DEFAULT }
    provider
  end

  def provider_with_release_counter
    calls = 0
    provider = Object.new
    provider.define_singleton_method(:__release) do |_lease|
      calls += 1
      :released
    end
    [provider, -> { calls }]
  end

  def interrupting_release_lease
    calls = 0
    provider = Object.new
    provider.define_singleton_method(:__release) do |_lease|
      calls += 1
      raise Interrupt, 'simulated asynchronous interruption after dispatch'
    end
    ownership = Dilock::Lease::Ownership.new(key: 'key', owner: 'owner')
    lease = Dilock::Lease.new(provider, ownership, 1_000,
                              Process.clock_gettime(Process::CLOCK_MONOTONIC) + 1)
    [lease, -> { calls }]
  end

  def identity_vectors
    # Ruby's JSON parser rejects a lone UTF-16 surrogate before the adapter can
    # validate it. Keep the shared vector parseable, then exercise the equivalent
    # invalid UTF-8 input directly in test_malformed_utf8_identity_is_rejected.
    raw = File.read(VECTORS, encoding: 'UTF-8').sub('"resource": "\\ud800"', '"resource": "surrogate-sentinel"')
    JSON.parse(raw)
  end

  def provider_with_unconfirmed_renewal
    calls = 0
    provider = Object.new
    provider.define_singleton_method(:__renew) do |_lease, _ttl|
      calls += 1
      raise Dilock::BackendUnavailable.new('renewal acknowledgement lost', category: :backend_unavailable)
    end
    provider.define_singleton_method(:__release) { |_lease| :released }
    [provider, -> { calls }]
  end

  def provider_with_pre_dispatch_renewal_errors
    provider = Object.new
    unsupported_error = Dilock::UnsupportedCapability.new(
      'TTL changes are unsupported', category: :unsupported_capability
    )
    provider.define_singleton_method(:__renew) do |_lease, ttl|
      raise ArgumentError, 'invalid TTL' if ttl.zero?
      raise unsupported_error if ttl == 2

      true
    end
    provider
  end

  def provider_with_scoped_acquire
    provider = Object.new
    provider.define_singleton_method(:__acquire) do |_id, _options, wait:|
      next unless wait

      ownership = Dilock::Lease::Ownership.new(key: 'key', owner: 'token')
      Dilock::Lease.new(self, ownership, 1_000, Process.clock_gettime(Process::CLOCK_MONOTONIC) + 1)
    end
    provider.define_singleton_method(:default_acquisition) { Dilock::AcquireOptions::DEFAULT }
    provider
  end

  def provider_with_interrupted_scope_cleanup
    provider = provider_with_scoped_acquire
    provider.define_singleton_method(:__release) { |_lease| raise Interrupt, 'cleanup interrupted' }
    provider
  end

  def provider_with_already_not_owned_release
    calls = 0
    provider = provider_with_scoped_acquire
    provider.define_singleton_method(:__release) do |_lease|
      calls += 1
      :already_not_owned
    end
    [provider, -> { calls }]
  end

  def lease_for(provider)
    ownership = Dilock::Lease::Ownership.new(key: 'key', owner: 'owner')
    Dilock::Lease.new(provider, ownership, 1_000, Process.clock_gettime(Process::CLOCK_MONOTONIC) + 1)
  end
end

class DilockIdentityTest < Minitest::Test
  include DilockCoreTestSupport

  def test_shared_identity_vectors
    vectors = identity_vectors
    vectors.fetch('valid').each do |vector|
      id = Dilock::Identity.resource_id(vector.fetch('namespace'), vector.fetch('resource'))
      assert_equal vector.fetch('sha256'), id, vector.fetch('id')
      assert_equal vector.fetch('redis_key'), Dilock::Identity.redis_key(id)
      assert_equal vector.fetch('etcd_lock_name'), Dilock::Identity.etcd_lock_name(id)
    end
  end

  def test_invalid_identity_vectors_are_rejected
    vectors = identity_vectors
    vectors.fetch('invalid').each do |vector|
      next if vector.fetch('id') == 'surrogate-resource'

      assert_raises(ArgumentError, vector.fetch('id')) do
        Dilock::Identity.resource_id(vector['namespace'], vector['resource'])
      end
    end
  end

  def test_malformed_utf8_identity_is_rejected
    ["\xed\xa0\x80", "\xf4\x90\x80\x80", "\xf0\x8f\xbf\xbf", "\xf1\x80\x80"].each do |bytes|
      invalid_utf8 = bytes.b.force_encoding(Encoding::UTF_8)
      assert_raises(ArgumentError) { Dilock::Identity.resource_id('default', invalid_utf8) }
    end
  end

  def test_maximum_unicode_scalar_is_valid_four_byte_utf8
    assert_equal 64, Dilock::Identity.resource_id('default', "\u{10FFFF}").length
  end

  def test_canonical_unicode_forms_remain_distinct
    composed = Dilock::Identity.resource_id('default', "caf\u00e9")
    decomposed = Dilock::Identity.resource_id('default', "cafe\u0301")
    refute_equal composed, decomposed
  end
end

class DilockOptionsMetadataTest < Minitest::Test
  def test_acquisition_options_require_finite_integer_milliseconds
    assert_equal 30_000, Dilock::AcquireOptions::DEFAULT.lease_ms
    assert_raises(ArgumentError) { Dilock::AcquireOptions.new(wait_ms: -1, lease_ms: 100) }
    assert_raises(ArgumentError) { Dilock::AcquireOptions.new(wait_ms: 0, lease_ms: 0) }
    assert_raises(ArgumentError) { Dilock::AcquireOptions.new(wait_ms: 1.5, lease_ms: 100) }
  end

  def test_invalid_namespace_metadata_is_mapped_to_protocol_mismatch
    invalid_metadata = ['{', '{"protocol_major":1,"protocol_major":1}']

    invalid_metadata.each do |raw|
      error = assert_raises(Dilock::ProtocolMismatch) do
        Dilock::Internal.validate_metadata!(raw, 'default', 'redis-single-authority-v1', 'epoch')
      end
      assert_equal :protocol_mismatch, error.category
    end
  end
end

class DilockScopedCleanupTest < Minitest::Test
  include DilockCoreTestSupport

  def test_block_cleanup_does_not_mask_user_exception
    error = assert_raises(RuntimeError) do
      Dilock::Lock.new(block_cleanup_provider, 'id').with_lease { raise 'work error' }
    end
    assert_equal 'work error', error.message
  end

  def test_interrupted_scope_cleanup_does_not_mask_user_exception
    provider = provider_with_interrupted_scope_cleanup
    error = assert_raises(RuntimeError) do
      Dilock::Lock.new(provider, 'id').with_lease { raise 'work error' }
    end

    assert_equal 'work error', error.message
  end

  def test_implicit_block_cleanup_reports_unknown_acknowledgement
    error = assert_raises(Dilock::CleanupError) do
      Dilock::Lock.new(block_cleanup_provider, 'id').with_lease { :done }
    end
    assert_equal :release_unknown, error.category
  end

  def test_implicit_block_cleanup_reports_already_not_owned
    provider, release_calls = provider_with_already_not_owned_release

    error = assert_raises(Dilock::LeaseLost) do
      Dilock::Lock.new(provider, 'id').with_lease { :done }
    end

    assert_equal :lease_lost, error.category
    assert_equal 1, release_calls.call
  end
end

class DilockLeaseLifecycleTest < Minitest::Test
  include DilockCoreTestSupport

  def test_release_attempts_owner_checked_cleanup_after_local_expiry
    provider, calls = provider_with_release_counter
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) - 1
    ownership = Dilock::Lease::Ownership.new(key: 'key', owner: 'owner')
    lease = Dilock::Lease.new(provider, ownership, 1_000, deadline)

    assert_equal :lost, lease.state
    assert_equal :released, lease.release
    assert_equal :released, lease.release
    assert_equal 1, calls.call, 'release after expiry should clean once and cache its acknowledged result'
  end

  def test_renewal_lease_lost_exception_moves_lease_to_terminal_lost_state
    provider = Object.new
    provider.define_singleton_method(:__renew) do |_lease, _ttl|
      raise Dilock::LeaseLost.new('server confirmed lease expiry', category: :lease_lost)
    end
    ownership = Dilock::Lease::Ownership.new(key: 'key', owner: 'owner')
    lease = Dilock::Lease.new(provider, ownership, 1_000,
                              Process.clock_gettime(Process::CLOCK_MONOTONIC) + 1)

    assert_raises(Dilock::LeaseLost) { lease.renew }
    assert_equal :lost, lease.state
    assert_raises(Dilock::LeaseLost) { lease.renew }
  end

  def test_unconfirmed_renewal_fails_closed_and_remains_releasable
    provider, calls = provider_with_unconfirmed_renewal
    lease = lease_for(provider)

    assert_raises(Dilock::BackendUnavailable) { lease.renew }
    assert_equal :lost, lease.state
    assert_raises(Dilock::LeaseLost) { lease.renew }
    assert_renewal_count(calls, 'a lost lease must not dispatch another renewal')
    assert_released_renewal_stays_terminal(lease, calls)
  end

  def test_pre_dispatch_renewal_validation_errors_preserve_held_state
    provider = provider_with_pre_dispatch_renewal_errors
    ownership = Dilock::Lease::Ownership.new(key: 'key', owner: 'owner')
    lease = Dilock::Lease.new(provider, ownership, 1_000,
                              Process.clock_gettime(Process::CLOCK_MONOTONIC) + 1)

    assert_raises(ArgumentError) { lease.renew(0) }
    assert_equal :held, lease.state
    assert_raises(Dilock::UnsupportedCapability) { lease.renew(2) }
    assert_equal :held, lease.state
    assert_held_after_successful_renewal(lease)
  end

  def test_async_exception_during_release_caches_unknown_without_retry
    lease, calls = interrupting_release_lease

    assert_raises(Interrupt) { lease.release }
    assert_equal :released_unknown, lease.state
    assert_equal :unknown, lease.release
    assert_equal 1, calls.call, 'an interrupted cleanup must not be dispatched a second time'
  end

  private

  def assert_held_after_successful_renewal(lease)
    assert lease.renew(1_000)
    assert_equal :held, lease.state
  end

  def assert_renewal_count(calls, message)
    assert_equal 1, calls.call, message
  end

  def assert_released_renewal_stays_terminal(lease, calls)
    assert_equal :released, lease.release
    assert_raises(Dilock::LeaseLost) { lease.renew }
    assert_renewal_count(calls, 'a released lease must not dispatch a renewal')
  end
end

class DilockScriptParityTest < Minitest::Test
  def test_bundled_redis_scripts_match_canonical_wire_scripts
    sdk = File.expand_path('../lib/dilock/redis', __dir__)
    canonical = File.expand_path('../../../spec/backends', __dir__)
    %w[redis-release.lua redis-renew.lua].each do |name|
      assert_equal File.binread(File.join(canonical, name)), File.binread(File.join(sdk, name)), name
    end
  end
end
