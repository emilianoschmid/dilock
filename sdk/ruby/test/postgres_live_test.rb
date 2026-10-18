# frozen_string_literal: true

require 'minitest/autorun'
require_relative '../lib/dilock'

class DilockPostgresLiveTest < Minitest::Test
  def test_contention_renew_release_and_handoff_against_postgres
    endpoint = ENV.fetch('DILOCK_POSTGRES_URL', nil)
    skip 'set DILOCK_POSTGRES_URL to run live PostgreSQL integration' unless endpoint

    namespace = "ruby#{SecureRandom.hex(6)}"
    epoch = "ruby-postgres-#{SecureRandom.hex(8)}"
    resource = "ruby-postgres-live-#{SecureRandom.hex(8)}"
    Dilock::PostgresProvider.provision_namespace!(endpoint, namespace:, epoch:)
    first = Dilock::PostgresProvider.connect(endpoint, namespace:, epoch:)
    second = Dilock::PostgresProvider.connect(endpoint, namespace:, epoch:)
    options = Dilock::AcquireOptions.new(wait_ms: 0, lease_ms: 10_000)

    begin
      first_lease = first.lock(resource).try_acquire(options)
      refute_nil first_lease
      assert_nil second.lock(resource).try_acquire(options)
      assert first_lease.renew
      assert_equal :released, first_lease.release

      successor = second.lock(resource).try_acquire(options)
      refute_nil successor
      assert_equal :released, successor.release
    ensure
      first_lease&.release
      successor&.release
      first.close
      second.close
    end
  end
end
