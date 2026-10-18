# frozen_string_literal: true

require 'pg'

module Dilock
  # PostgreSQL session-level advisory-lock adapter.
  # A successful acquisition owns one private PG::Connection until release.
  class PostgresProvider
    attr_reader :default_acquisition

    def self.connect(endpoint, epoch:, namespace: 'default', rpc_timeout_ms: Internal::DEFAULT_RPC_MS,
                     cleanup_timeout_ms: Internal::DEFAULT_CLEANUP_MS,
                     default_acquisition: AcquireOptions::DEFAULT, cleanup_observer: nil)
      new(endpoint, epoch:, namespace:, rpc_timeout_ms:, cleanup_timeout_ms:,
          default_acquisition:, cleanup_observer:)
    end

    def self.provision_namespace!(endpoint, namespace:, epoch:, timeout_ms: Internal::DEFAULT_RPC_MS)
      Identity.resource_id(namespace, 'validation')
      raise ArgumentError, 'epoch is required' unless epoch.is_a?(String) && !epoch.empty?

      connection = PG.connect(endpoint)
      created = connection.transaction do
        connection.exec("SELECT pg_advisory_xact_lock(hashtextextended('dilock:namespace-meta:v1', 0))")
        connection.exec('CREATE TABLE IF NOT EXISTS dilock_namespace_meta (namespace text PRIMARY KEY, protocol_major integer NOT NULL, identity_scheme text NOT NULL, profile text NOT NULL, coordination_epoch text NOT NULL, fixed_lease_ms bigint NULL)')
        result = connection.exec_params(
          'INSERT INTO dilock_namespace_meta (namespace, protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms) VALUES ($1, 1, $2, $3, $4, NULL) ON CONFLICT (namespace) DO NOTHING',
          [namespace, NamespaceMetadata::IDENTITY_SCHEME, Internal::PROFILES.fetch(:postgres), epoch]
        )
        result.cmd_tuples == 1
      end
      connection.close
      new(endpoint, epoch:, namespace:, rpc_timeout_ms: timeout_ms, cleanup_timeout_ms: timeout_ms)
      created
    rescue PG::Error => error
      connection&.close
      raise BackendUnavailable.new("PostgreSQL provisioning failed (#{error.class})", category: :backend_unavailable)
    end

    def initialize(endpoint, epoch:, namespace: 'default', rpc_timeout_ms: Internal::DEFAULT_RPC_MS,
                   cleanup_timeout_ms: Internal::DEFAULT_CLEANUP_MS,
                   default_acquisition: AcquireOptions::DEFAULT, cleanup_observer: nil)
      Internal.validate_provider!(namespace, epoch, rpc_timeout_ms, cleanup_timeout_ms, default_acquisition)
      Identity.resource_id(namespace, 'validation')
      @endpoint = endpoint
      @namespace = namespace
      @epoch = epoch
      @rpc_timeout_ms = rpc_timeout_ms
      @cleanup_timeout_ms = cleanup_timeout_ms
      @default_acquisition = default_acquisition
      @cleanup_observer = cleanup_observer
      @leases = []
      @closed = false
      validate_metadata!
    rescue PG::Error => error
      raise BackendUnavailable.new("PostgreSQL metadata validation failed (#{error.class})", category: :backend_unavailable)
    end

    def lock(resource)
      ensure_open!
      Lock.new(self, Identity.resource_id(@namespace, resource))
    end

    def close
      return if @closed

      @leases.dup.each(&:release)
      @closed = true
    end

    def __acquire(resource_id, options, wait:)
      ensure_open!
      raise ArgumentError, 'options must be AcquireOptions' unless options.is_a?(AcquireOptions)

      deadline = Internal.deadline_ms(wait ? options.wait_ms : 0)
      loop do
        connection = PG.connect(@endpoint)
        key = advisory_key(resource_id)
        acquired = connection.exec_params('SELECT pg_try_advisory_lock($1::bigint) AS acquired', [key.to_s]).getvalue(0, 0)
        if acquired == true || acquired.to_s == 't'
          lease = PostgresLease.new(self, connection, key, options.lease_ms)
          @leases << lease
          return lease
        end
        connection.close
        return nil unless wait
        raise AcquisitionTimeout.new('PostgreSQL advisory-lock wait budget expired', category: :acquisition_timeout) unless Internal.remaining_ms(deadline).positive?

        Internal.sleep_backoff(deadline)
      rescue PG::Error => error
        connection&.close
        raise BackendUnavailable.new("PostgreSQL acquisition failed (#{error.class})", category: :backend_unavailable)
      end
    end

    def __renew(lease, _lease_ms)
      lease.ensure_held
      lease.connection.exec('SELECT 1')
      true
    rescue PG::Error => error
      lease.mark_lost
      raise BackendUnavailable.new("PostgreSQL liveness check failed (#{error.class})", category: :backend_unavailable)
    end

    def __release(lease)
      result = lease.connection.exec_params('SELECT pg_advisory_unlock($1::bigint) AS released', [lease.key.to_s])
      result.getvalue(0, 0).to_s == 't' ? :released : :already_not_owned
    rescue PG::Error => error
      observe_cleanup('release-unknown', error.class.to_s)
      :unknown
    ensure
      lease.connection&.close
      @leases.delete(lease)
    end

    def observe_cleanup(reason, outcome)
      @cleanup_observer&.call(reason:, outcome:)
    rescue StandardError
      nil
    end

    private

    def advisory_key(resource_id)
      [resource_id[0, 16]].pack('H*').unpack1('q>')
    end

    def validate_metadata!
      connection = PG.connect(@endpoint)
      rows = connection.exec_params('SELECT protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms FROM dilock_namespace_meta WHERE namespace = $1', [@namespace])
      raise ProtocolMismatch.new('PostgreSQL namespace metadata is missing', category: :protocol_mismatch) unless rows.ntuples == 1

      row = rows[0]
      actual = JSON.generate(
        'protocol_major' => row['protocol_major'].to_i,
        'identity_scheme' => row['identity_scheme'],
        'profile' => row['profile'],
        'coordination_epoch' => row['coordination_epoch'],
        'fixed_lease_ms' => row['fixed_lease_ms']
      )
      NamespaceMetadata.validate!(actual, Internal::PROFILES.fetch(:postgres), @epoch)
    ensure
      connection&.close
    end

    def ensure_open!
      raise BackendUnavailable.new('PostgreSQL provider is closed', category: :provider_closed) if @closed
    end
  end

  # Lease retaining the exact session used for acquisition.
  class PostgresLease
    attr_reader :connection, :key, :effective_lease_ms

    def initialize(provider, connection, key, lease_ms)
      @provider = provider
      @connection = connection
      @key = key
      @effective_lease_ms = lease_ms
      @state = :held
      @outcome = nil
    end

    def state = @state
    def held? = @state == :held
    def ensure_held
      raise LeaseLost.new('PostgreSQL advisory-lock session is no longer available', category: :lease_lost) unless held?
    end
    def renew(lease_ms = @effective_lease_ms) = @provider.__renew(self, lease_ms)
    def release
      return @outcome if @outcome

      @state = :releasing
      @outcome = @provider.__release(self)
      @state = @outcome == :unknown ? :released_unknown : :released
      @outcome
    end
    def mark_lost = (@state = :lost)
  end
end
