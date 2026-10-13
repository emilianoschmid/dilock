# frozen_string_literal: true

require 'redis'
require_relative 'redis/acquisition'
require_relative 'redis/connection_setup'

# The Dilock SDK namespace.
module Dilock
  # Redis single-authority adapter. The provider owns its normal and bounded-cleanup clients.
  # This profile does not promise strong fencing or safety across unsafe failover/state loss.
  class RedisProvider
    # Policy used when a lock operation omits explicit acquisition options.
    # @return [AcquireOptions]
    attr_reader :default_acquisition

    # Connects to Redis and validates the pre-provisioned namespace metadata.
    # The returned provider owns two Redis clients and must be closed after use.
    #
    # @param endpoint [String] Redis URL accepted by the `redis` gem
    # @param epoch [String] approved coordination epoch recorded in namespace metadata
    # Keyword options are `namespace` (default: `default`), `rpc_timeout_ms`,
    # `cleanup_timeout_ms`, `default_acquisition`, and `cleanup_observer`.
    # @return [RedisProvider] connected provider
    # @raise [ArgumentError, ConfigurationError, ProtocolMismatch, BackendUnavailable]
    def self.connect(endpoint, epoch:, **)
      new(endpoint, epoch:, **)
    end

    # Explicit administrator helper. It only initializes an absent namespace key.
    # It does not overwrite an existing namespace. Call it through an approved
    # provisioning workflow, not as an application connection side effect.
    # @param endpoint [String] Redis URL
    # @param namespace [String] protocol namespace
    # @param epoch [String] new coordination epoch to record if metadata is absent
    # @param timeout_ms [Integer] bounded Redis command duration
    # @return [Boolean] true when metadata was created, false when it already existed
    # @raise [Error] if Redis cannot complete the create-if-absent operation
    def self.provision_namespace!(endpoint, namespace:, epoch:, timeout_ms: Internal::DEFAULT_RPC_MS)
      NamespaceProvisioner.provision!(endpoint, namespace, epoch, timeout_ms)
    end

    # Opens both provider-owned clients and validates Redis namespace metadata.
    # Do not close the provider concurrently with active operations.
    # @param endpoint [String] Redis URL
    # @param epoch [String] expected coordination epoch
    # Accepts the same keyword options as {self.connect}.
    # @raise [ArgumentError, ConfigurationError, ProtocolMismatch, BackendUnavailable]
    def initialize(endpoint, epoch:, **)
      setup_connection(endpoint, epoch:, **)
    rescue Redis::BaseError => e
      close
      raise BackendUnavailable.new("Redis metadata validation failed: #{e.class}", category: :backend_unavailable)
    rescue StandardError
      close
      raise
    end

    # Returns a reusable handle for one namespaced resource.
    # Each acquisition on the handle creates an independent owner generation.
    # @param resource [String] resource identifier; UTF-8 bytes define identity
    # @return [Lock] lock handle
    # @raise [BackendUnavailable] if this provider has been closed
    # @raise [ArgumentError] if resource identity is invalid
    def lock(resource)
      ensure_open!
      Lock.new(self, Identity.resource_id(@namespace, resource))
    end

    # Closes both owned Redis clients. Do not call concurrently with active operations.
    # @return [void]
    def close
      (@close_mutex ||= Mutex.new).synchronize do
        return if @closed

        @closed = true
        [@redis, @cleanup_redis].compact.each do |client|
          client.close
        rescue StandardError
          # Closing one owned client must not prevent cleanup of the other.
          nil
        end
        nil
      end
    end

    REDIS_RELEASE = File.read(File.expand_path('redis/redis-release.lua', __dir__)).freeze
    REDIS_RENEW = File.read(File.expand_path('redis/redis-renew.lua', __dir__)).freeze
    private_constant :REDIS_RELEASE, :REDIS_RENEW

    private

    def cleanup_owner(key, token, reason)
      result = @cleanup_redis.eval(REDIS_RELEASE, keys: [key], argv: [token])
      outcome = result.to_i == 1 ? :released : :already_not_owned
      observe_cleanup(reason, outcome == :released ? 'released' : 'not-owned')
      outcome
    rescue Redis::BaseError => e
      observe_cleanup(reason, "unknown:#{e.class}")
      :unknown
    rescue Interrupt => e
      observe_cleanup(reason, "unknown:interrupted:#{e.class}")
      :unknown
    end

    def observe_cleanup(reason, outcome)
      @cleanup_observer&.call(reason: reason, outcome: outcome)
    rescue StandardError
      nil
    end

    def ensure_open!
      raise BackendUnavailable.new('Redis provider is closed', category: :provider_closed) if @closed
    end
  end

  RedisProvider.include(RedisProvider::AcquisitionFlow)
  RedisProvider.private_constant :AcquisitionFlow
end
