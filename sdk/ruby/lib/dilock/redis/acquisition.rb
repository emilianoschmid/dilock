# frozen_string_literal: true

# Dilock SDK types for Redis backend operations.
module Dilock
  # Redis provider implementation and internal composed operations.
  class RedisProvider
    # Initializes namespace metadata through the Redis connection.
    # @private Implementation of the explicit administrator provisioning helper.
    module NamespaceProvisioner
      def self.provision!(endpoint, namespace, epoch, timeout_ms)
        raise ArgumentError, 'invalid namespace' unless namespace.is_a?(String) && Identity::NAMESPACE.match?(namespace)
        raise ArgumentError, 'epoch is required' unless epoch.is_a?(String) && !epoch.empty?

        client = Redis.new(url: endpoint, connect_timeout: timeout_ms / 1_000.0,
                           read_timeout: timeout_ms / 1_000.0, write_timeout: timeout_ms / 1_000.0,
                           reconnect_attempts: 0)
        initialize_metadata(client, namespace, epoch)
        true
      ensure
        client&.close
      end

      def self.initialize_metadata(client, namespace, epoch)
        key = "dilock:namespace:#{namespace}:meta"
        profile = Internal::PROFILES.fetch(:redis)
        value = Internal.metadata(namespace, profile, epoch)
        existing = client.get(key)
        return Internal.validate_metadata!(existing, namespace, profile, epoch) if existing

        result = client.set(key, value, nx: true)
        Internal.validate_metadata!(client.get(key), namespace, profile, epoch) unless result
      end
      private_class_method :initialize_metadata
    end

    # Owns construction and validation of the provider's Redis clients.
    module ConnectionManagement
      private

      def initialize_clients!
        @redis = client_for(@rpc_timeout_ms)
        @cleanup_redis = client_for(@cleanup_timeout_ms)
        metadata = @redis.get("dilock:namespace:#{@namespace}:meta")
        unless metadata
          raise ProtocolMismatch.new('namespace metadata is not provisioned',
                                     category: :protocol_mismatch)
        end

        Internal.validate_metadata!(metadata, @namespace, Internal::PROFILES.fetch(:redis), @epoch)
      end

      def client_for(timeout_ms)
        Redis.new(url: @endpoint, connect_timeout: timeout_ms / 1_000.0,
                  read_timeout: timeout_ms / 1_000.0, write_timeout: timeout_ms / 1_000.0,
                  reconnect_attempts: 0)
      end
    end

    # Applies bounded renewal and owner-token-checked release to a live lease.
    # @private Backend lifecycle adapter called by {Lease}.
    module LeaseLifecycle
      def __renew(lease, ttl_ms)
        validate_renewal!(lease, ttl_ms)
        previous_deadline = lease.__deadline
        sent_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        result = @redis.eval(REDIS_RENEW, keys: [lease.__key], argv: [lease.__owner, ttl_ms.to_s])
        return false unless result.to_i == 1
        return false if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= previous_deadline

        apply_renewal!(lease, ttl_ms, sent_at)
        true
      rescue Redis::BaseError => e
        raise BackendUnavailable.new("Redis renewal failed (#{e.class})", category: :backend_unavailable)
      end

      def __release(lease)
        result = @cleanup_redis.eval(REDIS_RELEASE, keys: [lease.__key], argv: [lease.__owner])
        result.to_i == 1 ? :released : :already_not_owned
      rescue Interrupt => e
        observe_cleanup('release-unknown', "interrupted:#{e.class}")
        raise
      rescue Redis::BaseError => e
        observe_cleanup('release-unknown', e.class.to_s)
        :unknown
      end

      private

      def validate_renewal!(lease, ttl_ms)
        unless ttl_ms.is_a?(Integer) && (1..86_400_000).cover?(ttl_ms)
          raise ArgumentError, 'lease_ms must be 1 through 86400000'
        end
        return unless lease.fence

        raise UnsupportedCapability.new('Redis scripts do not provide a strong fencing token',
                                        category: :unsupported_capability)
      end

      def apply_renewal!(lease, ttl_ms, sent_at)
        elapsed = ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - sent_at) * 1_000).ceil
        remaining = ttl_ms - Internal.safety_margin_ms(ttl_ms) - elapsed
        unless remaining.positive?
          raise LeaseLost.new('Redis renewal response arrived outside its validity budget',
                              category: :lease_lost)
        end

        lease.__set_effective_lease_ms(ttl_ms)
        lease.__set_deadline(sent_at + ((ttl_ms - Internal.safety_margin_ms(ttl_ms)) / 1_000.0))
      end
    end

    # Isolates the lock acquisition retry flow from provider connection and
    # renewal/release lifecycle management.
    # @private Acquisition implementation mixed into {RedisProvider}.
    module AcquisitionFlow
      def __acquire(resource_id, options, wait:)
        ensure_open!
        raise ArgumentError, 'options must be AcquireOptions' unless options.is_a?(AcquireOptions)

        key = Identity.redis_key(resource_id)
        deadline = Internal.deadline_ms(wait ? options.wait_ms : 0)
        loop do
          lease = acquire_once(key, options, wait_deadline: (deadline if wait))
          return lease if lease
          return nil unless wait

          wait_for_retry(deadline)
        end
      end

      private

      def acquire_once(key, options, wait_deadline: nil)
        token = Internal.token
        sent_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        answer = dispatch_set(key, token, options.lease_ms)
        unless answer
          validate_contention_key!(key)
          return nil
        end

        build_lease(key, token, options.lease_ms, sent_at, wait_deadline:)
      end

      def validate_contention_key!(key)
        type = @redis.type(key)
        return if %w[string none].include?(type)

        raise ProtocolMismatch.new("Redis lock key has unexpected type #{type.inspect}",
                                   category: :protocol_mismatch)
      rescue Redis::BaseError => e
        raise BackendUnavailable.new("Redis TYPE failed after contention observation (#{e.class})",
                                     category: :backend_unavailable)
      end

      def dispatch_set(key, token, lease_ms)
        @redis.set(key, token, nx: true, px: lease_ms)
      rescue Interrupt => e
        classify_interrupted_set(key, token, e)
        raise e
      rescue Redis::BaseError => e
        cleanup_owner(key, token, 'acquisition-unknown')
        raise AcquisitionUnknown.new("Redis SET outcome is unknown after dispatch (#{e.class})",
                                     category: :acquisition_unknown)
      end

      def classify_interrupted_set(key, token, error)
        return unless cleanup_owner(key, token, 'cancelled-acquisition') == :unknown

        raise AcquisitionUnknown.new(
          'Redis SET was interrupted after dispatch and owner cleanup was not acknowledged',
          category: :acquisition_unknown
        ), cause: error
      end

      def build_lease(key, token, lease_ms, sent_at, wait_deadline: nil)
        margin = Internal.safety_margin_ms(lease_ms)
        remaining = lease_ms - margin - elapsed_ms(sent_at)
        ensure_lease_validity!(key, token, remaining)
        ensure_wait_deadline!(key, token, wait_deadline)

        ownership = Lease::Ownership.new(key: key, owner: token)
        Lease.new(self, ownership, lease_ms, sent_at + ((lease_ms - margin) / 1_000.0))
      end

      def ensure_lease_validity!(key, token, remaining)
        return if remaining.positive?

        cleanup_owner(key, token, 'late-acquisition')
        raise AcquisitionUnknown.new('Redis acquisition response arrived outside its validity budget',
                                     category: :acquisition_unknown)
      end

      def ensure_wait_deadline!(key, token, wait_deadline)
        return unless wait_deadline && Process.clock_gettime(Process::CLOCK_MONOTONIC) >= wait_deadline

        if cleanup_owner(key, token, 'wait-deadline-acquisition') == :unknown
          raise AcquisitionUnknown.new('late Redis acquisition cleanup was not acknowledged',
                                       category: :acquisition_unknown)
        end

        raise AcquisitionTimeout.new('Redis acquisition completed after its wait deadline',
                                     category: :acquisition_timeout)
      end

      def elapsed_ms(sent_at)
        ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - sent_at) * 1_000).ceil
      end

      def wait_for_retry(deadline)
        raise AcquisitionTimeout.new('Redis acquisition wait budget expired', category: :acquisition_timeout) unless
          Internal.remaining_ms(deadline).positive?

        Internal.sleep_backoff(deadline)
        return if Internal.remaining_ms(deadline).positive?

        raise AcquisitionTimeout.new('Redis acquisition wait budget expired', category: :acquisition_timeout)
      end
    end

    include ConnectionManagement
    include LeaseLifecycle
  end
end
