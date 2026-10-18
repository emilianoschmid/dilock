# frozen_string_literal: true

require 'digest'
require 'json'
require 'securerandom'
require_relative 'core/namespace_metadata'

# Shared API errors, identity encoding, lease options, and lifecycle primitives.
module Dilock
  # Base exception with a stable machine-readable category.
  class Error < StandardError
    # Stable machine-readable failure category.
    # @return [Symbol]
    attr_reader :category

    def initialize(message, category: :backend_unavailable)
      @category = category
      super(message)
    end
  end

  # Raised when local options or provider configuration are invalid.
  class ConfigurationError < Error; end
  # Raised when existing namespace metadata or backend ownership data violates the protocol.
  class ProtocolMismatch < Error; end
  # Raised when the selected backend cannot provide a requested operation safely.
  class UnsupportedCapability < Error; end
  # Raised when the bounded contention wait expires before a lease is acquired.
  class AcquisitionTimeout < Error; end
  # Raised when acquisition may have committed remotely but its outcome is unknown.
  class AcquisitionUnknown < Error; end
  # Raised when a backend is unavailable or its provider has been closed.
  class BackendUnavailable < Error; end
  # Raised when this lease is no longer within its confirmed local validity budget.
  class LeaseLost < Error; end
  # Raised when block-scoped release could not be acknowledged.
  class CleanupError < Error; end

  # Immutable policy for one acquisition. Durations are milliseconds.
  AcquireOptions = Data.define(:wait_ms, :lease_ms) do
    def initialize(wait_ms:, lease_ms:)
      unless wait_ms.is_a?(Integer) && (0..86_400_000).cover?(wait_ms)
        raise ArgumentError,
              'wait_ms must be 0 through 86400000'
      end
      unless lease_ms.is_a?(Integer) && (1..86_400_000).cover?(lease_ms)
        raise ArgumentError,
              'lease_ms must be 1 through 86400000'
      end

      super
    end
  end
  AcquireOptions.const_set(:DEFAULT, AcquireOptions.new(wait_ms: 5_000, lease_ms: 30_000))

  # Draft 1 identity encoding. It deliberately performs no Unicode normalization.
  module Identity
    # Domain separator for the shared framed identity encoding.
    # @return [String]
    DOMAIN = "dilock.lock.identity\0".b.freeze
    # Accepted lower-case namespace syntax.
    # @return [Regexp]
    NAMESPACE = /\A[a-z0-9][a-z0-9_.-]{0,63}\z/

    module_function

    # Computes the shared SHA-256 identity for a namespace and UTF-8 resource.
    # @param namespace [String] lower-case protocol namespace
    # @param resource [String] non-empty resource with valid Unicode scalar values
    # @return [String] 64-character lower-case hexadecimal identity
    # @raise [ArgumentError] if either input violates the identity contract
    def resource_id(namespace, resource)
      validate_namespace!(namespace)
      bytes = resource_bytes(resource)
      ns = namespace.b
      frame = DOMAIN + [ns.bytesize].pack('N') + ns + [bytes.bytesize].pack('N') + bytes
      Digest::SHA256.hexdigest(frame)
    rescue EncodingError => e
      raise ArgumentError, "resource cannot be encoded as UTF-8: #{e.message}"
    end

    def validate_namespace!(namespace)
      return if namespace.is_a?(String) && NAMESPACE.match?(namespace)

      raise ArgumentError, 'namespace must match [a-z0-9][a-z0-9_.-]{0,63}'
    end
    private_class_method :validate_namespace!

    def resource_bytes(resource)
      raise ArgumentError, 'resource must be a String' unless resource.is_a?(String)

      value = resource.encode(Encoding::UTF_8)
      raise ArgumentError, 'resource must contain valid Unicode scalar values' unless value.valid_encoding?

      bytes = value.b
      raise ArgumentError, 'resource must be non-empty and contain no NUL' if bytes.empty? || bytes.include?("\0".b)
      return bytes if (1..1024).cover?(bytes.bytesize)

      raise ArgumentError, 'resource UTF-8 encoding must contain 1 to 1024 bytes'
    end
    private_class_method :resource_bytes

    # Builds the Redis key for an already validated resource identity.
    # @param resource_id [String] 64-character lower-case hexadecimal identity
    # @return [String] owner-generation lease key
    # @raise [ArgumentError] if the identity is malformed
    def redis_key(resource_id)
      validate_id(resource_id)
      "dilock:{#{resource_id}}:lease"
    end

    # Builds the etcd lock name for an already validated resource identity.
    # @param resource_id [String] 64-character lower-case hexadecimal identity
    # @return [String] absolute etcd lock name
    # @raise [ArgumentError] if the identity is malformed
    def etcd_lock_name(resource_id)
      validate_id(resource_id)
      "/dilock/locks/#{resource_id}"
    end

    def validate_id(resource_id)
      return if resource_id.is_a?(String) && /\A[0-9a-f]{64}\z/.match?(resource_id)

      raise ArgumentError,
            'resource id must be 64 lowercase hexadecimal characters'
    end
    private_class_method :validate_id
  end

  # Shared helpers are intentionally small; backend semantics remain in adapters.
  # @private Internal implementation details; use provider and lock APIs instead.
  module Internal
    PROFILES = { redis: 'redis-single-authority-v1', etcd: 'etcd-native-lock-v1',
                 consul: 'consul-kv-session-v1', postgres: 'postgres-advisory-session-v1' }.freeze
    DEFAULT_RPC_MS = 2_000
    DEFAULT_CLEANUP_MS = 2_000

    module_function

    def metadata(_namespace, profile, epoch)
      NamespaceMetadata.encode(profile, epoch)
    end

    def validate_metadata!(raw, _namespace, profile, epoch)
      NamespaceMetadata.validate!(raw, profile, epoch)
    end

    def validate_provider!(namespace, epoch, rpc_timeout_ms, cleanup_timeout_ms, acquisition)
      validate_provider_identity!(namespace, epoch)
      validate_timeouts!(rpc_timeout_ms, cleanup_timeout_ms)
      validate_default_acquisition!(acquisition)
      validate_lease_safety_margin!(rpc_timeout_ms, acquisition)
    end

    def validate_provider_identity!(namespace, epoch)
      unless namespace.is_a?(String) && Identity::NAMESPACE.match?(namespace)
        raise ConfigurationError.new('invalid namespace', category: :configuration_error)
      end
      return if epoch.is_a?(String) && !epoch.empty?

      raise ConfigurationError.new('epoch is required', category: :configuration_error)
    end
    private_class_method :validate_provider_identity!

    def validate_timeouts!(*timeouts)
      timeouts.each do |timeout|
        next if timeout.is_a?(Integer) && (1..60_000).cover?(timeout)

        raise ConfigurationError.new(
          'timeouts must be 1 through 60000 milliseconds',
          category: :configuration_error
        )
      end
    end
    private_class_method :validate_timeouts!

    def validate_default_acquisition!(acquisition)
      return if acquisition.is_a?(AcquireOptions)

      raise ConfigurationError.new(
        'default_acquisition must be AcquireOptions',
        category: :configuration_error
      )
    end
    private_class_method :validate_default_acquisition!

    def validate_lease_safety_margin!(rpc_timeout_ms, acquisition)
      margin_ms = [10, (acquisition.lease_ms + 99) / 100].max
      return if rpc_timeout_ms + margin_ms < acquisition.lease_ms

      raise ConfigurationError.new(
        'RPC timeout and safety margin must be shorter than the default lease',
        category: :configuration_error
      )
    end
    private_class_method :validate_lease_safety_margin!

    def token
      "dilock/1/#{SecureRandom.hex(32)}"
    end

    def deadline_ms(timeout_ms)
      Process.clock_gettime(Process::CLOCK_MONOTONIC) + (timeout_ms / 1_000.0)
    end

    def remaining_ms(deadline)
      ((deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)) * 1_000).floor
    end

    def bounded_rpc_timeout_ms(deadline, maximum_ms)
      remaining = remaining_ms(deadline)
      remaining.positive? ? [maximum_ms, remaining].min : 0
    end

    def safety_margin_ms(ttl_ms)
      [10, (ttl_ms + 99) / 100].max
    end

    def sleep_backoff(deadline)
      remaining = remaining_ms(deadline)
      return if remaining <= 0

      sleep([0.025 + (rand * 0.025), remaining / 1_000.0].min)
    end
  end

  # A successful acquisition. State is terminal after release or local expiry.
  class Lease
    # Internal backend ownership tuple; applications should use {#fence} where available.
    # @private
    Ownership = Data.define(:key, :owner)

    attr_reader :effective_lease_ms, :fence

    def initialize(provider, ownership, effective_lease_ms, deadline, fence: nil)
      @provider = provider
      @key = ownership.key
      @owner = ownership.owner
      @effective_lease_ms = effective_lease_ms
      @deadline = deadline
      @fence = fence
      @state = :held
      @release_outcome = nil
      @mutex = Mutex.new
    end

    def held?
      @mutex.synchronize { @state == :held && Internal.remaining_ms(@deadline).positive? }
    end

    # Reports the local lifecycle state, expiring a lease whose confirmed budget elapsed.
    # This does not query the backend.
    # @return [Symbol] one of `:held`, `:lost`, `:released`, or `:released_unknown`
    def state
      @mutex.synchronize do
        @state = :lost if @state == :held && Internal.remaining_ms(@deadline) <= 0
        @state
      end
    end

    # Renews this owner generation within its current confirmed validity budget.
    # Lifecycle operations on one lease are serialized by a mutex.
    # @param lease_ms [Integer] requested lease duration in milliseconds
    # @return [Boolean] true when renewal was acknowledged, false when ownership was lost
    # @raise [LeaseLost] if local validity has expired or ownership cannot be renewed
    # @raise [Error] for backend, configuration, or uncertain-outcome failures
    def renew(lease_ms = @effective_lease_ms)
      @mutex.synchronize { renew_locked(lease_ms) }
    end

    # Releases this owner generation at most once and caches the outcome.
    # @return [:released, :already_not_owned, :unknown] acknowledged release outcome
    # @raise [Error] if release fails before a remote request can be classified
    def release
      @mutex.synchronize { release_locked }
    end

    def cache_release_outcome(outcome)
      @state = outcome == :unknown ? :released_unknown : :released
      @release_outcome = outcome
    end

    def cache_unknown_release
      @state = :released_unknown
      @release_outcome = :unknown
    end
    private :cache_release_outcome, :cache_unknown_release

    # @private Backend adapter access only.
    def __owner = @owner

    # @private Backend adapter access only.
    def __key = @key

    # @private Backend adapter access only.
    def __deadline = @deadline

    # @private Backend adapter access only.
    def __set_deadline(deadline) = (@deadline = deadline)

    # @private Backend adapter access only.
    def __set_effective_lease_ms(value) = (@effective_lease_ms = value)

    private

    def renew_locked(lease_ms)
      ensure_held!
      result = @provider.__renew(self, lease_ms)
      @state = :lost unless result
      result
    rescue StandardError, Interrupt => e
      unless pre_dispatch_renewal_error?(e)
        # An unconfirmed renewal ends local permission; callers may still
        # release this owner generation but must not keep working under it.
        @state = :lost
      end
      raise
    end

    def pre_dispatch_renewal_error?(error)
      [ArgumentError, UnsupportedCapability, ConfigurationError].any? { |type| error.is_a?(type) }
    end

    def release_locked
      return @release_outcome if @release_outcome
      return :already_not_owned if @state == :released

      dispatch_release
    end

    def dispatch_release
      @state = :releasing
      begin
        cache_release_outcome(@provider.__release(self))
      ensure
        # After possible dispatch, never replay cleanup after an async exception.
        cache_unknown_release unless @release_outcome
      end
    end

    def ensure_held!
      return if @state == :held && Internal.remaining_ms(@deadline).positive?

      @state = :lost if @state == :held
      raise LeaseLost.new('lease is no longer within its confirmed validity budget', category: :lease_lost)
    end
  end

  # Resource handle shared by callers; each acquisition returns a distinct lease.
  class Lock
    def initialize(provider, resource_id)
      @provider = provider
      @resource_id = resource_id
    end

    # Attempts one acquisition without waiting for ordinary contention to clear.
    # @param options [AcquireOptions] zero-wait lease policy
    # @return [Lease, nil] lease on success, or `nil` for Redis contention only
    # @raise [UnsupportedCapability] when this backend cannot safely implement try-acquire
    # @raise [Error] for configuration, transport, or uncertain acquisition failures
    def try_acquire(options = @provider.default_acquisition)
      @provider.__acquire(@resource_id, options, wait: false)
    end

    # Waits up to `wait_ms` for a lease using the selected lease policy.
    # @param wait_ms [Integer] maximum contention wait in milliseconds
    # @param options [AcquireOptions] policy whose lease duration applies
    # @return [Lease] acquired owner generation
    # @raise [AcquisitionTimeout] when contention persists through the wait budget
    # @raise [Error] for backend failures, unsupported capability, or uncertain outcome
    def acquire(wait_ms = @provider.default_acquisition.wait_ms, options = @provider.default_acquisition)
      policy = AcquireOptions.new(wait_ms: wait_ms, lease_ms: options.lease_ms)
      @provider.__acquire(@resource_id, policy, wait: true)
    end

    # Ensures bounded release and never replaces an exception raised by the block.
    # Yields a lease and attempts bounded cleanup when the block exits.
    # The block's exception takes precedence over a cleanup exception.
    # @param options [AcquireOptions] acquisition and lease policy
    # @yield [lease] gives the acquired {Lease} to the caller
    # @yieldparam lease [Lease] owner generation valid within its confirmed budget
    # @return [Object, nil] block result, or `nil` when Redis is contended
    # @raise [Error] acquisition or cleanup failure; a block exception is preserved
    def with_lease(options = @provider.default_acquisition)
      lease = acquire(options.wait_ms, options)
      return nil unless lease

      with_release_cleanup(lease) { yield lease }
    end

    private

    def with_release_cleanup(lease)
      block_error = nil
      begin
        yield lease
      rescue Exception => e # rubocop:disable Lint/RescueException
        block_error = e
        raise
      ensure
        release_after_block(lease, block_error)
      end
    end

    def release_after_block(lease, block_error)
      outcome = lease.release
      if outcome == :already_not_owned
        raise LeaseLost.new('lease was no longer owned when scoped work finished', category: :lease_lost)
      end
      raise CleanupError.new('lease cleanup was not acknowledged', category: :release_unknown) if outcome == :unknown
    rescue Exception => e # rubocop:disable Lint/RescueException
      raise e unless block_error
    end
  end
end
