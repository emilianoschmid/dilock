require "../dilock"
require "./etcd"

module Dilock
  MAX_CONSUL_DURATION_MS = 86_400_000_i64
  MIN_CONSUL_LEASE_MS = 10_000_i64

  class ConsulError < Error; end
  class ConsulBusy < ConsulError; end
  class ConsulInvalidArgument < ConsulError; end
  class ConsulCancelled < ConsulError; end
  class ConsulWaitTimeout < ConsulError; end
  class ConsulBackendUnavailable < ConsulError; end
  class ConsulAcquisitionUnknown < ConsulError; end
  class ConsulProtocolMismatch < ConsulError; end
  class ConsulLeaseLost < ConsulError; end
  class ConsulUnsupportedCapability < ConsulError; end

  # Consul cancellation uses the C ABI's atomic cancellation object. It can be
  # requested from another Crystal thread and must remain alive until acquire
  # returns.
  alias ConsulCancellation = EtcdCancellation

  enum ConsulReleaseOutcome
    Released
    AlreadyNotOwned
    Unknown
  end

  struct ConsulFence
    getter coordination_epoch : String
    getter lock_index : UInt64

    def initialize(@coordination_epoch : String, @lock_index : UInt64); end
  end

  module ConsulInternal
    def self.validate_string(value : String, label : String, allow_empty : Bool = true) : Nil
      raise ArgumentError.new("#{label} must not be empty") if !allow_empty && value.empty?
      raise ArgumentError.new("#{label} must be valid UTF-8") unless value.valid_encoding?
      raise ArgumentError.new("#{label} must not contain NUL") if value.includes?('\0')
    end

    def self.validate_wait_ms(value : Int64) : Nil
      raise ArgumentError.new("wait_ms must be between 1 and 86400000") unless 1 <= value <= MAX_CONSUL_DURATION_MS
    end

    def self.validate_lease_ms(value : Int64) : Nil
      raise ArgumentError.new("Consul session TTL must be between 10000 and 86400000 ms") unless MIN_CONSUL_LEASE_MS <= value <= MAX_CONSUL_DURATION_MS
    end

    def self.validate_options!(endpoint : String, namespace : String, epoch : String,
                               token : String, wait_ms : Int64, lease_ms : Int64,
                               rpc_timeout_ms : Int64, cleanup_timeout_ms : Int64,
                               poll_interval_ms : Int64) : Nil
      Dilock.identity(namespace, "validation")
      validate_string(endpoint, "endpoint", allow_empty: false)
      validate_string(epoch, "coordination epoch", allow_empty: false)
      raise ArgumentError.new("coordination epoch must be at most 1024 UTF-8 bytes") if epoch.bytesize > MAX_COORDINATION_EPOCH_BYTES
      validate_string(token, "ACL token")
      validate_wait_ms(wait_ms)
      validate_lease_ms(lease_ms)
      [
        {"RPC timeout", rpc_timeout_ms},
        {"cleanup timeout", cleanup_timeout_ms},
        {"poll interval", poll_interval_ms},
      ].each do |label, value|
        raise ArgumentError.new("#{label} must be between 1 and 86400000 ms") unless 1 <= value <= MAX_CONSUL_DURATION_MS
      end
    end
  end

  # Synchronous facade over the shared C++ Consul engine. The native library
  # must be built with DILOCK_BUILD_CONSUL=ON and DILOCK_BUILD_C_API=ON.
  class ConsulProvider
    getter endpoint : String
    getter namespace : String
    getter coordination_epoch : String

    @provider : Pointer(Void)
    @lifecycle = EtcdProviderLifecycle.new

    def initialize(@coordination_epoch : String, @endpoint : String = "http://127.0.0.1:8500",
                   @namespace : String = "default", @acl_token : String = "",
                   @wait_ms : Int64 = 5_000, @lease_ms : Int64 = 30_000,
                   @rpc_timeout_ms : Int64 = 2_000, @cleanup_timeout_ms : Int64 = 2_000,
                   @poll_interval_ms : Int64 = 25)
      ConsulInternal.validate_options!(@endpoint, @namespace, @coordination_epoch,
        @acl_token, @wait_ms, @lease_ms, @rpc_timeout_ms, @cleanup_timeout_ms,
        @poll_interval_ms)
      version = DilockC.abi_version
      raise ConsulError.new("unsupported Dilock C ABI version #{version}; expected 1") unless version == 1

      options = uninitialized DilockC::ConsulOptions
      DilockC.consul_options_init(pointerof(options))
      options.endpoint = @endpoint.to_unsafe
      options.name_space = @namespace.to_unsafe
      options.coordination_epoch = @coordination_epoch.to_unsafe
      options.acl_token = @acl_token.to_unsafe
      options.rpc_timeout_ms = @rpc_timeout_ms
      options.cleanup_timeout_ms = @cleanup_timeout_ms
      options.poll_interval_ms = @poll_interval_ms
      options.default_wait_ms = @wait_ms
      options.default_lease_ms = @lease_ms

      error = uninitialized DilockC::Error
      provider = Pointer(Void).null
      status = DilockC.consul_provider_create(pointerof(options), pointerof(provider), pointerof(error))
      Dilock.raise_consul_error(status, error) unless status == 0
      raise ConsulError.new("native provider handle was null") unless provider
      @provider = provider
    end

    # Explicit, idempotent metadata provisioning. Acquisition only verifies
    # the provisioned profile and epoch.
    def provision_namespace : Nil
      @lifecycle.with_open_provider do
        options = native_options
        error = uninitialized DilockC::Error
        status = DilockC.consul_provision_namespace(pointerof(options), pointerof(error))
        Dilock.raise_consul_error(status, error) unless status == 0
      end
    end

    def lock(resource : String) : ConsulHandle
      Dilock.identity(@namespace, resource)
      @lifecycle.with_open_provider do
        error = uninitialized DilockC::Error
        handle = Pointer(Void).null
        status = DilockC.consul_lock_create(@provider, resource.to_unsafe, pointerof(handle), pointerof(error))
        Dilock.raise_consul_error(status, error) unless status == 0
        raise ConsulError.new("native lock handle was null") unless handle
        ConsulHandle.new(self, handle, @wait_ms, @lease_ms)
      end
    end

    def close : Nil
      @lifecycle.close { DilockC.consul_provider_destroy(@provider) }
    end

    private def native_options : DilockC::ConsulOptions
      options = uninitialized DilockC::ConsulOptions
      DilockC.consul_options_init(pointerof(options))
      options.endpoint = @endpoint.to_unsafe
      options.name_space = @namespace.to_unsafe
      options.coordination_epoch = @coordination_epoch.to_unsafe
      options.acl_token = @acl_token.to_unsafe
      options.rpc_timeout_ms = @rpc_timeout_ms
      options.cleanup_timeout_ms = @cleanup_timeout_ms
      options.poll_interval_ms = @poll_interval_ms
      options.default_wait_ms = @wait_ms
      options.default_lease_ms = @lease_ms
      options
    end
  end

  class ConsulHandle
    def initialize(@provider : ConsulProvider, @handle : Pointer(Void),
                   @default_wait_ms : Int64, @default_lease_ms : Int64)
      @closed = false
    end

    def acquire(wait_ms : Int64 = @default_wait_ms, lease_ms : Int64 = @default_lease_ms,
                cancellation : ConsulCancellation? = nil) : ConsulLease
      raise Error.new("lock handle is closed") if @closed
      ConsulInternal.validate_wait_ms(wait_ms)
      ConsulInternal.validate_lease_ms(lease_ms)
      error = uninitialized DilockC::Error
      lease = Pointer(Void).null
      status = if token = cancellation
                 DilockC.consul_acquire_with_cancellation(@handle, wait_ms, lease_ms,
                   token.to_unsafe, pointerof(lease), pointerof(error))
               else
                 DilockC.consul_acquire(@handle, wait_ms, lease_ms, pointerof(lease), pointerof(error))
               end
      Dilock.raise_consul_error(status, error) unless status == 0
      raise ConsulError.new("native lease handle was null") unless lease
      ConsulLease.new(@provider, lease)
    end

    def acquire_default(cancellation : ConsulCancellation? = nil) : ConsulLease
      raise Error.new("lock handle is closed") if @closed
      error = uninitialized DilockC::Error
      lease = Pointer(Void).null
      status = if token = cancellation
                 DilockC.consul_acquire_default_with_cancellation(@handle, token.to_unsafe,
                   pointerof(lease), pointerof(error))
               else
                 DilockC.consul_acquire_default(@handle, pointerof(lease), pointerof(error))
               end
      Dilock.raise_consul_error(status, error) unless status == 0
      raise ConsulError.new("native lease handle was null") unless lease
      ConsulLease.new(@provider, lease)
    end

    # Returns nil only for an acknowledged busy result.
    def try_acquire(lease_ms : Int64 = @default_lease_ms) : ConsulLease?
      raise Error.new("lock handle is closed") if @closed
      ConsulInternal.validate_lease_ms(lease_ms)
      error = uninitialized DilockC::Error
      lease = Pointer(Void).null
      status = DilockC.consul_try_acquire(@handle, lease_ms, pointerof(lease), pointerof(error))
      return nil if status == 1
      Dilock.raise_consul_error(status, error) unless status == 0
      raise ConsulError.new("native lease handle was null") unless lease
      ConsulLease.new(@provider, lease)
    end

    def with_lease(wait_ms : Int64 = @default_wait_ms, lease_ms : Int64 = @default_lease_ms,
                   cancellation : ConsulCancellation? = nil, &operation : ConsulLease -> T) : T forall T
      lease = acquire(wait_ms, lease_ms, cancellation)
      ScopeInternal.run(lease, "consul", ->(value : ConsulLease) {
        case value.release
        when ConsulReleaseOutcome::Released        then ScopeCleanupOutcome::Released
        when ConsulReleaseOutcome::AlreadyNotOwned then ScopeCleanupOutcome::AlreadyNotOwned
        else                                           ScopeCleanupOutcome::Unknown
        end
      }, ->(value : ConsulLease) { value.close }, &operation)
    end

    def close : Nil
      return if @closed
      DilockC.consul_lock_destroy(@handle)
      @closed = true
    end
  end

  class ConsulLease
    def initialize(@provider : ConsulProvider, @lease : Pointer(Void))
      @closed = false
      @released = false
    end

    def valid? : Bool
      !@closed && DilockC.consul_lease_valid(@lease) != 0
    end

    def effective_ttl : Time::Span
      raise Error.new("lease is closed") if @closed
      error = uninitialized DilockC::Error
      ttl_ms = 0_i64
      status = DilockC.consul_lease_effective_ttl(@lease, pointerof(ttl_ms), pointerof(error))
      Dilock.raise_consul_error(status, error) unless status == 0
      ttl_ms.milliseconds
    end

    def fence : ConsulFence
      raise Error.new("lease is closed") if @closed
      error = uninitialized DilockC::Error
      native_fence = uninitialized DilockC::ConsulFence
      epoch_bytes = Bytes.new(MAX_COORDINATION_EPOCH_BYTES + 1)
      status = DilockC.consul_lease_fence(@lease, pointerof(native_fence), epoch_bytes.to_unsafe,
        epoch_bytes.size, pointerof(error))
      Dilock.raise_consul_error(status, error) unless status == 0
      length = native_fence.coordination_epoch_bytes
      raise ConsulError.new("native fence returned an invalid epoch length") if length > MAX_COORDINATION_EPOCH_BYTES
      epoch = String.new(epoch_bytes.to_slice[0, length])
      ConsulFence.new(epoch, native_fence.lock_index)
    end

    def renew : Bool
      raise Error.new("lease is closed") if @closed
      error = uninitialized DilockC::Error
      renewed = 0_i32
      status = DilockC.consul_lease_renew(@lease, pointerof(renewed), pointerof(error))
      Dilock.raise_consul_error(status, error) unless status == 0
      renewed != 0
    end

    def release : ConsulReleaseOutcome
      raise Error.new("lease is closed") if @closed
      error = uninitialized DilockC::Error
      outcome = -1_i32
      status = DilockC.consul_lease_release(@lease, pointerof(outcome), pointerof(error))
      Dilock.raise_consul_error(status, error) unless status == 0
      @released = true
      ConsulReleaseOutcome.from_value(outcome)
    end

    def close : Nil
      return if @closed
      DilockC.consul_lease_destroy(@lease)
      @closed = true
    end

    def released? : Bool
      @released
    end
  end

  def self.raise_consul_error(status : Int32, error : DilockC::Error) : NoReturn
    message = String.new(Pointer(UInt8).new(error.message.to_unsafe.address))
    message = "Consul operation failed with status #{status}" if message.empty?
    case status
    when 1 then raise ConsulBusy.new(message)
    when 2 then raise ConsulInvalidArgument.new(message)
    when 3 then raise ConsulCancelled.new(message)
    when 4 then raise ConsulWaitTimeout.new(message)
    when 5 then raise ConsulBackendUnavailable.new(message)
    when 6 then raise ConsulAcquisitionUnknown.new(message)
    when 7 then raise ConsulProtocolMismatch.new(message)
    when 8 then raise ConsulLeaseLost.new(message)
    when 9 then raise ConsulUnsupportedCapability.new(message)
    else        raise ConsulError.new(message)
    end
  end
end
