@[Link("dilock_c")]
lib DilockC
  struct Error
    code : Int32
    message : StaticArray(UInt8, 384)
  end

  struct EtcdOptions
    endpoint : UInt8*
    name_space : UInt8*
    coordination_epoch : UInt8*
    tls_root_certificates : UInt8*
    tls_client_certificate : UInt8*
    tls_client_private_key : UInt8*
    cleanup_callback : Void*
    cleanup_user_data : Void*
    rpc_timeout_ms : Int64
    cleanup_timeout_ms : Int64
    safety_margin_ms : Int64
    default_wait_ms : Int64
    default_lease_ms : Int64
    auto_renew : Int32
  end

  struct ConsulOptions
    endpoint : UInt8*
    name_space : UInt8*
    coordination_epoch : UInt8*
    acl_token : UInt8*
    cleanup_callback : Void*
    cleanup_user_data : Void*
    rpc_timeout_ms : Int64
    cleanup_timeout_ms : Int64
    poll_interval_ms : Int64
    default_wait_ms : Int64
    default_lease_ms : Int64
  end

  struct Fence
    create_revision : Int64
    coordination_epoch_bytes : LibC::SizeT
  end

  struct ConsulFence
    lock_index : UInt64
    coordination_epoch_bytes : LibC::SizeT
  end

  fun etcd_options_init = dilock_etcd_options_init(options : EtcdOptions*) : Void
  fun consul_options_init = dilock_consul_options_init(options : ConsulOptions*) : Void
  fun abi_version = dilock_c_abi_version : UInt32
  fun cancellation_create = dilock_cancellation_create(error : Error*) : Void*
  fun cancellation_request = dilock_cancellation_request(cancellation : Void*) : Void
  fun cancellation_destroy = dilock_cancellation_destroy(cancellation : Void*) : Void
  fun provider_create = dilock_etcd_provider_create(options : EtcdOptions*, out : Void**, error : Error*) : Int32
  fun provider_destroy = dilock_etcd_provider_destroy(provider : Void*) : Void
  fun lock_create = dilock_etcd_lock_create(provider : Void*, resource : UInt8*, out : Void**, error : Error*) : Int32
  fun lock_destroy = dilock_etcd_lock_destroy(lock : Void*) : Void
  fun acquire = dilock_etcd_acquire(lock : Void*, wait_ms : Int64, lease_ms : Int64, out : Void**, error : Error*) : Int32
  fun acquire_with_cancellation = dilock_etcd_acquire_with_cancellation(lock : Void*, wait_ms : Int64, lease_ms : Int64, cancellation : Void*, out : Void**, error : Error*) : Int32
  fun lease_valid = dilock_etcd_lease_is_valid(lease : Void*) : Int32
  fun lease_effective_ttl = dilock_etcd_lease_effective_ttl_ms(lease : Void*, ttl_ms : Int64*, error : Error*) : Int32
  fun lease_fence = dilock_etcd_lease_fence(lease : Void*, fence : Fence*, epoch : UInt8*, epoch_capacity : LibC::SizeT, error : Error*) : Int32
  fun lease_release = dilock_etcd_lease_release(lease : Void*, outcome : Int32*, error : Error*) : Int32
  fun lease_destroy = dilock_etcd_lease_destroy(lease : Void*) : Void
  fun try_acquire = dilock_etcd_try_acquire(lock : Void*, lease_ms : Int64, out : Void**, error : Error*) : Int32
  fun consul_provider_create = dilock_consul_provider_create(options : ConsulOptions*, out : Void**, error : Error*) : Int32
  fun consul_provider_destroy = dilock_consul_provider_destroy(provider : Void*) : Void
  fun consul_lock_create = dilock_consul_lock_create(provider : Void*, resource : UInt8*, out : Void**, error : Error*) : Int32
  fun consul_lock_destroy = dilock_consul_lock_destroy(lock : Void*) : Void
  fun consul_acquire = dilock_consul_acquire(lock : Void*, wait_ms : Int64, lease_ms : Int64, out : Void**, error : Error*) : Int32
  fun consul_acquire_with_cancellation = dilock_consul_acquire_with_cancellation(lock : Void*, wait_ms : Int64, lease_ms : Int64, cancellation : Void*, out : Void**, error : Error*) : Int32
  fun consul_acquire_default = dilock_consul_acquire_default(lock : Void*, out : Void**, error : Error*) : Int32
  fun consul_acquire_default_with_cancellation = dilock_consul_acquire_default_with_cancellation(lock : Void*, cancellation : Void*, out : Void**, error : Error*) : Int32
  fun consul_try_acquire = dilock_consul_try_acquire(lock : Void*, lease_ms : Int64, out : Void**, error : Error*) : Int32
  fun consul_lease_valid = dilock_consul_lease_is_valid(lease : Void*) : Int32
  fun consul_lease_effective_ttl = dilock_consul_lease_effective_ttl_ms(lease : Void*, ttl_ms : Int64*, error : Error*) : Int32
  fun consul_lease_fence = dilock_consul_lease_fence(lease : Void*, fence : ConsulFence*, epoch : UInt8*, epoch_capacity : LibC::SizeT, error : Error*) : Int32
  fun consul_lease_renew = dilock_consul_lease_renew(lease : Void*, renewed : Int32*, error : Error*) : Int32
  fun consul_lease_release = dilock_consul_lease_release(lease : Void*, outcome : Int32*, error : Error*) : Int32
  fun consul_lease_destroy = dilock_consul_lease_destroy(lease : Void*) : Void
  fun consul_provision_namespace = dilock_consul_provision_namespace(options : ConsulOptions*, error : Error*) : Int32
end

module Dilock
  MAX_COORDINATION_EPOCH_BYTES =           1024
  MAX_ETCD_DURATION_MS         = 86_400_000_i64

  module EtcdInternal
    def self.validate_cancellation_open(closed : Bool) : Nil
      raise Error.new("cancellation token is closed") if closed
    end

    def self.validate_positive_wait_ms(value : Int64) : Nil
      raise ArgumentError.new("wait_ms must be between 1 and 86400000") unless 1 <= value <= MAX_ETCD_DURATION_MS
    end

    def self.validate_lease_ms(value : Int64, rpc_timeout_ms : Int64, safety_margin_ms : Int64) : Nil
      unless 1 <= rpc_timeout_ms <= MAX_ETCD_DURATION_MS && 0 <= safety_margin_ms <= MAX_ETCD_DURATION_MS
        raise ArgumentError.new("RPC timeout and safety margin must be valid before validating lease_ms")
      end
      unless 1 <= value <= MAX_ETCD_DURATION_MS && value > rpc_timeout_ms + safety_margin_ms
        raise ArgumentError.new("lease_ms must be between 1 and 86400000 and exceed rpc_timeout_ms plus safety_margin_ms")
      end
    end

    def self.validate_timeout_options(rpc_timeout_ms : Int64, cleanup_timeout_ms : Int64,
                                      safety_margin_ms : Int64) : Nil
      unless 1 <= rpc_timeout_ms <= MAX_ETCD_DURATION_MS
        raise ArgumentError.new("rpc_timeout_ms must be between 1 and 86400000")
      end
      unless 1 <= cleanup_timeout_ms <= MAX_ETCD_DURATION_MS
        raise ArgumentError.new("cleanup_timeout_ms must be between 1 and 86400000")
      end
      unless 0 <= safety_margin_ms <= MAX_ETCD_DURATION_MS
        raise ArgumentError.new("safety_margin_ms must be between 0 and 86400000")
      end
    end

    def self.validate_c_string(value : String, label : String, allow_empty : Bool = true) : Nil
      raise ArgumentError.new("#{label} must not be empty") if !allow_empty && value.empty?
      raise ArgumentError.new("#{label} must be valid UTF-8") unless value.valid_encoding?
      raise ArgumentError.new("#{label} must not contain NUL") if value.includes?('\0')
    end
  end

  # Base error reported by the shared C ABI runtime.
  class EtcdError < Error; end

  class EtcdBusy < EtcdError; end

  class EtcdInvalidArgument < EtcdError; end

  class EtcdCancelled < EtcdError; end

  class EtcdWaitTimeout < EtcdError; end

  class EtcdBackendUnavailable < EtcdError; end

  class EtcdAcquisitionUnknown < EtcdError; end

  class EtcdProtocolMismatch < EtcdError; end

  class EtcdLeaseLost < EtcdError; end

  class EtcdUnsupportedCapability < EtcdError; end

  # Thread-safe token for synchronous acquisition cancellation. Keep it alive
  # until the blocking acquire call has returned.
  class EtcdCancellation
    @token : Pointer(Void)
    @closed : Bool

    def initialize
      error = uninitialized DilockC::Error
      token = DilockC.cancellation_create(pointerof(error))
      Dilock.raise_etcd_error(error.code, error) unless token
      @token = token
      @closed = false
    end

    def request : Nil
      raise Error.new("cancellation token is closed") if @closed
      DilockC.cancellation_request(@token)
    end

    def close : Nil
      return if @closed
      DilockC.cancellation_destroy(@token)
      @closed = true
    end

    # Internal FFI bridge used by the blocking handle call.
    def to_unsafe : Pointer(Void)
      EtcdInternal.validate_cancellation_open(@closed)
      @token
    end
  end

  enum EtcdReleaseOutcome
    Released
    AlreadyNotOwned
    Unknown
  end

  struct EtcdFence
    getter coordination_epoch : String
    getter create_revision : Int64

    def initialize(@coordination_epoch : String, @create_revision : Int64); end
  end

  # C ABI-backed etcd client. Requires the matching Dilock C ABI shared
  # library and its C++/gRPC runtime dependencies at link and load time.
  # The provider owns one etcd channel; handles and leases must be explicitly
  # closed. This facade is synchronous and experimental while C ABI v1 is a
  # draft. Configure one approved coordination epoch shared by all clients.
  class EtcdProvider
    getter endpoint : String
    getter namespace : String
    getter coordination_epoch : String

    @provider : Pointer(Void)
    @lifecycle = EtcdProviderLifecycle.new

    def initialize(@coordination_epoch : String, @endpoint : String = "127.0.0.1:2379",
                   @namespace : String = "default", @wait_ms : Int64 = 5_000,
                   @lease_ms : Int64 = 30_000, @rpc_timeout_ms : Int64 = 2_000,
                   @cleanup_timeout_ms : Int64 = 2_000, @safety_margin_ms : Int64 = 25,
                   @auto_renew : Bool = false, @tls_root_certificates : String? = nil,
                   @tls_client_certificate : String? = nil, @tls_client_private_key : String? = nil)
      Dilock.identity(@namespace, "validation")
      EtcdInternal.validate_c_string(@coordination_epoch, "coordination epoch", allow_empty: false)
      raise ArgumentError.new("coordination epoch must be 1..1024 UTF-8 bytes") if @coordination_epoch.bytesize > MAX_COORDINATION_EPOCH_BYTES
      EtcdInternal.validate_c_string(@endpoint, "endpoint", allow_empty: false)
      [
        {"TLS root certificates", @tls_root_certificates},
        {"TLS client certificate", @tls_client_certificate},
        {"TLS client private key", @tls_client_private_key},
      ].each do |label, value|
        EtcdInternal.validate_c_string(value, label) if value
      end
      EtcdInternal.validate_positive_wait_ms(@wait_ms)
      EtcdInternal.validate_timeout_options(@rpc_timeout_ms, @cleanup_timeout_ms, @safety_margin_ms)
      EtcdInternal.validate_lease_ms(@lease_ms, @rpc_timeout_ms, @safety_margin_ms)
      version = DilockC.abi_version
      raise EtcdError.new("unsupported Dilock C ABI version #{version}; expected 1") unless version == 1

      options = uninitialized DilockC::EtcdOptions
      DilockC.etcd_options_init(pointerof(options))
      options.endpoint = @endpoint.to_unsafe
      options.name_space = @namespace.to_unsafe
      options.coordination_epoch = @coordination_epoch.to_unsafe
      options.tls_root_certificates = @tls_root_certificates.try(&.to_unsafe) || Pointer(UInt8).null
      options.tls_client_certificate = @tls_client_certificate.try(&.to_unsafe) || Pointer(UInt8).null
      options.tls_client_private_key = @tls_client_private_key.try(&.to_unsafe) || Pointer(UInt8).null
      options.rpc_timeout_ms = @rpc_timeout_ms
      options.cleanup_timeout_ms = @cleanup_timeout_ms
      options.safety_margin_ms = @safety_margin_ms
      options.default_wait_ms = @wait_ms
      options.default_lease_ms = @lease_ms
      options.auto_renew = @auto_renew ? 1 : 0

      error = uninitialized DilockC::Error
      provider = Pointer(Void).null
      status = DilockC.provider_create(pointerof(options), pointerof(provider), pointerof(error))
      Dilock.raise_etcd_error(status, error) unless status == 0 && provider
      @provider = provider
    end

    # Creates a reusable resource handle. The native C ABI validates namespace
    # metadata before any acquisition is attempted.
    def lock(resource : String) : EtcdHandle
      Dilock.identity(@namespace, resource)
      @lifecycle.with_open_provider do
        error = uninitialized DilockC::Error
        handle = Pointer(Void).null
        status = DilockC.lock_create(@provider, resource.to_unsafe, pointerof(handle), pointerof(error))
        Dilock.raise_etcd_error(status, error) unless status == 0 && handle
        EtcdHandle.new(self, handle, @wait_ms, @lease_ms, @rpc_timeout_ms, @safety_margin_ms)
      end
    end

    # Destroys the provider wrapper. Existing handles/leases retain their own
    # native shared state, but callers should close them before closing this.
    def close : Nil
      @lifecycle.close do
        DilockC.provider_destroy(@provider)
      end
    end
  end

  # Serializes native provider destruction with lock creation, which dereferences
  # the raw provider pointer. The FFI call stays inside the gate so close cannot
  # free that pointer between the open check and lock_create.
  class EtcdProviderLifecycle
    @mutex = Thread::Mutex.new
    @closed = false

    def with_open_provider(&)
      @mutex.synchronize do
        raise Error.new("provider is closed") if @closed
        yield
      end
    end

    def close(&destroy : ->)
      @mutex.synchronize do
        unless @closed
          destroy.call
          @closed = true
        end
        nil
      end
    end
  end

  # Reusable etcd lock handle. Acquisition blocks until the server grants the
  # native lock or the wait budget expires; failures remain typed errors.
  class EtcdHandle
    def initialize(@provider : EtcdProvider, @handle : Pointer(Void), @default_wait_ms : Int64,
                   @default_lease_ms : Int64, @rpc_timeout_ms : Int64, @safety_margin_ms : Int64)
      @closed = false
    end

    def acquire(wait_ms : Int64 = @default_wait_ms, lease_ms : Int64 = @default_lease_ms,
                cancellation : EtcdCancellation? = nil) : EtcdLease
      raise Error.new("lock handle is closed") if @closed
      EtcdInternal.validate_positive_wait_ms(wait_ms)
      EtcdInternal.validate_lease_ms(lease_ms, @rpc_timeout_ms, @safety_margin_ms)
      error = uninitialized DilockC::Error
      lease = Pointer(Void).null
      status = if token = cancellation
                 DilockC.acquire_with_cancellation(@handle, wait_ms, lease_ms, token.to_unsafe,
                   pointerof(lease), pointerof(error))
               else
                 DilockC.acquire(@handle, wait_ms, lease_ms, pointerof(lease), pointerof(error))
               end
      Dilock.raise_etcd_error(status, error) unless status == 0 && lease
      EtcdLease.new(@provider, lease)
    end

    # Acquires with the provider defaults unless durations are supplied,
    # yields the verified lease, performs one acknowledged release attempt,
    # and disposes the local native wrapper. When callback and cleanup both
    # fail, ScopeCallbackError keeps the callback as its cause and exposes the
    # cleanup outcome. The provider and this handle remain reusable.
    def with_lease(wait_ms : Int64 = @default_wait_ms, lease_ms : Int64 = @default_lease_ms,
                   cancellation : EtcdCancellation? = nil, &operation : EtcdLease -> T) : T forall T
      lease = acquire(wait_ms, lease_ms, cancellation)
      ScopeInternal.run(lease, "etcd", ->(value : EtcdLease) {
        case value.release
        when EtcdReleaseOutcome::Released        then ScopeCleanupOutcome::Released
        when EtcdReleaseOutcome::AlreadyNotOwned then ScopeCleanupOutcome::AlreadyNotOwned
        else                                          ScopeCleanupOutcome::Unknown
        end
      }, ->(value : EtcdLease) { value.close }, &operation)
    end

    # etcd's native Lock RPC has no safe non-blocking busy result.
    def try_acquire(lease_ms : Int64 = @default_lease_ms) : EtcdLease
      raise Error.new("lock handle is closed") if @closed
      EtcdInternal.validate_lease_ms(lease_ms, @rpc_timeout_ms, @safety_margin_ms)
      error = uninitialized DilockC::Error
      lease = Pointer(Void).null
      status = DilockC.try_acquire(@handle, lease_ms, pointerof(lease), pointerof(error))
      Dilock.raise_etcd_error(status, error)
      raise EtcdUnsupportedCapability.new("etcd does not provide non-blocking try_acquire")
    end

    def close : Nil
      return if @closed
      DilockC.lock_destroy(@handle)
      @closed = true
    end
  end

  # One etcd acquisition. Release is explicit; close/destroy of an unreleased
  # lease triggers only the C ABI's bounded best-effort cleanup and cannot
  # return its acknowledgement. No network work is performed by a finalizer.
  class EtcdLease
    def initialize(@provider : EtcdProvider, @lease : Pointer(Void))
      @closed = false
      @released = false
    end

    def valid? : Bool
      !@closed && DilockC.lease_valid(@lease) != 0
    end

    def effective_ttl : Time::Span
      raise Error.new("lease is closed") if @closed
      error = uninitialized DilockC::Error
      ttl_ms = 0_i64
      status = DilockC.lease_effective_ttl(@lease, pointerof(ttl_ms), pointerof(error))
      Dilock.raise_etcd_error(status, error) unless status == 0
      ttl_ms.milliseconds
    end

    # Returns the epoch and verified create_revision for resource-side fencing.
    def fence : EtcdFence
      raise Error.new("lease is closed") if @closed
      error = uninitialized DilockC::Error
      native_fence = uninitialized DilockC::Fence
      # The C ABI writes a trailing NUL and requires capacity > byte length.
      # Epochs up to the shared 1024-byte limit therefore need 1025 bytes here.
      epoch_bytes = Bytes.new(MAX_COORDINATION_EPOCH_BYTES + 1)
      status = DilockC.lease_fence(@lease, pointerof(native_fence), epoch_bytes.to_unsafe,
        epoch_bytes.size, pointerof(error))
      Dilock.raise_etcd_error(status, error) unless status == 0
      length = native_fence.coordination_epoch_bytes
      raise EtcdError.new("native fence returned an invalid epoch length") if length > epoch_bytes.size
      EtcdFence.new(String.new(epoch_bytes.to_slice[0, length]), native_fence.create_revision)
    end

    # Idempotently reports the first native release observation.
    def release : EtcdReleaseOutcome
      raise Error.new("lease is closed") if @closed
      error = uninitialized DilockC::Error
      outcome = -1_i32
      status = DilockC.lease_release(@lease, pointerof(outcome), pointerof(error))
      Dilock.raise_etcd_error(status, error) unless status == 0
      @released = true
      EtcdReleaseOutcome.from_value(outcome)
    end

    def close : Nil
      return if @closed
      DilockC.lease_destroy(@lease)
      @closed = true
    end

    def released? : Bool
      @released
    end
  end

  def self.raise_etcd_error(status : Int32, error : DilockC::Error) : NoReturn
    message = String.new(Pointer(UInt8).new(error.message.to_unsafe.address))
    message = "etcd operation failed with status #{status}" if message.empty?
    case status
    when 1 then raise EtcdBusy.new(message)
    when 2 then raise EtcdInvalidArgument.new(message)
    when 3 then raise EtcdCancelled.new(message)
    when 4 then raise EtcdWaitTimeout.new(message)
    when 5 then raise EtcdBackendUnavailable.new(message)
    when 6 then raise EtcdAcquisitionUnknown.new(message)
    when 7 then raise EtcdProtocolMismatch.new(message)
    when 8 then raise EtcdLeaseLost.new(message)
    when 9 then raise EtcdUnsupportedCapability.new(message)
    else        raise EtcdError.new(message)
    end
  end
end
