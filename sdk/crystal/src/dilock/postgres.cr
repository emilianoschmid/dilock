require "../dilock"
require "./etcd"

module Dilock
  # Synchronous C ABI facade for postgres-advisory-session-v1. The native
  # provider opens one private PostgreSQL session per successful acquisition.
  class PostgresError < Error; end

  class PostgresBusy < PostgresError; end

  class PostgresInvalidArgument < PostgresError; end

  class PostgresWaitTimeout < PostgresError; end

  class PostgresBackendUnavailable < PostgresError; end

  class PostgresAcquisitionUnknown < PostgresError; end

  class PostgresProtocolMismatch < PostgresError; end

  class PostgresLeaseLost < PostgresError; end

  class PostgresUnsupportedCapability < PostgresError; end

  enum PostgresReleaseOutcome
    Released
    AlreadyNotOwned
    Unknown
  end

  class PostgresProvider
    getter connection_string : String
    getter namespace : String
    getter coordination_epoch : String

    @provider : Pointer(Void)
    @lifecycle = EtcdProviderLifecycle.new

    def initialize(@connection_string : String, @coordination_epoch : String,
                   @namespace : String = "default", @wait_ms : Int64 = 5_000,
                   @lease_ms : Int64 = 30_000, @rpc_timeout_ms : Int64 = 2_000,
                   @cleanup_timeout_ms : Int64 = 2_000, @poll_interval_ms : Int64 = 25)
      Dilock.identity(@namespace, "validation")
      validate_string(@connection_string, "PostgreSQL connection string", false)
      validate_string(@coordination_epoch, "coordination epoch", false)
      raise ArgumentError.new("coordination epoch must be 1..1024 UTF-8 bytes") if @coordination_epoch.bytesize > MAX_COORDINATION_EPOCH_BYTES
      { {"wait_ms", @wait_ms}, {"lease_ms", @lease_ms}, {"rpc_timeout_ms", @rpc_timeout_ms},
       {"cleanup_timeout_ms", @cleanup_timeout_ms}, {"poll_interval_ms", @poll_interval_ms} }.each do |entry|
        raise ArgumentError.new("#{entry[0]} must be positive and no more than 86400000") unless 1 <= entry[1] <= MAX_ETCD_DURATION_MS
      end
      version = DilockC.abi_version
      raise PostgresError.new("unsupported Dilock C ABI version #{version}; expected 1") unless version == 1
      options = native_options
      error = uninitialized DilockC::Error
      provider = Pointer(Void).null
      status = DilockC.postgres_provider_create(pointerof(options), pointerof(provider), pointerof(error))
      Dilock.raise_postgres_error(status, error) unless status == 0
      raise PostgresError.new("native provider handle was null") unless provider
      @provider = provider
    end

    # Provisions metadata before the runtime-only provider is constructed.
    # Runtime construction remains read-only and validates the resulting row.
    def self.provision_namespace(connection_string : String, coordination_epoch : String,
                                 namespace : String = "default", wait_ms : Int64 = 5_000,
                                 lease_ms : Int64 = 30_000, rpc_timeout_ms : Int64 = 2_000,
                                 cleanup_timeout_ms : Int64 = 2_000, poll_interval_ms : Int64 = 25) : Nil
      Dilock.identity(namespace, "validation")
      raise ArgumentError.new("PostgreSQL connection string must not be empty") if connection_string.empty?
      raise ArgumentError.new("PostgreSQL connection string must be valid UTF-8") unless connection_string.valid_encoding?
      raise ArgumentError.new("PostgreSQL connection string must not contain NUL") if connection_string.includes?('\0')
      raise ArgumentError.new("coordination epoch must be 1..1024 UTF-8 bytes") if coordination_epoch.empty? || coordination_epoch.bytesize > MAX_COORDINATION_EPOCH_BYTES
      raise ArgumentError.new("coordination epoch must be valid UTF-8 and NUL-free") unless coordination_epoch.valid_encoding? && !coordination_epoch.includes?('\0')
      { {"wait_ms", wait_ms}, {"lease_ms", lease_ms}, {"rpc_timeout_ms", rpc_timeout_ms},
       {"cleanup_timeout_ms", cleanup_timeout_ms}, {"poll_interval_ms", poll_interval_ms} }.each do |entry|
        raise ArgumentError.new("#{entry[0]} must be positive and no more than 86400000") unless 1 <= entry[1] <= MAX_ETCD_DURATION_MS
      end
      version = DilockC.abi_version
      raise PostgresError.new("unsupported Dilock C ABI version #{version}; expected 1") unless version == 1

      options = uninitialized DilockC::PostgresOptions
      DilockC.postgres_options_init(pointerof(options))
      options.connection_string = connection_string.to_unsafe
      options.name_space = namespace.to_unsafe
      options.coordination_epoch = coordination_epoch.to_unsafe
      options.rpc_timeout_ms = rpc_timeout_ms
      options.cleanup_timeout_ms = cleanup_timeout_ms
      options.poll_interval_ms = poll_interval_ms
      options.default_wait_ms = wait_ms
      options.default_lease_ms = lease_ms
      error = uninitialized DilockC::Error
      status = DilockC.postgres_provision_namespace(pointerof(options), pointerof(error))
      Dilock.raise_postgres_error(status, error) unless status == 0
    end

    def provision_namespace : Nil
      self.class.provision_namespace(@connection_string, @coordination_epoch, @namespace,
        @wait_ms, @lease_ms, @rpc_timeout_ms, @cleanup_timeout_ms, @poll_interval_ms)
    end

    def lock(resource : String) : PostgresHandle
      Dilock.identity(@namespace, resource)
      @lifecycle.with_open_provider do
        error = uninitialized DilockC::Error
        handle = Pointer(Void).null
        status = DilockC.postgres_lock_create(@provider, resource.to_unsafe, pointerof(handle), pointerof(error))
        Dilock.raise_postgres_error(status, error) unless status == 0
        raise PostgresError.new("native lock handle was null") unless handle
        PostgresHandle.new(self, handle, @wait_ms, @lease_ms)
      end
    end

    def close : Nil
      @lifecycle.close { DilockC.postgres_provider_destroy(@provider) }
    end

    private def native_options : DilockC::PostgresOptions
      options = uninitialized DilockC::PostgresOptions
      DilockC.postgres_options_init(pointerof(options))
      options.connection_string = @connection_string.to_unsafe
      options.name_space = @namespace.to_unsafe
      options.coordination_epoch = @coordination_epoch.to_unsafe
      options.rpc_timeout_ms = @rpc_timeout_ms
      options.cleanup_timeout_ms = @cleanup_timeout_ms
      options.poll_interval_ms = @poll_interval_ms
      options.default_wait_ms = @wait_ms
      options.default_lease_ms = @lease_ms
      options
    end

    private def validate_string(value : String, label : String, allow_empty : Bool) : Nil
      raise ArgumentError.new("#{label} must not be empty") if !allow_empty && value.empty?
      raise ArgumentError.new("#{label} must be valid UTF-8") unless value.valid_encoding?
      raise ArgumentError.new("#{label} must not contain NUL") if value.includes?('\0')
    end
  end

  class PostgresHandle
    def initialize(@provider : PostgresProvider, @handle : Pointer(Void),
                   @default_wait_ms : Int64, @default_lease_ms : Int64)
      @closed = false
    end

    def acquire(wait_ms : Int64 = @default_wait_ms, lease_ms : Int64 = @default_lease_ms) : PostgresLease
      raise Error.new("lock handle is closed") if @closed
      error = uninitialized DilockC::Error
      lease = Pointer(Void).null
      status = DilockC.postgres_acquire(@handle, wait_ms, lease_ms, pointerof(lease), pointerof(error))
      Dilock.raise_postgres_error(status, error) unless status == 0
      raise PostgresError.new("native lease handle was null") unless lease
      PostgresLease.new(lease)
    end

    def acquire_default : PostgresLease
      raise Error.new("lock handle is closed") if @closed
      error = uninitialized DilockC::Error
      lease = Pointer(Void).null
      status = DilockC.postgres_acquire_default(@handle, pointerof(lease), pointerof(error))
      Dilock.raise_postgres_error(status, error) unless status == 0
      raise PostgresError.new("native lease handle was null") unless lease
      PostgresLease.new(lease)
    end

    def try_acquire : PostgresLease?
      raise Error.new("lock handle is closed") if @closed
      error = uninitialized DilockC::Error
      lease = Pointer(Void).null
      status = DilockC.postgres_try_acquire(@handle, pointerof(lease), pointerof(error))
      return nil if status == 1
      Dilock.raise_postgres_error(status, error) unless status == 0
      raise PostgresError.new("native lease handle was null") unless lease
      PostgresLease.new(lease)
    end

    def with_lease(&operation : PostgresLease -> T) : T forall T
      lease = acquire_default
      begin
        value = yield lease
        outcome = lease.release
        raise PostgresError.new("PostgreSQL scope cleanup was #{outcome}") unless outcome == PostgresReleaseOutcome::Released
        value
      ensure
        lease.close
      end
    end

    def close : Nil
      return if @closed
      DilockC.postgres_lock_destroy(@handle)
      @closed = true
    end
  end

  class PostgresLease
    def initialize(@lease : Pointer(Void))
      @closed = false
    end

    def valid? : Bool
      !@closed && DilockC.postgres_lease_valid(@lease) != 0
    end

    def renew : Bool
      raise Error.new("lease is closed") if @closed
      error = uninitialized DilockC::Error
      renewed = 0_i32
      status = DilockC.postgres_lease_renew(@lease, pointerof(renewed), pointerof(error))
      Dilock.raise_postgres_error(status, error) unless status == 0
      renewed != 0
    end

    def release : PostgresReleaseOutcome
      raise Error.new("lease is closed") if @closed
      error = uninitialized DilockC::Error
      outcome = -1_i32
      status = DilockC.postgres_lease_release(@lease, pointerof(outcome), pointerof(error))
      Dilock.raise_postgres_error(status, error) unless status == 0
      PostgresReleaseOutcome.from_value(outcome)
    end

    def close : Nil
      return if @closed
      DilockC.postgres_lease_destroy(@lease)
      @closed = true
    end
  end

  def self.raise_postgres_error(status : Int32, error : DilockC::Error) : NoReturn
    message = String.new(Pointer(UInt8).new(error.message.to_unsafe.address))
    message = "PostgreSQL operation failed with status #{status}" if message.empty?
    case status
    when 1 then raise PostgresBusy.new(message)
    when 2 then raise PostgresInvalidArgument.new(message)
    when 4 then raise PostgresWaitTimeout.new(message)
    when 5 then raise PostgresBackendUnavailable.new(message)
    when 6 then raise PostgresAcquisitionUnknown.new(message)
    when 7 then raise PostgresProtocolMismatch.new(message)
    when 8 then raise PostgresLeaseLost.new(message)
    when 9 then raise PostgresUnsupportedCapability.new(message)
    else        raise PostgresError.new(message)
    end
  end
end
