require "json"
require "digest/sha256"
require "random/secure"
require "set"
require "uri"
require "db"
require "redis"

module Dilock
  # Canonical namespace/resource key identifier, independent of any backend connection.
  def self.identity(namespace : String, resource : String) : String
    raise ArgumentError.new("namespace must match [a-z0-9][a-z0-9_.-]{0,63}") unless namespace.matches?(/\A[a-z0-9][a-z0-9_.-]{0,63}\z/)
    raise ArgumentError.new("resource must be 1..1024 valid UTF-8 bytes without NUL") if resource.empty? || !resource.valid_encoding? || resource.bytesize > 1024 || resource.includes?('\0')
    bytes = IO::Memory.new
    bytes.write("dilock.lock.identity".to_slice)
    bytes.write_byte(0_u8)
    bytes.write_bytes(namespace.bytesize.to_u32, IO::ByteFormat::BigEndian)
    bytes.write(namespace.to_slice)
    bytes.write_bytes(resource.bytesize.to_u32, IO::ByteFormat::BigEndian)
    bytes.write(resource.to_slice)
    Digest::SHA256.hexdigest(bytes.to_slice)
  end

  class Error < Exception; end

  class ProviderBusy < Error; end

  class ProtocolMismatch < Error; end

  class BackendUnavailable < Error; end

  class AcquisitionUnknown < Error; end

  class UnsupportedCapability < Error; end

  enum ScopeCleanupOutcome
    Released
    AlreadyNotOwned
    Unknown
  end

  # Raised after successful callback work when scope cleanup was not confirmed.
  # `AlreadyNotOwned` means ownership was lost; `Unknown` means the cleanup
  # acknowledgement could not be established.
  class ScopeReleaseError < Error
    getter backend : String
    getter outcome : ScopeCleanupOutcome
    getter cleanup_errors : Array(Exception)

    def initialize(@backend : String, @outcome : ScopeCleanupOutcome,
                   @cleanup_errors : Array(Exception) = [] of Exception)
      detail = @cleanup_errors.empty? ? "" : ": #{@cleanup_errors.first.message}"
      super("#{@backend} scope cleanup was #{@outcome.to_s.underscore}#{detail}")
    end

    def lost? : Bool
      @outcome == ScopeCleanupOutcome::AlreadyNotOwned
    end
  end

  # When both protected work and cleanup fail, the callback exception remains
  # the primary cause while this error also exposes the cleanup result.
  class ScopeCallbackError < Error
    getter callback_error : Exception
    getter release_error : ScopeReleaseError

    def initialize(@callback_error : Exception, @release_error : ScopeReleaseError)
      super("scope callback failed: #{@callback_error.message}; #{@release_error.message}", @callback_error)
    end
  end

  module ScopeInternal
    # Runs cleanup exactly once after the callback and always performs local
    # handle disposal. Crystal exceptions have a read-only cause, so when both
    # callback and cleanup fail a composite keeps the callback as its cause and
    # exposes the cleanup failure as structured data.
    def self.run(lease : T, backend : String,
                 release : Proc(T, ScopeCleanupOutcome), close : Proc(T, Nil)? = nil,
                 &operation : T -> U) : U forall T, U
      value = uninitialized U
      callback_error : Exception? = nil
      begin
        value = yield lease
      rescue ex
        callback_error = ex
      end

      outcome = ScopeCleanupOutcome::Unknown
      cleanup_errors = [] of Exception
      begin
        outcome = release.call(lease)
      rescue ex
        cleanup_errors << ex
      end
      if disposer = close
        begin
          disposer.call(lease)
        rescue ex
          cleanup_errors << ex
        end
      end

      release_error = nil
      if outcome != ScopeCleanupOutcome::Released || !cleanup_errors.empty?
        release_error = ScopeReleaseError.new(backend, outcome, cleanup_errors)
      end

      if work_error = callback_error
        if cleanup_failure = release_error
          raise ScopeCallbackError.new(work_error, cleanup_failure)
        end
        raise work_error
      end
      raise release_error if release_error
      value
    end
  end

  enum ReleaseOutcome
    Released
    AlreadyNotOwned
    Unknown
  end

  module Internal
    def self.safety_margin_ms(ttl_ms : Int32) : Int32
      {10, (ttl_ms + 99) // 100}.max
    end

    def self.remaining_lease_budget_ms(ttl_ms : Int32, elapsed_ms : Float64) : Float64
      ttl_ms - safety_margin_ms(ttl_ms) - elapsed_ms
    end

    # Monotonic Time::Instant readings are required here: wall-clock corrections must not
    # extend or shorten the local validity budget used for an acquired lease.
    def self.acquisition_elapsed_ms(dispatched_at : Time::Instant, acknowledged_at : Time::Instant) : Float64
      (acknowledged_at - dispatched_at).total_milliseconds
    end

    def self.validate_redis_metadata!(raw : String, epoch : String) : Nil
      raise ProtocolMismatch.new("namespace metadata exceeds 8192 bytes") if raw.bytesize > 8192
      begin
        parsed = JSON.parse(raw)
        unless parsed.raw.is_a?(Hash(String, JSON::Any))
          raise ProtocolMismatch.new("namespace metadata must be a JSON object")
        end
        metadata = parsed.as_h
        protocol_major = metadata["protocol_major"]?
        unless protocol_major && protocol_major.raw.is_a?(Int64) && protocol_major.as_i == 1
          raise ProtocolMismatch.new("namespace metadata has an incompatible protocol major")
        end
        expected = {
          "identity_scheme"    => "sha256-framed-utf8-v1",
          "profile"            => "redis-single-authority-v1",
          "coordination_epoch" => epoch,
          "fixed_lease_ms"     => nil,
        }
        expected.each do |key, value|
          raise ProtocolMismatch.new("incompatible namespace metadata") unless metadata[key]?.try(&.raw) == value
        end
        nil
      rescue ex : ProtocolMismatch
        raise ex
      rescue ex : JSON::ParseException
        raise ProtocolMismatch.new("invalid namespace metadata")
      end
    end

    # A nil SET NX reply is contention only if the key remains compatible with
    # the lock protocol (or expired before this read-only classification).
    def self.classify_contention(&) : Nil
      key_type = yield
      return if key_type == "string" || key_type == "none"
      raise ProtocolMismatch.new("lock key has an incompatible Redis type")
    rescue ex : ProtocolMismatch
      raise ex
    rescue
      raise BackendUnavailable.new("Redis TYPE check failed after contention")
    end
  end

  # Serializes release attempts and caches their exact terminal observation,
  # including an unknown acknowledgement.
  class ReleaseOnce
    @mutex = Mutex.new
    @outcome : ReleaseOutcome? = nil

    def run(&action : -> ReleaseOutcome) : ReleaseOutcome
      @mutex.synchronize do
        return @outcome.not_nil! if @outcome

        @outcome = begin
          action.call
        rescue
          ReleaseOutcome::Unknown
        end
      end
    end
  end

  # Serializes provider open/close transitions with acquisition reservations.
  # Kept independent of the Redis client so the lifecycle contract is testable
  # without backend I/O.
  class ProviderLifecycle
    @mutex = Mutex.new
    @closed = false
    @active_acquisitions = 0

    def ensure_open!
      @mutex.synchronize do
        raise Error.new("provider is closed") if @closed
      end
    end

    def with_acquisition(&)
      @mutex.synchronize do
        raise Error.new("provider is closed") if @closed
        @active_acquisitions += 1
      end
      begin
        yield
      ensure
        @mutex.synchronize do
          @active_acquisitions -= 1
        end
      end
    end

    # Returns false if another caller already closed the provider. If an
    # acquisition is in flight, leave it open so the caller can retry later.
    def close! : Bool
      @mutex.synchronize do
        return false if @closed
        if @active_acquisitions > 0
          raise ProviderBusy.new("cannot close provider while an acquisition is in progress")
        end
        @closed = true
        true
      end
    end
  end

  struct Options
    getter ttl_ms : Int32
    getter require_fencing : Bool

    def initialize(@ttl_ms : Int32 = 30_000, @require_fencing : Bool = false)
      raise ArgumentError.new("ttl_ms must be between 1 and 86400000") unless 1 <= @ttl_ms <= 86_400_000
    end
  end

  # Synchronous, thread-safe pooled Redis provider. Redis retry attempts are
  # disabled because retrying a dispatched SET can hide an ambiguous outcome.
  class RedisProvider
    getter namespace : String

    def initialize(url : String, @namespace : String = "default", @epoch : String = "")
      Dilock.identity(@namespace, "validation")
      raise ArgumentError.new("epoch must be non-empty and at most 1024 UTF-8 bytes") if @epoch.empty? || @epoch.bytesize > 1024
      uri = URI.parse(url)
      params = URI::Params.parse(uri.query || "")
      params["retry_attempts"] = "0"
      params["max_pool_size"] ||= "8"
      uri.query = params.to_s
      @redis = Redis::Client.new(uri)
      @lifecycle = ProviderLifecycle.new
      @mutex = Mutex.new
      @leases = Set(Lease).new
    end

    def lock(resource : String) : Handle
      @lifecycle.ensure_open!
      Handle.new(self, "dilock:{#{Dilock.identity(@namespace, resource)}}:lease")
    end

    def close
      return unless @lifecycle.close!
      leases = @mutex.synchronize do
        @leases.to_a
      end
      leases.each(&.release)
      @redis.close
    end

    getter redis : Redis::Client
    getter epoch : String

    # Reserves the provider lifecycle before any acquisition I/O. Closing while
    # the reservation is active fails explicitly, so no lease can be tracked
    # after the provider has closed its Redis client.
    def with_acquisition(&)
      @lifecycle.with_acquisition do
        yield
      end
    end

    def track(lease : Lease)
      @mutex.synchronize { @leases << lease }
    end

    def untrack(lease : Lease)
      @mutex.synchronize { @leases.delete(lease) }
    end

    def metadata!
      raw = @redis.get("dilock:namespace:#{@namespace}:meta")
      raise ProtocolMismatch.new("missing namespace metadata") unless raw
      Internal.validate_redis_metadata!(raw, @epoch)
    rescue ex : ProtocolMismatch
      raise ex
    rescue ex
      raise BackendUnavailable.new("namespace read failed")
    end
  end

  class Handle
    def initialize(@provider : RedisProvider, @key : String); end

    # Returns nil only for an acknowledged SET NX contention result.
    def try_acquire(options : Options = Options.new) : Lease?
      raise UnsupportedCapability.new("Redis does not provide strong fencing") if options.require_fencing
      @provider.with_acquisition do
        try_acquire_reserved(options)
      end
    end

    # Attempts acquisition once, runs the protected callback, then releases
    # exactly once. Returns nil when Redis acknowledged contention. A
    # configured call passes an Options value; omitted options use the 30s TTL.
    # If callback work raises and cleanup is also unconfirmed, ScopeCallbackError
    # preserves the callback as its cause and exposes release_error.
    def with_lease(options : Options = Options.new, &operation : Lease -> T) : T? forall T
      lease = try_acquire(options)
      return nil unless lease

      ScopeInternal.run(lease, "redis", ->(value : Lease) {
        case value.release
        when ReleaseOutcome::Released        then ScopeCleanupOutcome::Released
        when ReleaseOutcome::AlreadyNotOwned then ScopeCleanupOutcome::AlreadyNotOwned
        else                                      ScopeCleanupOutcome::Unknown
        end
      }, &operation)
    end

    private def try_acquire_reserved(options : Options) : Lease?
      @provider.metadata!
      owner = "dilock/1/#{Random::Secure.hex(32)}"
      dispatched_at = Time.instant
      begin
        result = @provider.redis.set(@key, owner, nx: true, px: options.ttl_ms)
      rescue ex
        cleanup(owner)
        raise AcquisitionUnknown.new("Redis did not acknowledge acquisition")
      end
      if result.nil?
        Internal.classify_contention { @provider.redis.type(@key) }
        return nil
      end
      unless result == "OK"
        cleanup(owner)
        raise AcquisitionUnknown.new("invalid Redis acquisition acknowledgement")
      end
      elapsed_ms = Internal.acquisition_elapsed_ms(dispatched_at, Time.instant)
      if Internal.remaining_lease_budget_ms(options.ttl_ms, elapsed_ms) <= 0
        cleanup(owner)
        raise AcquisitionUnknown.new("acquisition acknowledgement arrived outside its conservative validity budget")
      end
      lease = Lease.new(@provider, @key, owner)
      @provider.track(lease)
      lease
    end

    private def cleanup(owner : String)
      @provider.redis.eval(RELEASE_SCRIPT, [@key], [owner])
    rescue
      # Bounded single cleanup call is best effort; expiry remains the fallback.
    end

    RELEASE_SCRIPT = <<-LUA
      if redis.call('GET', KEYS[1]) == ARGV[1] then
        return redis.call('DEL', KEYS[1])
      end
      return 0
      LUA
  end

  class Lease
    @release_once = ReleaseOnce.new

    def initialize(@provider : RedisProvider, @key : String, @owner : String); end

    # Explicitly awaitable acknowledgement in this synchronous API. Release is
    # idempotent; a stale owner never deletes a successor.
    def release : ReleaseOutcome
      @release_once.run do
        begin
          result = @provider.redis.eval(Handle::RELEASE_SCRIPT, [@key], [@owner])
          result == 1 ? ReleaseOutcome::Released : ReleaseOutcome::AlreadyNotOwned
        ensure
          @provider.untrack(self)
        end
      end
    end
  end
end
