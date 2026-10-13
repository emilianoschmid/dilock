# frozen_string_literal: true

require 'uri'
require 'grpc'
require_relative 'etcd/acquisition'
require_relative 'etcd/lease_lifecycle'
require_relative 'etcd/keepalive'
require_relative 'etcd/connection_setup'
require_relative 'etcd/proto/services'

module Dilock
  # Epoch-scoped etcd creation revision. The protected resource must enforce it atomically.
  # Lease expiry does not stop stale work and a revision is not eternal after restore.
  # @!attribute epoch
  #   @return [String] approved coordination epoch
  # @!attribute revision
  #   @return [Integer] etcd create revision of the lock key
  EtcdFence = Data.define(:epoch, :revision)

  # etcd v3 native Lock adapter. Each acquisition owns a distinct lease and retains
  # the opaque key returned by Lock. A fence is meaningful only with its approved epoch.
  class EtcdProvider
    # Policy used when a lock operation omits explicit acquisition options.
    # @return [AcquireOptions]
    attr_reader :default_acquisition

    # Connects to etcd and validates the pre-provisioned namespace metadata.
    # The provider owns its gRPC channels and must be closed after use.
    #
    # @param endpoint [String] etcd v3 endpoint using `http` or `https`
    # @param epoch [String] approved coordination epoch recorded in metadata
    # Keyword options are `namespace` (default: `default`), `rpc_timeout_ms`,
    # `cleanup_timeout_ms`, `default_acquisition`, `cleanup_observer`,
    # `root_certificates`, `private_key`, and `certificate_chain`.
    # @return [EtcdProvider] connected provider
    # @raise [ArgumentError, ConfigurationError, ProtocolMismatch, BackendUnavailable]
    def self.connect(endpoint, epoch:, **)
      new(endpoint, epoch:, **)
    end

    # Opens gRPC stubs and verifies existing namespace metadata before returning.
    # Do not close the provider concurrently with active operations.
    # @param endpoint [String] etcd v3 endpoint
    # @param epoch [String] expected coordination epoch
    # Accepts the same keyword options as {self.connect}.
    # @raise [ArgumentError, ConfigurationError, ProtocolMismatch, BackendUnavailable]
    def initialize(endpoint, epoch:, **)
      setup_connection(endpoint, epoch:, **)
    rescue GRPC::BadStatus => e
      close
      raise BackendUnavailable.new("etcd namespace metadata validation failed (#{e.class})",
                                   category: :backend_unavailable)
    rescue URI::InvalidURIError => e
      raise ArgumentError, "invalid etcd endpoint: #{e.message}"
    rescue StandardError
      close
      raise
    end

    # Returns a reusable handle for one namespaced resource.
    # Each acquisition creates a distinct etcd lease and owner generation.
    # @param resource [String] resource identifier; UTF-8 bytes define identity
    # @return [Lock] lock handle
    # @raise [BackendUnavailable] if this provider has been closed
    # @raise [ArgumentError] if resource identity is invalid
    def lock(resource)
      ensure_open!
      Lock.new(self, Identity.resource_id(@namespace, resource))
    end

    # Closes the provider-owned gRPC channels. Do not call concurrently with active operations.
    # @return [void]
    def close
      (@close_mutex ||= Mutex.new).synchronize do
        return if @closed

        @closed = true
        [@kv, @lease, @lock].compact.each do |stub|
          stub.close if stub.respond_to?(:close)
        rescue StandardError
          nil
        end
      end
    end

    private

    def ensure_open!
      raise BackendUnavailable.new('etcd provider is closed', category: :provider_closed) if @closed
    end
  end
end
