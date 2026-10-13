# frozen_string_literal: true

module Dilock
  # Redis connection configuration used by the provider convenience factory.
  class RedisProvider
    ConnectionOptions = Data.define(
      :namespace, :rpc_timeout_ms, :cleanup_timeout_ms, :default_acquisition, :cleanup_observer
    ) do
      def initialize(namespace: 'default', rpc_timeout_ms: Internal::DEFAULT_RPC_MS,
                     cleanup_timeout_ms: Internal::DEFAULT_CLEANUP_MS,
                     default_acquisition: AcquireOptions::DEFAULT, cleanup_observer: nil)
        super
      end
    end

    # Validates provider options and initializes its backend-owned resources.
    module ConnectionSetup
      private

      def setup_connection(endpoint, epoch:, **provided_options)
        options = ConnectionOptions.new(**provided_options)
        validate_connection!(endpoint, epoch, options)
        assign_connection_state(endpoint, epoch, options)
        initialize_clients!
        @closed = false
        @mutex = Mutex.new
        @close_mutex = Mutex.new
      end

      def validate_connection!(endpoint, epoch, options)
        Internal.validate_provider!(options.namespace, epoch, options.rpc_timeout_ms,
                                    options.cleanup_timeout_ms, options.default_acquisition)
        validate_endpoint!(endpoint)
      end

      def assign_connection_state(endpoint, epoch, options)
        @endpoint = endpoint
        @namespace = options.namespace
        @epoch = epoch
        @rpc_timeout_ms = options.rpc_timeout_ms
        @cleanup_timeout_ms = options.cleanup_timeout_ms
        @default_acquisition = options.default_acquisition
        @cleanup_observer = options.cleanup_observer
      end

      def validate_endpoint!(endpoint)
        return if endpoint.is_a?(String) && !endpoint.empty?

        raise ArgumentError, 'endpoint is required'
      end
    end

    include ConnectionSetup

    private_constant :ConnectionOptions, :ConnectionSetup
  end
end
