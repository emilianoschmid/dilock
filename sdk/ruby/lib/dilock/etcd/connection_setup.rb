# frozen_string_literal: true

# etcd endpoint validation and provider setup for the Dilock SDK.
module Dilock
  # Native etcd connection options and setup mixed into EtcdProvider.
  class EtcdProvider
    ConnectionOptions = Data.define(
      :namespace, :rpc_timeout_ms, :cleanup_timeout_ms, :default_acquisition,
      :cleanup_observer, :root_certificates, :private_key, :certificate_chain
    ) do
      def initialize(namespace: 'default', rpc_timeout_ms: Internal::DEFAULT_RPC_MS,
                     cleanup_timeout_ms: Internal::DEFAULT_CLEANUP_MS,
                     default_acquisition: AcquireOptions::DEFAULT, cleanup_observer: nil,
                     root_certificates: nil, private_key: nil, certificate_chain: nil)
        super
      end
    end

    # Validates endpoint/options and builds the native RPC stubs for EtcdProvider.
    module ConnectionSetup
      private

      def setup_connection(endpoint, epoch:, **provided_options)
        options = ConnectionOptions.new(**provided_options)
        validate_options!(options, epoch)
        uri = parse_endpoint(endpoint)
        validate_tls_configuration!(uri.scheme, options)
        assign_provider_state(options, epoch, uri)
        create_rpc_stubs
        @closed = false
        @close_mutex = Mutex.new
        validate_metadata!
      end

      def validate_options!(options, epoch)
        Internal.validate_provider!(options.namespace, epoch, options.rpc_timeout_ms,
                                    options.cleanup_timeout_ms, options.default_acquisition)
      end

      def assign_provider_state(options, epoch, uri)
        @namespace = options.namespace
        @epoch = epoch
        @rpc_timeout_ms = options.rpc_timeout_ms
        @cleanup_timeout_ms = options.cleanup_timeout_ms
        @default_acquisition = options.default_acquisition
        @cleanup_observer = options.cleanup_observer
        @target = "#{uri.host}:#{uri.port || 2_379}"
        @credentials = channel_credentials(uri, options)
      end

      def channel_credentials(uri, options)
        return :this_channel_is_insecure unless uri.scheme == 'https'

        GRPC::Core::ChannelCredentials.new(options.root_certificates, options.private_key,
                                           options.certificate_chain)
      end

      def create_rpc_stubs
        @kv = Etcdserverpb::KV.rpc_stub_class.new(@target, @credentials)
        @lease = Etcdserverpb::Lease.rpc_stub_class.new(@target, @credentials)
        @lock = V3lockpb::Lock.rpc_stub_class.new(@target, @credentials)
      end

      def parse_endpoint(endpoint)
        raise ArgumentError, 'endpoint is required' unless endpoint.is_a?(String) && !endpoint.empty?

        uri = URI.parse(endpoint)
        validate_endpoint_uri!(uri)
        uri
      end

      def validate_endpoint_uri!(uri)
        validate_endpoint_scheme!(uri.scheme)
        validate_endpoint_host!(uri.host)
        validate_endpoint_shape!(uri)
      end

      def validate_endpoint_scheme!(scheme)
        return if %w[http https].include?(scheme)

        raise ArgumentError, 'etcd endpoint scheme must be http or https'
      end

      def validate_endpoint_host!(host)
        return if host && !host.empty?

        raise ArgumentError, 'etcd endpoint host is required'
      end

      def validate_endpoint_shape!(uri)
        invalid_path = uri.path && !uri.path.empty? && uri.path != '/'
        return unless uri.userinfo || invalid_path || uri.query || uri.fragment

        raise ArgumentError, 'etcd endpoint must not contain user info, a path, query or fragment'
      end

      def validate_tls_configuration!(scheme, options)
        cert_and_key_mismatch = options.private_key.nil? != options.certificate_chain.nil?
        raise ArgumentError, 'TLS client certificate and key must be provided together' if cert_and_key_mismatch
        return unless scheme == 'http' && tls_credentials?(options)

        raise ArgumentError, 'TLS credentials require an https endpoint'
      end

      def tls_credentials?(options)
        [options.root_certificates, options.private_key, options.certificate_chain].any?
      end

      def validate_metadata!
        key = "/dilock/namespaces/#{@namespace}/meta".b
        response = @kv.range(Etcdserverpb::RangeRequest.new(key: key), deadline: rpc_deadline(@rpc_timeout_ms))
        entry = response.kvs.find { |value| value.key == key }
        raise ProtocolMismatch.new('namespace metadata is not provisioned', category: :protocol_mismatch) unless entry

        Internal.validate_metadata!(entry.value, @namespace, Internal::PROFILES.fetch(:etcd), @epoch)
      end
    end

    include ConnectionSetup

    private_constant :ConnectionOptions, :ConnectionSetup
  end
end
