# frozen_string_literal: true

require 'base64'
require 'json'
require 'net/http'
require 'openssl'
require 'uri'

module Dilock
  # Epoch-scoped Consul LockIndex. The lock KV key must be retained forever.
  ConsulFence = Data.define(:epoch, :lock_index)

  # Consul HTTP KV/session adapter. Uses only Ruby's standard library.
  class ConsulProvider
    ConnectionOptions = Data.define(
      :namespace, :rpc_timeout_ms, :cleanup_timeout_ms, :default_acquisition, :token,
      :cleanup_observer
    ) do
      def initialize(namespace: 'default', rpc_timeout_ms: Internal::DEFAULT_RPC_MS,
                     cleanup_timeout_ms: Internal::DEFAULT_CLEANUP_MS,
                     default_acquisition: AcquireOptions::DEFAULT, token: nil, cleanup_observer: nil)
        super
      end
    end

    attr_reader :default_acquisition

    def self.connect(endpoint, epoch:, **options)
      new(endpoint, epoch:, **options)
    end

    # Creates exact namespace metadata only when CAS=0 succeeds.
    def self.provision_namespace!(endpoint, namespace:, epoch:, token: nil, timeout_ms: Internal::DEFAULT_RPC_MS)
      options = ConnectionOptions.new(namespace:, token:, rpc_timeout_ms: timeout_ms)
      validate_configuration!(endpoint, epoch, options)
      provider = allocate
      provider.send(:assign_configuration, endpoint, epoch, options)
      metadata = Internal.metadata(namespace, Internal::PROFILES.fetch(:consul), epoch)
      response = provider.send(:request, :put, "v1/kv/dilock/namespaces/#{namespace}/meta?cas=0",
                               metadata, timeout_ms, mutation: true)
      return true if response == true
      raise ProtocolMismatch.new('Consul metadata CAS response was malformed', category: :protocol_mismatch) unless response == false

      provider.send(:validate_metadata!, timeout_ms)
      false
    end

    def initialize(endpoint, epoch:, namespace: 'default', rpc_timeout_ms: Internal::DEFAULT_RPC_MS,
                   cleanup_timeout_ms: Internal::DEFAULT_CLEANUP_MS,
                   default_acquisition: AcquireOptions::DEFAULT, token: nil, cleanup_observer: nil)
      options = ConnectionOptions.new(namespace:, rpc_timeout_ms:, cleanup_timeout_ms:,
                                     default_acquisition:, token:, cleanup_observer:)
      self.class.send(:validate_configuration!, endpoint, epoch, options)
      assign_configuration(endpoint, epoch, options)
      validate_metadata!(@rpc_timeout_ms)
    end

    def lock(resource)
      ensure_open!
      Lock.new(self, Identity.resource_id(@namespace, resource))
    end

    def close
      @close_mutex.synchronize do
        return if @closed

        @leases.dup.each do |lease|
          outcome = lease.release
          observe_cleanup('provider-close', outcome.to_s) unless outcome == :released
        end
        @closed = true
      end
      nil
    end

    def __acquire(resource_id, options, wait:)
      ensure_open!
      raise ArgumentError, 'options must be AcquireOptions' unless options.is_a?(AcquireOptions)
      ttl = consul_ttl(options.lease_ms)
      wait_deadline = Internal.deadline_ms(options.wait_ms)
      session, sent = create_session(ttl, bounded_timeout(wait_deadline, @rpc_timeout_ms))
      cleanup_attempted = false
      lease_deadline = sent + ((ttl - Internal.safety_margin_ms(ttl)) / 1_000.0)
      last_renewal = sent
      key = "dilock/locks/#{resource_id}"

      loop do
        unless Internal.remaining_ms(lease_deadline).positive?
          cleanup_attempted = true
          cleanup_attempt!(key, session, 'acquisition-abort')
          raise AcquisitionUnknown.new('Consul session validity elapsed before ownership was confirmed',
                                       category: :acquisition_unknown)
        end
        if wait && !Internal.remaining_ms(wait_deadline).positive?
          cleanup_attempted = true
          cleanup_attempt!(key, session, 'wait-timeout')
          raise AcquisitionTimeout.new('Consul acquisition wait budget expired', category: :acquisition_timeout)
        end

        timeout = wait ? bounded_timeout(wait_deadline, @rpc_timeout_ms) : @rpc_timeout_ms
        response = request(:put, "v1/kv/#{key}?acquire=#{URI.encode_www_form_component(session)}", '', timeout, mutation: true)
        raise ProtocolMismatch.new('Consul acquire response must be boolean', category: :protocol_mismatch) unless [true, false].include?(response)

        if response
          owner, index = read_owner(key, @rpc_timeout_ms)
          unless owner == session && index.positive? && Internal.remaining_ms(lease_deadline).positive? &&
                 (!wait || Internal.remaining_ms(wait_deadline).positive?)
            cleanup_attempted = true
            cleanup_attempt!(key, session, 'ownership-unconfirmed')
            raise AcquisitionUnknown.new('Consul owner or LockIndex verification failed', category: :acquisition_unknown)
          end
          lease = Lease.new(self, Lease::Ownership.new(key:, owner: session), ttl, lease_deadline,
                            fence: ConsulFence.new(epoch: @epoch, lock_index: index))
          @leases << lease
          return lease
        end

        unless wait
          cleanup_attempted = true
          cleanup_attempt!(key, session, 'contended')
          return nil
        end

        remaining = Internal.remaining_ms(wait_deadline)
        sleep([0.025 + rand * 0.025, remaining / 1_000.0].min) if remaining.positive?
        if Process.clock_gettime(Process::CLOCK_MONOTONIC) - last_renewal >= (ttl * 2 / 3_000.0)
          renewed_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          renew_session!(session, @rpc_timeout_ms)
          last_renewal = renewed_at
          lease_deadline = renewed_at + ((ttl - Internal.safety_margin_ms(ttl)) / 1_000.0)
        end
      end
    rescue StandardError, Interrupt => error
      if defined?(session) && session && !cleanup_attempted
        cleanup_attempted = true
        cleanup_attempt!(key, session, 'acquisition-error')
      end
      raise error
    end

    def __renew(lease, lease_ms)
      unless lease_ms.is_a?(Integer) && lease_ms == lease.effective_lease_ms && lease_ms >= 10_000
        raise UnsupportedCapability.new('Consul sessions preserve their TTL; reacquire to change it', category: :unsupported_capability)
      end
      previous_deadline = lease.__deadline
      ttl = consul_ttl(lease_ms)
      sent = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      renew_session!(lease.__owner, @rpc_timeout_ms)
      owner, index = read_owner(lease.__key, @rpc_timeout_ms)
      return false unless owner == lease.__owner && index == lease.fence.lock_index
      return false unless Process.clock_gettime(Process::CLOCK_MONOTONIC) < previous_deadline
      deadline = sent + ((ttl - Internal.safety_margin_ms(ttl)) / 1_000.0)
      return false unless Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline

      lease.__set_deadline(deadline)
      true
    rescue BackendUnavailable, AcquisitionUnknown, ProtocolMismatch
      raise
    rescue StandardError => error
      raise BackendUnavailable.new("Consul renewal failed (#{error.class})", category: :backend_unavailable)
    end

    def __release(lease)
      operation_deadline = Internal.deadline_ms(@cleanup_timeout_ms)
      ownership = :unknown
      unlock = :unknown
      begin
        owner, = read_owner(lease.__key, bounded_timeout(operation_deadline, @cleanup_timeout_ms))
        ownership = owner == lease.__owner ? :owned : :mismatch
        if ownership == :owned
          value = request(:put, "v1/kv/#{lease.__key}?release=#{URI.encode_www_form_component(lease.__owner)}",
                          '', bounded_timeout(operation_deadline, @cleanup_timeout_ms), mutation: true)
          unlock = value == true ? :released : value == false ? :not_owned : :unknown
        end
      rescue StandardError => error
        observe_cleanup('release-owner-check', error.class.name)
      ensure
        destroy_ack = begin
          destroy_deadline = Internal.deadline_ms(@cleanup_timeout_ms)
          remaining = Internal.remaining_ms(destroy_deadline)
          remaining.positive? && destroy_session(lease.__owner, [remaining, @cleanup_timeout_ms].min)
        rescue StandardError => error
          observe_cleanup('session-destroy', error.class.name)
          false
        end
      end
      @leases.delete(lease)
      return :unknown unless destroy_ack
      return :already_not_owned if %i[mismatch not_owned].include?(ownership) || unlock == :not_owned
      return :released if ownership == :owned && unlock == :released

      observe_cleanup('release-unconfirmed', 'unknown')
      :unknown
    end

    def observe_cleanup(reason, outcome)
      @cleanup_observer&.call(reason:, outcome:)
    rescue StandardError
      nil
    end

    private

    def self.validate_configuration!(endpoint, epoch, options)
      Internal.validate_provider!(options.namespace, epoch, options.rpc_timeout_ms,
                                  options.cleanup_timeout_ms, options.default_acquisition)
      uri = URI.parse(endpoint)
      unless %w[http https].include?(uri.scheme) && uri.host && !uri.userinfo &&
             [nil, '', '/'].include?(uri.path) && !uri.query && !uri.fragment
        raise ArgumentError, 'endpoint must be an http(s) authority without credentials or path'
      end
      raise ArgumentError, 'epoch must be valid UTF-8 and at most 1024 bytes' unless epoch.valid_encoding? && epoch.bytesize <= 1_024
      if options.token && !/\A[!-~]+\z/.match?(options.token)
        raise ArgumentError, 'Consul token must be printable non-space ASCII'
      end
      ttl = options.default_acquisition.lease_ms
      raise ArgumentError, 'Consul session TTL must be between 10000 and 86400000 milliseconds' unless (10_000..86_400_000).cover?(ttl)
      true
    rescue URI::InvalidURIError => error
      raise ArgumentError, "invalid Consul endpoint: #{error.message}"
    end

    def assign_configuration(endpoint, epoch, options)
      @uri = URI.parse(endpoint)
      @epoch = epoch
      @namespace = options.namespace
      @rpc_timeout_ms = options.rpc_timeout_ms
      @cleanup_timeout_ms = options.cleanup_timeout_ms
      @default_acquisition = options.default_acquisition
      @token = options.token
      @cleanup_observer = options.cleanup_observer
      @closed = false
      @leases = []
      @mutex = Mutex.new
      @close_mutex = Mutex.new
    end

    def ensure_open!
      raise BackendUnavailable.new('Consul provider is closed', category: :provider_closed) if @closed
    end

    def consul_ttl(milliseconds)
      raise ArgumentError, 'Consul session TTL must be between 10000 and 86400000 milliseconds' unless (10_000..86_400_000).cover?(milliseconds)
      ((milliseconds + 999) / 1_000) * 1_000
    end

    def bounded_timeout(deadline, maximum)
      remaining = Internal.remaining_ms(deadline)
      raise AcquisitionTimeout.new('Consul operation budget expired', category: :acquisition_timeout) unless remaining.positive?
      [maximum, remaining].min
    end

    def validate_metadata!(timeout)
      key = "dilock/namespaces/#{@namespace}/meta"
      rows = request(:get, "v1/kv/#{key}?consistent", nil, timeout, mutation: false)
      unless rows.is_a?(Array) && rows.length == 1 && rows[0].is_a?(Hash) && rows[0]['Key'] == key && rows[0]['Value'].is_a?(String)
        raise ProtocolMismatch.new('Consul namespace metadata is missing or malformed', category: :protocol_mismatch)
      end
      raw = Base64.strict_decode64(rows[0]['Value'])
      raise ProtocolMismatch.new('Consul namespace metadata is too large', category: :protocol_mismatch) if raw.bytesize > 4_096
      Internal.validate_metadata!(raw, @namespace, Internal::PROFILES.fetch(:consul), @epoch)
    rescue ArgumentError => error
      raise ProtocolMismatch.new("invalid Consul metadata: #{error.message}", category: :protocol_mismatch)
    end

    def request(method, path, body, timeout_ms, mutation:)
      uri = @uri.dup
      uri.path = "/#{path.split('?', 2).first}"
      uri.query = path.include?('?') ? path.split('?', 2).last : nil
      klass = { get: Net::HTTP::Get, put: Net::HTTP::Put }.fetch(method)
      request = klass.new(uri)
      request['Accept'] = 'application/json'
      request['X-Consul-Token'] = @token if @token
      request['Content-Type'] = 'application/json' if body
      request.body = body if body
      http = Net::HTTP.new(uri.host, uri.port)
      http.use_ssl = uri.scheme == 'https'
      http.verify_mode = OpenSSL::SSL::VERIFY_PEER if http.use_ssl?
      http.open_timeout = timeout_ms / 1_000.0
      http.read_timeout = timeout_ms / 1_000.0
      http.write_timeout = timeout_ms / 1_000.0 if http.respond_to?(:write_timeout=)
      http.max_retries = 0 if http.respond_to?(:max_retries=)
      response = http.start { |client| client.request(request) }
      unless response.is_a?(Net::HTTPSuccess)
        if response.code.to_i == 404 && !mutation
          raise ProtocolMismatch.new('Consul metadata or owner key is missing', category: :protocol_mismatch)
        end
        if mutation
          raise AcquisitionUnknown.new("Consul mutation status #{response.code} is unconfirmed", category: :acquisition_unknown)
        end
        raise BackendUnavailable.new("Consul HTTP status #{response.code}", category: :backend_unavailable)
      end
      return nil if response.body.nil? || response.body.empty?
      raise ProtocolMismatch.new('Consul response exceeds 64 KiB', category: :protocol_mismatch) if response.body.bytesize > 65_536

      JSON.parse(response.body, create_additions: false, max_nesting: 32)
    rescue JSON::ParserError => error
      raise(mutation ? AcquisitionUnknown.new('Consul mutation response was malformed', category: :acquisition_unknown) :
                       ProtocolMismatch.new("Consul response was malformed: #{error.message}", category: :protocol_mismatch))
    rescue Net::OpenTimeout, Net::ReadTimeout, IOError, SystemCallError, OpenSSL::SSL::SSLError => error
      klass = mutation ? AcquisitionUnknown : BackendUnavailable
      category = mutation ? :acquisition_unknown : :backend_unavailable
      raise klass.new("Consul HTTP request failed (#{error.class})", category:)
    end

    def create_session(ttl_ms, timeout_ms)
      sent = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      body = JSON.generate(Name: 'dilock lease', Behavior: 'release', LockDelay: '0s', TTL: "#{ttl_ms / 1_000}s", Checks: [])
      response = request(:put, 'v1/session/create', body, timeout_ms, mutation: true)
      unless response.is_a?(Hash) && response['ID'].is_a?(String) && !response['ID'].empty?
        raise AcquisitionUnknown.new('Consul may have created a session without returning its identifier',
                                     category: :acquisition_unknown)
      end
      [response['ID'], sent]
    end

    def renew_session!(session, timeout_ms)
      response = request(:put, "v1/session/renew/#{URI.encode_www_form_component(session)}", nil, timeout_ms, mutation: true)
      unless response.is_a?(Array) && response.length == 1 && response[0].is_a?(Hash) && response[0]['ID'] == session
        raise ProtocolMismatch.new('Consul session renewal response is malformed', category: :protocol_mismatch)
      end
    end

    def read_owner(key, timeout_ms)
      rows = request(:get, "v1/kv/#{key}?consistent", nil, timeout_ms, mutation: false)
      unless rows.is_a?(Array) && rows.length == 1 && rows[0].is_a?(Hash) && rows[0]['Key'] == key &&
             rows[0]['LockIndex'].is_a?(Integer) && rows[0]['LockIndex'] >= 0 &&
             (rows[0]['Session'].nil? || rows[0]['Session'].is_a?(String))
        raise ProtocolMismatch.new('Consul KV owner response is malformed', category: :protocol_mismatch)
      end
      [rows[0]['Session'], rows[0]['LockIndex']]
    end

    def cleanup_attempt!(key, session, reason)
      released = request(:put, "v1/kv/#{key}?release=#{URI.encode_www_form_component(session)}", '', @cleanup_timeout_ms, mutation: true)
      destroyed = destroy_session(session, @cleanup_timeout_ms)
      return true if [true, false].include?(released) && destroyed

      observe_cleanup(reason, 'unknown')
      raise AcquisitionUnknown.new('Consul acquisition session cleanup was not confirmed', category: :acquisition_unknown)
    rescue Error
      raise
    rescue StandardError => error
      observe_cleanup(reason, error.class.name)
      raise AcquisitionUnknown.new('Consul acquisition session cleanup was not confirmed', category: :acquisition_unknown)
    end

    def destroy_session(session, timeout_ms)
      request(:put, "v1/session/destroy/#{URI.encode_www_form_component(session)}", nil, timeout_ms, mutation: true)
      true
    rescue StandardError => error
      observe_cleanup('session-destroy', error.class.name)
      false
    end
  end
end
