# frozen_string_literal: true

require 'json'
require 'securerandom'

module Dilock
  # JSONL conformance worker used by cross-SDK lifecycle tests.
  # Backend operations run on independent threads so the input
  # loop can still accept cancellation and cleanup commands while acquire waits.
  class ConformanceWorker
    # Maximum encoded request line accepted by the worker.
    # @return [Integer]
    MAX_LINE_BYTES = 65_536
    # Protocol operations accepted by the worker process.
    # @return [Array<String>]
    OPERATIONS = %w[hello acquire try_acquire renew release inspect cancel_request shutdown].freeze
    # JSON fields accepted across all worker operations.
    # @return [Array<String>]
    FIELDS = %w[id op resource lease_id target_id wait_ms lease_ms auto_renew].freeze

    # Raised for malformed, duplicate-key, oversized, or unsupported worker input.
    class InvalidRequest < StandardError; end

    # Hash used by JSON.parse to reject duplicate keys instead of silently
    # allowing a later value to replace an earlier safety-relevant field.
    class UniqueHash < Hash
      def []=(key, value)
        raise InvalidRequest, 'duplicate field' if key?(key)

        super
      end
    end
    private_constant :UniqueHash

    # Validates request JSON without depending on worker instance state.
    # @private Worker input implementation detail.
    module RequestParser
      module_function

      def parse(line)
        raise InvalidRequest, 'line exceeds 65536 bytes' if line.bytesize > MAX_LINE_BYTES

        request = JSON.parse(line, object_class: UniqueHash, max_nesting: 16)
        validate_envelope!(request)
        validate_request_fields!(request)
        request
      rescue JSON::ParserError => e
        raise InvalidRequest, "invalid JSON (#{e.class})"
      end

      def validate_envelope!(request)
        raise InvalidRequest, 'request must be an object' unless request.is_a?(Hash)
        raise InvalidRequest, 'unknown request field' unless (request.keys - FIELDS).empty?
        unless valid_request_id?(request['id'])
          raise InvalidRequest, 'id must be a non-empty string of at most 128 characters'
        end
        raise InvalidRequest, 'unknown operation' unless OPERATIONS.include?(request['op'])
      end
      private_class_method :validate_envelope!

      def valid_request_id?(request_id)
        request_id.is_a?(String) && !request_id.empty? && request_id.length <= 128
      end
      private_class_method :valid_request_id?

      def validate_request_fields!(request)
        validate_required_fields!(request)
        validate_string_fields!(request)
        validate_duration_fields!(request)
        validate_auto_renew!(request)
      end
      private_class_method :validate_request_fields!

      def validate_required_fields!(request)
        required = case request['op']
                   when 'acquire', 'try_acquire' then %w[resource]
                   when 'renew', 'release', 'inspect' then %w[lease_id]
                   when 'cancel_request' then %w[target_id]
                   else []
                   end
        raise InvalidRequest, 'required field missing' unless (required - request.keys).empty?
      end
      private_class_method :validate_required_fields!

      def validate_string_fields!(request)
        %w[resource lease_id target_id].each do |field|
          next unless request.key?(field)

          valid = request[field].is_a?(String) && !request[field].empty?
          raise InvalidRequest, "#{field} must be a non-empty string" unless valid
        end
      end
      private_class_method :validate_string_fields!

      def validate_duration_fields!(request)
        %w[wait_ms lease_ms].each do |field|
          next unless request.key?(field)

          minimum = field == 'wait_ms' ? 0 : 1
          valid = request[field].is_a?(Integer) && (minimum..86_400_000).cover?(request[field])
          raise InvalidRequest, "#{field} must be an integer in range" unless valid
        end
      end
      private_class_method :validate_duration_fields!

      def validate_auto_renew!(request)
        return unless request.key?('auto_renew') && ![true, false].include?(request['auto_renew'])

        raise InvalidRequest, 'auto_renew must be a boolean'
      end
      private_class_method :validate_auto_renew!
    end
    private_constant :RequestParser

    # Coordinates validated input with request threads and cancellation.
    module RequestDispatch
      private

      def process_line(line)
        request = parse_line(line)
        return write_message(id: '', status: 'invalid_argument') unless request

        dispatch_request(request)
      end

      def dispatch_request(request)
        id = request.fetch('id')
        operation = request.fetch('op')
        return write_message(id: id, status: 'invalid_argument') if duplicate_active_id?(id)
        return shutdown_response(id) if operation == 'shutdown'
        return cancel_request(id, request.fetch('target_id')) if operation == 'cancel_request'

        start_request(id, operation, request)
      end

      def shutdown_response(request_id)
        write_message(id: request_id, status: 'ok', outstanding_cleanup: cleanup)
        :shutdown
      end

      def parse_line(line)
        self.class.parse_request(line)
      rescue InvalidRequest => e
        diagnostic("invalid request (#{e.message})")
        nil
      end

      def duplicate_active_id?(id)
        @active_mutex.synchronize { @active.key?(id) }
      end

      def start_request(request_id, operation, request)
        gate = Queue.new
        ready = Queue.new
        state = { completed: false }
        thread = request_thread(request_id, operation, request, [ready, gate], state)
        ready.pop
        state[:thread] = thread
        @active_mutex.synchronize { @active[request_id] = state }
        gate << true
      end

      def request_thread(request_id, operation, request, gates, state)
        Thread.new { process_request_thread(request_id, operation, request, gates, state) }
      end

      def process_request_thread(request_id, operation, request, gates, state)
        ready, gate = gates
        response = { id: request_id }
        begin
          response = await_and_execute_request(request_id, operation, request, ready, gate)
        ensure
          finish_request_thread(request_id, state, response)
        end
      end

      def await_and_execute_request(request_id, operation, request, ready, gate)
        ready << true
        gate.pop
        execute(request_id, operation, request)
      rescue Interrupt
        cancellation_response(request_id, operation)
      end

      def finish_request_thread(request_id, state, response)
        state[:completed] = true
        write_message(response)
        @active_mutex.synchronize { @active.delete(request_id) }
      end

      def cancel_request(request_id, target_id)
        state = @active_mutex.synchronize { @active[target_id] }
        thread = state && !state[:completed] ? state[:thread] : nil
        begin
          thread&.raise(Interrupt, 'conformance cancellation')
        rescue ThreadError
          thread = nil
        end
        write_message(id: request_id, status: 'ok', cancelled: !thread.nil?)
      end
    end
    private_constant :RequestDispatch

    # Maps requests to protocol responses and capability/error statuses.
    module OperationDispatch
      private

      def execute(request_id, operation, request)
        response = { id: request_id }
        begin
          dispatch_operation(response, operation, request)
        rescue Interrupt
          response = cancellation_response(request_id, operation)
        rescue StandardError => e
          handle_operation_error(response, e, operation)
        end
        response
      end

      def dispatch_operation(response, operation, request)
        case operation
        when 'hello' then execute_hello(response)
        when 'acquire', 'try_acquire' then execute_acquire(response, operation, request)
        when 'renew' then execute_renew(response, request)
        when 'release' then execute_release(response, request)
        when 'inspect' then execute_inspect(response, request)
        end
      end

      def mark_unknown_outcome(response, error, operation)
        return unless %w[acquire try_acquire].include?(operation)

        response[:remote_outcome_unknown] = error.is_a?(AcquisitionUnknown)
      end

      def handle_operation_error(response, error, operation)
        response[:status] = mapped_status(error)
        mark_unknown_outcome(response, error, operation)
        diagnostic("#{error.class} category=#{error.respond_to?(:category) ? error.category : 'local'} " \
                   "message=#{error.message}")
      end

      def execute_hello(response)
        capabilities = { renewal: true, auto_renew: false, fencing: @backend == 'etcd',
                         try_acquire: @backend == 'redis' }
        client_gem = @backend == 'redis' ? 'redis' : 'grpc'
        response.merge!(status: 'ok', sdk: 'ruby', version: VERSION, protocol: 1,
                        backend: @backend, server_version: 'unknown',
                        client_version: Gem.loaded_specs[client_gem]&.version&.to_s || 'unknown',
                        capabilities: capabilities)
      end

      def cancellation_response(request_id, operation)
        response = { id: request_id, status: operation == 'release' ? 'release_unknown' : 'cancelled' }
        response[:remote_outcome_unknown] = %w[acquire try_acquire renew release].include?(operation)
        response
      end

      def mapped_status(operation_error)
        {
          ArgumentError => 'invalid_argument', KeyError => 'invalid_argument',
          AcquisitionUnknown => 'acquisition_unknown', BackendUnavailable => 'backend_unavailable',
          ProtocolMismatch => 'protocol_mismatch', UnsupportedCapability => 'unsupported_capability',
          AcquisitionTimeout => 'timeout', LeaseLost => 'already_not_owned'
        }.each do |error_type, status|
          return status if operation_error.is_a?(error_type)
        end
        'internal_error'
      end
    end
    private_constant :OperationDispatch

    # Applies supported lease operations and retains release outcomes.
    module LeaseOperations
      private

      def execute_acquire(response, operation, request)
        validate_acquire_capability!(operation, request)
        wait_ms = operation == 'acquire' ? request.fetch('wait_ms', 5_000) : 0
        options = AcquireOptions.new(wait_ms: wait_ms, lease_ms: request.fetch('lease_ms', 30_000))
        handle = @provider.lock(request.fetch('resource'))
        lease = operation == 'try_acquire' ? handle.try_acquire(options) : handle.acquire(wait_ms, options)
        return response[:status] = 'busy' unless lease

        record_lease(response, lease)
      end

      def validate_acquire_capability!(operation, request)
        if operation == 'try_acquire' && @backend == 'etcd'
          raise UnsupportedCapability.new('etcd native Lock does not support try-acquire',
                                          category: :unsupported_capability)
        end
        return unless request.fetch('auto_renew', false)

        raise UnsupportedCapability.new('automatic renewal is not available', category: :unsupported_capability)
      end

      def record_lease(response, lease)
        local_id = SecureRandom.uuid
        @leases_mutex.synchronize { @leases[local_id] = lease }
        response.merge!(status: 'acquired', lease_id: local_id, effective_lease_ms: lease.effective_lease_ms)
        response[:fence] = { epoch: lease.fence.epoch, value: lease.fence.revision.to_s } if lease.fence
      end

      def execute_renew(response, request)
        lease = @leases_mutex.synchronize { @leases.fetch(request.fetch('lease_id')) }
        response[:status] = lease.renew(request.fetch('lease_ms', lease.effective_lease_ms)) ? 'renewed' : 'lost'
      end

      def execute_release(response, request)
        lease_id = request.fetch('lease_id')
        cached_status = @released_mutex.synchronize { @released[lease_id] }
        response[:status] = cached_status || release_lease(lease_id)
      end

      def release_lease(lease_id)
        lease = @leases_mutex.synchronize { @leases.fetch(lease_id) }
        status = { released: 'released', already_not_owned: 'already_not_owned',
                   unknown: 'release_unknown' }.fetch(lease.release)
        @leases_mutex.synchronize { @leases.delete(lease_id) }
        @released_mutex.synchronize { @released[lease_id] = status }
        status
      end

      def execute_inspect(response, request)
        lease = @leases_mutex.synchronize { @leases.fetch(request.fetch('lease_id')) }
        response.merge!(status: 'ok', state: lease.state.to_s)
      end
    end
    private_constant :LeaseOperations

    # Performs bounded worker shutdown and best-effort lease cleanup.
    module WorkerCleanup
      private

      def cleanup
        return @cleanup_result if @cleanup_result

        cleanup_failures = stop_active_requests + release_outstanding_leases
        @provider.close
        @cleanup_result = cleanup_failures
      rescue StandardError
        diagnostic('provider close failed')
        @cleanup_result = 1
      end

      def stop_active_requests
        states = @active_mutex.synchronize { @active.values.uniq }
        threads = states.map { |state| state[:thread] }
        threads.each { |thread| thread.join(2) }
        interrupt_unfinished_requests(states)
        force_stop_unfinished_requests(threads)
        threads.count(&:alive?)
      end

      def force_stop_unfinished_requests(threads)
        threads.each { |thread| thread.join(2) }
        threads.select(&:alive?).each do |thread|
          thread.kill
          thread.join(1)
        end
      end

      def interrupt_unfinished_requests(states)
        states.each do |state|
          thread = state[:thread]
          next unless !state[:completed] && thread.alive?

          thread.raise(Interrupt, 'worker shutdown')
        rescue ThreadError
          nil
        end
      end

      def release_outstanding_leases
        leases = @leases_mutex.synchronize { @leases.values.dup }
        leases.sum { |lease| release_outstanding_lease(lease) }
      end

      def release_outstanding_lease(lease)
        lease.release == :unknown ? 1 : 0
      rescue StandardError
        diagnostic('outstanding lease cleanup failed')
        1
      end

      def write_message(object)
        @output_mutex.synchronize do
          @output.write(JSON.generate(object), "\n")
          @output.flush
        end
      rescue StandardError
        diagnostic('response write failed')
      end

      def diagnostic(message)
        @diagnostics.puts("worker: #{message}")
      rescue StandardError
        nil
      end
    end
    private_constant :WorkerCleanup

    include RequestDispatch
    include OperationDispatch
    include LeaseOperations
    include WorkerCleanup

    # Creates a worker around an already connected backend provider.
    # The worker owns and closes the provider when {#run} terminates.
    # @param provider [RedisProvider, EtcdProvider] backend provider to own
    # @param backend [String] protocol backend name (`redis` or `etcd`)
    # @param input [IO] JSONL request stream
    # @param output [IO] JSONL response stream
    # @param diagnostics [IO] diagnostic stream, separate from protocol output
    def initialize(provider:, backend:, input: $stdin, output: $stdout, diagnostics: $stderr)
      @provider = provider
      @backend = backend
      @input = input
      @output = output
      @diagnostics = diagnostics
      initialize_worker_state
    end

    def initialize_worker_state
      @leases = {}
      @released = {}
      @leases_mutex = Mutex.new
      @released_mutex = Mutex.new
      @output_mutex = Mutex.new
      @active_mutex = Mutex.new
      @active = {}
      @cleanup_result = nil
    end
    private :initialize_worker_state

    # Processes requests until EOF, a shutdown command, or invalid input, then cleans up.
    # This method blocks the calling thread.
    # @return [void]
    def run
      while (line = read_bounded_line)
        break if process_line(line) == :shutdown
      end
    rescue InvalidRequest => e
      diagnostic("invalid input line: #{e.message}")
    ensure
      cleanup
    end

    # Parses and validates one JSONL request without performing backend work.
    # @param line [String] one JSON object line, without a trailing newline
    # @return [Hash<String, Object>] validated request
    # @raise [InvalidRequest] for invalid JSON or a request outside the worker contract
    def self.parse_request(line)
      RequestParser.parse(line)
    end

    private

    def read_bounded_line
      bytes = +''.b
      while (byte = @input.getbyte) && byte != 10
        raise InvalidRequest, 'line exceeds 65536 bytes' if bytes.bytesize >= MAX_LINE_BYTES

        bytes << byte
      end
      return nil if byte.nil? && bytes.empty?

      bytes.force_encoding(Encoding::UTF_8)
      raise InvalidRequest, 'line is invalid UTF-8' unless bytes.valid_encoding?

      bytes.delete_suffix("\r")
    end
  end
end
