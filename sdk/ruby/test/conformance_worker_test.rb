# frozen_string_literal: true

require 'minitest/autorun'
require 'stringio'
require 'timeout'
require_relative '../lib/dilock'
require_relative '../lib/dilock/conformance_worker'

# Helpers for JSONL worker assertions and request execution.
module ConformanceWorkerTestSupport
  private

  def run_worker(backend, requests, provider:, diagnostics: StringIO.new)
    output = StringIO.new
    input = StringIO.new("#{requests.join("\n")}\n")
    Dilock::ConformanceWorker.new(provider: provider, backend: backend, input: input,
                                  output: output, diagnostics: diagnostics).run
    [output_messages(output), provider, diagnostics]
  end

  def output_messages(output)
    output.string.lines.map { |line| JSON.parse(line) }
  end

  def messages_by_id(output)
    output_messages(output).to_h { |message| [message.fetch('id'), message] }
  end

  def assert_hello_capabilities(messages)
    hello = messages.find { |message| message['id'] == 'hello' }
    assert_equal 'ok', hello.fetch('status')
    assert_equal({ 'renewal' => true, 'auto_renew' => false, 'fencing' => true,
                   'try_acquire' => false }, hello.fetch('capabilities'))
    assert_equal 'unknown', hello.fetch('server_version')
    refute_equal 'unknown', hello.fetch('client_version')
  end

  def assert_shutdown_cleanup(messages)
    shutdown = messages.find { |message| message['id'] == 'stop' } || messages.find do |message|
      message['id'] == 'shutdown'
    end
    assert_equal 'ok', shutdown.fetch('status')
    assert_equal 0, shutdown.fetch('outstanding_cleanup')
  end

  def assert_cancellation_outcome(messages)
    assert_equal 'ok', messages.fetch('cancel').fetch('status')
    assert_equal true, messages.fetch('cancel').fetch('cancelled')
    assert_equal 'cancelled', messages.fetch('acquire').fetch('status')
    assert_equal true, messages.fetch('acquire').fetch('remote_outcome_unknown')
  end
end

class ConformanceWorkerTest < Minitest::Test
  include ConformanceWorkerTestSupport

  Worker = Dilock::ConformanceWorker

  def test_parser_accepts_valid_envelope_and_defaults_are_worker_owned
    request = Worker.parse_request('{"id":"r1","op":"acquire","resource":"camera/1"}')

    assert_equal 'r1', request.fetch('id')
    assert_equal 'acquire', request.fetch('op')
    assert_equal 'camera/1', request.fetch('resource')
  end

  def test_parser_rejects_duplicate_keys_unknown_fields_and_invalid_types
    [
      '{"id":"r1","id":"r2","op":"hello"}',
      '{"id":"r1","op":"hello","secret":"value"}',
      '{"id":"r1","op":"acquire","resource":"x","wait_ms":-1}',
      '{"id":"r1","op":"renew","lease_id":"l1","lease_ms":true}',
      '{"id":"r1","op":"unknown"}'
    ].each do |json|
      assert_raises(Worker::InvalidRequest) { Worker.parse_request(json) }
    end
  end

  def test_parser_enforces_line_bound_and_utf8
    assert_raises(Worker::InvalidRequest) do
      Worker.parse_request(' ' * (Worker::MAX_LINE_BYTES + 1))
    end
    assert_raises(Worker::InvalidRequest) { Worker.parse_request("\xff".b.force_encoding(Encoding::UTF_8)) }
  end

  def test_worker_emits_jsonl_hello_and_reports_only_backend_capabilities
    provider = FakeProvider.new
    diagnostics = StringIO.new
    requests = ['{"id":"hello","op":"hello"}', '{"id":"stop","op":"shutdown"}']
    messages, provider, diagnostics = run_worker('etcd', requests, provider: provider, diagnostics: diagnostics)
    assert_hello_capabilities(messages)
    assert_shutdown_cleanup(messages)
    assert provider.closed
    assert_empty diagnostics.string
  end

  def test_worker_maps_etcd_try_acquire_to_unsupported_capability
    provider = FakeProvider.new
    requests = [
      '{"id":"try","op":"try_acquire","resource":"r"}',
      '{"id":"stop","op":"shutdown"}'
    ]
    messages, = run_worker('etcd', requests, provider: provider)
    response = messages.find { |message| message['id'] == 'try' }
    assert_equal 'unsupported_capability', response.fetch('status')
    assert_equal false, response.fetch('remote_outcome_unknown')
  end

  def test_cancel_request_preserves_cancelled_and_unknown_acquisition_outcome
    messages = cancellation_messages
    assert_cancellation_outcome(messages)
    assert_shutdown_cleanup(messages.values)
  end

  def cancellation_messages
    input, output, started, worker = cancellation_components
    run_cancellation_scenario(input, started, worker)
    messages_by_id(output)
  end

  def cancellation_components
    input = QueueInput.new
    output = StringIO.new
    started = Queue.new
    worker = Worker.new(provider: BlockingProvider.new(started), backend: 'redis', input: input,
                        output: output, diagnostics: StringIO.new)
    [input, output, started, worker]
  end

  def run_cancellation_scenario(input, started, worker)
    runner = Thread.new { worker.run }
    input.feed("{\"id\":\"acquire\",\"op\":\"acquire\",\"resource\":\"r\"}\n")
    Timeout.timeout(2) { started.pop }
    input.feed("{\"id\":\"cancel\",\"op\":\"cancel_request\",\"target_id\":\"acquire\"}\n")
    input.feed("{\"id\":\"shutdown\",\"op\":\"shutdown\"}\n")
    Timeout.timeout(2) { runner.join }
  ensure
    runner&.kill if runner&.alive?
    runner&.join
  end

  class FakeProvider
    attr_reader :closed

    def lock(_resource)
      FakeLock.new
    end

    def close
      @closed = true
    end
  end

  class FakeLock
    def try_acquire(_options) = nil
    def acquire(_wait_ms, _options) = nil
  end

  class QueueInput
    def initialize
      @bytes = Queue.new
    end

    def feed(value)
      value.each_byte { |byte| @bytes << byte }
    end

    def getbyte
      @bytes.pop
    end
  end

  class BlockingProvider < FakeProvider
    def initialize(started)
      super()
      @started = started
    end

    def lock(_resource)
      BlockingLock.new(@started)
    end
  end

  class BlockingLock
    def initialize(started)
      super()
      @started = started
    end

    def acquire(_wait_ms, _options)
      @started << true
      Queue.new.pop
    end
  end
end
