# frozen_string_literal: true

require 'minitest/autorun'
require 'json'
require 'open3'
require 'rbconfig'
require 'securerandom'
require 'fileutils'
require 'tmpdir'
require 'timeout'
require_relative '../lib/dilock'

class WorkerProcessIntegrationTest < Minitest::Test
  def setup
    @endpoint = ENV.fetch('DILOCK_TEST_WORKER_REDIS_URL', nil)
    skip 'set DILOCK_TEST_WORKER_REDIS_URL to a disposable Redis endpoint' unless @endpoint

    @namespace = "worker-test-#{SecureRandom.hex(8)}"
    @epoch = "worker-test-#{SecureRandom.hex(8)}"
    Dilock::RedisProvider.provision_namespace!(@endpoint, namespace: @namespace, epoch: @epoch)
    @directory = Dir.mktmpdir('dilock-ruby-worker-test-')
    config = File.join(@directory, 'worker-config.json')
    File.write(config, JSON.generate(backend: 'redis', endpoint: @endpoint,
                                     namespace: @namespace, epoch: @epoch), mode: 'w:UTF-8')
    @workers = [start_worker(config), start_worker(config)]
  end

  def teardown
    Array(@workers).each { |worker| stop_worker(worker) }
    FileUtils.remove_entry(@directory) if @directory && File.exist?(@directory)
  end

  def test_two_native_worker_processes_contend_handoff_and_release
    first, second = @workers
    assert_worker_hello(first, 'hello-a')
    assert_worker_hello(second, 'hello-b')
    acquired = assert_first_worker_acquires(first)
    assert_second_worker_is_busy(second)
    release_and_handoff(first, second, acquired)
  end

  private

  def start_worker(config)
    script = File.expand_path('../bin/dilock-worker', __dir__)
    stdin, stdout, stderr, wait = Open3.popen3(RbConfig.ruby, script, config,
                                               chdir: File.expand_path('..', __dir__))
    { stdin: stdin, stdout: stdout, stderr: stderr, wait: wait }
  end

  def stop_worker(worker)
    request(worker, 'teardown', 'shutdown')
    Timeout.timeout(5) { worker[:wait].value }
  rescue StandardError
    Process.kill('KILL', worker[:wait].pid) if worker[:wait].alive?
  ensure
    close_worker_streams(worker)
  end

  def close_worker_streams(worker)
    [worker[:stdin], worker[:stdout], worker[:stderr]].each do |stream|
      stream.close unless stream.closed?
    rescue IOError
      nil
    end
  end

  def assert_worker_hello(worker, request_id)
    assert_equal 'ok', request(worker, request_id, 'hello').fetch('status')
  end

  def assert_first_worker_acquires(worker)
    response = request(worker, 'acquire-a', 'try_acquire', resource: 'worker-process-resource', lease_ms: 10_000)
    assert_equal 'acquired', response.fetch('status')
    response
  end

  def assert_second_worker_is_busy(worker)
    response = request(worker, 'busy-b', 'try_acquire', resource: 'worker-process-resource', lease_ms: 10_000)
    assert_equal 'busy', response.fetch('status')
  end

  def release_and_handoff(first, second, acquired)
    lease_id = acquired.fetch('lease_id')
    assert_equal 'released', request(first, 'release-a', 'release', lease_id: lease_id).fetch('status')
    assert_equal 'released', request(first, 'release-a-repeat', 'release', lease_id: lease_id).fetch('status')

    successor = request(second, 'acquire-b', 'try_acquire', resource: 'worker-process-resource', lease_ms: 10_000)
    assert_equal 'acquired', successor.fetch('status')
    assert_equal 'released',
                 request(second, 'release-b', 'release', lease_id: successor.fetch('lease_id')).fetch('status')
  end

  def request(worker, request_id, operation, **fields)
    worker[:stdin].write("#{JSON.generate({ id: request_id, op: operation }.merge(fields))}\n")
    worker[:stdin].flush
    line = Timeout.timeout(10) { worker[:stdout].gets }
    flunk "worker exited before responding; stderr: #{worker[:stderr].read}" unless line

    JSON.parse(line)
  end
end
