# frozen_string_literal: true

require 'minitest/autorun'
require_relative '../lib/dilock'

class ProviderCloseConcurrencyTest < Minitest::Test
  class BlockingClose
    attr_reader :calls

    def initialize
      @calls = 0
      @started = Queue.new
      @continue = Queue.new
    end

    def close
      @calls += 1
      @started << true
      @continue.pop
    end

    def wait_until_started
      @started.pop
    end

    def continue
      @continue << true
    end
  end

  class CountingClose
    attr_reader :calls

    def initialize
      @calls = 0
    end

    def close
      @calls += 1
    end
  end

  class RaisingClose
    attr_reader :calls

    def initialize
      @calls = 0
    end

    def close
      @calls += 1
      raise 'client close failed'
    end
  end

  def test_concurrent_redis_close_waits_for_and_reuses_first_close
    first_client = BlockingClose.new
    cleanup_client = CountingClose.new
    provider = fake_provider(Dilock::RedisProvider, redis: first_client, cleanup_redis: cleanup_client)

    assert_close_is_serialized(provider, first_client)
    assert_equal [1], [first_client.calls]
    assert_equal [1], [cleanup_client.calls]
  end

  def test_concurrent_etcd_close_closes_each_stub_once
    first_stub = BlockingClose.new
    other_stubs = [CountingClose.new, CountingClose.new]
    provider = fake_provider(Dilock::EtcdProvider, kv: first_stub, lease: other_stubs[0], lock: other_stubs[1])

    assert_close_is_serialized(provider, first_stub)
    assert_equal [1], [first_stub.calls]
    assert_equal [1, 1], other_stubs.map(&:calls)
  end

  def test_redis_close_attempts_cleanup_client_after_primary_client_error
    primary_client = RaisingClose.new
    cleanup_client = CountingClose.new
    provider = fake_provider(Dilock::RedisProvider, redis: primary_client, cleanup_redis: cleanup_client)

    assert_nil provider.close
    assert_equal 1, primary_client.calls
    assert_equal 1, cleanup_client.calls
    assert_nil provider.close
    assert_equal 1, primary_client.calls
    assert_equal 1, cleanup_client.calls
  end

  private

  def fake_provider(type, **clients)
    type.allocate.tap do |provider|
      provider.instance_variable_set(:@closed, false)
      provider.instance_variable_set(:@close_mutex, Mutex.new)
      clients.each { |name, client| provider.instance_variable_set("@#{name}", client) }
    end
  end

  def assert_close_is_serialized(provider, blocking_client)
    first = Thread.new { provider.close }
    blocking_client.wait_until_started
    second_done, second = competing_close(provider)
    sleep 0.05
    assert second_done.empty?, 'second close returned before the first close finished'

    blocking_client.continue
    [first, second].each(&:join)
    assert_equal true, second_done.pop
  end

  def competing_close(provider)
    completed = Queue.new
    thread = Thread.new do
      provider.close
      completed << true
    end
    [completed, thread]
  end
end
