# frozen_string_literal: true
# Local-only example. No TTL, remote coordinator, renewal or wire protocol.
require 'thread'

module DilockStyle
  class Busy < StandardError; end

  # Thread-safe local fixture; object IDs are not serialized to a coordinator.
  class MemoryStore
    def initialize
      @owners = {}
      @gate = Mutex.new
    end

    def claim(resource, owner)
      @gate.synchronize do
        return false if @owners.key?(resource)
        @owners[resource] = owner
        true
      end
    end

    def release(resource, owner)
      @gate.synchronize do
        return :already_not_owned unless @owners[resource].equal?(owner)
        @owners.delete(resource)
        :released
      end
    end

    def expire_for_test(resource)
      @gate.synchronize { @owners.delete(resource) }
    end
  end

  class Lease
    def initialize(store, resource, owner)
      @store, @resource, @owner = store, resource, owner
      @closed = false
      @gate = Mutex.new
    end

    # One acquisition only. No finalizer is needed or relied on.
    def release
      @gate.synchronize do
        return :already_not_owned if @closed
        @closed = true
        @store.release(@resource, @owner)
      end
    end
  end

  class ResourceLock
    def initialize(store, resource)
      raise ArgumentError, 'empty resource' if resource.empty?
      @store, @resource = store, resource.dup.freeze
    end

    def try_acquire
      owner = Object.new
      @store.claim(@resource, owner) ? Lease.new(@store, @resource, owner) : nil
    end

    def synchronize
      lease = try_acquire or raise Busy, 'resource busy'
      begin
        yield lease
      ensure
        lease.release
      end
    end
  end

  class Provider
    def initialize(store)
      @store = store
    end
    def lock(resource)
      ResourceLock.new(@store, resource)
    end
  end
end

def check(value)
  raise 'style assertion failed' unless value
end
store = DilockStyle::MemoryStore.new
resource = DilockStyle::Provider.new(store).lock('example/resource')
first = resource.try_acquire
check(first && resource.try_acquire.nil?)
store.expire_for_test('example/resource')
second = resource.try_acquire
check(second)
check(first.release == :already_not_owned)
check(resource.try_acquire.nil?)
check(second.release == :released)
check(second.release == :already_not_owned)
class WorkFailure < StandardError; end
begin
  resource.synchronize { raise WorkFailure, 'consumer failure' }
rescue WorkFailure
  # The block's ensure has already released the local acquisition.
end
check(resource.synchronize { 42 } == 42)
puts 'PASS ruby: busy, stale owner, idempotence, block/ensure'
