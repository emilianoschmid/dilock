# frozen_string_literal: true

# This consumer is run against an installed .gem with sdk/ruby absent from the
# container. Backend endpoints and the exact archive path are supplied by the
# disposable Docker harness.
require 'rubygems/package'

gem 'dilock', '0.1.0'
require 'dilock'

gem_spec = Gem.loaded_specs.fetch('dilock')
abort "Dilock did not load from GEM_HOME: #{gem_spec.full_gem_path}" unless
  gem_spec.full_gem_path.start_with?(Gem.dir)

archive = Gem::Package.new(ENV.fetch('DILOCK_GEM_FILE'))
required_files = %w[
  lib/dilock/redis/redis-release.lua
  lib/dilock/redis/redis-renew.lua
  lib/dilock/etcd/proto/kv_pb.rb
  lib/dilock/etcd/proto/rpc_pb.rb
  lib/dilock/etcd/proto/v3lock_pb.rb
  lib/dilock/etcd/proto/services.rb
  lib/dilock/etcd/proto/kv.proto
  lib/dilock/etcd/proto/rpc.proto
  lib/dilock/etcd/proto/v3lock.proto
  lib/dilock/etcd/proto/LICENSE-etcd-api
  lib/dilock/etcd/proto/LICENSE-etcd-server
]
missing_files = required_files - archive.contents
abort "gem is missing runtime assets: #{missing_files.join(', ')}" unless missing_files.empty?
missing_installed_files = required_files.reject do |path|
  File.file?(File.join(gem_spec.full_gem_path, path))
end
abort "installed gem is missing runtime assets: #{missing_installed_files.join(', ')}" unless
  missing_installed_files.empty?

options = Dilock::AcquireOptions.new(wait_ms: 0, lease_ms: 5_000)

def close_providers(*providers)
  providers.compact.each(&:close)
end

redis_endpoint = ENV.fetch('DILOCK_TEST_REDIS_URL')
redis_namespace = ENV.fetch('DILOCK_TEST_REDIS_NAMESPACE')
redis_epoch = ENV.fetch('DILOCK_TEST_REDIS_EPOCH')
Dilock::RedisProvider.provision_namespace!(redis_endpoint, namespace: redis_namespace, epoch: redis_epoch)
redis_first = Dilock::RedisProvider.connect(redis_endpoint, namespace: redis_namespace, epoch: redis_epoch)
redis_second = Dilock::RedisProvider.connect(redis_endpoint, namespace: redis_namespace, epoch: redis_epoch)
redis_resource = "ruby-gem-redis-#{Process.pid}"
redis_handle_first = redis_first.lock(redis_resource)
redis_handle_second = redis_second.lock(redis_resource)
redis_owner = nil
redis_successor = nil
redis_stale = nil
begin
  redis_owner = redis_handle_first.try_acquire(options)
  abort 'Redis packaged owner failed to acquire' unless redis_owner
  abort 'Redis second packaged provider did not observe contention' if redis_handle_second.try_acquire(options)
  abort 'Redis packaged owner did not acknowledge release' unless redis_owner.release == :released

  redis_successor = redis_handle_second.try_acquire(options)
  abort 'Redis packaged successor did not acquire after release' unless redis_successor
  abort 'Redis packaged successor did not acknowledge release' unless redis_successor.release == :released
  redis_successor = nil

  expiring_options = Dilock::AcquireOptions.new(wait_ms: 0, lease_ms: 1_000)
  redis_stale = redis_handle_first.try_acquire(expiring_options)
  abort 'Redis expiring packaged owner failed to acquire' unless redis_stale
  expiry_deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 8
  redis_expiry_successor = nil
  until redis_expiry_successor
    redis_expiry_successor = redis_handle_second.try_acquire(options)
    break if redis_expiry_successor
    raise 'Redis packaged lease did not expire within eight seconds' if
      Process.clock_gettime(Process::CLOCK_MONOTONIC) >= expiry_deadline

    sleep 0.025
  end
  abort 'expired Redis owner released the successor lock' unless redis_stale.release == :already_not_owned
  abort 'Redis stale release allowed a third acquisition' if redis_handle_first.try_acquire(options)
  abort 'Redis expiry successor did not release' unless redis_expiry_successor.release == :released
  redis_expiry_successor = nil
  puts 'PASS: packaged Redis contention, handoff, expiry, and stale-owner release safety'
ensure
  redis_owner&.release
  redis_successor&.release
  redis_stale&.release
  redis_expiry_successor&.release
  close_providers(redis_first, redis_second)
end

etcd_endpoint = ENV.fetch('DILOCK_TEST_ETCD_ENDPOINT')
etcd_namespace = ENV.fetch('DILOCK_TEST_ETCD_NAMESPACE')
etcd_epoch = ENV.fetch('DILOCK_TEST_ETCD_EPOCH')
etcd_first = Dilock::EtcdProvider.connect(etcd_endpoint, namespace: etcd_namespace, epoch: etcd_epoch)
etcd_second = Dilock::EtcdProvider.connect(etcd_endpoint, namespace: etcd_namespace, epoch: etcd_epoch)
etcd_resource = "ruby-gem-etcd-#{Process.pid}"
etcd_owner = nil
etcd_successor = nil
etcd_waiter = nil
begin
  etcd_handle_first = etcd_first.lock(etcd_resource)
  etcd_handle_second = etcd_second.lock(etcd_resource)
  etcd_owner = etcd_handle_first.acquire(5_000, options)
  abort 'etcd packaged owner has no epoch-scoped fence' unless
    etcd_owner.fence.epoch == etcd_epoch && etcd_owner.fence.revision.positive?

  result = Queue.new
  etcd_waiter = Thread.new do
    result << [:acquired, etcd_handle_second.acquire(7_000, options)]
  rescue StandardError => e
    result << [:error, e]
  end
  sleep 0.3
  abort 'etcd packaged second provider did not queue behind the owner' unless etcd_waiter.alive?
  abort 'etcd packaged owner release failed' unless etcd_owner.release == :released
  status, value = result.pop
  abort "etcd packaged handoff failed: #{value.class}: #{value.message}" unless status == :acquired

  etcd_successor = value
  abort 'etcd packaged successor fence did not advance' unless
    etcd_successor.fence.epoch == etcd_epoch && etcd_successor.fence.revision > etcd_owner.fence.revision
  abort 'etcd packaged successor release failed' unless etcd_successor.release == :released
  etcd_successor = nil
  etcd_waiter.join(2)
  abort 'etcd packaged waiter thread did not terminate' if etcd_waiter.alive?
  puts 'PASS: packaged etcd queue, release/handoff, and increasing epoch-scoped fence'
ensure
  etcd_owner&.release
  etcd_successor&.release
  etcd_waiter&.kill if etcd_waiter&.alive?
  etcd_waiter&.join(2)
  close_providers(etcd_first, etcd_second)
end
