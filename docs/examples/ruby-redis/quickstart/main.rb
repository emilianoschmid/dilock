# frozen_string_literal: true

require 'dilock'

endpoint = ENV.fetch('DILOCK_REDIS_URL', 'redis://127.0.0.1:6379/0')
namespace = ENV.fetch('DILOCK_NAMESPACE', 'default')
epoch = ENV.fetch('DILOCK_EPOCH', 'local-redis-epoch')
provider = Dilock::RedisProvider.connect(endpoint, namespace: namespace, epoch: epoch)
begin
  # Different resources share the provider and its owned connections.
  result = provider.lock('reports/daily').with_lease do |lease|
    puts "Default Redis lease valid for at most #{lease.effective_lease_ms} ms"
    :work_completed
  end
  policy = Dilock::AcquireOptions.new(wait_ms: 3_000, lease_ms: 20_000)
  configured = provider.lock('reports/weekly').with_lease(policy) do |lease|
    puts "Configured Redis lease valid for at most #{lease.effective_lease_ms} ms"
    :configured_work_completed
  end
  puts([result, configured].inspect)
ensure
  provider.close
end
