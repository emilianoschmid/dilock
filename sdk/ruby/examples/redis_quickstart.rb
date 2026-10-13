# frozen_string_literal: true

require 'dilock'

endpoint = ENV.fetch('DILOCK_REDIS_URL', 'redis://127.0.0.1:6379/0')
namespace = ENV.fetch('DILOCK_NAMESPACE', 'default')
epoch = ENV.fetch('DILOCK_EPOCH', 'local-dev-redis')

# Provision once in an explicitly disposable development keyspace. Deployments
# should use the documented administrator workflow instead of provisioning at app startup.
if ENV['DILOCK_PROVISION'] == '1'
  Dilock::RedisProvider.provision_namespace!(endpoint, namespace: namespace,
                                                       epoch: epoch)
end

provider = Dilock::RedisProvider.connect(endpoint, namespace: namespace, epoch: epoch)
begin
  result = provider.lock('invoice-reconciliation').with_lease do |lease|
    puts "redis lease valid for at most #{lease.effective_lease_ms} ms"
    :work_completed
  end
  puts(result.inspect)
ensure
  provider.close
end
