# frozen_string_literal: true

require 'dilock'

endpoint = ENV.fetch('DILOCK_ETCD_ENDPOINT', 'http://127.0.0.1:2379')
namespace = ENV.fetch('DILOCK_NAMESPACE', 'default')
epoch = ENV.fetch('DILOCK_EPOCH', 'local-dev-etcd')

# etcd metadata is provisioned out of band; SDK connections never create or repair it.
provider = Dilock::EtcdProvider.connect(endpoint, namespace: namespace, epoch: epoch)
begin
  result = provider.lock('reports/daily').with_lease do |lease|
    puts "fence epoch=#{lease.fence.epoch} revision=#{lease.fence.revision}"
    :work_completed
  end
  policy = Dilock::AcquireOptions.new(wait_ms: 3_000, lease_ms: 20_000)
  configured = provider.lock('reports/weekly').with_lease(policy) do |lease|
    fence = lease.fence
    puts "configured etcd fence epoch=#{fence.epoch} revision=#{fence.revision}"
    :configured_work_completed
  end
  puts([result, configured].inspect)
ensure
  provider.close
end
