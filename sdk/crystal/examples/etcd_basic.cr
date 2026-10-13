require "dilock"
require "dilock/etcd"

provider = Dilock::EtcdProvider.new(
  coordination_epoch: ENV.fetch("DILOCK_EPOCH"),
  endpoint: ENV.fetch("DILOCK_ETCD_ENDPOINT", "127.0.0.1:2379"),
  namespace: ENV.fetch("DILOCK_NAMESPACE", "billing"),
  auto_renew: true,
)
invoice = provider.lock("invoice:42")
reconciliation = provider.lock("accounting:reconcile")

begin
  invoice_result = invoice.with_lease do |lease|
    fence = lease.fence
    puts "acquired in epoch #{fence.coordination_epoch}, revision #{fence.create_revision}"
    # The protected resource must atomically enforce this epoch/revision fence.
    "invoice processed"
  end
  puts invoice_result

  reconciliation_result = reconciliation.with_lease(wait_ms: 1_000_i64, lease_ms: 5_000_i64) do |lease|
    fence = lease.fence
    puts "configured scope fence revision #{fence.create_revision}"
    "reconciliation complete"
  end
  puts reconciliation_result
ensure
  invoice.close
  reconciliation.close
  provider.close
end
