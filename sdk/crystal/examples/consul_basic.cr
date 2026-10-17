require "../src/dilock"
require "../src/dilock/etcd"
require "../src/dilock/consul"

endpoint = ENV["DILOCK_CONSUL_ENDPOINT"]? || "http://127.0.0.1:8500"
namespace = ENV["DILOCK_NAMESPACE"]? || "billing"
epoch = ENV.fetch("DILOCK_EPOCH")
acl_token = ENV["DILOCK_CONSUL_ACL_TOKEN"]? || ""

provider = Dilock::ConsulProvider.new(epoch, endpoint, namespace, acl_token)
provider.provision_namespace
handle = provider.lock("invoice/42/reconcile")
lease = handle.acquire_default
raise Dilock::ConsulLeaseLost.new("lease lost") unless lease.valid?

fence = lease.fence
puts "Consul fence: #{fence.coordination_epoch}:#{fence.lock_index}"
raise Dilock::Error.new("release was not confirmed") unless lease.release == Dilock::ConsulReleaseOutcome::Released
lease.close
handle.close
provider.close
