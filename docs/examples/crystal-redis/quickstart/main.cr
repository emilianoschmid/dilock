require "dilock"

provider = Dilock::RedisProvider.new(ENV.fetch("REDIS_URL"), "billing", "prod-east-1")
begin
  invoice = provider.lock("invoice:42")
  reconciliation = provider.lock("accounting:reconcile")

  invoice_result = invoice.with_lease do |_lease|
    # Protect a short, idempotent operation. A lease does not stop stale work.
    "invoice processed"
  end
  puts(invoice_result || "invoice is busy")

  options = Dilock::Options.new(ttl_ms: 60_000, require_fencing: false)
  reconciliation_result = reconciliation.with_lease(options) do |_lease|
    "reconciliation complete"
  end
  puts(reconciliation_result || "reconciliation is busy")
ensure
  provider.close
end
