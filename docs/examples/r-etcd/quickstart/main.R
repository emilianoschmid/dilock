# Run with DILOCK_RUN_EXAMPLES=1 after provisioning the namespace metadata.
.run_etcd_quickstart <- function() {
  target <- Sys.getenv("DILOCK_ETCD_TARGET", "127.0.0.1:2379")
  namespace <- Sys.getenv("DILOCK_ETCD_NAMESPACE", "default")
  epoch <- Sys.getenv("DILOCK_COORDINATION_EPOCH", "")
  if (!nzchar(epoch)) stop("DILOCK_COORDINATION_EPOCH must match provisioned etcd metadata")

  provider <- dilock::dilock_etcd_provider(
    target = target,
    namespace = namespace,
    epoch = epoch
  )
  on.exit(dilock::dilock_etcd_close(provider), add = TRUE)

  # The common case uses the SDK's 5-second wait and 30-second requested lease.
  simple <- dilock::dilock_etcd_lock(provider, "examples/etcd/simple")
  print(dilock::with_dilock_etcd(simple, function(lease) {
    message("etcd lock held; fence: ", lease$fence$epoch, "/", lease$fence$create_revision)
    "work completed"
  }))

  # etcd's native Lock RPC queues waiters; this uses bounded queue waiting.
  configured <- dilock::dilock_etcd_lock(provider, "examples/etcd/configured")
  print(dilock::with_dilock_etcd(configured, function(lease) {
    message("etcd lock held with explicit timing")
    "configured work completed"
  }, wait_ms = 10000L, lease_ms = 60000L))
}

if (identical(Sys.getenv("DILOCK_RUN_EXAMPLES"), "1")) .run_etcd_quickstart()
