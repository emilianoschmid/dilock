# Run with DILOCK_RUN_EXAMPLES=1 after provisioning the namespace metadata.
.run_redis_quickstart <- function() {
  host <- Sys.getenv("DILOCK_REDIS_HOST", "127.0.0.1")
  port <- as.integer(Sys.getenv("DILOCK_REDIS_PORT", "6379"))
  namespace <- Sys.getenv("DILOCK_REDIS_NAMESPACE", "default")
  epoch <- Sys.getenv("DILOCK_COORDINATION_EPOCH", "")
  if (!nzchar(epoch)) stop("DILOCK_COORDINATION_EPOCH must match provisioned Redis metadata")

  connection <- redux::hiredis(host = host, port = port)
  provider <- dilock::dilock_provider(connection, namespace = namespace, epoch = epoch)

  # The common case uses the SDK's 5-second wait and 30-second lease defaults.
  simple <- dilock::dilock_lock(provider, "examples/redis/simple")
  print(dilock::with_dilock(simple, function(lease) {
    message("Redis lock held; do brief protected work here")
    "work completed"
  }))

  # Override both durations when the work needs an explicit policy.
  configured <- dilock::dilock_lock(provider, "examples/redis/configured")
  print(dilock::with_dilock(configured, function(lease) {
    message("Redis lock held with explicit timing")
    "configured work completed"
  }, wait_ms = 10000L, lease_ms = 60000L))
}

if (identical(Sys.getenv("DILOCK_RUN_EXAMPLES"), "1")) .run_redis_quickstart()
