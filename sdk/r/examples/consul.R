library(dilock)

run_consul_example <- function() {
provider <- dilock_consul_provider(
  endpoint = Sys.getenv("DILOCK_CONSUL_ENDPOINT", "http://127.0.0.1:8500"),
  namespace = Sys.getenv("DILOCK_CONSUL_NAMESPACE", "billing"),
  epoch = Sys.getenv("DILOCK_CONSUL_EPOCH"),
  token = { value <- Sys.getenv("DILOCK_CONSUL_TOKEN", ""); if (nzchar(value)) value else NULL }
)
on.exit(dilock_consul_close(provider), add = TRUE)
invoice <- dilock_consul_lock(provider, "invoice:42")
lease <- dilock_consul_acquire(invoice, wait_ms = 5000L, lease_ms = 30000L)
on.exit(dilock_consul_release(lease), add = TRUE)

fence <- dilock_consul_fence(lease)
message("fence: ", fence$coordination_epoch, "/", fence$lock_index)
stopifnot(dilock_consul_valid(lease))
# Renew explicitly during long-running work and enforce the fence atomically
# when writing the protected resource.
stopifnot(dilock_consul_renew(lease))
}
run_consul_example()
