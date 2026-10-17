test_that("Consul sessions acquire, fence, renew, contend, release, and transfer", {
  endpoint <- Sys.getenv("DILOCK_CONSUL_ENDPOINT", unset = "")
  skip_if(!nzchar(endpoint), "set DILOCK_CONSUL_ENDPOINT for live Consul lifecycle evidence")
  namespace <- Sys.getenv("DILOCK_CONSUL_NAMESPACE", unset = "")
  epoch <- Sys.getenv("DILOCK_CONSUL_EPOCH", unset = "")
  skip_if(!nzchar(namespace) || !nzchar(epoch), "configure a provisioned Consul namespace and epoch")
  token <- Sys.getenv("DILOCK_CONSUL_TOKEN", unset = "")
  if (!nzchar(token)) token <- NULL
  provider <- dilock_consul_provider(endpoint, namespace, epoch,
                                     token = token,
                                     rpc_timeout_ms = 1000L, poll_interval_ms = 50L)
  on.exit(dilock_consul_close(provider), add = TRUE)
  handle <- dilock_consul_lock(provider, paste0("r-live/", Sys.getpid(), "/", as.integer(Sys.time())))
  first <- dilock_consul_acquire(handle, wait_ms = 3000L, lease_ms = 10000L)
  on.exit(if (is.null(first$outcome)) dilock_consul_release(first), add = TRUE)
  expect_identical(first$fence$coordination_epoch, epoch)
  expect_match(first$fence$lock_index, "^[1-9][0-9]*$")
  expect_true(dilock_consul_valid(first))
  expect_null(dilock_consul_try_acquire(handle, lease_ms = 10000L))
  expect_true(dilock_consul_renew(first))
  first_index <- first$fence$lock_index
  expect_identical(dilock_consul_release(first), "released")
  second <- dilock_consul_acquire(handle, wait_ms = 3000L, lease_ms = 10000L)
  on.exit(if (is.null(second$outcome)) dilock_consul_release(second), add = TRUE)
  second_index <- second$fence$lock_index
  expect_true(nchar(second_index) > nchar(first_index) ||
                (nchar(second_index) == nchar(first_index) && second_index > first_index))
  expect_identical(dilock_consul_release(second), "released")
})
