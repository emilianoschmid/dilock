test_that("native etcd acquisition verifies lease, opaque key, fence and cleanup", {
  endpoint <- Sys.getenv("DILOCK_ETCD_ENDPOINT", unset = "")
  skip_if(!nzchar(endpoint), "set DILOCK_ETCD_ENDPOINT to run isolated live etcd tests")
  namespace <- Sys.getenv("DILOCK_ETCD_NAMESPACE", unset = "r-adapter-test")
  epoch <- Sys.getenv("DILOCK_ETCD_EPOCH", unset = "r-adapter-test-epoch")
  provider <- dilock_etcd_provider(endpoint, namespace = namespace, epoch = epoch,
                                   rpc_timeout_ms = 3000L, cleanup_timeout_ms = 1000L)
  on.exit(dilock_etcd_close(provider), add = TRUE)
  resource <- paste0("live/", Sys.getpid(), "/", as.integer(Sys.time()), "/", sample.int(1e8, 1L))
  handle <- dilock_etcd_lock(provider, resource)

  first <- dilock_etcd_acquire(handle, wait_ms = 3000L, lease_ms = 8000L)
  on.exit(if (is.null(first$outcome)) dilock_etcd_release(first), add = TRUE)
  exact <- dilock:::.dilock_etcd_range_lock_key(provider, first$client, first$key, 2000L)
  expect_identical(exact$key, first$key)
  expect_identical(exact$lease, first$id)
  expect_identical(exact$revision, first$fence$create_revision)
  expect_match(first$fence$create_revision, "^[1-9][0-9]*$")
  expect_true(dilock_etcd_renew(first))

  # Native Lock has no nonblocking try operation. A real queued acquire must
  # time out, independently revoke its lease, and leave the current owner held.
  expect_error(dilock_etcd_try_acquire(handle), class = "dilock_unsupported_capability")
  expect_error(dilock_etcd_acquire(handle, wait_ms = 700L, lease_ms = 5000L),
               class = "dilock_wait_timeout")
  expect_identical(first$state, "held")
  expect_identical(dilock_etcd_release(first), "released")

  second <- dilock_etcd_acquire(handle, wait_ms = 3000L, lease_ms = 8000L)
  expect_gt(as.numeric(second$fence$create_revision), as.numeric(first$fence$create_revision))
  expect_identical(dilock_etcd_release(second), "released")

  # An expired native key is not unlocked based on a stale assumption. Exact
  # Range confirms absence and the server's LeaseRevoke NOT_FOUND confirms the
  # acquisition has already expired.
  expired <- dilock_etcd_acquire(handle, wait_ms = 3000L, lease_ms = 1000L)
  # etcd expires leases asynchronously after their TTL. Wait for a linearizable
  # exact-key observation of absence instead of assuming the server's expiry
  # loop has run after a fixed client-side delay.
  expiry_deadline <- dilock:::.dilock_monotonic() + 10
  ownership <- "owned"
  while (identical(ownership, "owned") && dilock:::.dilock_monotonic() < expiry_deadline) {
    ownership <- dilock:::.dilock_etcd_release_ownership(provider, expired, 1000L)
    if (identical(ownership, "owned")) Sys.sleep(0.1)
  }
  expect_identical(ownership, "absent")
  expect_identical(dilock_etcd_release(expired), "already_not_owned")

  expect_identical(with_dilock_etcd(handle, function(lease) lease$fence$epoch,
                                    wait_ms = 3000L, lease_ms = 8000L), epoch)
  final <- dilock_etcd_acquire(handle, wait_ms = 3000L, lease_ms = 8000L)
  expect_identical(dilock_etcd_close(provider), "released")
  provider2 <- dilock_etcd_provider(endpoint, namespace = namespace, epoch = epoch,
                                    rpc_timeout_ms = 3000L, cleanup_timeout_ms = 1000L)
  on.exit(dilock_etcd_close(provider2), add = TRUE)
  remaining <- dilock:::.dilock_etcd_call(
    provider2$client, provider2$rpc$range,
    dilock:::.dilock_etcd_message("etcdserverpb.RangeRequest", key = final$key), 1000L
  )$response_message$count
  expect_identical(as.numeric(remaining), 0)
  expect_identical(dilock_etcd_close(provider2), "released")
})
