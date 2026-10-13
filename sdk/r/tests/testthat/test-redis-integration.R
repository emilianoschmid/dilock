test_that("real Redis ownership survives contention, expiry, and stale cleanup", {
  skip_if_not_installed("redux")
  host <- Sys.getenv("DILOCK_TEST_REDIS_HOST", "")
  skip_if(!nzchar(host), "set DILOCK_TEST_REDIS_HOST and DILOCK_TEST_REDIS_PORT for live Redis")
  port <- as.integer(Sys.getenv("DILOCK_TEST_REDIS_PORT", "6379"))
  expect_true(is.finite(port) && port > 0L && port <= 65535L)

  namespace <- paste0("it", paste(sprintf("%02x", as.integer(openssl::rand_bytes(6L))), collapse = ""))
  epoch <- paste0("test-", paste(sprintf("%02x", as.integer(openssl::rand_bytes(8L))), collapse = ""))
  metadata_key <- paste0("dilock:namespace:", namespace, ":meta")
  admin <- redux::hiredis(host = host, port = port)
  first_connection <- redux::hiredis(host = host, port = port)
  second_connection <- redux::hiredis(host = host, port = port)
  lock_key <- NULL
  first <- handoff <- stale <- successor <- NULL

  on.exit({
    if (!is.null(first)) try(dilock_release(first), silent = TRUE)
    if (!is.null(handoff)) try(dilock_release(handoff), silent = TRUE)
    if (!is.null(stale)) try(dilock_release(stale), silent = TRUE)
    if (!is.null(successor)) try(dilock_release(successor), silent = TRUE)
    if (!is.null(lock_key)) try(admin$command(list("DEL", lock_key)), silent = TRUE)
    try(admin$command(list("DEL", metadata_key)), silent = TRUE)
    rm(admin, first_connection, second_connection)
    invisible(gc())
  }, add = TRUE)

  metadata <- jsonlite::toJSON(list(
    protocol_major = 1L,
    identity_scheme = "sha256-framed-utf8-v1",
    profile = "redis-single-authority-v1",
    coordination_epoch = epoch,
    fixed_lease_ms = NULL
  ), auto_unbox = TRUE, null = "null")
  expect_identical(as.character(admin$command(list("SET", metadata_key, as.character(metadata)))), "OK")

  first_provider <- dilock_provider(first_connection, namespace, epoch)
  second_provider <- dilock_provider(second_connection, namespace, epoch)
  first_handle <- dilock_lock(first_provider, "redis-integration-resource")
  second_handle <- dilock_lock(second_provider, "redis-integration-resource")
  lock_key <- first_handle$key

  first <- dilock_try_acquire(first_handle, 2000L)
  expect_s3_class(first, "dilock_lease")
  expect_null(dilock_try_acquire(second_handle, 2000L))
  expect_identical(dilock_release(first), "released")

  handoff <- dilock_try_acquire(second_handle, 2000L)
  expect_s3_class(handoff, "dilock_lease")
  expect_identical(dilock_release(handoff), "released")

  stale <- dilock_try_acquire(first_handle, 100L)
  expect_s3_class(stale, "dilock_lease")
  Sys.sleep(0.15)
  successor <- dilock_try_acquire(second_handle, 2000L)
  expect_s3_class(successor, "dilock_lease")
  expect_identical(dilock_release(stale), "already_not_owned")
  expect_null(dilock_try_acquire(first_handle, 2000L))
  expect_identical(dilock_release(successor), "released")
})
