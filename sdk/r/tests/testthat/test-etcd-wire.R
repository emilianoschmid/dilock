test_that("etcd int64 varints retain exact lease and revision bytes", {
  expect_identical(dilock:::.dilock_etcd_decimal(as.raw(c(0xa0, 0x8d, 0x06))), "100000")
  expect_identical(
    dilock:::.dilock_etcd_decimal(as.raw(c(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x0f))),
    "9007199254740991"
  )
  expect_identical(
    dilock:::.dilock_etcd_decimal(as.raw(c(rep(0xff, 8L), 0x7f))),
    "9223372036854775807"
  )
  fields <- dilock:::.dilock_etcd_fields(as.raw(c(0x10, 0xac, 0x02)))
  expect_identical(dilock:::.dilock_etcd_one_field(fields, 2, 0)$bytes, as.raw(c(0xac, 0x02)))
})

test_that("R deadlines use the registered native monotonic clock", {
  before <- dilock:::.dilock_monotonic()
  Sys.sleep(0.02)
  after <- dilock:::.dilock_monotonic()
  expect_true(is.finite(before))
  expect_gte(after, before)
})

test_that("etcd release shares one monotonic budget across cleanup RPCs", {
  remaining <- dilock:::.dilock_etcd_remaining_ms
  expect_identical(remaining(10, now = 9.75), 250L)
  expect_identical(remaining(10, now = 10), 0L)
  expect_identical(remaining(10, now = 10.25), 0L)
  expect_identical(remaining(Inf, now = 9), 0L)
})

test_that("etcd release requires exact ownership and both positive acknowledgements", {
  classify <- dilock:::.dilock_etcd_release_outcome
  expect_identical(classify("owned", TRUE, "acknowledged"), "released")
  expect_identical(classify("owned", TRUE, "unknown"), "unknown")
  expect_identical(classify("owned", FALSE, "acknowledged"), "already_not_owned")
  expect_identical(classify("unknown", FALSE, "acknowledged"), "already_not_owned")
  expect_identical(classify("owned", TRUE, "not_found"), "already_not_owned")
  expect_identical(classify("absent", FALSE, "unknown"), "already_not_owned")
  expect_identical(classify("mismatch", FALSE, "unknown"), "already_not_owned")
  expect_identical(classify("unknown", FALSE, "unknown"), "unknown")
})

test_that("nonblocking etcd try fails explicitly without backend access", {
  provider <- new.env(parent = emptyenv())
  provider$closed <- FALSE
  handle <- structure(list(provider = provider), class = "dilock_etcd_handle")
  expect_error(dilock_etcd_try_acquire(handle), class = "dilock_unsupported_capability")
})

test_that("unreturned etcd acquisitions attempt lease cleanup at most once", {
  state <- new.env(parent = emptyenv())
  state$returned <- FALSE
  state$cleanup_attempted <- FALSE
  calls <- 0L
  cleanup <- function() calls <<- calls + 1L

  dilock:::.dilock_etcd_acquisition_cleanup(state, cleanup, suppress = TRUE)
  dilock:::.dilock_etcd_acquisition_cleanup(state, cleanup, suppress = TRUE)
  expect_identical(calls, 1L)
  expect_true(state$cleanup_attempted)

  returned <- new.env(parent = emptyenv())
  returned$returned <- TRUE
  returned$cleanup_attempted <- FALSE
  dilock:::.dilock_etcd_acquisition_cleanup(returned, cleanup, suppress = TRUE)
  expect_identical(calls, 1L)

  failed <- new.env(parent = emptyenv())
  failed$returned <- FALSE
  failed$cleanup_attempted <- FALSE
  expect_false(dilock:::.dilock_etcd_acquisition_cleanup(
    failed, function() stop("simulated revoke failure"), suppress = TRUE
  ))
  expect_true(failed$cleanup_attempted)
})

test_that("terminal etcd leases release provider tracking and bound retained state", {
  provider <- new.env(parent = emptyenv())
  provider$leases <- list()
  provider$unknown_cleanup <- FALSE

  released <- new.env(parent = emptyenv())
  released$outcome <- "released"
  released$stream <- new.env(parent = emptyenv())
  released$stream_id <- "stream-1"
  released$lock_call <- new.env(parent = emptyenv())
  released$client <- new.env(parent = emptyenv())
  provider$leases <- list(released)
  dilock:::.dilock_etcd_untrack_lease(provider, released)

  expect_length(provider$leases, 0L)
  expect_false(provider$unknown_cleanup)
  expect_null(released$stream)
  expect_null(released$stream_id)
  expect_null(released$lock_call)
  expect_null(released$client)

  uncertain <- new.env(parent = emptyenv())
  uncertain$outcome <- "unknown"
  provider$leases <- list(uncertain)
  dilock:::.dilock_etcd_untrack_lease(provider, uncertain)
  expect_length(provider$leases, 0L)
  expect_true(provider$unknown_cleanup)
})
