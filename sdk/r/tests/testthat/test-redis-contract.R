fake_redis <- function(set_reply = "OK", key_type = "string", release_reply = 1L,
                       fail_type = FALSE, fail_release = FALSE) {
  client <- new.env(parent = emptyenv())
  client$commands <- list()
  client$command <- function(command) {
    client$commands[[length(client$commands) + 1L]] <- command
    switch(command[[1L]],
      GET = paste0(
        '{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1",',
        '"profile":"redis-single-authority-v1","coordination_epoch":"epoch-1",',
        '"fixed_lease_ms":null}'
      ),
      SET = set_reply,
      TYPE = {
        if (fail_type) stop("simulated TYPE timeout")
        key_type
      },
      EVAL = {
        script <- command[[2L]]
        if (grepl("PEXPIRE", script, fixed = TRUE)) 1L else {
          if (fail_release) stop("simulated release timeout")
          release_reply
        }
      },
      stop(paste("unexpected fake Redis command:", command[[1L]]))
    )
  }
  client
}

test_that("namespace metadata requires fixed_lease_ms and an integer protocol", {
  missing_fixed_lease <- fake_redis()
  missing_fixed_lease$command <- function(command) {
    if (command[[1L]] == "GET") {
      paste0(
        '{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1",',
        '"profile":"redis-single-authority-v1","coordination_epoch":"epoch-1"}'
      )
    }
  }
  expect_error(
    dilock_provider(missing_fixed_lease, epoch = "epoch-1"),
    class = "dilock_protocol_mismatch"
  )

  fractional_major <- fake_redis()
  fractional_major$command <- function(command) {
    if (command[[1L]] == "GET") {
      paste0(
        '{"protocol_major":1.5,"identity_scheme":"sha256-framed-utf8-v1",',
        '"profile":"redis-single-authority-v1","coordination_epoch":"epoch-1",',
        '"fixed_lease_ms":null}'
      )
    }
  }
  expect_error(dilock_provider(fractional_major, epoch = "epoch-1"),
               class = "dilock_protocol_mismatch")
})

test_that("shared metadata validator rejects non-integer JSON protocol majors", {
  prefix <- '{"protocol_major":'
  suffix <- paste0(
    ',"identity_scheme":"sha256-framed-utf8-v1",',
    '"profile":"redis-single-authority-v1","coordination_epoch":"epoch-1",',
    '"fixed_lease_ms":null}'
  )
  expect_true(dilock:::.dilock_validate_metadata_json(
    paste0(prefix, "1", suffix), "redis-single-authority-v1", "epoch-1"
  ))
  etcd_suffix <- sub("redis-single-authority-v1", "etcd-native-lock-v1", suffix, fixed = TRUE)
  expect_true(dilock:::.dilock_validate_metadata_json(
    paste0(prefix, "1", etcd_suffix), "etcd-native-lock-v1", "epoch-1"
  ))
  for (major in c("1.0", "1.5", "\"1\"", "true", "4294967297")) {
    expect_error(
      dilock:::.dilock_validate_metadata_json(
        paste0(prefix, major, suffix), "redis-single-authority-v1", "epoch-1"
      ),
      class = "dilock_protocol_mismatch"
    )
    expect_error(
      dilock:::.dilock_validate_metadata_json(
        paste0(prefix, major, etcd_suffix), "etcd-native-lock-v1", "epoch-1"
      ),
      class = "dilock_protocol_mismatch"
    )
  }
})

test_that("acknowledged Redis contention validates key type", {
  for (key_type in c("string", "none")) {
    client <- fake_redis(set_reply = NULL, key_type = key_type)
    provider <- dilock_provider(client, epoch = "epoch-1")
    handle <- dilock_lock(provider, "resource")
    expect_null(dilock_try_acquire(handle))
    expect_identical(client$commands[[3L]][[1L]], "TYPE")
  }

  wrong_type <- fake_redis(set_reply = NULL, key_type = "hash")
  handle <- dilock_lock(dilock_provider(wrong_type, epoch = "epoch-1"), "resource")
  expect_error(dilock_try_acquire(handle), class = "dilock_protocol_mismatch")
  expect_length(wrong_type$commands, 3L)

  unavailable <- fake_redis(set_reply = NULL, fail_type = TRUE)
  handle <- dilock_lock(dilock_provider(unavailable, epoch = "epoch-1"), "resource")
  expect_error(dilock_try_acquire(handle), class = "dilock_backend_error")
})

test_that("acquire never returns SET ownership after the total wait deadline", {
  lease <- structure(list(owner = "test-owner"), class = "dilock_lease")
  cleanup_calls <- 0L
  confirmed_cleanup <- function(value) {
    expect_identical(value, lease)
    cleanup_calls <<- cleanup_calls + 1L
    "released"
  }

  expect_identical(
    dilock:::.dilock_reconcile_late_acquisition(lease, deadline = 10, now = 9.999,
                                                 release = confirmed_cleanup),
    lease
  )
  expect_identical(cleanup_calls, 0L)

  expect_error(
    dilock:::.dilock_reconcile_late_acquisition(lease, deadline = 10, now = 10,
                                                 release = confirmed_cleanup),
    class = "dilock_wait_timeout"
  )
  expect_identical(cleanup_calls, 1L)

  expect_error(
    dilock:::.dilock_reconcile_late_acquisition(lease, deadline = 10, now = 11,
                                                 release = function(value) "unknown"),
    class = "dilock_acquisition_unknown"
  )
})

test_that("Redis SET interruption triggers owner-checked cleanup", {
  client <- fake_redis()
  client$command <- function(command) {
    client$commands[[length(client$commands) + 1L]] <- command
    if (identical(command[[1L]], "GET")) {
      return(paste0(
        '{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1",',
        '"profile":"redis-single-authority-v1","coordination_epoch":"epoch-1",',
        '"fixed_lease_ms":null}'
      ))
    }
    if (identical(command[[1L]], "SET")) {
      stop(structure(list(message = "simulated user interrupt"),
                     class = c("interrupt", "condition")))
    }
    if (identical(command[[1L]], "EVAL")) return(1L)
    stop("unexpected command in interrupt regression")
  }
  handle <- dilock_lock(dilock_provider(client, epoch = "epoch-1"), "resource")
  interrupted <- tryCatch(dilock_try_acquire(handle), interrupt = identity)

  expect_s3_class(interrupted, "interrupt")
  expect_identical(
    vapply(client$commands, function(command) command[[1L]], character(1)),
    c("GET", "SET", "EVAL")
  )
})

test_that("invalid renewal duration is rejected before Redis and preserves ownership", {
  client <- fake_redis()
  handle <- dilock_lock(dilock_provider(client, epoch = "epoch-1"), "resource")
  lease <- dilock_try_acquire(handle)

  expect_error(dilock_renew(lease, 99L), class = "dilock_invalid_argument")
  expect_error(dilock_renew(lease, 100.5), class = "dilock_invalid_argument")
  expect_identical(lease$state, "held")
  expect_identical(vapply(client$commands, function(command) command[[1L]], character(1)), c("GET", "SET"))

  lease$deadline <- unname(proc.time()[["elapsed"]]) - 1
  expect_false(dilock_renew(lease))
  expect_identical(lease$state, "lost")
  expect_identical(vapply(client$commands, function(command) command[[1L]], character(1)), c("GET", "SET"))
})

test_that("scoped release returns the value but surfaces unknown cleanup", {
  client <- fake_redis()
  handle <- dilock_lock(dilock_provider(client, epoch = "epoch-1"), "resource")
  expect_identical(with_dilock(handle, function(lease) 42L), 42L)
  expect_true(any(vapply(client$commands, function(command) command[[1L]] == "EVAL", logical(1))))

  unknown <- fake_redis(fail_release = TRUE)
  handle <- dilock_lock(dilock_provider(unknown, epoch = "epoch-1"), "resource")
  expect_error(with_dilock(handle, function(lease) 42L), class = "dilock_release_unknown")

  no_longer_owned <- fake_redis(release_reply = 0L)
  handle <- dilock_lock(dilock_provider(no_longer_owned, epoch = "epoch-1"), "resource")
  expect_error(with_dilock(handle, function(lease) 42L), class = "dilock_lease_lost")

  null_reply <- fake_redis(release_reply = NULL)
  handle <- dilock_lock(dilock_provider(null_reply, epoch = "epoch-1"), "resource")
  expect_error(with_dilock(handle, function(lease) 42L), class = "dilock_release_unknown")

  body_error <- fake_redis(fail_release = TRUE)
  handle <- dilock_lock(dilock_provider(body_error, epoch = "epoch-1"), "resource")
  expect_warning(
    tryCatch(with_dilock(handle, function(lease) stop("body failure")),
             error = function(error) NULL),
    "cleanup outcome is unknown"
  )

  body_error_after_loss <- fake_redis(release_reply = 0L)
  handle <- dilock_lock(dilock_provider(body_error_after_loss, epoch = "epoch-1"), "resource")
  expect_warning(
    tryCatch(with_dilock(handle, function(lease) stop("body failure")),
             error = function(error) NULL),
    "lease was no longer owned"
  )
})
