.dilock_bytes <- function(x, label) {
  if (!is.character(x) || length(x) != 1L || is.na(x)) stop(label, " must be one non-NA string", call. = FALSE)
  encoded <- iconv(x, from = "UTF-8", to = "UTF-8", sub = NA_character_)
  if (is.na(encoded)) stop(label, " is not valid UTF-8", call. = FALSE)
  charToRaw(enc2utf8(encoded))
}

.u32be <- function(n) as.raw(c(bitwAnd(bitwShiftR(as.integer(n), 24L), 255L),
                               bitwAnd(bitwShiftR(as.integer(n), 16L), 255L),
                               bitwAnd(bitwShiftR(as.integer(n), 8L), 255L),
                               bitwAnd(as.integer(n), 255L)))

#' Hash strict draft 1 UTF-8 namespace/resource identity bytes.
dilock_resource_id <- function(namespace, resource) {
  ns <- .dilock_bytes(namespace, "namespace")
  res <- .dilock_bytes(resource, "resource")
  if (length(ns) < 1L || length(ns) > 64L ||
      !grepl("^[a-z0-9][a-z0-9_.-]{0,63}$", namespace)) stop("invalid namespace", call. = FALSE)
  if (length(res) < 1L || length(res) > 1024L || any(res == as.raw(0))) stop("invalid resource", call. = FALSE)
  framed <- c(charToRaw("dilock.lock.identity"), as.raw(0), .u32be(length(ns)), ns, .u32be(length(res)), res)
  paste(sprintf("%02x", as.integer(openssl::sha256(framed))), collapse = "")
}

.dilock_expect_meta <- list(protocol_major = 1L, identity_scheme = "sha256-framed-utf8-v1",
                            profile = "redis-single-authority-v1", fixed_lease_ms = NULL)

.dilock_validate_metadata_json <- function(text, profile, epoch) {
  parsed <- tryCatch(jsonlite::fromJSON(text, simplifyVector = TRUE), error = function(e) NULL)
  protocol_major <- if (is.list(parsed)) parsed$protocol_major else NULL
  required_fields <- c("protocol_major", "identity_scheme", "profile",
                       "coordination_epoch", "fixed_lease_ms")
  if (!is.list(parsed) || is.null(names(parsed)) || !all(required_fields %in% names(parsed)) ||
      !is.integer(protocol_major) || length(protocol_major) != 1L ||
      !identical(protocol_major, 1L) ||
      !identical(parsed$identity_scheme, "sha256-framed-utf8-v1") ||
      !identical(parsed$profile, profile) ||
      !identical(parsed$coordination_epoch, epoch) || !is.null(parsed$fixed_lease_ms)) {
    .dilock_stop("dilock_protocol_mismatch", "namespace metadata is absent or incompatible")
  }
  invisible(TRUE)
}

.dilock_stop <- function(class, message, parent = NULL) {
  condition <- list(message = message, call = NULL)
  if (!is.null(parent)) condition$parent <- parent
  class(condition) <- c(class, "error", "condition")
  stop(condition)
}

.dilock_validate_milliseconds <- function(value, label, minimum) {
  if (!is.numeric(value) || length(value) != 1L || is.na(value) || !is.finite(value) ||
      value != floor(value) || value < minimum || value > 86400000) {
    .dilock_stop("dilock_invalid_argument", paste("invalid", label))
  }
  as.integer(value)
}

#' Borrow a redux Redis API connection and verify existing namespace metadata.
dilock_provider <- function(connection, namespace = "default", epoch) {
  dilock_resource_id(namespace, "validation")
  epoch_bytes <- .dilock_bytes(epoch, "epoch")
  if (length(epoch_bytes) < 1L || length(epoch_bytes) > 1024L) stop("epoch must contain 1..1024 UTF-8 bytes", call. = FALSE)
  metadata <- tryCatch(connection$command(list("GET", paste0("dilock:namespace:", namespace, ":meta"))),
                       error = function(e) .dilock_stop("dilock_backend_error", "namespace read failed", e))
  if (is.raw(metadata)) {
    if (length(metadata) == 0L || length(metadata) > 8192L || any(metadata == as.raw(0))) {
      .dilock_stop("dilock_protocol_mismatch", "namespace metadata is absent, oversized, or contains NUL")
    }
    metadata <- rawToChar(metadata)
  }
  if (!is.character(metadata) || length(metadata) != 1L || is.na(metadata) ||
      nchar(metadata, type = "bytes") > 8192L) {
    .dilock_stop("dilock_protocol_mismatch", "namespace metadata is absent or oversized")
  }
  .dilock_validate_metadata_json(metadata, .dilock_expect_meta$profile, epoch)
  structure(list(connection = connection, namespace = namespace, epoch = epoch), class = "dilock_provider")
}

#' Create a lazy resource handle; the caller retains ownership of the Redis connection.
dilock_lock <- function(provider, resource) {
  stopifnot(inherits(provider, "dilock_provider"))
  structure(list(provider = provider,
                 key = paste0("dilock:{", dilock_resource_id(provider$namespace, resource), "}:lease")),
            class = "dilock_handle")
}

.dilock_eval <- function(client, script, key, ...) client$command(list("EVAL", script, "1", key, ...))
.dilock_unknown <- function(message, parent = NULL) {
  stop(structure(list(message = message, parent = parent), class = c("dilock_acquisition_unknown", "error", "condition")))
}
.dilock_release_script <- function() paste(readLines(system.file("redis-release.lua", package = "dilock"), warn = FALSE, encoding = "UTF-8"), collapse = "\n")
.dilock_renew_script <- function() paste(readLines(system.file("redis-renew.lua", package = "dilock"), warn = FALSE, encoding = "UTF-8"), collapse = "\n")

#' Try one SET NX PX. NULL represents only acknowledged contention.
dilock_try_acquire <- function(handle, lease_ms = 30000L) {
  stopifnot(inherits(handle, "dilock_handle"))
  lease_ms <- .dilock_validate_milliseconds(lease_ms, "lease duration", 100L)
  client <- handle$provider$connection
  owner <- paste0("dilock/1/", paste(sprintf("%02x", as.integer(openssl::rand_bytes(32L))), collapse = ""))
  sent <- .dilock_monotonic()
  ownership_transferred <- FALSE
  cleanup_attempted <- FALSE
  cleanup_once <- function() {
    if (cleanup_attempted) return(invisible(NULL))
    cleanup_attempted <<- TRUE
    try(.dilock_eval(client, .dilock_release_script(), handle$key, owner), silent = TRUE)
  }
  # R interrupts do not necessarily inherit from `error`, so an error handler
  # around SET is insufficient. If SET may have reached Redis, always attempt
  # owner-checked cleanup unless the handle is returned to the caller.
  on.exit({
    if (!ownership_transferred && !cleanup_attempted) cleanup_once()
  }, add = TRUE)
  response <- tryCatch(client$command(list("SET", handle$key, owner, "PX", as.character(lease_ms), "NX")),
                       error = function(e) {
                         cleanup_once()
                         .dilock_stop("dilock_acquisition_unknown", "SET was dispatched without an ownership acknowledgement", e)
                       })
  if (is.null(response)) {
    ownership_transferred <- TRUE
    type <- tryCatch(client$command(list("TYPE", handle$key)),
                     error = function(e) .dilock_stop("dilock_backend_error", "Redis TYPE check failed after acknowledged contention", e))
    if (is.raw(type)) type <- rawToChar(type)
    if (!is.character(type) || length(type) != 1L || is.na(type) || !(type %in% c("string", "none"))) {
      .dilock_stop("dilock_protocol_mismatch", "lock key has a non-string Redis type")
    }
    return(NULL)
  }
  # redux represents Redis status replies with the redis_status S3 class.
  # Compare the payload, not the driver's wrapper class.
  response_text <- tryCatch(as.character(response), error = function(e) character())
  if (length(response_text) != 1L || is.na(response_text) || !identical(response_text, "OK")) {
    cleanup_once()
    .dilock_unknown("unexpected SET reply")
  }
  margin <- max(10, ceiling(lease_ms * 0.01))
  deadline <- sent + (lease_ms - margin) / 1000
  if (.dilock_monotonic() >= deadline) {
    cleanup_once()
    .dilock_unknown("SET acknowledgement arrived after conservative deadline")
  }
  lease <- new.env(parent = emptyenv())
  lease$handle <- handle; lease$owner <- owner; lease$lease_ms <- as.integer(lease_ms)
  lease$deadline <- deadline; lease$state <- "held"; lease$outcome <- NULL
  class(lease) <- "dilock_lease"
  ownership_transferred <- TRUE
  lease
}

# A blocking Redis command can finish after the caller's wait budget. Do not
# hand that late ownership to user work: remove only this acquisition's owner
# and preserve ambiguity if the bounded cleanup is not acknowledged.
.dilock_reconcile_late_acquisition <- function(lease, deadline, now, release = dilock_release) {
  if (now < deadline) return(lease)
  outcome <- tryCatch(release(lease), error = function(e) "unknown")
  if (identical(outcome, "unknown")) {
    .dilock_unknown("acquire wait budget elapsed after SET; owner cleanup is unconfirmed")
  }
  .dilock_stop("dilock_wait_timeout", "acquire wait budget elapsed before ownership could be returned")
}

#' Wait with jitter; only acknowledged contention is retried.
dilock_acquire <- function(handle, wait_ms = 5000L, lease_ms = 30000L) {
  wait_ms <- .dilock_validate_milliseconds(wait_ms, "wait budget", 1L)
  lease_ms <- .dilock_validate_milliseconds(lease_ms, "lease duration", 100L)
  deadline <- .dilock_monotonic() + wait_ms / 1000
  repeat {
    remaining <- deadline - .dilock_monotonic()
    if (remaining <= 0) stop(structure(list(message = "contention wait exhausted"), class = c("dilock_wait_timeout", "error", "condition")))
    lease <- dilock_try_acquire(handle, lease_ms)
    if (!is.null(lease)) return(.dilock_reconcile_late_acquisition(lease, deadline, .dilock_monotonic()))
    Sys.sleep(min(remaining, stats::runif(1, 0.02, 0.05)))
  }
}

#' Owner-checked explicit release. Outcome strings are released/already_not_owned/unknown.
dilock_release <- function(lease) {
  stopifnot(inherits(lease, "dilock_lease"))
  if (!is.null(lease$outcome)) return(lease$outcome)
  lease$state <- "releasing"
  lease$outcome <- tryCatch({
    value <- .dilock_eval(lease$handle$provider$connection, .dilock_release_script(), lease$handle$key, lease$owner)
    reply <- as.character(value)
    if (length(reply) != 1L || is.na(reply) || !(reply %in% c("0", "1"))) {
      .dilock_unknown("unexpected Redis release reply")
    }
    if (identical(reply, "1")) "released" else "already_not_owned"
  }, error = function(e) "unknown")
  lease$state <- if (identical(lease$outcome, "unknown")) "released_unknown" else "released"
  lease$outcome
}

#' Renew this owner only; a late response cannot restore an expired local lease.
dilock_renew <- function(lease, lease_ms = lease$lease_ms) {
  stopifnot(inherits(lease, "dilock_lease"))
  lease_ms <- .dilock_validate_milliseconds(lease_ms, "lease duration", 100L)
  if (!identical(lease$state, "held")) return(FALSE)
  if (.dilock_monotonic() >= lease$deadline) {
    lease$state <- "lost"
    return(FALSE)
  }
  sent <- .dilock_monotonic()
  result <- tryCatch(.dilock_eval(lease$handle$provider$connection, .dilock_renew_script(),
                                 lease$handle$key, lease$owner, as.character(lease_ms)),
                     error = function(e) { lease$state <<- "lost"; .dilock_unknown("renewal acknowledgement is unknown", e) })
  deadline <- sent + (lease_ms - max(10, ceiling(lease_ms * 0.01))) / 1000
  if (identical(as.integer(result), 1L) && .dilock_monotonic() < min(deadline, lease$deadline)) {
    lease$lease_ms <- as.integer(lease_ms)
    lease$deadline <- deadline
    TRUE
  } else { lease$state <- "lost"; FALSE }
}

#' Scope release via on.exit; unknown cleanup is surfaced, the connection stays caller-owned.
with_dilock <- function(handle, code, wait_ms = 5000L, lease_ms = 30000L) {
  lease <- dilock_acquire(handle, wait_ms, lease_ms)
  released <- FALSE
  on.exit({
    if (!released) {
      outcome <- tryCatch(dilock_release(lease), error = function(e) "unknown")
      if (identical(outcome, "unknown")) {
        warning("Dilock scoped cleanup outcome is unknown", call. = FALSE)
      } else if (identical(outcome, "already_not_owned")) {
        warning("Dilock lease was no longer owned at scoped cleanup", call. = FALSE)
      }
    }
  }, add = TRUE)
  value <- force(code(lease))
  outcome <- dilock_release(lease)
  released <- TRUE
  if (identical(outcome, "already_not_owned")) {
    .dilock_stop("dilock_lease_lost", "lock was no longer owned at scoped cleanup")
  }
  if (identical(outcome, "unknown")) {
    .dilock_stop("dilock_release_unknown", "Dilock scoped cleanup outcome is unknown")
  }
  value
}
