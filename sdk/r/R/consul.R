.dilock_consul_profile <- "consul-kv-session-v1"
.dilock_consul_max_ms <- 86400000L

.dilock_consul_validate_meta <- function(text, epoch) {
  required <- c("protocol_major", "identity_scheme", "profile", "coordination_epoch", "fixed_lease_ms")
  occurrence_count <- vapply(required, function(name) {
    length(regmatches(text, gregexpr(paste0('"', name, '"[[:space:]]*:'), text, perl = TRUE)))
  }, integer(1))
  parsed <- tryCatch(jsonlite::fromJSON(text, simplifyVector = FALSE), error = function(e) NULL)
  expected <- list(protocol_major = 1L, identity_scheme = "sha256-framed-utf8-v1",
                   profile = .dilock_consul_profile, coordination_epoch = epoch,
                   fixed_lease_ms = NULL)
  if (!is.list(parsed) || any(occurrence_count != 1L) || !setequal(names(parsed), names(expected)) ||
      !is.numeric(parsed$protocol_major) || length(parsed$protocol_major) != 1L ||
      parsed$protocol_major != 1 ||
      !identical(parsed$identity_scheme, expected$identity_scheme) ||
      !identical(parsed$profile, expected$profile) ||
      !identical(parsed$coordination_epoch, epoch) || !is.null(parsed$fixed_lease_ms)) {
    .dilock_stop("dilock_protocol_mismatch", "Consul namespace metadata is absent or incompatible")
  }
  invisible(TRUE)
}

.dilock_consul_response <- function(provider, method, path, query = list(), body = NULL,
                                    timeout_ms = provider$rpc_timeout_ms, missing_ok = FALSE,
                                    mutation = FALSE) {
  url <- paste0(provider$endpoint, path)
  if (length(query)) url <- paste0(url, "?", paste(paste0(names(query), "=", vapply(query, utils::URLencode, "", reserved = TRUE)), collapse = "&"))
  headers <- c("Accept" = "application/json")
  if (!is.null(provider$token)) headers <- c(headers, "X-Consul-Token" = provider$token)
  handle <- curl::new_handle(timeout = timeout_ms / 1000, connecttimeout = min(2, timeout_ms / 1000),
                             followlocation = FALSE, ssl_verifypeer = TRUE, ssl_verifyhost = 2L,
                             httpheader = paste(names(headers), headers, sep = ": "), customrequest = method)
  if (!is.null(body)) {
    handle <- curl::handle_setheaders(handle, `Content-Type` = "application/json")
    handle <- curl::handle_setopt(handle, postfields = body)
  }
  response <- tryCatch(curl::curl_fetch_memory(url, handle = handle), error = function(e) {
    .dilock_stop(if (mutation) "dilock_acquisition_unknown" else "dilock_backend_error",
                 "Consul HTTP request failed", e)
  })
  if (response$status_code == 404L && missing_ok) return(response)
  if (response$status_code < 200L || response$status_code >= 300L) {
    .dilock_stop(if (mutation && response$status_code >= 500L) "dilock_acquisition_unknown" else "dilock_backend_error",
                 paste("Consul HTTP request returned", response$status_code))
  }
  response
}

.dilock_consul_metadata <- function(provider) {
  key <- paste0("dilock/namespaces/", provider$namespace, "/meta")
  response <- .dilock_consul_response(provider, "GET", paste0("/v1/kv/", key), list(consistent = "true"), missing_ok = TRUE)
  if (response$status_code == 404L) .dilock_stop("dilock_protocol_mismatch", "Consul namespace metadata is missing")
  text <- rawToChar(response$content)
  rows <- tryCatch(jsonlite::fromJSON(text, simplifyVector = FALSE), error = function(e) NULL)
  if (!is.list(rows) || length(rows) != 1L || !identical(rows[[1L]]$Key, key) || is.null(rows[[1L]]$Value)) {
    .dilock_stop("dilock_protocol_mismatch", "Consul metadata KV response is malformed")
  }
  raw <- tryCatch(jsonlite::base64_dec(rows[[1L]]$Value), error = function(e) raw())
  if (!is.raw(raw) || length(raw) < 1L || length(raw) > 8192L || any(raw == as.raw(0))) {
    .dilock_stop("dilock_protocol_mismatch", "Consul metadata value is malformed")
  }
  .dilock_consul_validate_meta(rawToChar(raw), provider$epoch)
}

#' Create a Consul provider after validating an already provisioned namespace.
#' @param endpoint Consul HTTP(S) agent URL.
#' @param namespace Strict Dilock namespace.
#' @param epoch Approved coordination epoch.
#' @param token Optional Consul ACL token.
#' @param rpc_timeout_ms Per-request deadline.
#' @param cleanup_timeout_ms Bounded cleanup deadline.
#' @param poll_interval_ms Waiter polling interval.
#' @param default_wait_ms Default acquisition budget.
#' @param default_lease_ms Session TTL in milliseconds (10 seconds minimum).
dilock_consul_provider <- function(endpoint = "http://127.0.0.1:8500", namespace = "default", epoch,
                                   token = NULL, rpc_timeout_ms = 2000L, cleanup_timeout_ms = 2000L,
                                   poll_interval_ms = 100L, default_wait_ms = 5000L,
                                   default_lease_ms = 30000L) {
  dilock_resource_id(namespace, "validation")
  if (!is.character(endpoint) || length(endpoint) != 1L || is.na(endpoint) ||
      !grepl("^https?://[^/]+/?$", endpoint) || grepl("[[:cntrl:]]", endpoint)) {
    .dilock_stop("dilock_invalid_argument", "endpoint must be an HTTP(S) Consul authority")
  }
  epoch_raw <- .dilock_bytes(epoch, "epoch")
  if (!length(epoch_raw) || length(epoch_raw) > 1024L || any(epoch_raw == as.raw(0))) {
    .dilock_stop("dilock_invalid_argument", "epoch must contain 1..1024 strict UTF-8 bytes")
  }
  if (!is.null(token) && (!is.character(token) || length(token) != 1L || is.na(token) ||
                          !nzchar(token) || !grepl("^[\\x20-\\x7E]+$", token, perl = TRUE))) {
    .dilock_stop("dilock_invalid_argument", "token must be a nonempty header-safe string")
  }
  rpc_timeout_ms <- .dilock_validate_milliseconds(rpc_timeout_ms, "RPC timeout", 1L)
  cleanup_timeout_ms <- .dilock_validate_milliseconds(cleanup_timeout_ms, "cleanup timeout", 1L)
  poll_interval_ms <- .dilock_validate_milliseconds(poll_interval_ms, "poll interval", 1L)
  default_wait_ms <- .dilock_validate_milliseconds(default_wait_ms, "wait budget", 1L)
  default_lease_ms <- .dilock_validate_milliseconds(default_lease_ms, "lease duration", 10000L)
  if (default_lease_ms <= rpc_timeout_ms + max(100, ceiling(default_lease_ms * 0.01))) {
    .dilock_stop("dilock_invalid_argument", "default lease must exceed the RPC timeout and safety margin")
  }
  provider <- new.env(parent = emptyenv())
  provider$endpoint <- sub("/$", "", endpoint); provider$namespace <- namespace; provider$epoch <- epoch
  provider$token <- token; provider$rpc_timeout_ms <- rpc_timeout_ms
  provider$cleanup_timeout_ms <- cleanup_timeout_ms; provider$poll_interval_ms <- poll_interval_ms
  provider$default_wait_ms <- default_wait_ms; provider$default_lease_ms <- default_lease_ms
  provider$closed <- FALSE; provider$leases <- list()
  class(provider) <- "dilock_consul_provider"
  .dilock_consul_metadata(provider)
  provider
}

#' Create a reusable Consul resource handle.
dilock_consul_lock <- function(provider, resource) {
  if (!inherits(provider, "dilock_consul_provider") || provider$closed) .dilock_stop("dilock_invalid_argument", "provider is closed or invalid")
  id <- dilock_resource_id(provider$namespace, resource)
  structure(list(provider = provider, key = paste0("dilock/locks/", id)), class = "dilock_consul_handle")
}

.dilock_consul_json <- function(response, label, error_class = "dilock_protocol_mismatch") {
  tryCatch(jsonlite::fromJSON(rawToChar(response$content), simplifyVector = FALSE), error = function(e) {
    .dilock_stop(error_class, paste("malformed Consul", label, "response"), e)
  })
}
.dilock_consul_session <- function(provider, lease_ms) {
  body <- jsonlite::toJSON(list(Name = "dilock lease", Behavior = "release", LockDelay = "0s",
                                TTL = paste0(ceiling(lease_ms / 1000), "s"), Checks = list()), auto_unbox = TRUE)
  sent <- .dilock_monotonic()
  response <- .dilock_consul_response(provider, "PUT", "/v1/session/create", body = body, mutation = TRUE)
  session <- .dilock_consul_json(response, "session create", "dilock_acquisition_unknown")$ID
  if (!is.character(session) || length(session) != 1L || !nzchar(session)) .dilock_stop("dilock_acquisition_unknown", "Consul returned no session ID")
  list(id = session, ttl_ms = ceiling(lease_ms / 1000) * 1000L,
       deadline = sent + ceiling(lease_ms / 1000) - max(0.1, ceiling(lease_ms / 1000) * 0.01))
}
.dilock_consul_renew_session <- function(provider, session) {
  sent <- .dilock_monotonic()
  response <- .dilock_consul_response(provider, "PUT", paste0("/v1/session/renew/", session$id), timeout_ms = provider$rpc_timeout_ms, mutation = TRUE)
  rows <- .dilock_consul_json(response, "session renewal")
  if (length(rows) != 1L || !identical(rows[[1L]]$ID, session$id)) .dilock_stop("dilock_lease_lost", "Consul session renewal was not confirmed")
  deadline <- sent + session$ttl_ms / 1000 - max(0.1, session$ttl_ms / 100000)
  if (.dilock_monotonic() >= deadline) .dilock_stop("dilock_lease_lost", "Consul renewal arrived after lease safety deadline")
  list(deadline = deadline, sent = sent)
}
.dilock_consul_session_destroy <- function(provider, session, timeout_ms = provider$cleanup_timeout_ms) {
  response <- .dilock_consul_response(provider, "PUT", paste0("/v1/session/destroy/", session$id), timeout_ms = timeout_ms, mutation = TRUE)
  isTRUE(.dilock_consul_json(response, "session destroy"))
}
.dilock_consul_owner <- function(provider, key, timeout_ms = provider$rpc_timeout_ms) {
  response <- .dilock_consul_response(provider, "GET", paste0("/v1/kv/", key), list(consistent = "true"), timeout_ms = timeout_ms, missing_ok = TRUE)
  if (response$status_code == 404L) return(NULL)
  rows <- .dilock_consul_json(response, "KV read")
  if (!is.list(rows) || length(rows) != 1L || !identical(rows[[1L]]$Key, key)) .dilock_stop("dilock_protocol_mismatch", "Consul KV lock observation is malformed")
  index <- .dilock_consul_lock_index(rawToChar(response$content))
  list(session = if (is.null(rows[[1L]]$Session)) "" else rows[[1L]]$Session, index = index)
}
.dilock_consul_lock_index <- function(raw_text) {
  matches <- regmatches(raw_text, gregexpr('"LockIndex"[[:space:]]*:[[:space:]]*[0-9]+[[:space:]]*[,}]', raw_text, perl = TRUE))
  if (length(matches) != 1L || length(matches[[1L]]) != 1L) {
    .dilock_stop("dilock_protocol_mismatch", "Consul LockIndex is missing or duplicated")
  }
  index <- sub('.*:[[:space:]]*', "", matches[[1L]], perl = TRUE)
  index <- sub('[[:space:]]*[,}]$', "", index, perl = TRUE)
  if (!grepl("^(0|[1-9][0-9]*)$", index)) .dilock_stop("dilock_protocol_mismatch", "Consul LockIndex is malformed")
  index
}

#' Attempt a single Consul KV acquire.
dilock_consul_try_acquire <- function(handle, lease_ms = handle$provider$default_lease_ms) {
  if (!inherits(handle, "dilock_consul_handle") || handle$provider$closed) .dilock_stop("dilock_invalid_argument", "handle is closed or invalid")
  provider <- handle$provider
  lease_ms <- .dilock_validate_milliseconds(lease_ms, "lease duration", 10000L)
  if (lease_ms <= provider$rpc_timeout_ms + max(100, ceiling(lease_ms * 0.01))) {
    .dilock_stop("dilock_invalid_argument", "lease must exceed the RPC timeout and safety margin")
  }
  session <- .dilock_consul_session(provider, lease_ms)
  acquired <- FALSE
  on.exit(if (!acquired) try(.dilock_consul_session_destroy(provider, session), silent = TRUE), add = TRUE)
  if (.dilock_monotonic() >= session$deadline) .dilock_stop("dilock_acquisition_unknown", "Consul session creation arrived after its safety deadline")
  reply <- .dilock_consul_response(provider, "PUT", paste0("/v1/kv/", handle$key), list(acquire = session$id), body = "", mutation = TRUE)
  ok <- .dilock_consul_json(reply, "KV acquire", "dilock_acquisition_unknown")
  if (isFALSE(ok)) return(NULL)
  if (!isTRUE(ok)) .dilock_stop("dilock_acquisition_unknown", "Consul KV acquire response was not a JSON boolean")
  owner <- .dilock_consul_owner(provider, handle$key)
  if (is.null(owner) || !identical(owner$session, session$id) || owner$index == "0") .dilock_stop("dilock_acquisition_unknown", "Consul ownership could not be verified")
  if (.dilock_monotonic() >= session$deadline) .dilock_stop("dilock_acquisition_unknown", "Consul acquire exceeded the lease safety deadline")
  lease <- new.env(parent = emptyenv())
  lease$handle <- handle; lease$session <- session; lease$state <- "held"; lease$outcome <- NULL
  lease$fence <- list(coordination_epoch = provider$epoch, lock_index = owner$index)
  lease$effective_ttl_ms <- max(1L, floor((session$deadline - .dilock_monotonic()) * 1000))
  class(lease) <- "dilock_consul_lease"
  provider$leases[[length(provider$leases) + 1L]] <- lease
  acquired <- TRUE
  lease
}

#' Wait for a Consul lock, renewing the waiter session while polling.
dilock_consul_acquire <- function(handle, wait_ms = handle$provider$default_wait_ms,
                                  lease_ms = handle$provider$default_lease_ms) {
  wait_ms <- .dilock_validate_milliseconds(wait_ms, "wait budget", 1L)
  lease_ms <- .dilock_validate_milliseconds(lease_ms, "lease duration", 10000L)
  if (lease_ms <= handle$provider$rpc_timeout_ms + max(100, ceiling(lease_ms * 0.01))) {
    .dilock_stop("dilock_invalid_argument", "lease must exceed the RPC timeout and safety margin")
  }
  deadline <- .dilock_monotonic() + wait_ms / 1000
  repeat {
    if (.dilock_monotonic() >= deadline) .dilock_stop("dilock_wait_timeout", "Consul contention wait budget exhausted")
    lease <- tryCatch(dilock_consul_try_acquire(handle, lease_ms), error = function(e) {
      if (inherits(e, "dilock_backend_error")) .dilock_stop("dilock_acquisition_unknown", "Consul acquire acknowledgement is unknown", e)
      stop(e)
    })
    if (!is.null(lease)) {
      if (.dilock_monotonic() < deadline) return(lease)
      outcome <- dilock_consul_release(lease)
      if (identical(outcome, "unknown")) .dilock_stop("dilock_acquisition_unknown", "late Consul acquisition cleanup is unknown")
      .dilock_stop("dilock_wait_timeout", "Consul acquisition completed after the caller wait deadline")
    }
    Sys.sleep(min(handle$provider$poll_interval_ms / 1000, max(0, deadline - .dilock_monotonic())))
  }
}

#' Return exact epoch-scoped Consul fencing data.
dilock_consul_fence <- function(lease) {
  if (!inherits(lease, "dilock_consul_lease")) .dilock_stop("dilock_invalid_argument", "invalid Consul lease")
  lease$fence
}
#' Check the local lease deadline.
dilock_consul_valid <- function(lease) inherits(lease, "dilock_consul_lease") && identical(lease$state, "held") && .dilock_monotonic() < lease$session$deadline
#' Renew the active Consul session.
dilock_consul_renew <- function(lease) {
  if (!dilock_consul_valid(lease)) { lease$state <- "lost"; return(FALSE) }
  tryCatch({
    old_deadline <- lease$session$deadline
    renewal <- .dilock_consul_renew_session(lease$handle$provider, lease$session)
    owner <- .dilock_consul_owner(lease$handle$provider, lease$handle$key)
    if (is.null(owner) || !identical(owner$session, lease$session$id) ||
        !identical(owner$index, lease$fence$lock_index) ||
        .dilock_monotonic() >= old_deadline || .dilock_monotonic() >= renewal$deadline) {
      .dilock_stop("dilock_lease_lost", "Consul no longer confirms this session and LockIndex")
    }
    lease$session$deadline <- renewal$deadline
    lease$effective_ttl_ms <- max(1L, floor((lease$session$deadline - .dilock_monotonic()) * 1000))
    TRUE
  }, error = function(e) { lease$state <- "lost"; stop(e) })
}
#' Release and destroy a Consul session. Returns released/already_not_owned/unknown.
dilock_consul_release <- function(lease) {
  if (!inherits(lease, "dilock_consul_lease")) .dilock_stop("dilock_invalid_argument", "invalid Consul lease")
  if (!is.null(lease$outcome)) return(lease$outcome)
  provider <- lease$handle$provider
  operation_deadline <- .dilock_monotonic() + provider$cleanup_timeout_ms / 1000
  operation_remaining <- function() {
    value <- floor((operation_deadline - .dilock_monotonic()) * 1000)
    if (value < 1L) .dilock_stop("dilock_release_unknown", "Consul cleanup deadline expired")
    as.integer(value)
  }
  outcome <- tryCatch({
    owner <- .dilock_consul_owner(provider, lease$handle$key, operation_remaining())
    if (is.null(owner) || !identical(owner$session, lease$session$id) || !identical(owner$index, lease$fence$lock_index)) {
      "already_not_owned"
    } else {
      response <- .dilock_consul_response(provider, "PUT", paste0("/v1/kv/", lease$handle$key),
                                          list(release = lease$session$id), body = "", timeout_ms = operation_remaining(), mutation = TRUE)
      ack <- .dilock_consul_json(response, "KV release")
      if (identical(ack, TRUE)) {
        "released"
      } else {
        after <- .dilock_consul_owner(provider, lease$handle$key, operation_remaining())
        if (is.null(after) || !identical(after$session, lease$session$id) || !identical(after$index, lease$fence$lock_index)) {
          "already_not_owned"
        } else {
          "unknown"
        }
      }
    }
  }, error = function(e) "unknown")
  destroy_deadline <- .dilock_monotonic() + provider$cleanup_timeout_ms / 1000
  destroy_remaining <- max(1L, as.integer(floor((destroy_deadline - .dilock_monotonic()) * 1000)))
  destroyed <- tryCatch(
    .dilock_consul_session_destroy(provider, lease$session, destroy_remaining),
    error = function(e) FALSE
  )
  lease$outcome <- .dilock_consul_finish_release(outcome, destroyed)
  lease$state <- "released"
  lease$outcome
}

.dilock_consul_finish_release <- function(outcome, destroyed) {
  if (isTRUE(destroyed)) outcome else "unknown"
}

#' Run a callback under a Consul lease and report cleanup uncertainty.
with_dilock_consul <- function(handle, code, wait_ms = handle$provider$default_wait_ms,
                               lease_ms = handle$provider$default_lease_ms) {
  lease <- dilock_consul_acquire(handle, wait_ms, lease_ms)
  released <- FALSE
  on.exit({
    if (!released) {
      outcome <- dilock_consul_release(lease)
      if (outcome != "released") warning("Consul scoped cleanup outcome: ", outcome, call. = FALSE)
    }
  }, add = TRUE)
  value <- force(code(lease))
  outcome <- dilock_consul_release(lease)
  released <- TRUE
  if (identical(outcome, "already_not_owned")) .dilock_stop("dilock_lease_lost", "Consul lock ownership was lost before scoped cleanup")
  if (identical(outcome, "unknown")) .dilock_stop("dilock_release_unknown", "Consul scoped cleanup outcome is unknown")
  value
}

#' Close a Consul provider after releasing its active sessions.
dilock_consul_close <- function(provider) {
  if (!inherits(provider, "dilock_consul_provider")) .dilock_stop("dilock_invalid_argument", "invalid Consul provider")
  if (provider$closed) return(invisible(TRUE))
  outcomes <- vapply(provider$leases, dilock_consul_release, "")
  provider$closed <- TRUE
  if (any(outcomes == "unknown")) .dilock_stop("dilock_release_unknown", "one or more Consul sessions could not be confirmed released")
  invisible(TRUE)
}

#' Explicitly create the namespace metadata key with Consul CAS=0.
dilock_consul_provision_namespace <- function(endpoint, namespace = "default", epoch, token = NULL,
                                              rpc_timeout_ms = 2000L) {
  dilock_resource_id(namespace, "validation")
  if (!is.character(endpoint) || length(endpoint) != 1L || is.na(endpoint) || !grepl("^https?://[^/]+/?$", endpoint)) {
    .dilock_stop("dilock_invalid_argument", "endpoint must be an HTTP(S) Consul authority")
  }
  epoch_raw <- .dilock_bytes(epoch, "epoch")
  if (!length(epoch_raw) || length(epoch_raw) > 1024L || any(epoch_raw == as.raw(0))) {
    .dilock_stop("dilock_invalid_argument", "epoch must contain 1..1024 strict UTF-8 bytes")
  }
  if (!is.null(token) && (!is.character(token) || length(token) != 1L || is.na(token) || !nzchar(token) ||
                          !grepl("^[\\x20-\\x7E]+$", token, perl = TRUE))) {
    .dilock_stop("dilock_invalid_argument", "token must be a nonempty header-safe string")
  }
  rpc_timeout_ms <- .dilock_validate_milliseconds(rpc_timeout_ms, "RPC timeout", 1L)
  provider <- new.env(parent = emptyenv()); provider$endpoint <- sub("/$", "", endpoint)
  provider$namespace <- namespace; provider$epoch <- epoch; provider$token <- token
  provider$rpc_timeout_ms <- rpc_timeout_ms; provider$cleanup_timeout_ms <- rpc_timeout_ms
  meta <- list(protocol_major = 1L, identity_scheme = "sha256-framed-utf8-v1", profile = .dilock_consul_profile,
               coordination_epoch = epoch, fixed_lease_ms = NULL)
  value <- jsonlite::base64_enc(charToRaw(as.character(jsonlite::toJSON(meta, auto_unbox = TRUE, null = "null"))))
  body <- as.character(jsonlite::toJSON(value, auto_unbox = TRUE))
  reply <- .dilock_consul_response(provider, "PUT", paste0("/v1/kv/dilock/namespaces/", namespace, "/meta"), list(cas = "0"), body = body, mutation = TRUE)
  result <- .dilock_consul_json(reply, "namespace provisioning")
  if (isTRUE(result)) return(invisible(TRUE))
  .dilock_consul_metadata(provider)
  invisible(TRUE)
}
