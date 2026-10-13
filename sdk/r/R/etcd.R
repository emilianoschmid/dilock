.dilock_etcd_profile <- "etcd-native-lock-v1"
.dilock_etcd_scheme <- "sha256-framed-utf8-v1"
.dilock_etcd_descriptor_state <- new.env(parent = emptyenv())
.dilock_etcd_descriptor_state$loaded <- FALSE

.dilock_etcd_message <- function(type, ...) {
  RProtoBuf::new(RProtoBuf::P(type), ...)
}

.dilock_etcd_descriptors <- function() {
  if (!isTRUE(.dilock_etcd_descriptor_state$loaded)) {
    proto_dir <- system.file("proto", "etcd-v3.7.2", package = "dilock")
    if (!nzchar(proto_dir)) {
      .dilock_stop("dilock_protocol_mismatch", "packaged etcd protocol definitions are missing")
    }
    loaded <- tryCatch({
      RProtoBuf::P("etcdserverpb.RangeRequest")
      TRUE
    }, error = function(e) FALSE)
    if (!loaded) {
      tryCatch(
        RProtoBuf::readProtoFiles2(c("rpc.proto", "v3lock.proto"), protoPath = proto_dir),
        error = function(e) .dilock_stop("dilock_protocol_mismatch", "etcd protobuf descriptors could not be loaded", e)
      )
    }
    .dilock_etcd_descriptor_state$loaded <- TRUE
  }
  list(
    range = rgrpc::grpc_method(rgrpc::grpc_service("etcdserverpb.RangeRequest", "KV"), "Range"),
    grant = rgrpc::grpc_method(rgrpc::grpc_service("etcdserverpb.LeaseGrantRequest", "Lease"), "LeaseGrant"),
    revoke = rgrpc::grpc_method(rgrpc::grpc_service("etcdserverpb.LeaseRevokeRequest", "Lease"), "LeaseRevoke"),
    keepalive = rgrpc::grpc_method(rgrpc::grpc_service("etcdserverpb.LeaseKeepAliveRequest", "Lease"), "LeaseKeepAlive"),
    lock = rgrpc::grpc_method(rgrpc::grpc_service("v3lockpb.LockRequest", "Lock"), "Lock"),
    unlock = rgrpc::grpc_method(rgrpc::grpc_service("v3lockpb.UnlockRequest", "Lock"), "Unlock")
  )
}

.dilock_etcd_varint <- function(value) {
  value <- as.numeric(value)
  if (length(value) != 1L || !is.finite(value) || value < 0 || value != floor(value) || value > 2^53) {
    .dilock_stop("dilock_invalid_argument", "protobuf field length is outside the exact supported range")
  }
  out <- raw()
  repeat {
    byte <- value %% 128
    value <- floor(value / 128)
    if (value > 0) byte <- byte + 128
    out <- c(out, as.raw(byte))
    if (value == 0) return(out)
  }
}

.dilock_etcd_read_varint <- function(bytes, at) {
  start <- at
  value <- 0
  factor <- 1
  repeat {
    if (at > length(bytes)) .dilock_stop("dilock_protocol_mismatch", "truncated etcd protobuf varint")
    byte <- as.integer(bytes[[at]])
    at <- at + 1L
    value <- value + (byte %% 128) * factor
    if (byte < 128) break
    factor <- factor * 128
    if (at - start > 10L) .dilock_stop("dilock_protocol_mismatch", "invalid etcd protobuf varint")
  }
  list(value = value, bytes = bytes[start:(at - 1L)], next_at = at)
}

.dilock_etcd_fields <- function(bytes) {
  fields <- list()
  at <- 1L
  while (at <= length(bytes)) {
    tag <- .dilock_etcd_read_varint(bytes, at)
    at <- tag$next_at
    number <- floor(tag$value / 8)
    wire <- tag$value %% 8
    if (number < 1 || !wire %in% c(0, 1, 2, 5)) {
      .dilock_stop("dilock_protocol_mismatch", "unsupported etcd protobuf wire field")
    }
    if (wire == 0) {
      item <- .dilock_etcd_read_varint(bytes, at)
      at <- item$next_at
      value <- list(wire = wire, bytes = item$bytes, number = item$value)
    } else if (wire == 2) {
      length_field <- .dilock_etcd_read_varint(bytes, at)
      at <- length_field$next_at
      size <- length_field$value
      if (size > length(bytes) - at + 1L) {
        .dilock_stop("dilock_protocol_mismatch", "truncated etcd protobuf field")
      }
      payload <- if (size == 0) raw() else bytes[at:(at + size - 1L)]
      at <- at + size
      value <- list(wire = wire, bytes = payload, number = size)
    } else {
      size <- if (wire == 1) 8L else 4L
      if (size > length(bytes) - at + 1L) {
        .dilock_stop("dilock_protocol_mismatch", "truncated etcd protobuf fixed field")
      }
      value <- list(wire = wire, bytes = bytes[at:(at + size - 1L)], number = NA_real_)
      at <- at + size
    }
    key <- as.character(number)
    fields[[key]] <- c(fields[[key]], list(value))
  }
  fields
}

.dilock_etcd_one_field <- function(fields, number, wire) {
  items <- fields[[as.character(number)]]
  if (is.null(items) || length(items) != 1L || items[[1L]]$wire != wire) {
    .dilock_stop("dilock_protocol_mismatch", paste("missing or malformed etcd protobuf field", number))
  }
  items[[1L]]
}

.dilock_etcd_bytes_field <- function(number, bytes) {
  c(.dilock_etcd_varint(number * 8 + 2), .dilock_etcd_varint(length(bytes)), bytes)
}

.dilock_etcd_raw_varint_field <- function(number, encoded_value) {
  c(.dilock_etcd_varint(number * 8), encoded_value)
}

# Decimal string conversion avoids R double rounding for int64 lease ids and
# revisions. The exact varint bytes remain the canonical value on the wire.
.dilock_etcd_decimal <- function(encoded) {
  digits <- 0L
  # Protobuf varints store the least-significant base-128 digit first. Fold
  # from the most-significant digit when converting to the decimal digit array.
  for (byte in rev(as.integer(encoded))) {
    digit <- byte %% 128
    carry <- digit
    for (i in seq_along(digits)) {
      value <- digits[[i]] * 128L + carry
      digits[[i]] <- value %% 10L
      carry <- value %/% 10L
    }
    while (carry > 0L) {
      digits <- c(digits, carry %% 10L)
      carry <- carry %/% 10L
    }
  }
  paste(rev(digits), collapse = "")
}

.dilock_etcd_call <- function(client, method, request, timeout_ms) {
  call <- tryCatch(
    rgrpc::grpc_call(client, method, request, deadline_ms = as.integer(timeout_ms)),
    error = function(e) .dilock_stop("dilock_backend_error", paste("could not start", method$path), e)
  )
  event <- tryCatch(rgrpc::grpc_await(call, timeout_ms = as.integer(timeout_ms)), error = function(e) list())
  if (length(event) != 1L) {
    try(rgrpc::grpc_cancel(call), silent = TRUE)
    .dilock_stop("dilock_acquisition_unknown", paste(method$path, "completion was not observed"))
  }
  event <- event[[1L]]
  if (!identical(event$status_name, "OK")) {
    condition <- list(
      message = paste(method$path, "failed:", event$status_name, event$message),
      call = NULL,
      status_name = event$status_name,
      rpc_message = event$message
    )
    class(condition) <- c("dilock_backend_error", "error", "condition")
    stop(condition)
  }
  event
}

.dilock_etcd_new_client <- function(provider) {
  if (provider$closed) .dilock_stop("dilock_invalid_argument", "etcd provider is closed")
  tryCatch(rgrpc::grpc_client(provider$target, credentials = provider$credentials),
           error = function(e) .dilock_stop("dilock_backend_error", "could not create etcd gRPC client", e))
}

.dilock_etcd_metadata <- function(provider, client, timeout_ms) {
  key <- charToRaw(paste0("/dilock/namespaces/", provider$namespace, "/meta"))
  event <- .dilock_etcd_call(client, provider$rpc$range,
                             .dilock_etcd_message("etcdserverpb.RangeRequest", key = key), timeout_ms)
  response <- event$response_message
  if (response$count != 1 || length(response$kvs) != 1L || !identical(response$kvs[[1L]]$key, key)) {
    .dilock_stop("dilock_protocol_mismatch", "etcd namespace metadata is missing")
  }
  raw <- response$kvs[[1L]]$value
  if (!is.raw(raw) || length(raw) < 1L || length(raw) > 8192L || any(raw == as.raw(0))) {
    .dilock_stop("dilock_protocol_mismatch", "etcd namespace metadata is malformed")
  }
  text <- tryCatch(rawToChar(raw), error = function(e) "")
  .dilock_validate_metadata_json(text, .dilock_etcd_profile, provider$epoch)
  invisible(TRUE)
}

.dilock_etcd_lease_grant <- function(provider, client, ttl_seconds, timeout_ms) {
  request <- .dilock_etcd_message("etcdserverpb.LeaseGrantRequest", TTL = as.integer(ttl_seconds))
  sent <- .dilock_monotonic()
  event <- .dilock_etcd_call(client, provider$rpc$grant, request, timeout_ms)
  fields <- .dilock_etcd_fields(event$response)
  id <- .dilock_etcd_one_field(fields, 2, 0)$bytes
  ttl <- .dilock_etcd_one_field(fields, 3, 0)$number
  if (length(id) < 1L || identical(.dilock_etcd_decimal(id), "0") || ttl < 1 || ttl > 86400) {
    .dilock_stop("dilock_protocol_mismatch", "etcd returned an invalid lease grant")
  }
  list(id = id, ttl_seconds = as.integer(ttl), grant_sent = sent)
}

.dilock_etcd_keepalive_start <- function(provider, lease, timeout_ms) {
  lease$stream <- tryCatch(rgrpc::grpc_stream(lease$client, provider$rpc$keepalive),
                           error = function(e) .dilock_stop("dilock_backend_error", "could not start etcd lease keepalive", e))
  lease$stream_id <- lease$stream$id
  .dilock_etcd_keepalive_send(provider, lease, timeout_ms)
}

.dilock_etcd_keepalive_send <- function(provider, lease, timeout_ms) {
  sent <- .dilock_monotonic()
  ok <- tryCatch(rgrpc::grpc_send(lease$stream,
                                  .dilock_etcd_raw_varint_field(1, lease$id)),
                 error = function(e) FALSE)
  if (!isTRUE(ok)) .dilock_stop("dilock_backend_error", "etcd keepalive request was not accepted")
  lease$keepalive_sent <- c(lease$keepalive_sent, sent)
  lease$keepalive_sent_count <- lease$keepalive_sent_count + 1L
  lease$last_keepalive_sent <- sent
  lease$next_keepalive <- sent + max(0.1, lease$ttl_seconds / 3)
  invisible(TRUE)
}

.dilock_etcd_keepalive_event <- function(provider, lease, event) {
  if (identical(event$kind, "stream_status")) {
    lease$state <- "lost"
    .dilock_stop("dilock_lease_lost", paste("etcd keepalive stream ended:", event$status_name))
  }
  if (!identical(event$kind, "stream_msg")) return(invisible(FALSE))
  if (length(lease$keepalive_sent) == 0L) {
    lease$state <- "lost"
    .dilock_stop("dilock_protocol_mismatch", "etcd returned an unsolicited keepalive response")
  }
  fields <- .dilock_etcd_fields(event$response)
  id <- .dilock_etcd_one_field(fields, 2, 0)$bytes
  ttl <- .dilock_etcd_one_field(fields, 3, 0)$number
  if (!identical(id, lease$id) || ttl < 1 || ttl > 86400) {
    lease$state <- "lost"
    .dilock_stop("dilock_protocol_mismatch", "etcd keepalive response has an invalid lease id or TTL")
  }
  now <- .dilock_monotonic()
  margin <- max(0.05, ttl * 0.01)
  acknowledged_send <- lease$keepalive_sent[[1L]]
  lease$keepalive_sent <- lease$keepalive_sent[-1L]
  lease$keepalive_ack_count <- lease$keepalive_ack_count + 1L
  confirmed <- acknowledged_send + ttl - margin
  if (now >= confirmed) {
    lease$state <- "lost"
    .dilock_stop("dilock_lease_lost", "etcd keepalive response arrived after the conservative lease deadline")
  }
  lease$ttl_seconds <- as.integer(ttl)
  lease$deadline <- confirmed
  lease$next_keepalive <- lease$last_keepalive_sent + max(0.1, ttl / 3)
  invisible(TRUE)
}

.dilock_etcd_new_lease <- function(provider, client, name, grant, lease_ms) {
  lease <- new.env(parent = emptyenv())
  lease$provider <- provider
  lease$client <- client
  lease$name <- name
  lease$id <- grant$id
  lease$ttl_seconds <- grant$ttl_seconds
  lease$effective_lease_ms <- grant$ttl_seconds * 1000L
  lease$deadline <- grant$grant_sent + grant$ttl_seconds - max(0.05, grant$ttl_seconds * 0.01)
  lease$next_keepalive <- .dilock_monotonic() + max(0.1, grant$ttl_seconds / 3)
  lease$key <- NULL
  lease$fence_revision <- NULL
  lease$fence <- NULL
  lease$state <- "held"
  lease$outcome <- NULL
  lease$release_error <- NULL
  lease$stream <- NULL
  lease$stream_id <- NULL
  lease$lock_call <- NULL
  lease$keepalive_pending <- FALSE
  lease$keepalive_sent <- numeric()
  lease$keepalive_sent_count <- 0L
  lease$keepalive_ack_count <- 0L
  class(lease) <- "dilock_etcd_lease"
  lease
}

.dilock_etcd_remaining_ms <- function(deadline, now = .dilock_monotonic()) {
  if (length(deadline) != 1L || length(now) != 1L ||
      !is.finite(deadline) || !is.finite(now)) return(0L)
  remaining <- floor((deadline - now) * 1000)
  if (!is.finite(remaining) || remaining < 1) return(0L)
  as.integer(min(remaining, .Machine$integer.max))
}

.dilock_etcd_revoke <- function(provider, lease, timeout_ms = provider$cleanup_timeout_ms,
                                cleanup_deadline = NULL) {
  cleanup <- .dilock_etcd_new_client(provider)
  on.exit(try(rgrpc::grpc_close(cleanup), silent = TRUE), add = TRUE)
  if (!is.null(cleanup_deadline)) {
    timeout_ms <- .dilock_etcd_remaining_ms(cleanup_deadline)
    if (timeout_ms < 1L) {
      .dilock_stop("dilock_cleanup_timeout", "etcd cleanup budget expired before LeaseRevoke")
    }
  }
  .dilock_etcd_call(cleanup, provider$rpc$revoke,
                    .dilock_etcd_raw_varint_field(1, lease$id), timeout_ms)
  invisible(TRUE)
}

.dilock_etcd_abandon <- function(provider, lease, lock_call = NULL) {
  if (!is.null(lock_call)) try(rgrpc::grpc_cancel(lock_call), silent = TRUE)
  if (!is.null(lease$stream)) try(rgrpc::grpc_cancel(lease$stream), silent = TRUE)
  outcome <- tryCatch({
    .dilock_etcd_revoke(provider, lease)
    "released"
  }, error = function(e) "unknown")
  lease$outcome <- outcome
  lease$state <- if (identical(outcome, "released")) "released" else "released_unknown"
  try(rgrpc::grpc_close(lease$client), silent = TRUE)
  if (!identical(outcome, "released")) {
    .dilock_stop("dilock_acquisition_unknown", "etcd waiter cleanup via independent LeaseRevoke is unknown")
  }
  invisible(TRUE)
}

.dilock_etcd_acquisition_cleanup <- function(state, cleanup, suppress = FALSE) {
  if (isTRUE(state$returned) || isTRUE(state$cleanup_attempted)) return(invisible(TRUE))
  state$cleanup_attempted <- TRUE
  if (suppress) {
    tryCatch({ cleanup(); TRUE }, error = function(e) FALSE)
  } else {
    cleanup()
    TRUE
  }
}

# The provider tracks only outstanding acquisitions. Retaining every completed
# lease would keep its gRPC stream/client objects alive and make provider close
# grow with the lifetime acquisition count. Preserve the bounded diagnostic
# that close should report if any earlier release was uncertain.
.dilock_etcd_untrack_lease <- function(provider, lease) {
  if (identical(lease$outcome, "unknown")) provider$unknown_cleanup <- TRUE
  provider$leases <- Filter(function(active) !identical(active, lease), provider$leases)
  lease$stream <- NULL
  lease$stream_id <- NULL
  lease$lock_call <- NULL
  lease$client <- NULL
  invisible(NULL)
}

.dilock_etcd_range_lock_key <- function(provider, client, key, timeout_ms) {
  request <- .dilock_etcd_message("etcdserverpb.RangeRequest", key = key)
  event <- .dilock_etcd_call(client, provider$rpc$range, request, timeout_ms)
  response <- event$response_message
  fields <- .dilock_etcd_fields(event$response)
  kvs <- fields[["2"]]
  if (response$count != 1 || is.null(kvs) || length(kvs) != 1L || kvs[[1L]]$wire != 2) {
    .dilock_stop("dilock_protocol_mismatch", "etcd native Lock key did not resolve to exactly one KeyValue")
  }
  kv_fields <- .dilock_etcd_fields(kvs[[1L]]$bytes)
  observed_key <- .dilock_etcd_one_field(kv_fields, 1, 2)$bytes
  observed_lease <- .dilock_etcd_one_field(kv_fields, 6, 0)$bytes
  revision <- .dilock_etcd_one_field(kv_fields, 2, 0)$bytes
  list(key = observed_key, lease = observed_lease, revision = .dilock_etcd_decimal(revision))
}

# A linearizable exact-key observation used before native Unlock. Only the
# matching key/lease pair authorizes Unlock; malformed or unavailable Range
# results stay unknown, while a valid empty result or different lease proves
# this acquisition is no longer the owner.
.dilock_etcd_release_ownership <- function(provider, lease, timeout_ms) {
  if (length(timeout_ms) != 1L || is.na(timeout_ms) || timeout_ms < 1L) return("unknown")
  tryCatch({
    event <- .dilock_etcd_call(
      lease$client, provider$rpc$range,
      .dilock_etcd_message("etcdserverpb.RangeRequest", key = lease$key), timeout_ms
    )
    response <- event$response_message
    fields <- .dilock_etcd_fields(event$response)
    kvs <- fields[["2"]]
    count <- suppressWarnings(as.numeric(response$count))
    if (identical(count, 0) && is.null(kvs)) {
      "absent"
    } else if (identical(count, 1) && length(kvs) == 1L && kvs[[1L]]$wire == 2) {
      kv_fields <- .dilock_etcd_fields(kvs[[1L]]$bytes)
      observed_key <- .dilock_etcd_one_field(kv_fields, 1, 2)$bytes
      observed_lease <- .dilock_etcd_one_field(kv_fields, 6, 0)$bytes
      if (!identical(observed_key, lease$key)) {
        "unknown"
      } else if (identical(observed_lease, lease$id)) {
        "owned"
      } else {
        "mismatch"
      }
    } else {
      "unknown"
    }
  }, error = function(e) "unknown")
}

.dilock_etcd_release_outcome <- function(ownership, unlock_acknowledged, revoke_result) {
  if (ownership %in% c("absent", "mismatch") || identical(revoke_result, "not_found")) {
    return("already_not_owned")
  }
  if (identical(ownership, "owned") && isTRUE(unlock_acknowledged) &&
      identical(revoke_result, "acknowledged")) {
    return("released")
  }
  if (identical(revoke_result, "acknowledged")) return("already_not_owned")
  "unknown"
}

.dilock_etcd_lock_failure <- function(event, deadline, cleanup) {
  status <- event$status_name
  cleanup_ok <- tryCatch({ cleanup(); TRUE }, error = function(e) FALSE)
  if (!cleanup_ok) {
    .dilock_stop("dilock_acquisition_unknown",
                 "etcd Lock failed and lease cleanup was not acknowledged")
  }
  if (identical(status, "DEADLINE_EXCEEDED") && .dilock_monotonic() >= deadline) {
    .dilock_stop("dilock_wait_timeout", "etcd contention wait budget exhausted")
  }
  .dilock_stop("dilock_backend_error",
               paste("etcd Lock failed:", status, event$message))
}

#' Open an experimental native etcd gRPC provider.
#' @param target A gRPC authority such as "localhost:2379".
#' @param namespace Dilock namespace already provisioned in etcd.
#' @param epoch Operator-approved coordinator incarnation recorded in metadata.
#' @param credentials Optional rgrpc TLS credentials from `rgrpc::grpc_tls()`.
#' @param rpc_timeout_ms Per-RPC timeout; cleanup uses its own bounded value.
#' @param cleanup_timeout_ms Independent best-effort cleanup deadline.
#' @return A provider. It owns a gRPC channel; call `dilock_etcd_close()`.
dilock_etcd_provider <- function(target, namespace = "default", epoch,
                                 credentials = NULL, rpc_timeout_ms = 5000L,
                                 cleanup_timeout_ms = 1000L) {
  dilock_resource_id(namespace, "validation")
  epoch_raw <- .dilock_bytes(epoch, "epoch")
  if (length(epoch_raw) < 1L || length(epoch_raw) > 1024L || any(epoch_raw == as.raw(0))) {
    .dilock_stop("dilock_invalid_argument", "epoch must contain 1..1024 strict UTF-8 bytes")
  }
  if (!is.character(target) || length(target) != 1L || is.na(target) || !nzchar(target) ||
      grepl("[/#?]", target, perl = TRUE)) {
    .dilock_stop("dilock_invalid_argument", "target must be a gRPC host:port authority")
  }
  rpc_timeout_ms <- .dilock_validate_milliseconds(rpc_timeout_ms, "RPC timeout", 1L)
  cleanup_timeout_ms <- .dilock_validate_milliseconds(cleanup_timeout_ms, "cleanup timeout", 1L)
  if (!isTRUE(rgrpc::grpc_available())) {
    .dilock_stop("dilock_backend_error", "rgrpc was installed without a usable native gRPC C++ runtime")
  }
  rpc <- .dilock_etcd_descriptors()
  if (!isTRUE(rpc$keepalive$client_streaming) || !isTRUE(rpc$keepalive$server_streaming)) {
    .dilock_stop("dilock_protocol_mismatch", "etcd LeaseKeepAlive descriptor is not bidirectional")
  }
  client <- tryCatch(rgrpc::grpc_client(target, credentials = credentials),
                     error = function(e) .dilock_stop("dilock_backend_error", "could not connect to etcd gRPC target", e))
  provider <- new.env(parent = emptyenv())
  provider$target <- target
  provider$namespace <- namespace
  provider$epoch <- epoch
  provider$credentials <- credentials
  provider$rpc_timeout_ms <- rpc_timeout_ms
  provider$cleanup_timeout_ms <- cleanup_timeout_ms
  provider$rpc <- rpc
  provider$client <- client
  provider$closed <- FALSE
  provider$leases <- list()
  provider$unknown_cleanup <- FALSE
  class(provider) <- "dilock_etcd_provider"
  provider
}

#' Make a reusable native etcd lock-name handle.
dilock_etcd_lock <- function(provider, resource) {
  if (!inherits(provider, "dilock_etcd_provider") || provider$closed) {
    .dilock_stop("dilock_invalid_argument", "provider is closed or invalid")
  }
  id <- dilock_resource_id(provider$namespace, resource)
  structure(list(provider = provider, name = charToRaw(paste0("/dilock/locks/", id))),
            class = "dilock_etcd_handle")
}

.dilock_etcd_acquire <- function(handle, wait_ms, lease_ms) {
  if (!inherits(handle, "dilock_etcd_handle") || handle$provider$closed) {
    .dilock_stop("dilock_invalid_argument", "handle is closed or invalid")
  }
  provider <- handle$provider
  wait_ms <- .dilock_validate_milliseconds(wait_ms, "wait budget", 1L)
  lease_ms <- .dilock_validate_milliseconds(lease_ms, "lease duration", 1000L)
  deadline <- .dilock_monotonic() + wait_ms / 1000
  remaining <- function() max(0, floor((deadline - .dilock_monotonic()) * 1000))
  .dilock_etcd_metadata(provider, provider$client, min(provider$rpc_timeout_ms, max(1L, remaining())))
  budget <- min(provider$rpc_timeout_ms, remaining())
  if (budget < 1L) .dilock_stop("dilock_wait_timeout", "etcd acquisition budget expired before lease grant")

  attempt_client <- .dilock_etcd_new_client(provider)
  grant_client_transferred <- FALSE
  on.exit(if (!grant_client_transferred) try(rgrpc::grpc_close(attempt_client), silent = TRUE), add = TRUE)
  grant <- tryCatch(.dilock_etcd_lease_grant(provider, attempt_client,
                                              ceiling(lease_ms / 1000), budget),
                    error = function(e) {
                      if (inherits(e, "dilock_backend_error")) {
                        .dilock_stop("dilock_acquisition_unknown", "etcd LeaseGrant result is unknown", e)
                      }
                      stop(e)
                    })
  grant_client_transferred <- TRUE
  lease <- .dilock_etcd_new_lease(provider, attempt_client, handle$name, grant, lease_ms)
  cleanup_state <- new.env(parent = emptyenv())
  cleanup_state$returned <- FALSE
  cleanup_state$cleanup_attempted <- FALSE
  lock_call <- NULL
  perform_abandon <- function(call = lock_call) .dilock_etcd_abandon(provider, lease, call)
  abandon <- function(call = lock_call, suppress = FALSE) {
    .dilock_etcd_acquisition_cleanup(cleanup_state, function() perform_abandon(call), suppress)
  }
  on.exit(abandon(suppress = TRUE), add = TRUE)
  lease$deadline <- grant$grant_sent + grant$ttl_seconds - max(0.05, grant$ttl_seconds * 0.01)
  tryCatch(.dilock_etcd_keepalive_start(provider, lease, provider$rpc_timeout_ms),
           error = function(e) {
             abandon()
             stop(e)
           })

  lock_budget <- remaining()
  if (lock_budget < 1L) {
    abandon()
    .dilock_stop("dilock_wait_timeout", "etcd acquisition budget expired before Lock")
  }
  request <- c(.dilock_etcd_bytes_field(1, handle$name),
               .dilock_etcd_raw_varint_field(2, lease$id))
  lock_call <- tryCatch(
    rgrpc::grpc_call(attempt_client, provider$rpc$lock, request, deadline_ms = as.integer(lock_budget)),
    error = function(e) {
      abandon()
      .dilock_stop("dilock_acquisition_unknown", "could not start native etcd Lock", e)
    }
  )
  lease$lock_call <- lock_call
  next_keepalive <- lease$next_keepalive
  lock_event <- NULL
  tryCatch({
    repeat {
      now <- .dilock_monotonic()
      if (now >= deadline) break
      if (now >= lease$deadline) {
        lease$state <- "lost"
        .dilock_stop("dilock_lease_lost", "etcd lease expired while waiting for Lock")
      }
      if (now >= next_keepalive) {
        .dilock_etcd_keepalive_send(provider, lease, provider$rpc_timeout_ms)
        next_keepalive <- lease$next_keepalive
      }
      events <- rgrpc::grpc_poll(attempt_client, timeout_ms = as.integer(max(1, min(100, remaining()))))
      for (event in events) {
        if (identical(event$id, lock_call$id) && identical(event$kind, "unary")) {
          lock_event <- event
          break
        }
        if (identical(event$id, lease$stream_id)) {
          .dilock_etcd_keepalive_event(provider, lease, event)
          next_keepalive <- lease$next_keepalive
        }
      }
      if (!is.null(lock_event)) break
    }
  }, error = function(e) {
    abandon(lock_call, suppress = TRUE)
    if (inherits(e, "dilock_lease_lost")) stop(e)
    .dilock_stop("dilock_acquisition_unknown", "etcd Lock wait or lease keepalive failed", e)
  })

  if (is.null(lock_event)) {
    cleanup <- abandon(lock_call, suppress = TRUE)
    if (!cleanup) .dilock_stop("dilock_acquisition_unknown", "timed-out etcd waiter cleanup is unknown")
    .dilock_stop("dilock_wait_timeout", "etcd contention wait budget exhausted")
  }
  if (!identical(lock_event$status_name, "OK")) {
    .dilock_etcd_lock_failure(
      lock_event, deadline,
      function() abandon(lock_call)
    )
  }
  key <- lock_event$response_message$key
  if (!is.raw(key) || length(key) < 1L) {
    abandon()
    .dilock_stop("dilock_protocol_mismatch", "etcd Lock returned an empty opaque key")
  }
  lease$key <- key
  verify_budget <- min(provider$rpc_timeout_ms, remaining())
  if (verify_budget < 1L) {
    abandon()
    .dilock_stop("dilock_acquisition_unknown", "etcd key verification budget expired")
  }
  verified <- tryCatch(.dilock_etcd_range_lock_key(provider, attempt_client, key, verify_budget),
                       error = function(e) {
                         abandon()
                         .dilock_stop("dilock_acquisition_unknown", "etcd opaque key verification failed", e)
                       })
  if (!identical(verified$key, key) || !identical(verified$lease, lease$id)) {
    abandon()
    .dilock_stop("dilock_protocol_mismatch", "etcd Lock key is missing or attached to another lease")
  }
  lease$fence_revision <- verified$revision
  lease$fence <- list(epoch = provider$epoch, create_revision = verified$revision)
  now <- .dilock_monotonic()
  if (now >= lease$deadline) {
    abandon()
    .dilock_stop("dilock_acquisition_unknown", "etcd key verification exceeded the conservative lease budget")
  }
  provider$leases[[length(provider$leases) + 1L]] <- lease
  cleanup_state$returned <- TRUE
  lease
}

#' Attempt a nonblocking etcd acquisition.
#'
#' etcd's native Lock RPC queues and has no nonblocking try operation. This
#' function therefore fails immediately with `dilock_unsupported_capability`;
#' use `dilock_etcd_acquire()` when waiting is acceptable.
#' @param handle Reusable etcd lock-name handle.
#' @return Does not return; raises `dilock_unsupported_capability` before any RPC.
dilock_etcd_try_acquire <- function(handle) {
  if (!inherits(handle, "dilock_etcd_handle") || isTRUE(handle$provider$closed)) {
    .dilock_stop("dilock_invalid_argument", "handle is closed or invalid")
  }
  .dilock_stop("dilock_unsupported_capability",
               "etcd native Lock queues requests and has no nonblocking try-acquire")
}

#' Queue on native etcd Lock until acquired or the wait budget expires.
dilock_etcd_acquire <- function(handle, wait_ms = 5000L, lease_ms = 30000L) {
  .dilock_etcd_acquire(handle, wait_ms, lease_ms)
}

#' Renew a held etcd lease. Call explicitly during long work; R has no background renewal.
dilock_etcd_renew <- function(lease) {
  if (!inherits(lease, "dilock_etcd_lease")) .dilock_stop("dilock_invalid_argument", "lease is invalid")
  if (!identical(lease$state, "held") || is.null(lease$stream)) return(FALSE)
  now <- .dilock_monotonic()
  if (now >= lease$deadline) {
    lease$state <- "lost"
    return(FALSE)
  }
  tryCatch({
    .dilock_etcd_keepalive_send(lease$provider, lease, lease$provider$rpc_timeout_ms)
    target_ack <- lease$keepalive_sent_count
    deadline <- now + lease$provider$rpc_timeout_ms / 1000
    repeat {
      remaining <- floor((deadline - .dilock_monotonic()) * 1000)
      if (remaining < 1) stop("keepalive reply deadline expired")
      events <- rgrpc::grpc_await(lease$stream, timeout_ms = as.integer(min(remaining, 1000)))
      for (event in events) {
        if (identical(event$kind, "stream_writable")) next
        if (identical(event$kind, "stream_msg")) {
          .dilock_etcd_keepalive_event(lease$provider, lease, event)
          if (lease$keepalive_ack_count >= target_ack) return(TRUE)
        }
        if (identical(event$kind, "stream_status")) {
          .dilock_etcd_keepalive_event(lease$provider, lease, event)
        }
      }
    }
  }, error = function(e) {
    lease$state <- "lost"
    .dilock_stop("dilock_lease_lost", "etcd keepalive renewal was not confirmed", e)
  })
}

#' Release via native Unlock and bounded independent LeaseRevoke.
dilock_etcd_release <- function(lease) {
  if (!inherits(lease, "dilock_etcd_lease")) .dilock_stop("dilock_invalid_argument", "lease is invalid")
  if (!is.null(lease$outcome)) return(lease$outcome)
  lease$state <- "releasing"
  cleanup_deadline <- .dilock_monotonic() + lease$provider$cleanup_timeout_ms / 1000
  if (!is.null(lease$stream)) {
    try(rgrpc::grpc_cancel(lease$stream), silent = TRUE)
    lease$stream <- NULL
    lease$stream_id <- NULL
  }
  observation_budget <- min(
    lease$provider$rpc_timeout_ms,
    .dilock_etcd_remaining_ms(cleanup_deadline)
  )
  ownership <- .dilock_etcd_release_ownership(lease$provider, lease, observation_budget)
  unlock_acknowledged <- FALSE
  if (identical(ownership, "owned")) {
    unlock_budget <- min(
      lease$provider$rpc_timeout_ms,
      .dilock_etcd_remaining_ms(cleanup_deadline)
    )
    if (unlock_budget < 1L) {
      lease$release_error <- structure(
        list(message = "etcd cleanup budget expired before Unlock", call = NULL),
        class = c("dilock_cleanup_timeout", "error", "condition")
      )
    } else {
      unlock_request <- .dilock_etcd_message("v3lockpb.UnlockRequest", key = lease$key)
      tryCatch({
        .dilock_etcd_call(lease$client, lease$provider$rpc$unlock, unlock_request, unlock_budget)
        unlock_acknowledged <- TRUE
        invisible(NULL)
      }, error = function(e) {
        lease$release_error <- e
        invisible(NULL)
      })
    }
  }
  revoke_result <- tryCatch({
    .dilock_etcd_revoke(lease$provider, lease, cleanup_deadline = cleanup_deadline)
    "acknowledged"
  }, error = function(e) {
    lease$release_error <- e
    if (inherits(e, "dilock_backend_error") && identical(e$status_name, "NOT_FOUND")) {
      "not_found"
    } else {
      "unknown"
    }
  })
  try(rgrpc::grpc_close(lease$client), silent = TRUE)
  lease$outcome <- .dilock_etcd_release_outcome(ownership, unlock_acknowledged, revoke_result)
  lease$state <- if (identical(lease$outcome, "unknown")) "released_unknown" else "released"
  .dilock_etcd_untrack_lease(lease$provider, lease)
  lease$outcome
}

#' Run code under one etcd lock and release on every exit path.
with_dilock_etcd <- function(handle, code, wait_ms = 5000L, lease_ms = 30000L) {
  lease <- dilock_etcd_acquire(handle, wait_ms, lease_ms)
  on.exit({
    if (is.null(lease$outcome)) {
      outcome <- tryCatch(dilock_etcd_release(lease), error = function(e) "unknown")
      if (identical(outcome, "unknown")) {
        warning("Dilock etcd scoped cleanup outcome is unknown", call. = FALSE)
      } else if (identical(outcome, "already_not_owned")) {
        warning("Dilock etcd lease was no longer owned at scoped cleanup", call. = FALSE)
      }
    }
  }, add = TRUE)
  value <- force(code(lease))
  outcome <- dilock_etcd_release(lease)
  if (identical(outcome, "already_not_owned")) {
    .dilock_stop("dilock_lease_lost", "etcd lock was no longer owned at scoped cleanup")
  }
  if (identical(outcome, "unknown")) {
    .dilock_stop("dilock_release_unknown", "Dilock etcd scoped cleanup outcome is unknown")
  }
  value
}

#' Release tracked leases and close the provider's own channel.
dilock_etcd_close <- function(provider) {
  if (!inherits(provider, "dilock_etcd_provider") || isTRUE(provider$closed)) return(invisible(NULL))
  outcomes <- if (isTRUE(provider$unknown_cleanup)) "unknown" else character()
  for (lease in rev(provider$leases)) {
    if (is.null(lease$outcome)) {
      outcome <- tryCatch(dilock_etcd_release(lease), error = function(e) "unknown")
    } else {
      outcome <- lease$outcome
    }
    outcomes <- c(outcomes, outcome)
  }
  provider$closed <- TRUE
  try(rgrpc::grpc_close(provider$client), silent = TRUE)
  result <- if (any(outcomes == "unknown")) "unknown" else "released"
  if (identical(result, "unknown")) {
    warning("Dilock etcd provider closed with one or more unknown lease cleanup outcomes", call. = FALSE)
  }
  invisible(result)
}
