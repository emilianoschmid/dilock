.dilock_pg_profile <- "postgres-advisory-session-v1"
.dilock_pg_scheme <- "sha256-framed-utf8-v1"

.dilock_pg_connection_args <- function(dsn) {
  if (!is.character(dsn) || length(dsn) != 1L || is.na(dsn) || !nzchar(trimws(dsn))) {
    .dilock_stop("dilock_invalid_argument", "PostgreSQL DSN must be one non-empty string")
  }
  value <- trimws(dsn)
  args <- list()
  if (grepl("^postgres(?:ql)?://", value, ignore.case = TRUE)) {
    uri <- sub("^postgres(?:ql)?://", "", value, ignore.case = TRUE)
    authority <- sub("/.*$", "", uri)
    database <- sub("^[^/]*/", "", uri)
    database <- sub("\\?.*$", "", database)
    authority_parts <- strsplit(authority, "@", fixed = TRUE)[[1L]]
    host_port <- authority_parts[[length(authority_parts)]]
    if (length(authority_parts) > 1L) {
      userinfo <- paste(authority_parts[-length(authority_parts)], collapse = "@")
      credentials <- strsplit(userinfo, ":", fixed = TRUE)[[1L]]
      args$user <- utils::URLdecode(credentials[[1L]])
      if (length(credentials) > 1L) args$password <- utils::URLdecode(paste(credentials[-1L], collapse = ":"))
    }
    if (startsWith(host_port, "[")) {
      closing <- regexpr("]", host_port, fixed = TRUE)[[1L]]
      args$host <- substr(host_port, 2L, closing - 1L)
      if (nchar(host_port) > closing + 1L && startsWith(substr(host_port, closing + 1L, closing + 1L), ":")) {
        args$port <- substr(host_port, closing + 2L, nchar(host_port))
      }
    } else {
      port_match <- regexec(":([0-9]+)$", host_port)
      port_parts <- regmatches(host_port, port_match)[[1L]]
      if (length(port_parts) > 0L) {
        args$host <- substr(host_port, 1L, nchar(host_port) - nchar(port_parts[[1L]]))
        args$port <- port_parts[[2L]]
      } else {
        args$host <- host_port
      }
    }
    if (nzchar(database)) args$dbname <- utils::URLdecode(database)
  } else {
    value <- sub("^DBI:Pg:", "", value, ignore.case = TRUE)
    tokens <- unlist(strsplit(gsub(";", " ", value, fixed = TRUE), "[[:space:]]+"), use.names = FALSE)
    tokens <- tokens[nzchar(tokens)]
    for (token in tokens) {
      pair <- strsplit(token, "=", fixed = TRUE)[[1L]]
      if (length(pair) < 2L) next
      key <- tolower(pair[[1L]])
      mapped <- switch(key, database = "dbname", dbname = "dbname", host = "host", port = "port", user = "user", username = "user", password = "password", NULL)
      if (!is.null(mapped)) args[[mapped]] <- utils::URLdecode(paste(pair[-1L], collapse = "="))
    }
  }
  args
}

.dilock_pg_connect <- function(dsn) {
  if (!requireNamespace("DBI", quietly = TRUE) || !requireNamespace("RPostgres", quietly = TRUE)) {
    .dilock_stop("dilock_backend_error", "PostgreSQL requires DBI and RPostgres")
  }
  tryCatch(do.call(DBI::dbConnect, c(list(RPostgres::Postgres()), .dilock_pg_connection_args(dsn))),
           error = function(e) .dilock_stop("dilock_backend_error", "PostgreSQL connection failed", e))
}

.dilock_pg_validate <- function(connection, namespace, epoch) {
  rows <- tryCatch(DBI::dbGetQuery(connection, "SELECT protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms FROM dilock_namespace_meta WHERE namespace = $1", params = list(namespace)), error = function(e) .dilock_stop("dilock_backend_error", "PostgreSQL metadata read failed", e))
  if (nrow(rows) != 1L || !identical(as.integer(rows$protocol_major[[1L]]), 1L) || !identical(as.character(rows$identity_scheme[[1L]]), .dilock_pg_scheme) || !identical(as.character(rows$profile[[1L]]), .dilock_pg_profile) || !identical(as.character(rows$coordination_epoch[[1L]]), epoch) || !is.na(rows$fixed_lease_ms[[1L]])) .dilock_stop("dilock_protocol_mismatch", "PostgreSQL namespace metadata is absent or incompatible")
  invisible(TRUE)
}

dilock_pg_provider <- function(dsn, namespace = "default", epoch) {
  dilock_resource_id(namespace, "validation")
  if (!is.character(epoch) || length(epoch) != 1L || is.na(epoch) || nchar(epoch, type = "bytes") < 1L || nchar(epoch, type = "bytes") > 1024L) .dilock_stop("dilock_invalid_argument", "epoch must be 1..1024 UTF-8 bytes")
  connection <- .dilock_pg_connect(dsn); on.exit(DBI::dbDisconnect(connection), add = TRUE)
  .dilock_pg_validate(connection, namespace, epoch)
  structure(list(dsn = dsn, namespace = namespace, epoch = epoch, rpc_timeout_ms = 2000L, cleanup_timeout_ms = 2000L, closed = FALSE), class = "dilock_pg_provider")
}

dilock_pg_provision_namespace <- function(dsn, namespace = "default", epoch) {
  dilock_resource_id(namespace, "validation")
  connection <- .dilock_pg_connect(dsn); on.exit(DBI::dbDisconnect(connection), add = TRUE)
  inserted <- tryCatch({
    DBI::dbBegin(connection)
    DBI::dbExecute(connection, "SELECT pg_advisory_xact_lock(hashtextextended('dilock:namespace-meta:v1', 0))")
    DBI::dbExecute(connection, "CREATE TABLE IF NOT EXISTS dilock_namespace_meta (namespace text PRIMARY KEY, protocol_major integer NOT NULL, identity_scheme text NOT NULL, profile text NOT NULL, coordination_epoch text NOT NULL, fixed_lease_ms bigint NULL)")
    result <- DBI::dbExecute(connection, "INSERT INTO dilock_namespace_meta (namespace, protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms) VALUES ($1, 1, $2, $3, $4, NULL) ON CONFLICT (namespace) DO NOTHING", params = list(namespace, .dilock_pg_scheme, .dilock_pg_profile, epoch))
    DBI::dbCommit(connection)
    result
  }, error = function(error) {
    try(DBI::dbRollback(connection), silent = TRUE)
    stop(error)
  })
  .dilock_pg_validate(connection, namespace, epoch)
  if (identical(as.integer(inserted), 1L)) "created" else "compatible-existing"
}

dilock_pg_lock <- function(provider, resource) {
  stopifnot(inherits(provider, "dilock_pg_provider"))
  if (isTRUE(provider$closed)) .dilock_stop("dilock_backend_error", "PostgreSQL provider is closed")
  structure(list(provider = provider, resource_id = dilock_resource_id(provider$namespace, resource)), class = "dilock_pg_handle")
}

.dilock_pg_key_sql <- "(('x' || $1)::bit(64))::bigint"

dilock_pg_try_acquire <- function(handle) {
  stopifnot(inherits(handle, "dilock_pg_handle"))
  connection <- .dilock_pg_connect(handle$provider$dsn)
  key_hex <- substr(handle$resource_id, 1L, 16L)
  result <- tryCatch(DBI::dbGetQuery(connection, paste0("SELECT pg_try_advisory_lock(", .dilock_pg_key_sql, ") AS acquired"), params = list(key_hex)), error = function(e) { DBI::dbDisconnect(connection); .dilock_stop("dilock_acquisition_unknown", "PostgreSQL advisory-lock acknowledgement is unknown", e) })
  if (!isTRUE(result$acquired[[1L]])) { DBI::dbDisconnect(connection); return(NULL) }
  lease <- new.env(parent = emptyenv()); lease$connection <- connection; lease$key_hex <- key_hex; lease$state <- "held"; lease$outcome <- NULL; class(lease) <- "dilock_pg_lease"; lease
}

dilock_pg_acquire <- function(handle, wait_ms = 5000L) {
  wait_ms <- .dilock_validate_milliseconds(wait_ms, "wait budget", 1L); deadline <- .dilock_monotonic() + wait_ms / 1000
  repeat { lease <- dilock_pg_try_acquire(handle); if (!is.null(lease)) return(lease); remaining <- deadline - .dilock_monotonic(); if (remaining <= 0) .dilock_stop("dilock_wait_timeout", "PostgreSQL advisory-lock contention wait exhausted"); Sys.sleep(min(remaining, 0.025)) }
}

dilock_pg_renew <- function(lease) {
  stopifnot(inherits(lease, "dilock_pg_lease")); if (!identical(lease$state, "held")) return(FALSE)
  tryCatch({ DBI::dbGetQuery(lease$connection, "SELECT 1"); TRUE }, error = function(e) { lease$state <- "lost"; .dilock_stop("dilock_backend_error", "PostgreSQL liveness check failed", e) })
}

dilock_pg_release <- function(lease) {
  stopifnot(inherits(lease, "dilock_pg_lease")); if (!is.null(lease$outcome)) return(lease$outcome); lease$state <- "releasing"
  lease$outcome <- tryCatch({ result <- DBI::dbGetQuery(lease$connection, paste0("SELECT pg_advisory_unlock(", .dilock_pg_key_sql, ") AS released"), params = list(lease$key_hex)); if (isTRUE(result$released[[1L]])) "released" else "already_not_owned" }, error = function(e) "unknown")
  try(DBI::dbDisconnect(lease$connection), silent = TRUE); lease$state <- if (identical(lease$outcome, "unknown")) "released_unknown" else "released"; lease$outcome
}

with_dilock_pg <- function(handle, code, wait_ms = 5000L) {
  lease <- dilock_pg_acquire(handle, wait_ms); value <- tryCatch(force(code(lease)), error = function(e) { dilock_pg_release(lease); stop(e) }); outcome <- dilock_pg_release(lease)
  if (identical(outcome, "already_not_owned")) .dilock_stop("dilock_lease_lost", "PostgreSQL lock was no longer owned at scope exit")
  if (identical(outcome, "unknown")) .dilock_stop("dilock_release_unknown", "PostgreSQL lock release was not acknowledged")
  value
}
