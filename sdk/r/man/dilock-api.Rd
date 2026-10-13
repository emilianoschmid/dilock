\name{dilock-api}
\alias{dilock-api}
\alias{dilock_provider}
\alias{dilock_lock}
\alias{dilock_try_acquire}
\alias{dilock_acquire}
\alias{dilock_release}
\alias{dilock_renew}
\alias{with_dilock}
\alias{dilock_resource_id}
\title{Redis distributed lock operations}
\description{
Create a Redis-backed Dilock provider and resource handle, acquire bounded
leases, and release or renew a lease while preserving unknown backend outcomes.
These experimental operations use Redis single-authority semantics.
}
\usage{
dilock_provider(connection, namespace = "default", epoch)

dilock_lock(provider, resource)

dilock_try_acquire(handle, lease_ms = 30000L)

dilock_acquire(handle, wait_ms = 5000L, lease_ms = 30000L)

dilock_release(lease)

dilock_renew(lease, lease_ms = lease$lease_ms)

with_dilock(handle, code, wait_ms = 5000L, lease_ms = 30000L)

dilock_resource_id(namespace, resource)
}
\arguments{
  \item{connection}{A caller-owned \pkg{redux} connection exposing a
  \code{command()} method. The provider borrows it and never closes it.}
  \item{namespace}{A namespace matching the shared identity contract.}
  \item{epoch}{The exact coordination epoch recorded in provisioned Redis
  namespace metadata.}
  \item{provider}{A provider returned by \code{dilock_provider()}.}
  \item{resource}{A UTF-8 resource name. Its exact bytes participate in the
  shared framed identity.}
  \item{handle}{A reusable resource handle.}
  \item{lease}{A lease returned by an acquisition call. Lease environments
  are mutable and must not be copied as independent ownership.}
  \item{lease_ms}{Lease duration in integral milliseconds, from 100 through
  86400000. It is a Redis expiry duration, not a guarantee that expiry stops
  work already running in the caller.}
  \item{wait_ms}{Maximum local contention-wait budget in milliseconds. It
  does not bound a blocked Redis command. Only acknowledged contention is
  retried.}
  \item{code}{A function called with the acquired lease. Its result is
  returned after owner-checked cleanup.}
}
\value{
\code{dilock_resource_id()} returns the lowercase hexadecimal SHA-256
identity. Provider and handle functions return their respective objects.
\code{dilock_try_acquire()} returns a lease or \code{NULL} for acknowledged
contention. \code{dilock_acquire()} returns a lease or raises a typed timeout,
backend, protocol, or unknown-acquisition condition. \code{dilock_release()}
returns \code{"released"}, \code{"already_not_owned"}, or \code{"unknown"}.
\code{dilock_renew()} returns a logical success indicator. \code{with_dilock()}
returns the body value after confirmed release, raises \code{dilock_lease_lost}
after acknowledged loss of ownership, and raises \code{dilock_release_unknown}
when cleanup acknowledgement is unknown.
}
\details{
Backend failures are not contention. Acquisition uncertainty is surfaced as
\code{dilock_acquisition_unknown}; release uncertainty is reported as
\code{"unknown"}; scoped cleanup raises \code{dilock_lease_lost} for known loss
of ownership and warns about that loss or unknown cleanup when the body itself
fails. Redis executes the packaged owner-token-checked
Lua scripts; no server-side installation is needed. The lease does not fence
external side effects, stop stale processes, or promise exactly-once effects.
There is no automatic renewal or verified thread-safety guarantee. Callers
should use one lease from one R execution context and provide application-level
fencing or idempotency for protected external writes. See
\code{dilock-etcd} for the separate experimental native etcd adapter.
}
\seealso{\code{redux::hiredis()}}
