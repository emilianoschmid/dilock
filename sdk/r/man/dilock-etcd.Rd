\name{dilock-etcd}
\alias{dilock-etcd}
\alias{dilock_etcd_provider}
\alias{dilock_etcd_lock}
\alias{dilock_etcd_try_acquire}
\alias{dilock_etcd_acquire}
\alias{dilock_etcd_renew}
\alias{dilock_etcd_release}
\alias{with_dilock_etcd}
\alias{dilock_etcd_close}
\title{Experimental native etcd lock operations}
\description{
Experimental R adapter using etcd v3 native gRPC Lock and Lease services. This
interface is not a production support claim.
}
\usage{
dilock_etcd_provider(target, namespace = "default", epoch, credentials = NULL,
  rpc_timeout_ms = 5000L, cleanup_timeout_ms = 1000L)

dilock_etcd_lock(provider, resource)

dilock_etcd_try_acquire(handle)

dilock_etcd_acquire(handle, wait_ms = 5000L, lease_ms = 30000L)

dilock_etcd_renew(lease)

dilock_etcd_release(lease)

with_dilock_etcd(handle, code, wait_ms = 5000L, lease_ms = 30000L)

dilock_etcd_close(provider)
}
\arguments{
  \item{target}{gRPC authority such as \code{"localhost:2379"}.}
  \item{namespace}{Namespace whose etcd metadata was provisioned by the operator.}
  \item{epoch}{Exact deployment coordination epoch from the namespace metadata.}
  \item{credentials}{Optional TLS credentials supported by \pkg{rgrpc}.}
  \item{rpc_timeout_ms}{Bound for each gRPC request.}
  \item{cleanup_timeout_ms}{One total monotonic budget shared by Unlock and LeaseRevoke cleanup.}
  \item{provider}{Provider returned by \code{dilock_etcd_provider()}. It owns a gRPC channel.}
  \item{resource}{UTF-8 lock resource identity.}
  \item{handle}{Reusable lock-name handle.}
  \item{lease_ms}{Requested lease duration in integral milliseconds. etcd applies whole-second TTLs.}
  \item{wait_ms}{Maximum local acquisition wait budget. It does not stop protected work after lease expiry.}
  \item{lease}{Mutable lease returned by an acquisition call. Do not copy it as independent ownership.}
  \item{code}{Function called with the acquired lease; its value is returned after cleanup.}
}
\value{
Provider, handle, and lease functions return objects. Try-acquire raises
\code{dilock_unsupported_capability} before making an RPC because native etcd
Lock queues requests. Acquire raises a typed timeout or
backend/protocol/unknown condition. Renewal returns \code{TRUE} only after a
keepalive acknowledgement. Release linearly ranges the exact opaque key and
confirms its lease ID before calling native \code{Unlock}. It returns
\code{"released"} only after exact ownership, an Unlock acknowledgement, and a
positive LeaseRevoke acknowledgement. A confirmed absent or mismatched key,
LeaseRevoke \code{NOT_FOUND}, or a Revoke acknowledgement without the other
required evidence returns \code{"already_not_owned"}. Uncertain ownership or
cleanup returns \code{"unknown"}. Range, Unlock, and LeaseRevoke share one
monotonic cleanup budget; an Unlock acknowledgement alone is insufficient
because etcd may acknowledge an Unlock after the key has expired. The
scoped helper returns the body value after confirmed release, raises
\code{dilock_lease_lost} after known loss of ownership, and raises
\code{dilock_release_unknown} when cleanup remains uncertain. Closing a
provider returns \code{"released"} after known cleanup or
\code{"unknown"} and warns if any tracked lease cleanup is uncertain.
Completed leases are removed from provider tracking; an earlier uncertain
release is retained as a bounded provider-level status and reported again
when the provider closes.
}
\details{
The experimental adapter pins \pkg{rgrpc} 0.1.2 and \pkg{RProtoBuf} 0.4.28,
uses packaged focused etcd v3.7.2 protobuf definitions, and requires a native
gRPC C++ runtime and protobuf development toolchain. Namespace metadata must be
pre-provisioned; the SDK never creates or repairs it. Lease renewal is explicit
because this synchronous R adapter provides no background renewal. Unknown
waiter cleanup is fail-closed. A nonblocking try operation is unsupported; use
acquire with a finite wait budget. etcd fencing is the coordination epoch and
the opaque lock key's observed \code{create_revision}; downstream systems must
enforce the fence when stale effects need rejection. Lease expiry does not
stop R code, guarantee exactly-once effects, or make external side effects
fenced. TLS, broad failure-injection coverage, and production suitability have
not been validated.
}
\seealso{\code{dilock_etcd_provider()}, \code{with_dilock_etcd()}}
