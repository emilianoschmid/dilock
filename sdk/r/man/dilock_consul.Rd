\name{dilock_consul_provider}
\alias{dilock_consul_provider}
\alias{dilock_consul_lock}
\alias{dilock_consul_try_acquire}
\alias{dilock_consul_acquire}
\alias{dilock_consul_fence}
\alias{dilock_consul_valid}
\alias{dilock_consul_renew}
\alias{dilock_consul_release}
\alias{with_dilock_consul}
\alias{dilock_consul_close}
\alias{dilock_consul_provision_namespace}
\title{Consul distributed lock adapter}
\description{
Experimental Consul HTTP KV and session adapter. Namespace metadata is
validated when a provider is created and can only be created explicitly with
\code{dilock_consul_provision_namespace()} using Consul CAS=0.
}
\usage{
dilock_consul_provider(endpoint = "http://127.0.0.1:8500", namespace = "default",
  epoch, token = NULL, rpc_timeout_ms = 2000L, cleanup_timeout_ms = 2000L,
  poll_interval_ms = 100L, default_wait_ms = 5000L, default_lease_ms = 30000L)
dilock_consul_lock(provider, resource)
dilock_consul_try_acquire(handle, lease_ms = handle$provider$default_lease_ms)
dilock_consul_acquire(handle, wait_ms = handle$provider$default_wait_ms,
  lease_ms = handle$provider$default_lease_ms)
dilock_consul_fence(lease)
dilock_consul_valid(lease)
dilock_consul_renew(lease)
dilock_consul_release(lease)
with_dilock_consul(handle, code, wait_ms = handle$provider$default_wait_ms,
  lease_ms = handle$provider$default_lease_ms)
dilock_consul_close(provider)
dilock_consul_provision_namespace(endpoint, namespace = "default", epoch,
  token = NULL, rpc_timeout_ms = 2000L)
}
\arguments{
  \item{endpoint}{Consul HTTP(S) authority URL.}
  \item{namespace}{Strict Dilock namespace.}
  \item{epoch}{Approved coordination epoch.}
  \item{token}{Optional Consul ACL token.}
  \item{rpc_timeout_ms}{Per-request timeout in milliseconds.}
  \item{cleanup_timeout_ms}{Bounded cleanup timeout in milliseconds.}
  \item{poll_interval_ms}{Waiter polling interval in milliseconds.}
  \item{default_wait_ms}{Default finite acquisition budget.}
  \item{default_lease_ms}{Consul session TTL in milliseconds; at least 10000.}
  \item{provider}{Consul provider returned by \code{dilock_consul_provider()}.}
  \item{resource}{Resource identifier.}
  \item{handle}{Reusable resource handle.}
  \item{lease_ms}{Requested session TTL in milliseconds.}
  \item{wait_ms}{Finite acquisition budget in milliseconds.}
  \item{lease}{Lease returned by an acquisition function.}
  \item{code}{Function called with the acquired lease.}
}
\value{
Provider and handle functions return their corresponding values. Acquisition
returns a lease or \code{NULL} only for acknowledged contention in the
nonblocking function. Release returns \code{"released"},
\code{"already_not_owned"}, or \code{"unknown"}. Fencing returns the
coordination epoch and an exact decimal LockIndex string.
}
\details{
Renew long-running work explicitly. Expiry cannot stop stale application code;
the protected resource must atomically enforce the epoch and LockIndex fence.
}
\seealso{\code{dilock_provider}, \code{dilock_etcd_provider}}
