#ifndef DILOCK_HASKELL_FFI_H
#define DILOCK_HASKELL_FFI_H

#include <stddef.h>
#include <stdint.h>

int hs_dilock_abi_version(void);
int hs_dilock_cancellation_create(void **out, char *message);
void hs_dilock_cancellation_request(void *cancellation);
void hs_dilock_cancellation_destroy(void *cancellation);
int hs_dilock_etcd_provider_create(
    const char *endpoint, const char *name_space, const char *epoch,
    const char *tls_roots, const char *tls_certificate, const char *tls_key,
    int64_t rpc_timeout_ms, int64_t cleanup_timeout_ms, int64_t safety_margin_ms,
    int64_t default_wait_ms, int64_t default_lease_ms, int auto_renew,
    void **out, char *message);
void hs_dilock_etcd_provider_destroy(void *provider);
int hs_dilock_etcd_lock_create(void *provider, const char *resource,
                               void **out, char *message);
void hs_dilock_etcd_lock_destroy(void *lock);
int hs_dilock_etcd_acquire(void *lock, int64_t wait_ms, int64_t lease_ms,
                           void *cancellation, void **out, char *message);
int hs_dilock_etcd_lease_valid(void *lease);
int hs_dilock_etcd_lease_ttl_ms(void *lease, int64_t *ttl_ms, char *message);
int hs_dilock_etcd_lease_fence(void *lease, int64_t *revision, char *epoch,
                               size_t epoch_capacity, char *message);
int hs_dilock_etcd_lease_release(void *lease, int *outcome, char *message);
void hs_dilock_etcd_lease_destroy(void *lease);

int hs_dilock_consul_provider_create(
    const char *endpoint, const char *name_space, const char *epoch, const char *token,
    int64_t rpc_timeout_ms, int64_t cleanup_timeout_ms, int64_t poll_interval_ms,
    int64_t default_wait_ms, int64_t default_lease_ms, void **out, char *message);
int hs_dilock_consul_namespace_provision(
    const char *endpoint, const char *name_space, const char *epoch, const char *token,
    int64_t rpc_timeout_ms, int64_t cleanup_timeout_ms, int64_t poll_interval_ms,
    int64_t default_wait_ms, int64_t default_lease_ms, char *message);
void hs_dilock_consul_provider_destroy(void *provider);
int hs_dilock_consul_lock_create(void *provider, const char *resource,
                                 void **out, char *message);
void hs_dilock_consul_lock_destroy(void *lock);
int hs_dilock_consul_acquire(void *lock, int64_t wait_ms, int64_t lease_ms,
                             void *cancellation, void **out, char *message);
int hs_dilock_consul_try_acquire(void *lock, int64_t lease_ms,
                                 void **out, char *message);
int hs_dilock_consul_lease_valid(void *lease);
int hs_dilock_consul_lease_ttl_ms(void *lease, int64_t *ttl_ms, char *message);
int hs_dilock_consul_lease_fence(void *lease, uint64_t *lock_index, char *epoch,
                                 size_t epoch_capacity, char *message);
int hs_dilock_consul_lease_renew(void *lease, int *renewed, char *message);
int hs_dilock_consul_lease_release(void *lease, int *outcome, char *message);
void hs_dilock_consul_lease_destroy(void *lease);

int hs_dilock_postgres_provider_create(
    const char *connection_string, const char *name_space, const char *epoch,
    int64_t rpc_timeout_ms, int64_t cleanup_timeout_ms, int64_t poll_interval_ms,
    int64_t default_wait_ms, int64_t default_lease_ms, void **out, char *message);
int hs_dilock_postgres_namespace_provision(
    const char *connection_string, const char *name_space, const char *epoch,
    int64_t rpc_timeout_ms, int64_t cleanup_timeout_ms, int64_t poll_interval_ms,
    int64_t default_wait_ms, int64_t default_lease_ms, char *message);
void hs_dilock_postgres_provider_destroy(void *provider);
int hs_dilock_postgres_lock_create(void *provider, const char *resource,
                                   void **out, char *message);
void hs_dilock_postgres_lock_destroy(void *lock);
int hs_dilock_postgres_acquire(void *lock, int64_t wait_ms, int64_t lease_ms,
                               void **out, char *message);
int hs_dilock_postgres_try_acquire(void *lock, void **out, char *message);
int hs_dilock_postgres_lease_valid(void *lease);
int hs_dilock_postgres_lease_renew(void *lease, int *renewed, char *message);
int hs_dilock_postgres_lease_release(void *lease, int *outcome, char *message);
void hs_dilock_postgres_lease_destroy(void *lease);

#endif
