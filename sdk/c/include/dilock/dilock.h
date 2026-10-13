#ifndef DILOCK_C_DILOCK_H
#define DILOCK_C_DILOCK_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#if defined(_WIN32) && defined(DILOCK_C_SHARED)
#  if defined(DILOCK_C_BUILD)
#    define DILOCK_C_API __declspec(dllexport)
#  else
#    define DILOCK_C_API __declspec(dllimport)
#  endif
#elif defined(__GNUC__) && defined(DILOCK_C_SHARED)
#  define DILOCK_C_API __attribute__((visibility("default")))
#else
#  define DILOCK_C_API
#endif

#define DILOCK_C_ABI_VERSION 1u
#define DILOCK_C_ERROR_MESSAGE_CAPACITY 384u

typedef struct dilock_error {
    int32_t code;
    char message[DILOCK_C_ERROR_MESSAGE_CAPACITY];
} dilock_error;

typedef enum dilock_status {
    DILOCK_STATUS_OK = 0,
    DILOCK_STATUS_BUSY = 1,
    DILOCK_STATUS_INVALID_ARGUMENT = 2,
    DILOCK_STATUS_CANCELLED = 3,
    DILOCK_STATUS_WAIT_TIMEOUT = 4,
    DILOCK_STATUS_BACKEND_UNAVAILABLE = 5,
    DILOCK_STATUS_ACQUISITION_UNKNOWN = 6,
    DILOCK_STATUS_PROTOCOL_MISMATCH = 7,
    DILOCK_STATUS_LEASE_LOST = 8,
    DILOCK_STATUS_UNSUPPORTED_CAPABILITY = 9,
    DILOCK_STATUS_INTERNAL = 10
} dilock_status;

typedef enum dilock_release_outcome {
    DILOCK_RELEASED = 0,
    DILOCK_ALREADY_NOT_OWNED = 1,
    DILOCK_RELEASE_UNKNOWN = 2
} dilock_release_outcome;

typedef enum dilock_backend {
    DILOCK_BACKEND_REDIS = 0,
    DILOCK_BACKEND_ETCD = 1
} dilock_backend;

/* Cleanup notifications may run synchronously on whichever thread destroys a
 * lease. Notifications from separate leases may overlap. The reason string is
 * borrowed and valid only until this callback returns; copy it to retain it. */
typedef void (*dilock_cleanup_callback)(dilock_backend backend,
    dilock_release_outcome outcome, const char *reason, void *user_data);

typedef struct dilock_redis_provider dilock_redis_provider;
typedef struct dilock_redis_lock dilock_redis_lock;
typedef struct dilock_redis_lease dilock_redis_lease;
typedef struct dilock_etcd_provider dilock_etcd_provider;
typedef struct dilock_etcd_lock dilock_etcd_lock;
typedef struct dilock_etcd_lease dilock_etcd_lease;
typedef struct dilock_cancellation dilock_cancellation;

typedef struct dilock_etcd_fence {
    int64_t create_revision;
    size_t coordination_epoch_bytes;
} dilock_etcd_fence;

typedef struct dilock_redis_options {
    const char *host;
    int32_t port;
    const char *username;
    const char *password;
    int32_t database;
    const char *name_space;
    const char *coordination_epoch;
    dilock_cleanup_callback cleanup_callback;
    void *cleanup_user_data;
    int64_t rpc_timeout_ms;
    int64_t cleanup_timeout_ms;
    int64_t safety_margin_ms;
    int64_t poll_interval_ms;
    int64_t default_wait_ms;
    int64_t default_lease_ms;
} dilock_redis_options;

typedef struct dilock_etcd_options {
    const char *endpoint;
    const char *name_space;
    const char *coordination_epoch;
    const char *tls_root_certificates;
    const char *tls_client_certificate;
    const char *tls_client_private_key;
    dilock_cleanup_callback cleanup_callback;
    void *cleanup_user_data;
    int64_t rpc_timeout_ms;
    int64_t cleanup_timeout_ms;
    int64_t safety_margin_ms;
    int64_t default_wait_ms;
    int64_t default_lease_ms;
    int32_t auto_renew;
} dilock_etcd_options;

/* Set ABI defaults: default namespace, 2s RPC/cleanup, 5s wait and 30s lease.
 * coordination_epoch must still be supplied by the deployment. */
DILOCK_C_API uint32_t dilock_c_abi_version(void);
DILOCK_C_API void dilock_redis_options_init(dilock_redis_options *options);
DILOCK_C_API void dilock_etcd_options_init(dilock_etcd_options *options);
DILOCK_C_API dilock_cancellation *dilock_cancellation_create(dilock_error *error);
DILOCK_C_API void dilock_cancellation_request(dilock_cancellation *cancellation);
/* Keep the cancellation object alive until the acquire call returns. */
DILOCK_C_API void dilock_cancellation_destroy(dilock_cancellation *cancellation);

/* Providers own their backend transports. Resource handles retain shared
 * transport state and leases own one acquisition. Explicitly release leases
 * before destroying them; destroying a held lease makes bounded best-effort
 * cleanup, whose acknowledgement is available only from release(). Keep each
 * cleanup callback's user_data valid until all associated providers, resource
 * handles, and leases have been destroyed and all cleanup callbacks have
 * returned. The callback must be safe for concurrent calls and must copy the
 * borrowed reason string if it needs it after returning. Destroying a provider
 * alone does not end the user_data lifetime. */
DILOCK_C_API dilock_status dilock_redis_provider_create(
    const dilock_redis_options *options, dilock_redis_provider **out, dilock_error *error);
DILOCK_C_API void dilock_redis_provider_destroy(dilock_redis_provider *provider);
DILOCK_C_API dilock_status dilock_redis_lock_create(
    dilock_redis_provider *provider, const char *resource,
    dilock_redis_lock **out, dilock_error *error);
DILOCK_C_API void dilock_redis_lock_destroy(dilock_redis_lock *lock);
DILOCK_C_API dilock_status dilock_redis_acquire(
    dilock_redis_lock *lock, int64_t wait_ms, int64_t lease_ms,
    dilock_redis_lease **out, dilock_error *error);
DILOCK_C_API dilock_status dilock_redis_acquire_default(
    dilock_redis_lock *lock, dilock_redis_lease **out, dilock_error *error);
DILOCK_C_API dilock_status dilock_redis_acquire_with_cancellation(
    dilock_redis_lock *lock, int64_t wait_ms, int64_t lease_ms,
    dilock_cancellation *cancellation, dilock_redis_lease **out, dilock_error *error);
DILOCK_C_API dilock_status dilock_redis_acquire_default_with_cancellation(
    dilock_redis_lock *lock, dilock_cancellation *cancellation,
    dilock_redis_lease **out, dilock_error *error);
DILOCK_C_API dilock_status dilock_redis_try_acquire(
    dilock_redis_lock *lock, int64_t lease_ms,
    dilock_redis_lease **out, dilock_error *error);
DILOCK_C_API int32_t dilock_redis_lease_is_valid(const dilock_redis_lease *lease);
/* Initialize outcome to RELEASE_UNKNOWN before validation; status remains authoritative. */
DILOCK_C_API dilock_status dilock_redis_lease_release(
    dilock_redis_lease *lease, dilock_release_outcome *outcome, dilock_error *error);
DILOCK_C_API dilock_status dilock_redis_lease_renew(
    dilock_redis_lease *lease, int64_t lease_ms, int32_t *renewed, dilock_error *error);
DILOCK_C_API void dilock_redis_lease_destroy(dilock_redis_lease *lease);
DILOCK_C_API dilock_status dilock_redis_provision_namespace(
    const dilock_redis_options *options, dilock_error *error);

DILOCK_C_API dilock_status dilock_etcd_provider_create(
    const dilock_etcd_options *options, dilock_etcd_provider **out, dilock_error *error);
DILOCK_C_API void dilock_etcd_provider_destroy(dilock_etcd_provider *provider);
DILOCK_C_API dilock_status dilock_etcd_lock_create(
    dilock_etcd_provider *provider, const char *resource,
    dilock_etcd_lock **out, dilock_error *error);
DILOCK_C_API void dilock_etcd_lock_destroy(dilock_etcd_lock *lock);
DILOCK_C_API dilock_status dilock_etcd_acquire(
    dilock_etcd_lock *lock, int64_t wait_ms, int64_t lease_ms,
    dilock_etcd_lease **out, dilock_error *error);
DILOCK_C_API dilock_status dilock_etcd_acquire_default(
    dilock_etcd_lock *lock, dilock_etcd_lease **out, dilock_error *error);
DILOCK_C_API dilock_status dilock_etcd_acquire_with_cancellation(
    dilock_etcd_lock *lock, int64_t wait_ms, int64_t lease_ms,
    dilock_cancellation *cancellation, dilock_etcd_lease **out, dilock_error *error);
DILOCK_C_API dilock_status dilock_etcd_acquire_default_with_cancellation(
    dilock_etcd_lock *lock, dilock_cancellation *cancellation,
    dilock_etcd_lease **out, dilock_error *error);
DILOCK_C_API int32_t dilock_etcd_lease_is_valid(const dilock_etcd_lease *lease);
/* Return the server-granted whole-second etcd TTL in milliseconds. */
DILOCK_C_API dilock_status dilock_etcd_lease_effective_ttl_ms(
    const dilock_etcd_lease *lease, int64_t *effective_ttl_ms,
    dilock_error *error);
/* The epoch buffer must hold coordination_epoch_bytes plus a trailing NUL.
 * If it is null or too small, return INVALID_ARGUMENT while still filling
 * fence->create_revision and fence->coordination_epoch_bytes; callers may use
 * that byte count to allocate a buffer and retry. */
DILOCK_C_API dilock_status dilock_etcd_lease_fence(
    const dilock_etcd_lease *lease, dilock_etcd_fence *fence,
    char *coordination_epoch, size_t epoch_capacity, dilock_error *error);
DILOCK_C_API dilock_status dilock_etcd_lease_release(
    dilock_etcd_lease *lease, dilock_release_outcome *outcome, dilock_error *error);
DILOCK_C_API void dilock_etcd_lease_destroy(dilock_etcd_lease *lease);
DILOCK_C_API dilock_status dilock_etcd_try_acquire(
    dilock_etcd_lock *lock, int64_t lease_ms,
    dilock_etcd_lease **out, dilock_error *error);

#ifdef __cplusplus
}
#endif

#endif
