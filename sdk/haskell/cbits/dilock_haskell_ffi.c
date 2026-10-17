#include "dilock_haskell_ffi.h"
#include <dilock/dilock.h>

#include <string.h>

_Static_assert(DILOCK_C_ABI_VERSION == 1u, "Haskell facade supports only Dilock C ABI v1");

static void copy_message(char *destination, const dilock_error *error) {
    if (destination == NULL) return;
    if (error == NULL) {
        destination[0] = '\0';
        return;
    }
    memcpy(destination, error->message, DILOCK_C_ERROR_MESSAGE_CAPACITY);
    destination[DILOCK_C_ERROR_MESSAGE_CAPACITY - 1u] = '\0';
}

int hs_dilock_abi_version(void) {
    return (int)dilock_c_abi_version();
}

int hs_dilock_cancellation_create(void **out, char *message) {
    dilock_error error = {0};
    dilock_cancellation *value = dilock_cancellation_create(&error);
    copy_message(message, &error);
    if (value == NULL) return error.code == DILOCK_STATUS_OK ? DILOCK_STATUS_INTERNAL : error.code;
    *out = value;
    return DILOCK_STATUS_OK;
}

void hs_dilock_cancellation_request(void *cancellation) {
    dilock_cancellation_request((dilock_cancellation *)cancellation);
}

void hs_dilock_cancellation_destroy(void *cancellation) {
    dilock_cancellation_destroy((dilock_cancellation *)cancellation);
}

int hs_dilock_etcd_provider_create(
    const char *endpoint, const char *name_space, const char *epoch,
    const char *tls_roots, const char *tls_certificate, const char *tls_key,
    int64_t rpc_timeout_ms, int64_t cleanup_timeout_ms, int64_t safety_margin_ms,
    int64_t default_wait_ms, int64_t default_lease_ms, int auto_renew,
    void **out, char *message) {
    dilock_etcd_options options;
    dilock_etcd_options_init(&options);
    options.endpoint = endpoint;
    options.name_space = name_space;
    options.coordination_epoch = epoch;
    options.tls_root_certificates = tls_roots;
    options.tls_client_certificate = tls_certificate;
    options.tls_client_private_key = tls_key;
    options.rpc_timeout_ms = rpc_timeout_ms;
    options.cleanup_timeout_ms = cleanup_timeout_ms;
    options.safety_margin_ms = safety_margin_ms;
    options.default_wait_ms = default_wait_ms;
    options.default_lease_ms = default_lease_ms;
    options.auto_renew = auto_renew;
    dilock_error error = {0};
    dilock_etcd_provider *value = NULL;
    const dilock_status status = dilock_etcd_provider_create(&options, &value, &error);
    copy_message(message, &error);
    *out = value;
    return (int)status;
}

void hs_dilock_etcd_provider_destroy(void *provider) {
    dilock_etcd_provider_destroy((dilock_etcd_provider *)provider);
}

int hs_dilock_etcd_lock_create(void *provider, const char *resource,
                               void **out, char *message) {
    dilock_error error = {0};
    dilock_etcd_lock *value = NULL;
    const dilock_status status = dilock_etcd_lock_create(
        (dilock_etcd_provider *)provider, resource, &value, &error);
    copy_message(message, &error);
    *out = value;
    return (int)status;
}

void hs_dilock_etcd_lock_destroy(void *lock) {
    dilock_etcd_lock_destroy((dilock_etcd_lock *)lock);
}

int hs_dilock_etcd_acquire(void *lock, int64_t wait_ms, int64_t lease_ms,
                           void *cancellation, void **out, char *message) {
    dilock_error error = {0};
    dilock_etcd_lease *value = NULL;
    const dilock_status status = dilock_etcd_acquire_with_cancellation(
        (dilock_etcd_lock *)lock, wait_ms, lease_ms,
        (dilock_cancellation *)cancellation, &value, &error);
    copy_message(message, &error);
    *out = value;
    return (int)status;
}

int hs_dilock_etcd_lease_valid(void *lease) {
    return dilock_etcd_lease_is_valid((const dilock_etcd_lease *)lease);
}

int hs_dilock_etcd_lease_ttl_ms(void *lease, int64_t *ttl_ms, char *message) {
    dilock_error error = {0};
    const dilock_status status = dilock_etcd_lease_effective_ttl_ms(
        (const dilock_etcd_lease *)lease, ttl_ms, &error);
    copy_message(message, &error);
    return (int)status;
}

int hs_dilock_etcd_lease_fence(void *lease, int64_t *revision, char *epoch,
                               size_t epoch_capacity, char *message) {
    dilock_error error = {0};
    dilock_etcd_fence fence = {0, 0u};
    const dilock_status status = dilock_etcd_lease_fence(
        (const dilock_etcd_lease *)lease, &fence, epoch, epoch_capacity, &error);
    copy_message(message, &error);
    *revision = fence.create_revision;
    return (int)status;
}

int hs_dilock_etcd_lease_release(void *lease, int *outcome, char *message) {
    dilock_error error = {0};
    dilock_release_outcome value = DILOCK_RELEASE_UNKNOWN;
    const dilock_status status = dilock_etcd_lease_release(
        (dilock_etcd_lease *)lease, &value, &error);
    copy_message(message, &error);
    *outcome = (int)value;
    return (int)status;
}

void hs_dilock_etcd_lease_destroy(void *lease) {
    dilock_etcd_lease_destroy((dilock_etcd_lease *)lease);
}

static void consul_options_init(dilock_consul_options *options,
    const char *endpoint, const char *name_space, const char *epoch, const char *token,
    int64_t rpc_timeout_ms, int64_t cleanup_timeout_ms, int64_t poll_interval_ms,
    int64_t default_wait_ms, int64_t default_lease_ms) {
    dilock_consul_options_init(options);
    options->endpoint = endpoint;
    options->name_space = name_space;
    options->coordination_epoch = epoch;
    options->acl_token = token;
    options->rpc_timeout_ms = rpc_timeout_ms;
    options->cleanup_timeout_ms = cleanup_timeout_ms;
    options->poll_interval_ms = poll_interval_ms;
    options->default_wait_ms = default_wait_ms;
    options->default_lease_ms = default_lease_ms;
}

int hs_dilock_consul_provider_create(
    const char *endpoint, const char *name_space, const char *epoch, const char *token,
    int64_t rpc_timeout_ms, int64_t cleanup_timeout_ms, int64_t poll_interval_ms,
    int64_t default_wait_ms, int64_t default_lease_ms, void **out, char *message) {
    dilock_consul_options options;
    consul_options_init(&options, endpoint, name_space, epoch, token, rpc_timeout_ms,
        cleanup_timeout_ms, poll_interval_ms, default_wait_ms, default_lease_ms);
    dilock_error error = {0};
    dilock_consul_provider *value = NULL;
    const dilock_status status = dilock_consul_provider_create(&options, &value, &error);
    copy_message(message, &error);
    *out = value;
    return (int)status;
}

int hs_dilock_consul_namespace_provision(
    const char *endpoint, const char *name_space, const char *epoch, const char *token,
    int64_t rpc_timeout_ms, int64_t cleanup_timeout_ms, int64_t poll_interval_ms,
    int64_t default_wait_ms, int64_t default_lease_ms, char *message) {
    dilock_consul_options options;
    consul_options_init(&options, endpoint, name_space, epoch, token, rpc_timeout_ms,
        cleanup_timeout_ms, poll_interval_ms, default_wait_ms, default_lease_ms);
    dilock_error error = {0};
    const dilock_status status = dilock_consul_provision_namespace(&options, &error);
    copy_message(message, &error);
    return (int)status;
}

void hs_dilock_consul_provider_destroy(void *provider) {
    dilock_consul_provider_destroy((dilock_consul_provider *)provider);
}

int hs_dilock_consul_lock_create(void *provider, const char *resource,
                                 void **out, char *message) {
    dilock_error error = {0};
    dilock_consul_lock *value = NULL;
    const dilock_status status = dilock_consul_lock_create(
        (dilock_consul_provider *)provider, resource, &value, &error);
    copy_message(message, &error);
    *out = value;
    return (int)status;
}

void hs_dilock_consul_lock_destroy(void *lock) {
    dilock_consul_lock_destroy((dilock_consul_lock *)lock);
}

int hs_dilock_consul_acquire(void *lock, int64_t wait_ms, int64_t lease_ms,
                             void *cancellation, void **out, char *message) {
    dilock_error error = {0};
    dilock_consul_lease *value = NULL;
    const dilock_status status = dilock_consul_acquire_with_cancellation(
        (dilock_consul_lock *)lock, wait_ms, lease_ms,
        (dilock_cancellation *)cancellation, &value, &error);
    copy_message(message, &error);
    *out = value;
    return (int)status;
}

int hs_dilock_consul_try_acquire(void *lock, int64_t lease_ms,
                                 void **out, char *message) {
    dilock_error error = {0};
    dilock_consul_lease *value = NULL;
    const dilock_status status = dilock_consul_try_acquire(
        (dilock_consul_lock *)lock, lease_ms, &value, &error);
    copy_message(message, &error);
    *out = value;
    return (int)status;
}

int hs_dilock_consul_lease_valid(void *lease) {
    return dilock_consul_lease_is_valid((const dilock_consul_lease *)lease);
}

int hs_dilock_consul_lease_ttl_ms(void *lease, int64_t *ttl_ms, char *message) {
    dilock_error error = {0};
    const dilock_status status = dilock_consul_lease_effective_ttl_ms(
        (const dilock_consul_lease *)lease, ttl_ms, &error);
    copy_message(message, &error);
    return (int)status;
}

int hs_dilock_consul_lease_fence(void *lease, uint64_t *lock_index, char *epoch,
                                 size_t epoch_capacity, char *message) {
    dilock_error error = {0};
    dilock_consul_fence fence = {0, 0u};
    const dilock_status status = dilock_consul_lease_fence(
        (const dilock_consul_lease *)lease, &fence, epoch, epoch_capacity, &error);
    copy_message(message, &error);
    *lock_index = fence.lock_index;
    return (int)status;
}

int hs_dilock_consul_lease_renew(void *lease, int *renewed, char *message) {
    dilock_error error = {0};
    int32_t value = 0;
    const dilock_status status = dilock_consul_lease_renew(
        (dilock_consul_lease *)lease, &value, &error);
    copy_message(message, &error);
    *renewed = (int)value;
    return (int)status;
}

int hs_dilock_consul_lease_release(void *lease, int *outcome, char *message) {
    dilock_error error = {0};
    dilock_release_outcome value = DILOCK_RELEASE_UNKNOWN;
    const dilock_status status = dilock_consul_lease_release(
        (dilock_consul_lease *)lease, &value, &error);
    copy_message(message, &error);
    *outcome = (int)value;
    return (int)status;
}

void hs_dilock_consul_lease_destroy(void *lease) {
    dilock_consul_lease_destroy((dilock_consul_lease *)lease);
}
