#include "dilock/dilock.h"

#include <string.h>

static char current_epoch[1501];
static int fake_provider, fake_lock, fake_lease;
static int fake_cancellation;
static int cancellation_requested;
static int next_redis_lock_status;
static int next_redis_lock_success_without_output;
static int next_redis_release_status;
static int next_redis_release_outcome = DILOCK_RELEASED;
static void success_error(dilock_error *error);
static dilock_status invalid_argument(dilock_error *error, const char *message);

void dilock_test_set_next_redis_lock_status(int status) {
    next_redis_lock_status = status;
}

void dilock_test_set_next_redis_lock_success_without_output(void) {
    next_redis_lock_success_without_output = 1;
}

void dilock_test_set_next_redis_release(int status, int outcome) {
    next_redis_release_status = status;
    next_redis_release_outcome = outcome;
}

dilock_cancellation *dilock_cancellation_create(dilock_error *error) {
    cancellation_requested = 0;
    success_error(error);
    return (dilock_cancellation *)&fake_cancellation;
}

void dilock_cancellation_request(dilock_cancellation *cancellation) {
    if (cancellation != NULL) cancellation_requested = 1;
}

void dilock_cancellation_destroy(dilock_cancellation *cancellation) { (void)cancellation; }

static dilock_status cancellable_status(dilock_cancellation *cancellation,
                                        dilock_error *error) {
    if (cancellation == NULL) return invalid_argument(error, "cancellation handle is null");
    if (cancellation_requested) {
        error->code = DILOCK_STATUS_CANCELLED;
        const char *message = "acquisition cancelled by test shim";
        size_t message_bytes = strlen(message);
        memcpy(error->message, message, message_bytes + 1);
        return DILOCK_STATUS_CANCELLED;
    }
    success_error(error);
    return DILOCK_STATUS_OK;
}

static void success_error(dilock_error *error) {
    error->code = DILOCK_STATUS_OK;
    error->message[0] = '\0';
}

static dilock_status invalid_argument(dilock_error *error, const char *message) {
    error->code = DILOCK_STATUS_INVALID_ARGUMENT;
    size_t message_bytes = strlen(message);
    if (message_bytes >= sizeof(error->message)) message_bytes = sizeof(error->message) - 1;
    memcpy(error->message, message, message_bytes);
    error->message[message_bytes] = '\0';
    return DILOCK_STATUS_INVALID_ARGUMENT;
}

void dilock_redis_options_init(dilock_redis_options *options) {
    memset(options, 0, sizeof(*options));
    options->port = 6379;
    options->rpc_timeout_ms = 2000;
    options->cleanup_timeout_ms = 2000;
    options->default_wait_ms = 5000;
    options->default_lease_ms = 30000;
}

dilock_status dilock_redis_provider_create(const dilock_redis_options *options,
                                           dilock_redis_provider **provider,
                                           dilock_error *error) {
    if (strcmp(options->host, "redis  ") != 0 || strcmp(options->name_space, "ns ") != 0 ||
        strcmp(options->coordination_epoch, "epoch   ") != 0) {
        *provider = NULL;
        return invalid_argument(error, "Redis options changed their exact input bytes");
    }
    *provider = (dilock_redis_provider *)&fake_provider;
    success_error(error);
    return DILOCK_STATUS_OK;
}

void dilock_redis_provider_destroy(dilock_redis_provider *provider) { (void)provider; }

dilock_status dilock_redis_lock_create(dilock_redis_provider *provider, const char *resource,
                                       dilock_redis_lock **lock, dilock_error *error) {
    (void)provider;
    if (next_redis_lock_status != 0) {
        int status = next_redis_lock_status;
        next_redis_lock_status = 0;
        *lock = NULL;
        error->code = status;
        strcpy(error->message, "test shim returned unknown status");
        return (dilock_status)status;
    }
    if (next_redis_lock_success_without_output) {
        next_redis_lock_success_without_output = 0;
        *lock = NULL;
        success_error(error);
        return DILOCK_STATUS_OK;
    }
    if (strcmp(resource, "job  ") != 0) {
        *lock = NULL;
        return invalid_argument(error, "Redis resource changed its exact input bytes");
    }
    *lock = (dilock_redis_lock *)&fake_lock;
    success_error(error);
    return DILOCK_STATUS_OK;
}

void dilock_redis_lock_destroy(dilock_redis_lock *lock) { (void)lock; }

dilock_status dilock_redis_acquire(dilock_redis_lock *lock, int64_t wait_ms,
                                   int64_t lease_ms, dilock_redis_lease **lease,
                                   dilock_error *error) {
    (void)lock; (void)wait_ms; (void)lease_ms;
    *lease = (dilock_redis_lease *)&fake_lease;
    success_error(error);
    return DILOCK_STATUS_OK;
}

dilock_status dilock_redis_acquire_default(dilock_redis_lock *lock,
                                           dilock_redis_lease **lease,
                                           dilock_error *error) {
    return dilock_redis_acquire(lock, 5000, 30000, lease, error);
}

dilock_status dilock_redis_try_acquire(dilock_redis_lock *lock, int64_t lease_ms,
                                       dilock_redis_lease **lease, dilock_error *error) {
    return dilock_redis_acquire(lock, 0, lease_ms, lease, error);
}

dilock_status dilock_redis_lease_release(dilock_redis_lease *lease,
                                         dilock_release_outcome *outcome,
                                         dilock_error *error) {
    (void)lease;
    int requested_outcome = next_redis_release_outcome;
    next_redis_release_outcome = DILOCK_RELEASED;
    if (next_redis_release_status != 0) {
        int status = next_redis_release_status;
        next_redis_release_status = 0;
        error->code = status;
        strcpy(error->message, "test shim returned unknown release status");
        return (dilock_status)status;
    }
    *outcome = (dilock_release_outcome)requested_outcome;
    success_error(error);
    return DILOCK_STATUS_OK;
}

void dilock_redis_lease_destroy(dilock_redis_lease *lease) { (void)lease; }

dilock_status dilock_redis_acquire_with_cancellation(dilock_redis_lock *lock,
    int64_t wait_ms, int64_t lease_ms, dilock_cancellation *cancellation,
    dilock_redis_lease **lease, dilock_error *error) {
    (void)lock; (void)wait_ms; (void)lease_ms;
    dilock_status status = cancellable_status(cancellation, error);
    *lease = status == DILOCK_STATUS_OK ? (dilock_redis_lease *)&fake_lease : NULL;
    return status;
}

dilock_status dilock_redis_acquire_default_with_cancellation(dilock_redis_lock *lock,
    dilock_cancellation *cancellation, dilock_redis_lease **lease, dilock_error *error) {
    (void)lock;
    dilock_status status = cancellable_status(cancellation, error);
    *lease = status == DILOCK_STATUS_OK ? (dilock_redis_lease *)&fake_lease : NULL;
    return status;
}

void dilock_etcd_options_init(dilock_etcd_options *options) {
    memset(options, 0, sizeof(*options));
    options->rpc_timeout_ms = 2000;
    options->cleanup_timeout_ms = 2000;
    options->default_wait_ms = 5000;
    options->default_lease_ms = 30000;
    options->auto_renew = 1;
}

dilock_status dilock_etcd_provider_create(const dilock_etcd_options *options,
                                          dilock_etcd_provider **provider,
                                          dilock_error *error) {
    if (strcmp(options->endpoint, "etcd:2379") != 0 ||
        strlen(options->coordination_epoch) != sizeof(current_epoch) - 1) {
        *provider = NULL;
        return invalid_argument(error, "etcd endpoint or epoch was not preserved");
    }
    strcpy(current_epoch, options->coordination_epoch);
    *provider = (dilock_etcd_provider *)&fake_provider;
    success_error(error);
    return DILOCK_STATUS_OK;
}

void dilock_etcd_provider_destroy(dilock_etcd_provider *provider) { (void)provider; }

dilock_status dilock_etcd_lock_create(dilock_etcd_provider *provider, const char *resource,
                                      dilock_etcd_lock **lock, dilock_error *error) {
    (void)provider; (void)resource;
    *lock = (dilock_etcd_lock *)&fake_lock;
    success_error(error);
    return DILOCK_STATUS_OK;
}

void dilock_etcd_lock_destroy(dilock_etcd_lock *lock) { (void)lock; }

dilock_status dilock_etcd_acquire(dilock_etcd_lock *lock, int64_t wait_ms,
                                  int64_t lease_ms, dilock_etcd_lease **lease,
                                  dilock_error *error) {
    (void)lock; (void)wait_ms; (void)lease_ms;
    *lease = (dilock_etcd_lease *)&fake_lease;
    success_error(error);
    return DILOCK_STATUS_OK;
}

dilock_status dilock_etcd_acquire_default(dilock_etcd_lock *lock,
                                          dilock_etcd_lease **lease,
                                          dilock_error *error) {
    return dilock_etcd_acquire(lock, 5000, 30000, lease, error);
}

dilock_status dilock_etcd_try_acquire(dilock_etcd_lock *lock, int64_t lease_ms,
                                      dilock_etcd_lease **lease, dilock_error *error) {
    (void)lock; (void)lease_ms;
    *lease = NULL;
    error->code = DILOCK_STATUS_UNSUPPORTED_CAPABILITY;
    const char *message = "etcd native Lock has no safe try-acquire operation";
    size_t message_bytes = strlen(message);
    if (message_bytes >= sizeof(error->message)) message_bytes = sizeof(error->message) - 1;
    memcpy(error->message, message, message_bytes);
    error->message[message_bytes] = '\0';
    return DILOCK_STATUS_UNSUPPORTED_CAPABILITY;
}

dilock_status dilock_etcd_lease_release(dilock_etcd_lease *lease,
                                        dilock_release_outcome *outcome,
                                        dilock_error *error) {
    (void)lease; *outcome = DILOCK_RELEASED; success_error(error); return DILOCK_STATUS_OK;
}

dilock_status dilock_etcd_lease_fence(const dilock_etcd_lease *lease, dilock_etcd_fence *fence,
                                      char *epoch, size_t epoch_capacity, dilock_error *error) {
    (void)lease;
    size_t epoch_bytes = strlen(current_epoch);
    if (epoch_capacity < epoch_bytes + 1) {
        return invalid_argument(error, "epoch output capacity too small");
    }
    memcpy(epoch, current_epoch, epoch_bytes + 1);
    fence->create_revision = 42;
    fence->coordination_epoch_bytes = epoch_bytes;
    success_error(error);
    return DILOCK_STATUS_OK;
}

void dilock_etcd_lease_destroy(dilock_etcd_lease *lease) { (void)lease; }

dilock_status dilock_etcd_acquire_with_cancellation(dilock_etcd_lock *lock,
    int64_t wait_ms, int64_t lease_ms, dilock_cancellation *cancellation,
    dilock_etcd_lease **lease, dilock_error *error) {
    (void)lock; (void)wait_ms; (void)lease_ms;
    dilock_status status = cancellable_status(cancellation, error);
    *lease = status == DILOCK_STATUS_OK ? (dilock_etcd_lease *)&fake_lease : NULL;
    return status;
}

dilock_status dilock_etcd_acquire_default_with_cancellation(dilock_etcd_lock *lock,
    dilock_cancellation *cancellation, dilock_etcd_lease **lease, dilock_error *error) {
    (void)lock;
    dilock_status status = cancellable_status(cancellation, error);
    *lease = status == DILOCK_STATUS_OK ? (dilock_etcd_lease *)&fake_lease : NULL;
    return status;
}

void dilock_consul_options_init(dilock_consul_options *options) {
    memset(options, 0, sizeof(*options));
    options->rpc_timeout_ms = 2000;
    options->cleanup_timeout_ms = 2000;
    options->poll_interval_ms = 50;
    options->default_wait_ms = 5000;
    options->default_lease_ms = 30000;
}

dilock_status dilock_consul_provision_namespace(const dilock_consul_options *options,
                                                 dilock_error *error) {
    if (strcmp(options->endpoint, "consul:8500") != 0 ||
        strcmp(options->name_space, "billing") != 0 ||
        strcmp(options->coordination_epoch, "consul-epoch") != 0 ||
        strcmp(options->acl_token, "test-token") != 0) {
        return invalid_argument(error, "Consul provisioning inputs changed");
    }
    success_error(error);
    return DILOCK_STATUS_OK;
}

dilock_status dilock_consul_provider_create(const dilock_consul_options *options,
                                            dilock_consul_provider **provider,
                                            dilock_error *error) {
    if (strcmp(options->endpoint, "consul:8500") != 0 ||
        strcmp(options->name_space, "billing") != 0 ||
        strcmp(options->coordination_epoch, "consul-epoch") != 0 ||
        strcmp(options->acl_token, "test-token") != 0) {
        *provider = NULL;
        return invalid_argument(error, "Consul options changed their exact input bytes");
    }
    strcpy(current_epoch, options->coordination_epoch);
    *provider = (dilock_consul_provider *)&fake_provider;
    success_error(error);
    return DILOCK_STATUS_OK;
}

void dilock_consul_provider_destroy(dilock_consul_provider *provider) { (void)provider; }

dilock_status dilock_consul_lock_create(dilock_consul_provider *provider, const char *resource,
                                        dilock_consul_lock **lock, dilock_error *error) {
    (void)provider;
    if (strcmp(resource, "invoice/42") != 0) {
        *lock = NULL;
        return invalid_argument(error, "Consul resource changed its exact input bytes");
    }
    *lock = (dilock_consul_lock *)&fake_lock;
    success_error(error);
    return DILOCK_STATUS_OK;
}

void dilock_consul_lock_destroy(dilock_consul_lock *lock) { (void)lock; }

dilock_status dilock_consul_acquire(dilock_consul_lock *lock, int64_t wait_ms,
                                    int64_t lease_ms, dilock_consul_lease **lease,
                                    dilock_error *error) {
    (void)lock; (void)wait_ms; (void)lease_ms;
    *lease = (dilock_consul_lease *)&fake_lease;
    success_error(error);
    return DILOCK_STATUS_OK;
}

dilock_status dilock_consul_acquire_default(dilock_consul_lock *lock,
                                            dilock_consul_lease **lease,
                                            dilock_error *error) {
    return dilock_consul_acquire(lock, 5000, 30000, lease, error);
}

dilock_status dilock_consul_try_acquire(dilock_consul_lock *lock, int64_t lease_ms,
                                        dilock_consul_lease **lease, dilock_error *error) {
    return dilock_consul_acquire(lock, 0, lease_ms, lease, error);
}

dilock_status dilock_consul_acquire_with_cancellation(dilock_consul_lock *lock,
    int64_t wait_ms, int64_t lease_ms, dilock_cancellation *cancellation,
    dilock_consul_lease **lease, dilock_error *error) {
    (void)lock; (void)wait_ms; (void)lease_ms;
    dilock_status status = cancellable_status(cancellation, error);
    *lease = status == DILOCK_STATUS_OK ? (dilock_consul_lease *)&fake_lease : NULL;
    return status;
}

dilock_status dilock_consul_acquire_default_with_cancellation(dilock_consul_lock *lock,
    dilock_cancellation *cancellation, dilock_consul_lease **lease, dilock_error *error) {
    (void)lock;
    dilock_status status = cancellable_status(cancellation, error);
    *lease = status == DILOCK_STATUS_OK ? (dilock_consul_lease *)&fake_lease : NULL;
    return status;
}

dilock_status dilock_consul_lease_release(dilock_consul_lease *lease,
                                         dilock_release_outcome *outcome,
                                         dilock_error *error) {
    (void)lease;
    *outcome = DILOCK_RELEASED;
    success_error(error);
    return DILOCK_STATUS_OK;
}

dilock_status dilock_consul_lease_fence(const dilock_consul_lease *lease,
    dilock_consul_fence *fence, char *epoch, size_t epoch_capacity, dilock_error *error) {
    (void)lease;
    size_t epoch_bytes = strlen(current_epoch);
    fence->lock_index = 42;
    fence->coordination_epoch_bytes = epoch_bytes;
    if (epoch_capacity < epoch_bytes + 1)
        return invalid_argument(error, "Consul epoch output capacity too small");
    memcpy(epoch, current_epoch, epoch_bytes + 1);
    success_error(error);
    return DILOCK_STATUS_OK;
}

dilock_status dilock_consul_lease_renew(dilock_consul_lease *lease, int32_t *renewed,
                                        dilock_error *error) {
    (void)lease;
    *renewed = 1;
    success_error(error);
    return DILOCK_STATUS_OK;
}

dilock_status dilock_consul_lease_effective_ttl_ms(const dilock_consul_lease *lease,
    int64_t *ttl_ms, dilock_error *error) {
    (void)lease;
    *ttl_ms = 30000;
    success_error(error);
    return DILOCK_STATUS_OK;
}

void dilock_consul_lease_destroy(dilock_consul_lease *lease) { (void)lease; }
