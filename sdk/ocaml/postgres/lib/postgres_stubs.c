#include <dilock/dilock.h>

#include <string.h>

/* Keep this translation unit in native executables. The OCaml adapter uses
 * ctypes.foreign, so the remaining wrapper symbols are resolved by dlsym at
 * runtime rather than by a normal static reference. */
void dilock_ocaml_postgres_keep_stubs(void) {}

static void copy_message(char *destination, const dilock_error *error) {
    if (destination == NULL) return;
    if (error == NULL) { destination[0] = '\0'; return; }
    memcpy(destination, error->message, DILOCK_C_ERROR_MESSAGE_CAPACITY);
    destination[DILOCK_C_ERROR_MESSAGE_CAPACITY - 1u] = '\0';
}

static void postgres_options_init(dilock_postgres_options *options,
    const char *connection_string, const char *name_space, const char *epoch,
    int64_t rpc_timeout_ms, int64_t cleanup_timeout_ms, int64_t poll_interval_ms,
    int64_t default_wait_ms, int64_t default_lease_ms) {
    dilock_postgres_options_init(options);
    options->connection_string = connection_string;
    options->name_space = name_space;
    options->coordination_epoch = epoch;
    options->rpc_timeout_ms = rpc_timeout_ms;
    options->cleanup_timeout_ms = cleanup_timeout_ms;
    options->poll_interval_ms = poll_interval_ms;
    options->default_wait_ms = default_wait_ms;
    options->default_lease_ms = default_lease_ms;
}

int dilock_ocaml_postgres_provision(const char *connection_string, const char *name_space,
    const char *epoch, int64_t rpc_timeout_ms, int64_t cleanup_timeout_ms,
    int64_t poll_interval_ms, int64_t default_wait_ms, int64_t default_lease_ms,
    char *message) {
    dilock_postgres_options options;
    postgres_options_init(&options, connection_string, name_space, epoch, rpc_timeout_ms,
        cleanup_timeout_ms, poll_interval_ms, default_wait_ms, default_lease_ms);
    dilock_error error = {0};
    const dilock_status status = dilock_postgres_provision_namespace(&options, &error);
    copy_message(message, &error);
    return (int)status;
}

int dilock_ocaml_postgres_provider_create(const char *connection_string, const char *name_space,
    const char *epoch, int64_t rpc_timeout_ms, int64_t cleanup_timeout_ms,
    int64_t poll_interval_ms, int64_t default_wait_ms, int64_t default_lease_ms,
    void **out, char *message) {
    dilock_postgres_options options;
    postgres_options_init(&options, connection_string, name_space, epoch, rpc_timeout_ms,
        cleanup_timeout_ms, poll_interval_ms, default_wait_ms, default_lease_ms);
    dilock_error error = {0};
    dilock_postgres_provider *value = NULL;
    const dilock_status status = dilock_postgres_provider_create(&options, &value, &error);
    copy_message(message, &error); *out = value; return (int)status;
}

void dilock_ocaml_postgres_provider_destroy(void *provider) { dilock_postgres_provider_destroy(provider); }

int dilock_ocaml_postgres_lock_create(void *provider, const char *resource,
    void **out, char *message) {
    dilock_error error = {0}; dilock_postgres_lock *value = NULL;
    const dilock_status status = dilock_postgres_lock_create(provider, resource, &value, &error);
    copy_message(message, &error); *out = value; return (int)status;
}
void dilock_ocaml_postgres_lock_destroy(void *lock) { dilock_postgres_lock_destroy(lock); }

int dilock_ocaml_postgres_acquire(void *lock, int64_t wait_ms, int64_t lease_ms,
    void **out, char *message) {
    dilock_error error = {0}; dilock_postgres_lease *value = NULL;
    const dilock_status status = dilock_postgres_acquire(lock, wait_ms, lease_ms, &value, &error);
    copy_message(message, &error); *out = value; return (int)status;
}
int dilock_ocaml_postgres_try_acquire(void *lock, void **out, char *message) {
    dilock_error error = {0}; dilock_postgres_lease *value = NULL;
    const dilock_status status = dilock_postgres_try_acquire(lock, &value, &error);
    copy_message(message, &error); *out = value; return (int)status;
}
int dilock_ocaml_postgres_lease_valid(void *lease) { return dilock_postgres_lease_is_valid(lease); }
int dilock_ocaml_postgres_lease_renew(void *lease, int *renewed, char *message) {
    dilock_error error = {0}; int32_t value = 0;
    const dilock_status status = dilock_postgres_lease_renew(lease, &value, &error);
    copy_message(message, &error); *renewed = (int)value; return (int)status;
}
int dilock_ocaml_postgres_lease_release(void *lease, int *outcome, char *message) {
    dilock_error error = {0}; dilock_release_outcome value = DILOCK_RELEASE_UNKNOWN;
    const dilock_status status = dilock_postgres_lease_release(lease, &value, &error);
    copy_message(message, &error); *outcome = (int)value; return (int)status;
}
void dilock_ocaml_postgres_lease_destroy(void *lease) { dilock_postgres_lease_destroy(lease); }
