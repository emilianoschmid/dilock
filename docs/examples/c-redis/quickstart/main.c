#include <dilock/dilock.h>

#include <stdio.h>
#include <stdlib.h>

static int report_error(const char *operation, dilock_status status, const dilock_error *error) {
    fprintf(stderr, "%s failed (%d): %s\n", operation, (int)status,
        error != NULL ? error->message : "no diagnostic");
    return 1;
}

int main(int argc, char **unused_argv) {
    dilock_error error = {0};
    dilock_redis_options options;
    dilock_redis_provider *provider = NULL;
    dilock_redis_lock *lock = NULL;
    dilock_redis_lease *lease = NULL;
    dilock_release_outcome outcome = DILOCK_RELEASE_UNKNOWN;
    const char *epoch = getenv("DILOCK_EPOCH");
    dilock_status status;
    int configured = argc > 1;
    const char *host = getenv("DILOCK_REDIS_HOST");
    (void)unused_argv;

    dilock_redis_options_init(&options);
    if (host != NULL) options.host = host;
    options.coordination_epoch = epoch != NULL ? epoch : "local-dev-1";
    if (configured) {
        options.name_space = "billing";
        options.port = 6379;
        options.database = 0;
        options.rpc_timeout_ms = 1000;
        options.cleanup_timeout_ms = 1000;
        options.safety_margin_ms = 50;
        options.poll_interval_ms = 25;
        options.default_wait_ms = 10000;
        options.default_lease_ms = 60000;
    }
    status = dilock_redis_provider_create(&options, &provider, &error);
    if (status != DILOCK_STATUS_OK) return report_error("provider creation", status, &error);
    status = dilock_redis_lock_create(provider, "quickstart/report", &lock, &error);
    if (status != DILOCK_STATUS_OK) { dilock_redis_provider_destroy(provider); return report_error("lock creation", status, &error); }

    status = configured
        ? dilock_redis_acquire(lock, options.default_wait_ms, options.default_lease_ms, &lease, &error)
        : dilock_redis_acquire_default(lock, &lease, &error);
    if (status != DILOCK_STATUS_OK) {
        dilock_redis_lock_destroy(lock);
        dilock_redis_provider_destroy(provider);
        return report_error("acquisition", status, &error);
    }
    puts("Short protected Redis operation completed.");
    status = dilock_redis_lease_release(lease, &outcome, &error);
    if (status != DILOCK_STATUS_OK) {
        dilock_redis_lease_destroy(lease);
        dilock_redis_lock_destroy(lock);
        dilock_redis_provider_destroy(provider);
        return report_error("release acknowledgement", status, &error);
    }
    if (outcome != DILOCK_RELEASED) {
        if (outcome == DILOCK_ALREADY_NOT_OWNED) {
            fputs("Redis lease is no longer owned; protected work may be stale.\n", stderr);
        } else {
            fputs("Redis release was not confirmed.\n", stderr);
        }
        dilock_redis_lease_destroy(lease);
        dilock_redis_lock_destroy(lock);
        dilock_redis_provider_destroy(provider);
        return 1;
    }
    dilock_redis_lease_destroy(lease);
    dilock_redis_lock_destroy(lock);
    dilock_redis_provider_destroy(provider);
    return 0;
}
