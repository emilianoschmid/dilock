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
    dilock_etcd_options options;
    dilock_etcd_provider *provider = NULL;
    dilock_etcd_lock *lock = NULL;
    dilock_etcd_lease *lease = NULL;
    dilock_release_outcome outcome = DILOCK_RELEASE_UNKNOWN;
    const char *endpoint = getenv("DILOCK_ETCD_ENDPOINT");
    const char *epoch = getenv("DILOCK_EPOCH");
    dilock_status status;
    int configured = argc > 1;
    (void)unused_argv;

    dilock_etcd_options_init(&options);
    if (endpoint != NULL) options.endpoint = endpoint;
    options.coordination_epoch = epoch != NULL ? epoch : "local-dev-1";
    if (configured) {
        options.name_space = "billing";
        options.rpc_timeout_ms = 1000;
        options.cleanup_timeout_ms = 1000;
        options.safety_margin_ms = 50;
        options.default_wait_ms = 10000;
        options.default_lease_ms = 60000;
        options.auto_renew = 1;
    }
    status = dilock_etcd_provider_create(&options, &provider, &error);
    if (status != DILOCK_STATUS_OK) return report_error("provider creation", status, &error);
    status = dilock_etcd_lock_create(provider, "quickstart/report", &lock, &error);
    if (status != DILOCK_STATUS_OK) { dilock_etcd_provider_destroy(provider); return report_error("lock creation", status, &error); }

    status = configured
        ? dilock_etcd_acquire(lock, options.default_wait_ms, options.default_lease_ms, &lease, &error)
        : dilock_etcd_acquire_default(lock, &lease, &error);
    if (status != DILOCK_STATUS_OK) {
        dilock_etcd_lock_destroy(lock);
        dilock_etcd_provider_destroy(provider);
        return report_error("acquisition", status, &error);
    }
    puts("Short protected etcd operation completed.");
    status = dilock_etcd_lease_release(lease, &outcome, &error);
    if (status != DILOCK_STATUS_OK) {
        dilock_etcd_lease_destroy(lease);
        dilock_etcd_lock_destroy(lock);
        dilock_etcd_provider_destroy(provider);
        return report_error("release acknowledgement", status, &error);
    }
    if (outcome != DILOCK_RELEASED) {
        if (outcome == DILOCK_ALREADY_NOT_OWNED) {
            fputs("etcd lease is no longer owned; protected work may be stale.\n", stderr);
        } else {
            fputs("etcd release was not confirmed.\n", stderr);
        }
        dilock_etcd_lease_destroy(lease);
        dilock_etcd_lock_destroy(lock);
        dilock_etcd_provider_destroy(provider);
        return 1;
    }
    dilock_etcd_lease_destroy(lease);
    dilock_etcd_lock_destroy(lock);
    dilock_etcd_provider_destroy(provider);
    return 0;
}
