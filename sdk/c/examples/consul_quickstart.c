#include <dilock/dilock.h>

#include <stdio.h>
#include <stdlib.h>

static const char *environment(const char *name, const char *fallback) {
    const char *value = getenv(name);
    return value == NULL || value[0] == '\0' ? fallback : value;
}

int main(int argc, char **argv) {
    (void)argv;
    dilock_error error = {0};
    dilock_consul_options options;
    dilock_consul_options_init(&options);
    options.endpoint = environment("DILOCK_CONSUL_ENDPOINT", options.endpoint);
    options.name_space = environment("DILOCK_NAMESPACE", "default");
    options.acl_token = environment("DILOCK_CONSUL_TOKEN", options.acl_token);
    options.coordination_epoch = environment("DILOCK_EPOCH", "local-dev-1");
    if (argc > 1) {
        options.default_wait_ms = 10000;
        options.default_lease_ms = 60000;
        options.rpc_timeout_ms = 1000;
        options.cleanup_timeout_ms = 1000;
    }

    dilock_consul_provider *provider = NULL;
    dilock_consul_lock *lock = NULL;
    dilock_consul_lease *lease = NULL;
    dilock_consul_fence fence = {0};
    dilock_release_outcome outcome = DILOCK_RELEASE_UNKNOWN;
    char epoch[256];
    dilock_status status = dilock_consul_provider_create(&options, &provider, &error);
    if (status != DILOCK_STATUS_OK) goto failed;
    status = dilock_consul_lock_create(provider, "billing/monthly-close", &lock, &error);
    if (status != DILOCK_STATUS_OK) goto failed;
    status = dilock_consul_acquire_default(lock, &lease, &error);
    if (status != DILOCK_STATUS_OK) goto failed;
    status = dilock_consul_lease_fence(lease, &fence, epoch, sizeof(epoch), &error);
    if (status != DILOCK_STATUS_OK) goto failed;
    printf("Short protected operation; Consul LockIndex=%llu, epoch=%s\n",
        (unsigned long long)fence.lock_index, epoch);
    status = dilock_consul_lease_release(lease, &outcome, &error);
    if (status != DILOCK_STATUS_OK || outcome != DILOCK_RELEASED) goto failed;

    dilock_consul_lease_destroy(lease);
    dilock_consul_lock_destroy(lock);
    dilock_consul_provider_destroy(provider);
    return 0;

failed:
    fprintf(stderr, "Consul operation failed (status=%d): %s\n", (int)status, error.message);
    dilock_consul_lease_destroy(lease);
    dilock_consul_lock_destroy(lock);
    dilock_consul_provider_destroy(provider);
    return 1;
}
