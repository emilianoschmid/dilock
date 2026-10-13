#include "dilock_style.h"
#include <stdlib.h>
#include <string.h>

struct dilock_style_provider {
    char *resource;
    struct dilock_style_lease *owner;
};
struct dilock_style_lease {
    dilock_style_provider *provider;
    int closed;
};

dilock_style_status dilock_style_provider_create(dilock_style_provider **out) {
    if (!out) return DILOCK_STYLE_INVALID_ARGUMENT;
    *out = calloc(1, sizeof **out);
    return *out ? DILOCK_STYLE_OK : DILOCK_STYLE_OUT_OF_MEMORY;
}
void dilock_style_provider_destroy(dilock_style_provider *provider) {
    if (!provider) return;
    free(provider->resource);
    free(provider);
}
dilock_style_status dilock_style_try_acquire(dilock_style_provider *provider,
                                            const char *resource, dilock_style_lease **out) {
    if (!out) return DILOCK_STYLE_INVALID_ARGUMENT;
    *out = NULL;
    if (!provider || !resource || !*resource) return DILOCK_STYLE_INVALID_ARGUMENT;
    /* Deliberately one resource per fixture; never silently alias different names. */
    if (provider->resource && strcmp(provider->resource, resource) != 0)
        return DILOCK_STYLE_INVALID_ARGUMENT;
    if (provider->owner) return DILOCK_STYLE_BUSY;
    if (!provider->resource) {
        size_t len = strlen(resource);
        provider->resource = malloc(len + 1);
        if (!provider->resource) return DILOCK_STYLE_OUT_OF_MEMORY;
        memcpy(provider->resource, resource, len + 1);
    }
    dilock_style_lease *lease = calloc(1, sizeof *lease);
    if (!lease) return DILOCK_STYLE_OUT_OF_MEMORY;
    lease->provider = provider;
    provider->owner = lease; /* Identity retained until caller destroys the lease. */
    *out = lease;
    return DILOCK_STYLE_OK;
}
dilock_style_status dilock_style_release(dilock_style_lease *lease) {
    if (!lease) return DILOCK_STYLE_INVALID_ARGUMENT;
    if (lease->closed) return DILOCK_STYLE_ALREADY_NOT_OWNED;
    lease->closed = 1;
    if (lease->provider->owner != lease) return DILOCK_STYLE_ALREADY_NOT_OWNED;
    lease->provider->owner = NULL;
    return DILOCK_STYLE_OK;
}
void dilock_style_lease_destroy(dilock_style_lease *lease) {
    if (!lease) return;
    (void)dilock_style_release(lease);
    free(lease);
}
void dilock_style_expire_for_test(dilock_style_provider *provider) {
    if (provider) provider->owner = NULL;
}
