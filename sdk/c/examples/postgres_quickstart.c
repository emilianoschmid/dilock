#include "dilock/dilock.h"

#include <stdlib.h>

int main(void) {
    const char *dsn = getenv("DILOCK_POSTGRES_CONNECTION_STRING");
    const char *epoch = getenv("DILOCK_COORDINATION_EPOCH");
    if (dsn == NULL || epoch == NULL) return 77;
    dilock_postgres_options options;
    dilock_postgres_options_init(&options);
    options.connection_string = dsn;
    options.coordination_epoch = epoch;
    dilock_error error = {0};
    dilock_postgres_provider *provider = NULL;
    if (dilock_postgres_provider_create(&options, &provider, &error) != DILOCK_STATUS_OK) return 1;
    dilock_postgres_lock *lock = NULL;
    if (dilock_postgres_lock_create(provider, "example/resource", &lock, &error) != DILOCK_STATUS_OK) {
        dilock_postgres_provider_destroy(provider);
        return 1;
    }
    dilock_postgres_lease *lease = NULL;
    const dilock_status status = dilock_postgres_try_acquire(lock, &lease, &error);
    if (status == DILOCK_STATUS_OK) {
        dilock_release_outcome outcome = DILOCK_RELEASE_UNKNOWN;
        if (dilock_postgres_lease_release(lease, &outcome, &error) != DILOCK_STATUS_OK || outcome != DILOCK_RELEASED) return 1;
    } else if (status != DILOCK_STATUS_BUSY) return 1;
    dilock_postgres_lease_destroy(lease);
    dilock_postgres_lock_destroy(lock);
    dilock_postgres_provider_destroy(provider);
    return 0;
}
