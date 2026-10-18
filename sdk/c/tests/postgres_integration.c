#include <dilock/dilock.h>

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int fail(const char *operation, dilock_status status, const dilock_error *error) {
    fprintf(stderr, "%s failed: status=%d message=%s\n", operation, (int)status,
        error == NULL ? "" : error->message);
    return 1;
}

int main(void) {
    const char *dsn = getenv("DILOCK_POSTGRES_DSN");
    if (dsn == NULL || dsn[0] == '\0') return 77;

    dilock_postgres_options options;
    dilock_postgres_options_init(&options);
    options.connection_string = dsn;
    options.name_space = "c-abi-postgres-live";
    options.coordination_epoch = "c-abi-postgres-live-epoch";
    options.default_wait_ms = 500;
    options.default_lease_ms = 30000;

    dilock_error error = {0};
    dilock_status status = dilock_postgres_provision_namespace(&options, &error);
    if (status != DILOCK_STATUS_OK) return fail("provision", status, &error);

    dilock_postgres_provider *first = NULL;
    dilock_postgres_provider *second = NULL;
    dilock_postgres_lock *first_lock = NULL;
    dilock_postgres_lock *second_lock = NULL;
    dilock_postgres_lock *other_lock = NULL;
    dilock_postgres_lease *first_lease = NULL;
    dilock_postgres_lease *second_lease = NULL;
    dilock_postgres_lease *other_lease = NULL;
    dilock_release_outcome outcome = DILOCK_RELEASE_UNKNOWN;
    int32_t renewed = 0;
    int result = 1;

    status = dilock_postgres_provider_create(&options, &first, &error);
    if (status != DILOCK_STATUS_OK) { result = fail("first provider", status, &error); goto cleanup; }
    status = dilock_postgres_provider_create(&options, &second, &error);
    if (status != DILOCK_STATUS_OK) { result = fail("second provider", status, &error); goto cleanup; }
    status = dilock_postgres_lock_create(first, "c-abi/shared", &first_lock, &error);
    if (status != DILOCK_STATUS_OK) { result = fail("first lock", status, &error); goto cleanup; }
    status = dilock_postgres_lock_create(second, "c-abi/shared", &second_lock, &error);
    if (status != DILOCK_STATUS_OK) { result = fail("second lock", status, &error); goto cleanup; }
    status = dilock_postgres_lock_create(second, "c-abi/other", &other_lock, &error);
    if (status != DILOCK_STATUS_OK) { result = fail("other lock", status, &error); goto cleanup; }

    status = dilock_postgres_acquire_default(first_lock, &first_lease, &error);
    if (status != DILOCK_STATUS_OK || first_lease == NULL) { result = fail("first acquire", status, &error); goto cleanup; }
    status = dilock_postgres_try_acquire(second_lock, &second_lease, &error);
    if (status != DILOCK_STATUS_BUSY || second_lease != NULL) { result = fail("contention", status, &error); goto cleanup; }
    status = dilock_postgres_acquire(other_lock, 500, 30000, &other_lease, &error);
    if (status != DILOCK_STATUS_OK || other_lease == NULL) { result = fail("different resource", status, &error); goto cleanup; }
    status = dilock_postgres_lease_renew(first_lease, &renewed, &error);
    if (status != DILOCK_STATUS_OK || renewed == 0) { result = fail("renew liveness", status, &error); goto cleanup; }
    status = dilock_postgres_lease_release(other_lease, &outcome, &error);
    if (status != DILOCK_STATUS_OK || outcome != DILOCK_RELEASED) { result = fail("other release", status, &error); goto cleanup; }
    status = dilock_postgres_lease_release(first_lease, &outcome, &error);
    if (status != DILOCK_STATUS_OK || outcome != DILOCK_RELEASED) { result = fail("first release", status, &error); goto cleanup; }
    status = dilock_postgres_acquire(second_lock, 500, 30000, &second_lease, &error);
    if (status != DILOCK_STATUS_OK || second_lease == NULL) { result = fail("handoff", status, &error); goto cleanup; }
    status = dilock_postgres_lease_release(second_lease, &outcome, &error);
    if (status != DILOCK_STATUS_OK || outcome != DILOCK_RELEASED) { result = fail("second release", status, &error); goto cleanup; }
    result = 0;

cleanup:
    if (other_lease != NULL) dilock_postgres_lease_destroy(other_lease);
    if (second_lease != NULL) dilock_postgres_lease_destroy(second_lease);
    if (first_lease != NULL) dilock_postgres_lease_destroy(first_lease);
    if (other_lock != NULL) dilock_postgres_lock_destroy(other_lock);
    if (second_lock != NULL) dilock_postgres_lock_destroy(second_lock);
    if (first_lock != NULL) dilock_postgres_lock_destroy(first_lock);
    if (second != NULL) dilock_postgres_provider_destroy(second);
    if (first != NULL) dilock_postgres_provider_destroy(first);
    return result;
}
