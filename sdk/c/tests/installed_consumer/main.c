#include <dilock/dilock.h>

#include <stdio.h>
#include <threads.h>
#include <time.h>

static int report_failure(const char *step, dilock_status status,
                          const dilock_error *error) {
    fprintf(stderr, "%s: status=%d code=%d message=%s\n", step, (int)status,
            (int)error->code, error->message);
    return 1;
}

int main(void) {
    dilock_error error = {0};
    dilock_redis_options options;
    dilock_redis_options_init(&options);
    options.coordination_epoch = "installed-consumer-epoch";
    options.rpc_timeout_ms = 100;
    options.safety_margin_ms = 10;

    dilock_status status = dilock_redis_provision_namespace(&options, &error);
    if (status != DILOCK_STATUS_OK) return report_failure("provision", status, &error);

    dilock_redis_provider *first = NULL;
    dilock_redis_provider *second = NULL;
    dilock_redis_lock *first_lock = NULL;
    dilock_redis_lock *second_lock = NULL;
    dilock_redis_lease *first_lease = NULL;
    dilock_redis_lease *second_lease = NULL;
    dilock_redis_lease *probe_lease = NULL;
    dilock_release_outcome outcome = DILOCK_RELEASE_UNKNOWN;

    status = dilock_redis_provider_create(&options, &first, &error);
    if (status != DILOCK_STATUS_OK) return report_failure("provider one", status, &error);
    status = dilock_redis_provider_create(&options, &second, &error);
    if (status != DILOCK_STATUS_OK) return report_failure("provider two", status, &error);
    status = dilock_redis_lock_create(first, "installed-consumer/resource",
                                      &first_lock, &error);
    if (status != DILOCK_STATUS_OK) return report_failure("lock one", status, &error);
    status = dilock_redis_lock_create(second, "installed-consumer/resource",
                                      &second_lock, &error);
    if (status != DILOCK_STATUS_OK) return report_failure("lock two", status, &error);

    status = dilock_redis_try_acquire(first_lock, 1000, &first_lease, &error);
    if (status != DILOCK_STATUS_OK) return report_failure("acquire one", status, &error);
    status = dilock_redis_try_acquire(second_lock, 30000, &second_lease, &error);
    if (status != DILOCK_STATUS_BUSY || second_lease != NULL)
        return report_failure("busy from provider two", status, &error);

    const struct timespec lease_expiry = {1, 200000000};
    thrd_sleep(&lease_expiry, NULL);
    status = dilock_redis_try_acquire(second_lock, 30000, &second_lease, &error);
    if (status != DILOCK_STATUS_OK) return report_failure("acquire successor", status, &error);

    status = dilock_redis_lease_release(first_lease, &outcome, &error);
    if (status != DILOCK_STATUS_OK || outcome != DILOCK_ALREADY_NOT_OWNED)
        return report_failure("late release", status, &error);
    status = dilock_redis_try_acquire(first_lock, 30000, &probe_lease, &error);
    if (status != DILOCK_STATUS_BUSY || probe_lease != NULL)
        return report_failure("successor remains protected", status, &error);

    status = dilock_redis_lease_release(second_lease, &outcome, &error);
    if (status != DILOCK_STATUS_OK || outcome != DILOCK_RELEASED)
        return report_failure("release successor", status, &error);

    dilock_redis_lease_destroy(first_lease);
    dilock_redis_lease_destroy(second_lease);
    dilock_redis_lock_destroy(first_lock);
    dilock_redis_lock_destroy(second_lock);
    dilock_redis_provider_destroy(first);
    dilock_redis_provider_destroy(second);
    puts("installed C17 consumer passed cross-provider contention, expiry and stale-release checks");
    return 0;
}
