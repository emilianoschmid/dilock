#include <dilock/dilock.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <threads.h>
#include <time.h>

static int report_failure(const char *step, dilock_status status,
                          const dilock_error *error) {
    fprintf(stderr, "%s: status=%d code=%d message=%s\n", step, (int)status,
            (int)error->code, error->message);
    return 1;
}

int main(void) {
    const char *endpoint = getenv("DILOCK_ETCD_ENDPOINT");
    if (endpoint == NULL || *endpoint == '\0') {
        puts("SKIP: set DILOCK_ETCD_ENDPOINT to an explicitly disposable etcd instance");
        return 77;
    }

    dilock_error error = {0};
    dilock_etcd_options options;
    dilock_etcd_options_init(&options);
    options.endpoint = endpoint;
    options.name_space = "installed-consumer-etcd";
    options.coordination_epoch = "installed-consumer-etcd-epoch";
    options.rpc_timeout_ms = 500;
    options.cleanup_timeout_ms = 1000;
    options.safety_margin_ms = 25;
    options.default_wait_ms = 1000;
    options.default_lease_ms = 3000;
    options.auto_renew = 1;

    dilock_etcd_provider *first = NULL;
    dilock_etcd_provider *second = NULL;
    dilock_etcd_lock *first_lock = NULL;
    dilock_etcd_lock *second_lock = NULL;
    dilock_etcd_lease *first_lease = NULL;
    dilock_etcd_lease *second_lease = NULL;
    dilock_release_outcome outcome = DILOCK_RELEASE_UNKNOWN;
    dilock_status status = dilock_etcd_provider_create(&options, &first, &error);
    if (status != DILOCK_STATUS_OK) return report_failure("provider one", status, &error);
    status = dilock_etcd_provider_create(&options, &second, &error);
    if (status != DILOCK_STATUS_OK) return report_failure("provider two", status, &error);
    status = dilock_etcd_lock_create(first, "installed-consumer/resource",
                                     &first_lock, &error);
    if (status != DILOCK_STATUS_OK) return report_failure("lock one", status, &error);
    status = dilock_etcd_lock_create(second, "installed-consumer/resource",
                                     &second_lock, &error);
    if (status != DILOCK_STATUS_OK) return report_failure("lock two", status, &error);

    status = dilock_etcd_acquire(first_lock, 2000, 3000, &first_lease, &error);
    if (status != DILOCK_STATUS_OK) return report_failure("acquire owner", status, &error);
    int64_t effective_ttl_ms = 0;
    status = dilock_etcd_lease_effective_ttl_ms(first_lease, &effective_ttl_ms,
                                                &error);
    if (status != DILOCK_STATUS_OK)
        return report_failure("effective TTL", status, &error);
    if (effective_ttl_ms != 3000) {
        fprintf(stderr, "effective TTL mismatch: requested=3000 effective=%lld\n",
                (long long)effective_ttl_ms);
        return 1;
    }
    dilock_etcd_fence first_fence = {0};
    char first_epoch[96] = {0};
    status = dilock_etcd_lease_fence(first_lease, &first_fence, NULL, 0,
                                     &error);
    if (status != DILOCK_STATUS_INVALID_ARGUMENT ||
        error.code != DILOCK_STATUS_INVALID_ARGUMENT ||
        first_fence.create_revision <= 0 ||
        first_fence.coordination_epoch_bytes != strlen(options.coordination_epoch)) {
        fprintf(stderr, "fence sizing probe failed: status=%d code=%d revision=%lld epoch_bytes=%zu\n",
                (int)status, (int)error.code,
                (long long)first_fence.create_revision,
                first_fence.coordination_epoch_bytes);
        return 1;
    }
    status = dilock_etcd_lease_fence(first_lease, &first_fence, first_epoch,
                                     sizeof(first_epoch), &error);
    if (status != DILOCK_STATUS_OK) return report_failure("owner fence", status, &error);
    if (first_fence.create_revision <= 0 ||
        strcmp(first_epoch, options.coordination_epoch) != 0) {
        fprintf(stderr, "owner fence invalid: revision=%lld epoch=%s\n",
                (long long)first_fence.create_revision, first_epoch);
        return 1;
    }

    status = dilock_etcd_acquire(second_lock, 100, 3000, &second_lease, &error);
    if (status != DILOCK_STATUS_WAIT_TIMEOUT || second_lease != NULL) {
        return report_failure("contender did not time out", status, &error);
    }
    status = dilock_etcd_lease_release(first_lease, &outcome, &error);
    if (status != DILOCK_STATUS_OK || outcome != DILOCK_RELEASED)
        return report_failure("release owner", status, &error);

    status = dilock_etcd_acquire(second_lock, 2000, 3000, &second_lease, &error);
    if (status != DILOCK_STATUS_OK) return report_failure("acquire successor", status, &error);
    dilock_etcd_fence second_fence = {0};
    char second_epoch[96] = {0};
    status = dilock_etcd_lease_fence(second_lease, &second_fence, second_epoch,
                                     sizeof(second_epoch), &error);
    if (status != DILOCK_STATUS_OK) return report_failure("successor fence", status, &error);
    if (second_fence.create_revision <= first_fence.create_revision ||
        strcmp(second_epoch, options.coordination_epoch) != 0) {
        fprintf(stderr, "successor fence did not advance: first=%lld second=%lld epoch=%s\n",
                (long long)first_fence.create_revision,
                (long long)second_fence.create_revision, second_epoch);
        return 1;
    }
    status = dilock_etcd_lease_release(second_lease, &outcome, &error);
    if (status != DILOCK_STATUS_OK || outcome != DILOCK_RELEASED)
        return report_failure("release successor", status, &error);

    /* Exercise the C ABI result after etcd expires the exact lock key. */
    dilock_etcd_options expiry_options = options;
    expiry_options.auto_renew = 0;
    expiry_options.default_lease_ms = 2000;
    dilock_etcd_provider *expiry_provider = NULL;
    dilock_etcd_lock *expiry_lock = NULL;
    dilock_etcd_lock *expiry_probe_lock = NULL;
    dilock_etcd_lease *expired_lease = NULL;
    dilock_etcd_lease *expiry_successor = NULL;
    dilock_etcd_lease *expiry_probe_lease = NULL;
    status = dilock_etcd_provider_create(&expiry_options, &expiry_provider, &error);
    if (status != DILOCK_STATUS_OK)
        return report_failure("expiry provider", status, &error);
    status = dilock_etcd_lock_create(expiry_provider, "installed-consumer/expiry",
                                     &expiry_lock, &error);
    if (status != DILOCK_STATUS_OK)
        return report_failure("expiry lock", status, &error);
    status = dilock_etcd_acquire(expiry_lock, 2000, 2000, &expired_lease, &error);
    if (status != DILOCK_STATUS_OK)
        return report_failure("acquire expiring owner", status, &error);
    const struct timespec server_expiry = {4, 0};
    thrd_sleep(&server_expiry, NULL);
    if (dilock_etcd_lease_is_valid(expired_lease)) {
        fprintf(stderr, "non-renewing etcd lease remained locally valid after server TTL\n");
        return 1;
    }
    status = dilock_etcd_lock_create(second, "installed-consumer/expiry",
                                     &expiry_probe_lock, &error);
    if (status != DILOCK_STATUS_OK)
        return report_failure("expiry probe lock", status, &error);
    status = dilock_etcd_acquire(expiry_lock, 2000, 2000, &expiry_successor,
                                 &error);
    if (status != DILOCK_STATUS_OK)
        return report_failure("successor after natural expiry", status, &error);
    status = dilock_etcd_lease_release(expired_lease, &outcome, &error);
    if (status != DILOCK_STATUS_OK || outcome != DILOCK_ALREADY_NOT_OWNED)
        return report_failure("expired owner release outcome", status, &error);
    dilock_etcd_lease_destroy(expired_lease);
    expired_lease = NULL;
    status = dilock_etcd_acquire(expiry_probe_lock, 100, 2000,
                                 &expiry_probe_lease, &error);
    if (status != DILOCK_STATUS_WAIT_TIMEOUT || expiry_probe_lease != NULL) {
        fprintf(stderr, "stale expired-owner release disturbed its successor: status=%d\n",
                (int)status);
        return 1;
    }
    status = dilock_etcd_lease_release(expiry_successor, &outcome, &error);
    if (status != DILOCK_STATUS_OK || outcome != DILOCK_RELEASED)
        return report_failure("expired-owner successor release outcome", status, &error);

    dilock_etcd_lease_destroy(first_lease);
    dilock_etcd_lease_destroy(second_lease);
    dilock_etcd_lease_destroy(expiry_successor);
    dilock_etcd_lease_destroy(expiry_probe_lease);
    dilock_etcd_lock_destroy(expiry_lock);
    dilock_etcd_lock_destroy(expiry_probe_lock);
    dilock_etcd_provider_destroy(expiry_provider);
    dilock_etcd_lock_destroy(first_lock);
    dilock_etcd_lock_destroy(second_lock);
    dilock_etcd_provider_destroy(first);
    dilock_etcd_provider_destroy(second);
    puts("PASS: installed C17 consumer etcd contention, handoff, natural expiry, stale-owner release, successor protection, and increasing revision fence");
    return 0;
}
