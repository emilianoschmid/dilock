#include <dilock/dilock.h>

#include <stddef.h>

int main(void) {
    dilock_error error = {0};
    dilock_redis_options redis;
    dilock_etcd_options etcd;
    dilock_redis_provider *redis_provider = (dilock_redis_provider *)(uintptr_t)1;
    dilock_etcd_provider *etcd_provider = (dilock_etcd_provider *)(uintptr_t)1;
    dilock_redis_lock *redis_lock = (dilock_redis_lock *)(uintptr_t)1;
    dilock_redis_lease *redis_lease = (dilock_redis_lease *)(uintptr_t)1;
    dilock_cancellation *cancellation = NULL;
    dilock_release_outcome release_outcome = DILOCK_RELEASED;
    int32_t renewed = -1;
    dilock_etcd_fence fence = {0};
    char epoch[8] = {0};

    if (dilock_c_abi_version() != DILOCK_C_ABI_VERSION) return 1;
    dilock_redis_options_init(&redis);
    dilock_etcd_options_init(&etcd);
    dilock_redis_options_init(NULL);
    dilock_etcd_options_init(NULL);
    if (redis.port != 6379 || redis.default_wait_ms != 5000 || redis.default_lease_ms != 30000) return 2;
    if (etcd.default_wait_ms != 5000 || etcd.default_lease_ms != 30000) return 3;
    if (redis.coordination_epoch == NULL || redis.coordination_epoch[0] != '\0') return 4;

    /* Exercise the linked ABI boundary without requiring either backend. Bad
     * handles must fail deterministically and output handles must be cleared. */
    if (dilock_redis_provider_create(NULL, &redis_provider, &error) != DILOCK_STATUS_INVALID_ARGUMENT ||
        redis_provider != NULL || error.code != DILOCK_STATUS_INVALID_ARGUMENT || error.message[0] == '\0') return 6;
    if (dilock_etcd_provider_create(NULL, &etcd_provider, &error) != DILOCK_STATUS_INVALID_ARGUMENT ||
        etcd_provider != NULL || error.code != DILOCK_STATUS_INVALID_ARGUMENT || error.message[0] == '\0') return 7;
    if (dilock_redis_provider_create(&redis, NULL, &error) != DILOCK_STATUS_INVALID_ARGUMENT ||
        error.code != DILOCK_STATUS_INVALID_ARGUMENT || error.message[0] == '\0') return 15;
    if (dilock_etcd_provider_create(&etcd, NULL, &error) != DILOCK_STATUS_INVALID_ARGUMENT ||
        error.code != DILOCK_STATUS_INVALID_ARGUMENT || error.message[0] == '\0') return 16;
    if (dilock_redis_lock_create(NULL, "resource", &redis_lock, &error) != DILOCK_STATUS_INVALID_ARGUMENT ||
        redis_lock != NULL || error.code != DILOCK_STATUS_INVALID_ARGUMENT) return 8;
    if (dilock_redis_acquire_default(NULL, NULL, &error) != DILOCK_STATUS_INVALID_ARGUMENT ||
        error.code != DILOCK_STATUS_INVALID_ARGUMENT) return 17;
    if (dilock_redis_acquire_default(NULL, &redis_lease, &error) != DILOCK_STATUS_INVALID_ARGUMENT ||
        redis_lease != NULL || error.code != DILOCK_STATUS_INVALID_ARGUMENT) return 9;
    if (dilock_redis_lease_release(NULL, &release_outcome, &error) != DILOCK_STATUS_INVALID_ARGUMENT ||
        release_outcome != DILOCK_RELEASE_UNKNOWN || error.code != DILOCK_STATUS_INVALID_ARGUMENT) return 10;
    release_outcome = DILOCK_RELEASED;
    if (dilock_etcd_lease_release(NULL, &release_outcome, &error) != DILOCK_STATUS_INVALID_ARGUMENT ||
        release_outcome != DILOCK_RELEASE_UNKNOWN || error.code != DILOCK_STATUS_INVALID_ARGUMENT) return 14;
    if (dilock_redis_lease_renew(NULL, 1000, &renewed, &error) != DILOCK_STATUS_INVALID_ARGUMENT ||
        renewed != 0 || error.code != DILOCK_STATUS_INVALID_ARGUMENT) return 11;
    if (dilock_etcd_lease_fence(NULL, &fence, epoch, sizeof(epoch), &error) != DILOCK_STATUS_INVALID_ARGUMENT ||
        error.code != DILOCK_STATUS_INVALID_ARGUMENT) return 12;
    cancellation = dilock_cancellation_create(&error);
    if (cancellation == NULL || error.code != DILOCK_STATUS_OK) return 13;
    dilock_cancellation_request(cancellation);
    dilock_cancellation_destroy(cancellation);

    /* Taking function addresses proves this C translation unit links the
     * provider, handle, acquisition and explicit-release ABI in both adapters. */
    (void)&dilock_redis_provider_create;
    (void)&dilock_redis_lock_create;
    (void)&dilock_redis_acquire_default;
    (void)&dilock_redis_acquire_default_with_cancellation;
    (void)&dilock_cancellation_request;
    (void)&dilock_redis_lease_release;
    (void)&dilock_redis_lease_renew;
    (void)&dilock_etcd_provider_create;
    (void)&dilock_etcd_lock_create;
    (void)&dilock_etcd_acquire_default;
    (void)&dilock_etcd_acquire_default_with_cancellation;
    (void)&dilock_etcd_try_acquire;
    (void)&dilock_etcd_lease_release;
    (void)&dilock_etcd_lease_fence;
    return DILOCK_STATUS_BUSY == 1 && DILOCK_RELEASE_UNKNOWN == 2 ? 0 : 5;
}
