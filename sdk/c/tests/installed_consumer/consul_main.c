#include <dilock/dilock.h>

int main(void) {
    dilock_consul_options options;
    dilock_consul_options_init(&options);
    if (options.endpoint == 0 || options.poll_interval_ms <= 0) return 1;
    if (DILOCK_BACKEND_CONSUL != 2 || DILOCK_C_ABI_VERSION != 1u) return 2;
    (void)&dilock_consul_provider_create;
    (void)&dilock_consul_lock_create;
    (void)&dilock_consul_acquire_default;
    (void)&dilock_consul_lease_fence;
    (void)&dilock_consul_lease_renew;
    (void)&dilock_consul_lease_release;
    return 0;
}
