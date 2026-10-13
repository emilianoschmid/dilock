#ifndef DILOCK_STYLE_H
#define DILOCK_STYLE_H
/* Local single-threaded example only: no remote backend, TTL or wire protocol. */
#ifdef __cplusplus
extern "C" {
#endif

typedef struct dilock_style_provider dilock_style_provider;
typedef struct dilock_style_lease dilock_style_lease;
typedef enum {
    DILOCK_STYLE_OK,
    DILOCK_STYLE_BUSY,
    DILOCK_STYLE_ALREADY_NOT_OWNED,
    DILOCK_STYLE_INVALID_ARGUMENT,
    DILOCK_STYLE_OUT_OF_MEMORY
} dilock_style_status;

/* The provider must outlive all leases. This fixture supports one named resource. */
dilock_style_status dilock_style_provider_create(dilock_style_provider **out);
void dilock_style_provider_destroy(dilock_style_provider *provider);
dilock_style_status dilock_style_try_acquire(dilock_style_provider *provider,
                                            const char *resource, dilock_style_lease **out);
/* Repeated release is valid before destroy. Destroy consumes the handle. */
dilock_style_status dilock_style_release(dilock_style_lease *lease);
void dilock_style_lease_destroy(dilock_style_lease *lease);
/* Test hook only; not part of the proposed production API. */
void dilock_style_expire_for_test(dilock_style_provider *provider);
#ifdef __cplusplus
}
#endif
#endif
