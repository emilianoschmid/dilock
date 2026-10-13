#include "dilock_style.h"
#include <assert.h>
#include <stdio.h>

int main(void) {
    dilock_style_provider *provider = NULL;
    dilock_style_lease *first = NULL, *second = NULL, *busy = NULL;
    assert(dilock_style_provider_create(&provider) == DILOCK_STYLE_OK);
    assert(dilock_style_try_acquire(provider, "example/resource", &first) == DILOCK_STYLE_OK);
    assert(dilock_style_try_acquire(provider, "example/resource", &busy) == DILOCK_STYLE_BUSY);
    assert(busy == NULL);
    dilock_style_expire_for_test(provider);
    assert(dilock_style_try_acquire(provider, "example/resource", &second) == DILOCK_STYLE_OK);
    assert(dilock_style_release(first) == DILOCK_STYLE_ALREADY_NOT_OWNED);
    assert(dilock_style_try_acquire(provider, "example/resource", &busy) == DILOCK_STYLE_BUSY);
    assert(dilock_style_release(second) == DILOCK_STYLE_OK);
    assert(dilock_style_release(second) == DILOCK_STYLE_ALREADY_NOT_OWNED);
    assert(dilock_style_try_acquire(provider, "", &busy) == DILOCK_STYLE_INVALID_ARGUMENT);
    dilock_style_lease_destroy(first);
    dilock_style_lease_destroy(second);
    dilock_style_provider_destroy(provider);
    puts("PASS c: busy, stale owner, idempotence, ownership/destruction, validation");
    return 0;
}
