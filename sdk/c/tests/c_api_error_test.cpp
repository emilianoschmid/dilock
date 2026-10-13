#include "../src/c_api_error.hpp"

#include <new>
#include <stdexcept>

int main() {
    const std::bad_alloc allocation_failure;
    const std::runtime_error client_failure("transport error");
    if (dilock::c_api_detail::standard_exception_status(allocation_failure) != DILOCK_STATUS_INTERNAL) {
        return 1;
    }
    if (dilock::c_api_detail::standard_exception_status(client_failure) != DILOCK_STATUS_BACKEND_UNAVAILABLE) {
        return 2;
    }
    return 0;
}
