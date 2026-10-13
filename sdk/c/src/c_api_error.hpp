#ifndef DILOCK_C_API_ERROR_HPP
#define DILOCK_C_API_ERROR_HPP

#include <dilock/dilock.h>

#include <new>
#include <exception>

namespace dilock::c_api_detail {

// Allocation failure is a local SDK failure, not evidence that the backend is
// unavailable. Preserve the broad mapping for client-library transport errors.
inline dilock_status standard_exception_status(const std::exception& failure) noexcept {
    if (dynamic_cast<const std::bad_alloc*>(&failure) != nullptr) {
        return DILOCK_STATUS_INTERNAL;
    }
    return DILOCK_STATUS_BACKEND_UNAVAILABLE;
}

} // namespace dilock::c_api_detail

#endif
