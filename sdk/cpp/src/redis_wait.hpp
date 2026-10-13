#pragma once

#include "dilock/redis.hpp"

#include <stop_token>

namespace dilock::detail {

inline void throw_if_cancelled(std::stop_token stop, const char* message) {
    if (stop.stop_requested()) throw Error(ErrorCode::cancelled, message);
}

inline ErrorCode late_acquisition_error(ReleaseResult cleanup) noexcept {
    return cleanup == ReleaseResult::unknown
        ? ErrorCode::acquisition_outcome_unknown
        : ErrorCode::wait_timeout;
}

inline ErrorCode cancelled_acquisition_error(ReleaseResult cleanup) noexcept {
    return cleanup == ReleaseResult::unknown
        ? ErrorCode::acquisition_outcome_unknown
        : ErrorCode::cancelled;
}

} // namespace dilock::detail
