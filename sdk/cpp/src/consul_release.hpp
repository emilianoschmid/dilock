#pragma once

#include "dilock/redis.hpp"

#include <utility>

namespace dilock::detail {

template <typename ReleaseOperation, typename DestroyOperation>
ReleaseResult release_with_cleanup(ReleaseOperation&& release_operation,
                                   DestroyOperation&& destroy_operation) noexcept {
    auto outcome = ReleaseResult::unknown;
    try {
        outcome = std::forward<ReleaseOperation>(release_operation)();
    } catch (...) {
        outcome = ReleaseResult::unknown;
    }

    bool destroyed = false;
    try {
        destroyed = std::forward<DestroyOperation>(destroy_operation)();
    } catch (...) {
        destroyed = false;
    }
    return destroyed ? outcome : ReleaseResult::unknown;
}

} // namespace dilock::detail
