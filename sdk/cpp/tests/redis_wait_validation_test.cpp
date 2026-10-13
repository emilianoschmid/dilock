#include <dilock/redis.hpp>
#include "../src/redis_wait.hpp"

#include <chrono>
#include <cstdlib>
#include <iostream>

int main() {
    if (dilock::detail::late_acquisition_error(dilock::ReleaseResult::released) != dilock::ErrorCode::wait_timeout ||
        dilock::detail::late_acquisition_error(dilock::ReleaseResult::already_not_owned) != dilock::ErrorCode::wait_timeout ||
        dilock::detail::late_acquisition_error(dilock::ReleaseResult::unknown) != dilock::ErrorCode::acquisition_outcome_unknown ||
        dilock::detail::cancelled_acquisition_error(dilock::ReleaseResult::released) != dilock::ErrorCode::cancelled ||
        dilock::detail::cancelled_acquisition_error(dilock::ReleaseResult::already_not_owned) != dilock::ErrorCode::cancelled ||
        dilock::detail::cancelled_acquisition_error(dilock::ReleaseResult::unknown) != dilock::ErrorCode::acquisition_outcome_unknown) {
        std::cerr << "Redis acquisition cleanup produced the wrong failure category\n";
        return EXIT_FAILURE;
    }
    std::stop_source cancelled_contention;
    cancelled_contention.request_stop();
    try {
        // This is the post-classification gate used before try_acquire returns
        // null after a Redis SET NX miss.
        dilock::detail::throw_if_cancelled(
            cancelled_contention.get_token(), "acquisition cancelled while classifying contention");
        std::cerr << "cancelled Redis contention unexpectedly remained ordinary busy\n";
        return EXIT_FAILURE;
    } catch (const dilock::Error& error) {
        if (error.code() != dilock::ErrorCode::cancelled) {
            std::cerr << "cancelled Redis contention returned the wrong error category\n";
            return EXIT_FAILURE;
        }
    }
    dilock::ResourceLock empty;
    try {
        (void)empty.acquire(std::chrono::milliseconds::zero(), std::chrono::seconds(1));
        std::cerr << "zero wait budget unexpectedly accepted\n";
        return EXIT_FAILURE;
    } catch (const dilock::Error& error) {
        if (error.code() != dilock::ErrorCode::invalid_argument) {
            std::cerr << "zero wait budget returned the wrong error category\n";
            return EXIT_FAILURE;
        }
    }
    return EXIT_SUCCESS;
}
