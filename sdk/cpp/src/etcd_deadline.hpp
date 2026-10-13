#pragma once

#include "dilock/redis.hpp"

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <stop_token>

namespace dilock::detail {

template <class Rep, class Period, class Predicate>
bool wait_until_ready_or_stopped(std::condition_variable& changed,
                                 std::unique_lock<std::mutex>& lock,
                                 std::chrono::duration<Rep, Period> budget,
                                 Predicate ready,
                                 std::stop_token stop) {
    std::stop_callback wake_on_stop(stop, [&changed] { changed.notify_all(); });
    changed.wait_for(lock, budget, [&] { return ready() || stop.stop_requested(); });
    return ready() && !stop.stop_requested();
}

inline ErrorCode classify_lock_failure(bool cancellation_requested,
                                       bool wait_deadline_expired) noexcept {
    if (cancellation_requested) return ErrorCode::cancelled;
    if (wait_deadline_expired) return ErrorCode::wait_timeout;
    return ErrorCode::backend_unavailable;
}

inline ErrorCode classify_initial_keepalive_failure(bool cancellation_requested,
                                                    bool wait_deadline_expired,
                                                    bool revoke_acknowledged) noexcept {
    if (!revoke_acknowledged) return ErrorCode::acquisition_outcome_unknown;
    return classify_lock_failure(cancellation_requested, wait_deadline_expired);
}

inline ErrorCode classify_abandoned_acquisition(ErrorCode intended_failure,
                                                bool cleanup_acknowledged) noexcept {
    return cleanup_acknowledged ? intended_failure : ErrorCode::acquisition_outcome_unknown;
}

enum class EtcdOwnershipObservation { owned, not_owned, unknown };

inline ReleaseResult classify_etcd_release(bool unlock_acknowledged,
                                           EtcdOwnershipObservation ownership,
                                           bool revoke_acknowledged,
                                           bool revoke_not_found) noexcept {
    if (revoke_not_found || ownership == EtcdOwnershipObservation::not_owned) {
        return ReleaseResult::already_not_owned;
    }
    if (ownership == EtcdOwnershipObservation::owned && unlock_acknowledged && revoke_acknowledged) {
        return ReleaseResult::released;
    }
    if (ownership == EtcdOwnershipObservation::owned && revoke_acknowledged) {
        return ReleaseResult::already_not_owned;
    }
    return ReleaseResult::unknown;
}

// Once a confirmed deadline has elapsed, this acquisition can never become
// valid again, even if a delayed keepalive response arrives later.
class ConfirmedDeadline final {
public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    explicit ConfirmedDeadline(TimePoint initial) noexcept : deadline_(initial) {}

    [[nodiscard]] bool valid(TimePoint now) noexcept {
        std::lock_guard guard(mutex_);
        expire_if_needed(now);
        return !lost_;
    }

    [[nodiscard]] std::optional<TimePoint> snapshot(TimePoint now) noexcept {
        std::lock_guard guard(mutex_);
        expire_if_needed(now);
        if (lost_) return std::nullopt;
        return deadline_;
    }

    [[nodiscard]] bool confirm(TimePoint sent, std::chrono::milliseconds ttl,
                               std::chrono::milliseconds margin, TimePoint now) noexcept {
        std::lock_guard guard(mutex_);
        expire_if_needed(now);
        if (lost_) return false;
        const auto next = sent + ttl - margin;
        if (next <= now) {
            lost_ = true;
            return false;
        }
        deadline_ = next;
        return true;
    }

private:
    void expire_if_needed(TimePoint now) noexcept {
        if (now >= deadline_) lost_ = true;
    }

    mutable std::mutex mutex_;
    TimePoint deadline_;
    bool lost_{};
};

} // namespace dilock::detail
