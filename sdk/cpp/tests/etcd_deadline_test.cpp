#include "../src/etcd_deadline.hpp"
#include "dilock/etcd.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <stop_token>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

void delayed_keepalive_cannot_resurrect_expired_deadline() {
    const auto origin = Clock::time_point{};
    dilock::detail::ConfirmedDeadline deadline(origin + 1s);

    require(!deadline.valid(origin + 1001ms), "expired deadline remained valid");
    require(!deadline.confirm(origin + 900ms, 1s, 0ms, origin + 1100ms),
            "late keepalive reply resurrected an expired confirmed deadline");
    require(!deadline.valid(origin + 1100ms), "terminally expired deadline became valid again");
    require(!deadline.snapshot(origin + 1100ms), "terminally expired deadline still had a budget snapshot");
}

void deadline_reads_and_extensions_are_synchronized() {
    const auto origin = Clock::time_point{};
    dilock::detail::ConfirmedDeadline deadline(origin + 1s);
    std::atomic<bool> failed{false};
    std::vector<std::thread> workers;
    for (int i = 0; i < 4; ++i) {
        workers.emplace_back([&] {
            for (int iteration = 0; iteration < 10000; ++iteration) {
                if (!deadline.confirm(origin, 1s, 0ms, origin) ||
                    !deadline.valid(origin) || !deadline.snapshot(origin)) {
                    failed.store(true, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto& worker : workers) worker.join();
    require(!failed.load(std::memory_order_relaxed), "concurrent deadline operations lost a valid confirmation");
}

void lock_rpc_failures_are_not_misreported_as_wait_timeouts() {
    using dilock::ErrorCode;
    require(dilock::detail::classify_lock_failure(false, false) == ErrorCode::backend_unavailable,
            "an early DEADLINE_EXCEEDED or server CANCELLED status was classified as wait timeout");
    require(dilock::detail::classify_lock_failure(false, true) == ErrorCode::wait_timeout,
            "an elapsed caller deadline was not classified as wait timeout");
    require(dilock::detail::classify_lock_failure(true, false) == ErrorCode::cancelled,
            "caller cancellation was not preserved");
}

void initial_keepalive_failure_uses_cleanup_and_operation_outcomes() {
    using dilock::ErrorCode;
    require(dilock::detail::classify_initial_keepalive_failure(false, false, true) == ErrorCode::backend_unavailable,
            "confirmed cleanup of an early keepalive failure was not classified as backend failure");
    require(dilock::detail::classify_initial_keepalive_failure(false, true, true) == ErrorCode::wait_timeout,
            "confirmed cleanup after the caller deadline was not classified as wait timeout");
    require(dilock::detail::classify_initial_keepalive_failure(true, false, true) == ErrorCode::cancelled,
            "confirmed cleanup after caller cancellation was not classified as cancellation");
    require(dilock::detail::classify_initial_keepalive_failure(false, false, false) == ErrorCode::acquisition_outcome_unknown,
            "unacknowledged lease revoke was not classified as unknown");
}

void failed_acquisition_cleanup_overrides_timeout_or_protocol_failure() {
    using dilock::ErrorCode;
    require(dilock::detail::classify_abandoned_acquisition(ErrorCode::wait_timeout, true) == ErrorCode::wait_timeout,
            "acknowledged revoke did not preserve the caller wait timeout");
    require(dilock::detail::classify_abandoned_acquisition(ErrorCode::wait_timeout, false) == ErrorCode::acquisition_outcome_unknown,
            "unacknowledged revoke after wait expiry was reported as an ordinary timeout");
    require(dilock::detail::classify_abandoned_acquisition(ErrorCode::protocol_mismatch, true) == ErrorCode::protocol_mismatch,
            "acknowledged revoke did not preserve the protocol mismatch");
    require(dilock::detail::classify_abandoned_acquisition(ErrorCode::protocol_mismatch, false) == ErrorCode::acquisition_outcome_unknown,
            "unacknowledged revoke after malformed ownership evidence was reported as a definite protocol failure");
    require(dilock::detail::classify_abandoned_acquisition(ErrorCode::invalid_argument, true) == ErrorCode::invalid_argument,
            "acknowledged revoke did not preserve the invalid effective TTL result");
    require(dilock::detail::classify_abandoned_acquisition(ErrorCode::invalid_argument, false) == ErrorCode::acquisition_outcome_unknown,
            "unacknowledged revoke after an unusable effective TTL was reported as a definite argument error");
}

void etcd_release_uses_ownership_evidence_for_unlock_and_revoke_acknowledgements() {
    using dilock::ReleaseResult;
    using Observation = dilock::detail::EtcdOwnershipObservation;
    using dilock::detail::classify_etcd_release;

    require(classify_etcd_release(true, Observation::owned, true, false) == ReleaseResult::released,
            "acknowledged Unlock and revoke after confirmed ownership were not classified as released");
    require(classify_etcd_release(true, Observation::owned, false, false) == ReleaseResult::unknown,
            "Unlock ACK without confirmed lease cleanup overstated removal across the expiry race");
    require(classify_etcd_release(false, Observation::owned, true, false) == ReleaseResult::already_not_owned,
            "revoke-only acknowledgement was not classified as already not owned");
    require(classify_etcd_release(false, Observation::not_owned, true, false) == ReleaseResult::already_not_owned,
            "acknowledged revoke after a missing or successor key was misclassified");
    require(classify_etcd_release(false, Observation::unknown, true, false) == ReleaseResult::unknown,
            "revoke ACK after uncertain ownership was overstated as a confirmed release");
    require(classify_etcd_release(true, Observation::unknown, false, false) == ReleaseResult::unknown,
            "Unlock ACK after uncertain ownership was overstated as a confirmed release");
    require(classify_etcd_release(true, Observation::not_owned, false, false) == ReleaseResult::already_not_owned,
            "a no-op Unlock ACK overrode proven absence or successor ownership");
    require(classify_etcd_release(false, Observation::owned, false, true) == ReleaseResult::already_not_owned,
            "LeaseNotFound after observed ownership did not report that the lease is gone");
    require(classify_etcd_release(false, Observation::unknown, false, true) == ReleaseResult::already_not_owned,
            "LeaseNotFound did not confirm that this acquisition no longer owns a lease");
    require(classify_etcd_release(false, Observation::owned, false, false) == ReleaseResult::unknown,
            "two unacknowledged cleanup calls were reported as a definite outcome");
}

void initial_keepalive_wait_wakes_for_cancellation() {
    std::mutex mutex;
    std::condition_variable changed;
    bool ready = false;
    std::stop_source stop;
    std::jthread requester([&] {
        std::this_thread::sleep_for(20ms);
        stop.request_stop();
    });
    const auto started = Clock::now();
    std::unique_lock guard(mutex);
    const bool result = dilock::detail::wait_until_ready_or_stopped(
        changed, guard, 2s, [&] { return ready; }, stop.get_token());
    const auto elapsed = Clock::now() - started;
    require(!result, "cancelled initial keepalive wait reported ready");
    require(elapsed < 500ms, "initial keepalive wait ignored cancellation until its RPC budget expired");
}

} // namespace

int main() {
    try {
        const dilock::EtcdOptions defaults;
        require(defaults.default_acquisition.wait == 5s && defaults.default_acquisition.lease == 30s,
                "etcd default acquisition policy differs from shared semantic defaults");
        using DefaultAcquire = dilock::EtcdLease (dilock::EtcdResourceLock::*)(std::stop_token) const;
        using PolicyAcquire = dilock::EtcdLease (dilock::EtcdResourceLock::*)(const dilock::AcquireOptions&, std::stop_token) const;
        static_assert(std::is_same_v<decltype(static_cast<DefaultAcquire>(&dilock::EtcdResourceLock::acquire)), DefaultAcquire>);
        static_assert(std::is_same_v<decltype(static_cast<PolicyAcquire>(&dilock::EtcdResourceLock::acquire)), PolicyAcquire>);
        delayed_keepalive_cannot_resurrect_expired_deadline();
        deadline_reads_and_extensions_are_synchronized();
        lock_rpc_failures_are_not_misreported_as_wait_timeouts();
        initial_keepalive_failure_uses_cleanup_and_operation_outcomes();
        failed_acquisition_cleanup_overrides_timeout_or_protocol_failure();
        etcd_release_uses_ownership_evidence_for_unlock_and_revoke_acknowledgements();
        initial_keepalive_wait_wakes_for_cancellation();
        std::cout << "PASS: etcd deadline expiry is terminal and deadline snapshots/extensions are synchronized\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
