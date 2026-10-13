#include "dilock/redis.hpp"

#include <sw/redis++/redis++.h>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <stop_token>
#include <thread>
#include <iterator>
#include <utility>
#include <vector>

namespace {
void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}
}

int main() {
    using namespace std::chrono_literals;
    if (std::getenv("DILOCK_REDIS_INTEGRATION") == nullptr &&
        std::getenv("DILOCK_REDIS_LATE_ACQUIRE_INTEGRATION") == nullptr &&
        std::getenv("DILOCK_REDIS_CANCEL_DURING_SET_INTEGRATION") == nullptr &&
        std::getenv("DILOCK_REDIS_LATE_RENEW_INTEGRATION") == nullptr) {
        std::cout << "SKIPPED: set a live Redis integration mode to enable this check\n";
        return 77;
    }
    try {
        dilock::RedisOptions options;
        if (const auto* host = std::getenv("DILOCK_REDIS_HOST")) options.host = host;
        if (const auto* port = std::getenv("DILOCK_REDIS_PORT")) options.port = std::stoi(port);
        options.name_space = "cpp-integration-test";
        if (const auto* ns = std::getenv("DILOCK_REDIS_NAMESPACE")) options.name_space = ns;
        if (const auto* epoch = std::getenv("DILOCK_REDIS_EPOCH")) options.coordination_epoch = epoch;
        else options.coordination_epoch = "cpp-integration-test";
        options.rpc_timeout = std::chrono::milliseconds(300);
        options.safety_margin = std::chrono::milliseconds(20);

        sw::redis::ConnectionOptions raw_connection;
        raw_connection.host = options.host;
        raw_connection.port = options.port;
        raw_connection.user = options.username.empty() ? "default" : options.username;
        raw_connection.password = options.password;
        raw_connection.db = options.database;
        raw_connection.connect_timeout = options.rpc_timeout;
        raw_connection.socket_timeout = options.rpc_timeout;
        sw::redis::Redis raw(raw_connection);

        dilock::provision_redis_namespace(options);
        dilock::RedisProvider provider(options);
        if (std::getenv("DILOCK_REDIS_LATE_RENEW_INTEGRATION") != nullptr) {
            auto owner = provider.lock("late-renew-ack").try_acquire(600ms);
            require(owner.has_value(), "late-renew owner did not acquire");
            std::this_thread::sleep_for(450ms);
            require(owner->valid(), "fixture expired before renewal was dispatched");
            bool lost = false;
            try {
                (void)owner->renew(2s);
            } catch (const dilock::Error& error) {
                if (error.code() != dilock::ErrorCode::lease_lost) throw;
                lost = true;
            }
            require(lost, "late renewal ACK restored reliance after the old confirmed window");
            require(!owner->valid(), "late-renewed lease remained locally valid");
            bool remained_lost = false;
            try { (void)owner->renew(2s); }
            catch (const dilock::Error& error) { remained_lost = error.code() == dilock::ErrorCode::lease_lost; }
            require(remained_lost, "a second renewal revived a terminally lost lease");
            require(owner->release() == dilock::ReleaseResult::released,
                    "owner-checked cleanup was unavailable after local validity loss");
            auto successor = provider.lock("late-renew-ack").try_acquire(2s);
            require(successor.has_value(), "successor could not acquire after late-renew cleanup");
            require(successor->release() == dilock::ReleaseResult::released, "successor cleanup failed");
            std::cout << "PASS: applied renewal ACK crossed the prior window; loss stayed terminal and owner cleanup passed\n";
            return EXIT_SUCCESS;
        }
        if (std::getenv("DILOCK_REDIS_CANCEL_DURING_SET_INTEGRATION") != nullptr) {
            const auto resource = "cpp-cancel-during-set-" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
            std::stop_source cancellation;
            std::jthread cancel_during_reply([&] {
                // The fault proxy applies SET before holding its successful reply.
                // This delay is shorter than the configured proxy hold (200 ms).
                std::this_thread::sleep_for(50ms);
                cancellation.request_stop();
            });
            bool cancelled = false;
            try {
                auto unexpected = provider.lock(resource).try_acquire(2s, cancellation.get_token());
                (void)unexpected;
            } catch (const dilock::Error& error) {
                cancelled = error.code() == dilock::ErrorCode::cancelled;
            }
            require(cancelled,
                    "cancellation during an applied SET reply did not surface as cancelled");

            sw::redis::ConnectionOptions verify_connection;
            if (const auto* host = std::getenv("DILOCK_REDIS_VERIFY_HOST")) verify_connection.host = host;
            if (const auto* port = std::getenv("DILOCK_REDIS_VERIFY_PORT")) verify_connection.port = std::stoi(port);
            verify_connection.connect_timeout = options.rpc_timeout;
            verify_connection.socket_timeout = options.rpc_timeout;
            sw::redis::Redis verify(verify_connection);
            const auto key = "dilock:{" + dilock::resource_id(options.name_space, resource) + "}:lease";
            require(!verify.get(key).has_value(),
                    "owner-checked cleanup did not remove the applied SET after cancellation");
            std::cout << "PASS: cancellation arrived during the applied SET reply; acquisition was cancelled and owner cleanup was confirmed\n";
            return EXIT_SUCCESS;
        }
        if (std::getenv("DILOCK_REDIS_ACK_DROP_INTEGRATION") != nullptr) {
            constexpr std::string_view resource = "cpp-ack-drop-release";
            auto owner = provider.lock(resource).try_acquire(10s);
            require(owner.has_value(), "ACK-drop owner did not acquire through the fault proxy");
            require(owner->release() == dilock::ReleaseResult::unknown,
                    "applied Redis release with a withheld reply was not reported unknown");
            require(owner->release() == dilock::ReleaseResult::unknown,
                    "repeated ambiguous release did not preserve its terminal unknown result");

            sw::redis::ConnectionOptions verify_connection;
            if (const auto* host = std::getenv("DILOCK_REDIS_VERIFY_HOST")) verify_connection.host = host;
            if (const auto* port = std::getenv("DILOCK_REDIS_VERIFY_PORT")) verify_connection.port = std::stoi(port);
            verify_connection.connect_timeout = options.rpc_timeout;
            verify_connection.socket_timeout = options.rpc_timeout;
            sw::redis::Redis verify(verify_connection);
            const auto key = "dilock:{" + dilock::resource_id(options.name_space, resource) + "}:lease";
            require(!verify.get(key).has_value(), "Redis still contains the lease after applying the owner-checked release");
            std::cout << "PASS: Redis EVAL applied but its reply was withheld; release stayed unknown without retry\n";
            return EXIT_SUCCESS;
        }
        if (std::getenv("DILOCK_REDIS_LATE_ACQUIRE_INTEGRATION") != nullptr) {
            const auto resource = "cpp-late-acquire-" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
            const auto wait = 50ms;
            const auto started = std::chrono::steady_clock::now();
            bool timed_out = false;
            try {
                auto late = provider.lock(resource).acquire(wait, 2s);
                (void)late;
            } catch (const dilock::Error& error) {
                if (error.code() != dilock::ErrorCode::wait_timeout) throw;
                timed_out = true;
            }
            require(timed_out, "Redis returned a lease after the caller wait deadline");
            require(std::chrono::steady_clock::now() - started >= wait,
                    "late-acquisition fixture did not cross the caller wait deadline");

            sw::redis::ConnectionOptions verify_connection;
            if (const auto* host = std::getenv("DILOCK_REDIS_VERIFY_HOST")) verify_connection.host = host;
            else verify_connection.host = "redis";
            if (const auto* port = std::getenv("DILOCK_REDIS_VERIFY_PORT")) verify_connection.port = std::stoi(port);
            verify_connection.db = options.database;
            verify_connection.connect_timeout = options.rpc_timeout;
            verify_connection.socket_timeout = options.rpc_timeout;
            sw::redis::Redis verify(verify_connection);
            const auto key = "dilock:{" + dilock::resource_id(options.name_space, resource) + "}:lease";
            require(!verify.get(key).has_value(), "late Redis acquisition remained held after owner cleanup");
            std::cout << "PASS: Redis SET reply crossed wait deadline; no lease was returned and owner cleanup was confirmed\n";
            return EXIT_SUCCESS;
        }
        {
            auto daily = provider.lock("cpp-default-scope/daily");
            auto weekly = provider.lock("cpp-default-scope/weekly");
            auto daily_lease = daily.acquire();
            auto weekly_lease = weekly.acquire(dilock::AcquireOptions{2s, 3s});
            require(daily_lease.valid() && weekly_lease.valid(),
                    "one provider did not acquire two distinct resources with default and explicit policies");
        }
        auto scope_successor = provider.lock("cpp-default-scope/daily").try_acquire();
        require(scope_successor.has_value(), "RAII scope exit did not release the default-policy lease");
        require(scope_successor->release() == dilock::ReleaseResult::released,
                "default-policy successor release was not acknowledged");

        const auto exception_lock = provider.lock("cpp-scope-exception");
        const std::runtime_error body_failure("preserve this Redis work failure");
        bool body_failure_preserved = false;
        try {
            auto scoped = exception_lock.acquire();
            (void)scoped;
            throw body_failure;
        } catch (const std::runtime_error& error) {
            body_failure_preserved = &error == &body_failure || std::string(error.what()) == body_failure.what();
        }
        require(body_failure_preserved, "RAII scope replaced the primary Redis work exception");
        auto after_exception = exception_lock.try_acquire();
        require(after_exception.has_value(), "RAII exception exit did not release for an independent Redis acquisition");
        require(after_exception->release() == dilock::ReleaseResult::released,
                "post-exception Redis successor release was not acknowledged");

        const auto moved_resource = provider.lock("cpp-move-source");
        const auto replaced_resource = provider.lock("cpp-move-destination");
        auto destination = replaced_resource.acquire();
        {
            auto source = moved_resource.acquire();
            destination = std::move(source);
            require(!source.valid(), "moved-from Redis lease retained local validity");
        }
        auto replaced_successor = replaced_resource.try_acquire();
        require(replaced_successor.has_value(), "Redis move assignment did not clean up the overwritten destination");
        require(replaced_successor->release() == dilock::ReleaseResult::released,
                "overwritten Redis destination successor release failed");
        require(!moved_resource.try_acquire(), "moved-from Redis lease destructor released the transferred owner");
        require(destination.valid(), "Redis move destination lost the transferred lease when its source left scope");
        require(destination.release() == dilock::ReleaseResult::released,
                "transferred Redis owner release was not acknowledged");
        auto moved_successor = moved_resource.try_acquire();
        require(moved_successor.has_value(), "Redis successor could not acquire after moved-to lease release");
        require(moved_successor->release() == dilock::ReleaseResult::released,
                "moved Redis successor release was not acknowledged");

        auto lock = provider.lock("cpp-integration-resource");
        auto owner = lock.try_acquire(std::chrono::seconds(2));
        require(owner.has_value(), "first acquisition failed");
        require(!lock.try_acquire(std::chrono::seconds(2)), "second acquisition was not busy");
        require(owner->renew(std::chrono::seconds(2)), "same-owner renewal failed");
        require(owner->release() == dilock::ReleaseResult::released, "owner release was not acknowledged");
        require(owner->release() == dilock::ReleaseResult::released, "release was not idempotent");
        auto successor = lock.try_acquire(std::chrono::seconds(2));
        require(successor.has_value(), "successor did not acquire after release");
        require(owner->release() == dilock::ReleaseResult::released, "stale release changed terminal observation");
        require(!lock.try_acquire(std::chrono::seconds(2)), "stale release removed successor ownership");
        require(successor->release() == dilock::ReleaseResult::released, "successor release was not acknowledged");

        auto expiring = lock.try_acquire(std::chrono::milliseconds(400));
        require(expiring.has_value(), "expiry test owner did not acquire");
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        auto after_expiry = lock.try_acquire(std::chrono::seconds(2));
        require(after_expiry.has_value(), "successor did not acquire after Redis expiry");
        require(expiring->release() == dilock::ReleaseResult::already_not_owned,
                "late expired-owner release did not report owner mismatch");
        require(!lock.try_acquire(std::chrono::seconds(2)), "expired-owner release removed the new owner");
        require(after_expiry->release() == dilock::ReleaseResult::released, "post-expiry successor release failed");

        const std::string wrong_type_resource = "cpp-integration-wrong-type";
        const auto wrong_type_key = "dilock:{" + dilock::resource_id(options.name_space, wrong_type_resource) + "}:lease";
        (void)raw.del(wrong_type_key);
        require(raw.lpush(wrong_type_key, "preserve-me") == 1, "could not create wrong-type fixture");
        bool saw_protocol_mismatch = false;
        try {
            (void)provider.lock(wrong_type_resource).try_acquire(std::chrono::seconds(2));
        } catch (const dilock::Error& error) {
            saw_protocol_mismatch = error.code() == dilock::ErrorCode::protocol_mismatch;
        }
        require(saw_protocol_mismatch, "list-valued lock key was not classified as protocol mismatch");
        require(raw.type(wrong_type_key) == "list", "wrong-type classification changed the key type");
        std::vector<std::string> wrong_type_contents;
        raw.lrange(wrong_type_key, 0, -1, std::back_inserter(wrong_type_contents));
        require(wrong_type_contents == std::vector<std::string>{"preserve-me"},
                "wrong-type classification changed the key contents");
        std::cout << "PASS: Redis acquisition, contention, wrong-type preservation, renewal, expiry, stale-owner safety\n";
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
