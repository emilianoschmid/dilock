#include "dilock/postgres.hpp"

#include <chrono>
#include <cstdlib>
#include <stop_token>
#include <string>
#include <thread>

#define CHECK(condition) do { if (!(condition)) return __LINE__; } while (false)

int main() {
    const char* dsn = std::getenv("DILOCK_POSTGRES_DSN");
    if (dsn == nullptr || *dsn == '\0') return 77;

    dilock::PostgresOptions options{
        .connection_string = dsn,
        .name_space = "ci-postgres-cpp",
        .coordination_epoch = "ci-postgres-cpp-epoch",
        .rpc_timeout = std::chrono::milliseconds(500),
        .cleanup_timeout = std::chrono::milliseconds(500),
        .poll_interval = std::chrono::milliseconds(10),
        .default_acquisition = dilock::AcquireOptions{
            .wait = std::chrono::milliseconds(250),
            .lease = std::chrono::seconds(5),
        },
    };
    dilock::provision_postgres_namespace(options);
    dilock::PostgresProvider first(options);
    dilock::PostgresProvider second(options);
    auto first_lock = first.lock("cpp-provider");
    auto second_lock = second.lock("cpp-provider");
    auto lease = first_lock.try_acquire();
    CHECK(lease.has_value() && lease->renew());
    CHECK(!second_lock.try_acquire().has_value());

    auto different = second.lock("cpp-different-resource").try_acquire();
    CHECK(different.has_value() && different->release() == dilock::ReleaseResult::released);

    bool timed_out = false;
    try {
        (void)second_lock.acquire(dilock::AcquireOptions{
            .wait = std::chrono::milliseconds(150),
            .lease = std::chrono::seconds(5),
        });
    } catch (const dilock::Error& error) {
        timed_out = error.code() == dilock::ErrorCode::wait_timeout;
    }
    CHECK(timed_out);

    std::stop_source cancellation;
    std::jthread canceller([&cancellation] {
        std::this_thread::sleep_for(std::chrono::milliseconds(40));
        cancellation.request_stop();
    });
    bool cancelled = false;
    try {
        (void)second_lock.acquire(dilock::AcquireOptions{
            .wait = std::chrono::seconds(5),
            .lease = std::chrono::seconds(5),
        }, cancellation.get_token());
    } catch (const dilock::Error& error) {
        cancelled = error.code() == dilock::ErrorCode::cancelled;
    }
    CHECK(cancelled);

    CHECK(lease->release() == dilock::ReleaseResult::released);
    auto handoff = second_lock.try_acquire();
    CHECK(handoff.has_value() && handoff->release() == dilock::ReleaseResult::released);
    return 0;
}
