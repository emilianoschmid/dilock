#pragma once

#include "dilock/redis.hpp"

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>

namespace dilock {

/// Configuration for the experimental PostgreSQL session-advisory profile.
struct PostgresOptions {
    std::string connection_string;
    std::string name_space{"default"};
    std::string coordination_epoch;
    std::chrono::milliseconds rpc_timeout{2000};
    std::chrono::milliseconds cleanup_timeout{2000};
    std::chrono::milliseconds poll_interval{25};
    AcquireOptions default_acquisition{};
    CleanupObserver cleanup_observer{};
};

class PostgresLease final {
public:
    PostgresLease() noexcept = default;
    ~PostgresLease() noexcept;
    PostgresLease(const PostgresLease&) = delete;
    PostgresLease& operator=(const PostgresLease&) = delete;
    PostgresLease(PostgresLease&& other) noexcept;
    PostgresLease& operator=(PostgresLease&& other) noexcept;

    [[nodiscard]] bool valid() const noexcept;
    /// Checks that the private PostgreSQL session is still responsive. It does
    /// not extend a server TTL because this profile has no PostgreSQL TTL.
    bool renew();
    ReleaseResult release() noexcept;

private:
    struct State;
    explicit PostgresLease(std::shared_ptr<State> state) noexcept;
    std::shared_ptr<State> state_;
    friend class PostgresResourceLock;
};

class PostgresResourceLock final {
public:
    PostgresResourceLock() noexcept = default;
    [[nodiscard]] std::optional<PostgresLease> try_acquire(std::stop_token stop = {}) const;
    [[nodiscard]] std::optional<PostgresLease> try_acquire(
        const AcquireOptions& options, std::stop_token stop = {}) const;
    [[nodiscard]] PostgresLease acquire(std::stop_token stop = {}) const;
    [[nodiscard]] PostgresLease acquire(const AcquireOptions& options,
                                        std::stop_token stop = {}) const;

private:
    struct State;
    explicit PostgresResourceLock(std::shared_ptr<State> state) noexcept;
    std::shared_ptr<State> state_;
    friend class PostgresProvider;
};

/// One authoritative PostgreSQL database. Each successful acquisition owns a
/// fresh physical session until its same-session advisory unlock completes.
class PostgresProvider final {
public:
    struct State;
    explicit PostgresProvider(PostgresOptions options);
    [[nodiscard]] PostgresResourceLock lock(std::string_view resource) const;
    [[nodiscard]] const PostgresOptions& options() const noexcept;

private:
    std::shared_ptr<State> state_;
    friend class PostgresResourceLock;
};

/// Operator-only create-if-absent metadata provisioning. Runtime constructors
/// only read and validate the row and never repair a conflict.
void provision_postgres_namespace(const PostgresOptions& options);

/// Converts the first 64 identity bits to PostgreSQL's signed bigint key.
[[nodiscard]] std::int64_t postgres_advisory_key(std::string_view resource_id);

} // namespace dilock
