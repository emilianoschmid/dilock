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

/// Configuration for the optional Consul KV/session profile.
struct ConsulOptions {
    std::string endpoint{"http://127.0.0.1:8500"};
    std::string name_space{"default"};
    std::string coordination_epoch;
    std::string acl_token;
    std::chrono::milliseconds rpc_timeout{2000};
    std::chrono::milliseconds cleanup_timeout{2000};
    std::chrono::milliseconds poll_interval{50};
    AcquireOptions default_acquisition{};
    CleanupObserver cleanup_observer{};
};

/// Consul's epoch-scoped KV LockIndex. The corresponding lock key must be retained.
struct ConsulFence {
    std::string coordination_epoch;
    std::uint64_t lock_index{};
};

/// One confirmed Consul session ownership generation.
class ConsulLease final {
public:
    ConsulLease() noexcept = default;
    ~ConsulLease() noexcept;
    ConsulLease(const ConsulLease&) = delete;
    ConsulLease& operator=(const ConsulLease&) = delete;
    ConsulLease(ConsulLease&& other) noexcept;
    ConsulLease& operator=(ConsulLease&& other) noexcept;

    [[nodiscard]] bool valid() const noexcept;
    [[nodiscard]] std::chrono::milliseconds effective_ttl() const noexcept;
    [[nodiscard]] ConsulFence fence() const;
    /// Renews the same session, then confirms the same owner and LockIndex.
    bool renew();
    /// Owner-checked, bounded, idempotent cleanup.
    ReleaseResult release() noexcept;

private:
    struct State;
    explicit ConsulLease(std::shared_ptr<State> state) noexcept;
    void dispose_scope(const char* reason) noexcept;
    std::shared_ptr<State> state_;
    friend class ConsulResourceLock;
};

/// Reusable Consul lock key; each attempt creates its own TTL session.
class ConsulResourceLock final {
public:
    ConsulResourceLock() noexcept = default;
    [[nodiscard]] std::optional<ConsulLease> try_acquire(
        std::chrono::milliseconds ttl, std::stop_token stop = {}) const;
    [[nodiscard]] std::optional<ConsulLease> try_acquire(std::stop_token stop = {}) const;
    [[nodiscard]] std::optional<ConsulLease> try_acquire(
        const AcquireOptions& options, std::stop_token stop = {}) const;
    [[nodiscard]] ConsulLease acquire(
        std::chrono::milliseconds wait, std::chrono::milliseconds ttl,
        std::stop_token stop = {}) const;
    [[nodiscard]] ConsulLease acquire(std::stop_token stop = {}) const;
    [[nodiscard]] ConsulLease acquire(
        const AcquireOptions& options, std::stop_token stop = {}) const;

private:
    struct State;
    explicit ConsulResourceLock(std::shared_ptr<State> state) noexcept;
    [[nodiscard]] std::optional<ConsulLease> attempt(
        std::chrono::milliseconds wait_budget, std::chrono::milliseconds ttl,
        bool wait, std::stop_token stop) const;
    std::shared_ptr<State> state_;
    friend class ConsulProvider;
};

/// Consul HTTP authority with explicitly provisioned namespace metadata.
class ConsulProvider final {
public:
    explicit ConsulProvider(ConsulOptions options);
    [[nodiscard]] ConsulResourceLock lock(std::string_view resource) const;

private:
    struct State;
    std::shared_ptr<State> state_;
    friend class ConsulResourceLock;
};

/// Explicitly creates compatible metadata with Consul KV CAS=0; never overwrites it.
void provision_consul_namespace(const ConsulOptions& options);

} // namespace dilock
