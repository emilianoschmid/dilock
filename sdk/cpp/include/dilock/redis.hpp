#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <stop_token>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace dilock {

enum class ErrorCode {
    invalid_argument,
    cancelled,
    wait_timeout,
    backend_unavailable,
    acquisition_outcome_unknown,
    protocol_mismatch,
    lease_lost,
    unsupported_capability,
};

/// An SDK error with a stable machine-readable category.
class Error final : public std::runtime_error {
public:
    Error(ErrorCode code, std::string message)
        : std::runtime_error(std::move(message)), code_(code) {}
    [[nodiscard]] ErrorCode code() const noexcept { return code_; }

private:
    ErrorCode code_;
};

/// Immutable wait and TTL policy for one acquisition.
struct AcquireOptions {
    std::chrono::milliseconds wait{5000};
    std::chrono::milliseconds lease{30000};
};

enum class ReleaseResult { released, already_not_owned, unknown };

/// Safe diagnostic for an unacknowledged scope cleanup; it contains no owner token or endpoint.
struct CleanupObservation {
    std::string backend;
    ReleaseResult outcome{ReleaseResult::unknown};
    std::string reason;
};
using CleanupObserver = std::function<void(const CleanupObservation&)>;

/// Connection and lease policy for a single authoritative Redis endpoint.
/// rpc_timeout bounds one synchronous client call. It cannot interrupt a call
/// already in flight. The safety margin is subtracted from server TTLs locally.
struct RedisOptions {
    std::string host{"127.0.0.1"};
    int port{6379};
    std::string username;
    std::string password;
    int database{0};
    std::string name_space{"default"};
    std::string coordination_epoch;
    std::chrono::milliseconds rpc_timeout{2000};
    std::chrono::milliseconds cleanup_timeout{2000};
    std::chrono::milliseconds safety_margin{25};
    std::chrono::milliseconds poll_interval{25};
    AcquireOptions default_acquisition{};
    CleanupObserver cleanup_observer{};
};

class Lease final {
public:
    Lease() noexcept = default;
    ~Lease() noexcept;

    Lease(const Lease&) = delete;
    Lease& operator=(const Lease&) = delete;
    Lease(Lease&& other) noexcept;
    Lease& operator=(Lease&& other) noexcept;

    /// True only while this process's conservative local lease deadline remains.
    [[nodiscard]] bool valid() const noexcept;

    /// Renew the same owner. Throws lease_lost when ownership is not confirmed.
    /// The reply must arrive before both the previous and next local deadlines;
    /// a lost lease can never regain validity through a delayed acknowledgement.
    /// Transport failures also lose local certainty and throw backend_unavailable.
    bool renew(std::chrono::milliseconds ttl);

    /// Perform bounded owner-checked release. Repeated calls return the cached
    /// terminal observation; no call can reactivate this lease.
    ReleaseResult release() noexcept;

private:
    struct State;
    explicit Lease(std::shared_ptr<State> state) noexcept;
    void dispose_scope(const char* reason) noexcept;
    std::shared_ptr<State> state_;
    friend class ResourceLock;
};

/// A reusable handle for one named resource. It holds shared provider state.
class ResourceLock final {
public:
    ResourceLock() noexcept = default;

    /// One network attempt. Empty means and only means ordinary contention.
    [[nodiscard]] std::optional<Lease> try_acquire(
        std::chrono::milliseconds ttl,
        std::stop_token stop = {}) const;
    /// One network attempt using provider default TTL.
    [[nodiscard]] std::optional<Lease> try_acquire(std::stop_token stop = {}) const;
    /// One network attempt using the complete acquisition policy's TTL.
    [[nodiscard]] std::optional<Lease> try_acquire(
        const AcquireOptions& options, std::stop_token stop = {}) const;

    /// Poll with bounded jitter until success, cancellation, backend error,
    /// or the supplied positive wait budget expires. A lease confirmed after
    /// that deadline is released with its owner token; acknowledged cleanup
    /// returns wait_timeout, while unknown cleanup returns acquisition-outcome-unknown.
    [[nodiscard]] Lease acquire(
        std::chrono::milliseconds wait,
        std::chrono::milliseconds ttl,
        std::stop_token stop = {}) const;
    /// Acquire using the provider's immutable default wait and lease policy.
    [[nodiscard]] Lease acquire(std::stop_token stop = {}) const;
    /// Acquire using a complete immutable wait and lease policy.
    [[nodiscard]] Lease acquire(const AcquireOptions& options,
                                std::stop_token stop = {}) const;

private:
    struct State;
    explicit ResourceLock(std::shared_ptr<State> state) noexcept;
    std::shared_ptr<State> state_;
    friend class RedisProvider;
};

/// Shared Redis client, validated namespace identity and resource factory.
class RedisProvider final {
public:
    explicit RedisProvider(RedisOptions options);

    /// Handles share this provider's connection pool and outlive the provider
    /// object when necessary. Resource names are strict UTF-8, 1..1024 bytes.
    [[nodiscard]] ResourceLock lock(std::string_view resource) const;

private:
    struct State;
    std::shared_ptr<State> state_;
};

/// Provision metadata explicitly during deployment. Uses SET NX and fails if
/// the existing JSON is malformed or incompatible; it never overwrites it.
void provision_redis_namespace(const RedisOptions& options);

/// The frozen draft 1 identity function, exposed for diagnostics and vectors.
[[nodiscard]] std::string resource_id(std::string_view name_space,
                                      std::string_view resource);

} // namespace dilock
