#pragma once

#include "dilock/redis.hpp"

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>

namespace dilock {

/// Connection and identity for one etcd cluster. Namespace metadata and epoch
/// must be provisioned and approved by the deployment before connecting.
/// Construction can throw `Error` for invalid options, an unavailable backend,
/// or missing/incompatible metadata.
struct EtcdOptions {
    std::string endpoint{"127.0.0.1:2379"};
    std::string name_space{"default"};
    std::string coordination_epoch;
    std::shared_ptr<grpc::ChannelCredentials> credentials{grpc::InsecureChannelCredentials()};
    std::chrono::milliseconds rpc_timeout{2000};
    std::chrono::milliseconds cleanup_timeout{2000};
    std::chrono::milliseconds safety_margin{25};
    AcquireOptions default_acquisition{};
    CleanupObserver cleanup_observer{};
    /// Continue keepalive after native Lock succeeds. Keepalive always runs
    /// while queued so the server does not discard a waiting attempt.
    bool auto_renew{false};
};

/// etcd create_revision is an epoch-scoped fence. A protected resource must
/// atomically register it and reject stale writes; etcd cannot stop a process.
struct EtcdFence {
    std::string coordination_epoch;
    std::int64_t create_revision{};
};

class EtcdLease final {
public:
    EtcdLease() noexcept = default;
    ~EtcdLease() noexcept;
    EtcdLease(const EtcdLease&) = delete;
    EtcdLease& operator=(const EtcdLease&) = delete;
    EtcdLease(EtcdLease&& other) noexcept;
    EtcdLease& operator=(EtcdLease&& other) noexcept;

    /// Thread-safe local observation while conservative validity remains; it
    /// is not a server query and does not guarantee protected effects are safe.
    [[nodiscard]] bool valid() const noexcept;
    /// The effective whole-second TTL returned by etcd LeaseGrant.
    [[nodiscard]] std::chrono::milliseconds effective_ttl() const noexcept;
    /// The verified epoch and create_revision returned by a linearizable Range.
    [[nodiscard]] EtcdFence fence() const;
    /// Thread-safe, idempotent release: verify the exact key still belongs to
    /// this acquisition, then attempt native Unlock and revoke its unique lease.
    /// Returns released only when ownership and both Unlock and lease Revoke
    /// are acknowledged;
    /// already_not_owned when the key is absent, belongs to another lease, or
    /// the lease is already gone; unknown when ownership or cleanup cannot be
    /// confirmed. The first result is cached.
    ReleaseResult release() noexcept;

private:
    struct State;
    explicit EtcdLease(std::shared_ptr<State> state) noexcept;
    void dispose_scope(const char* reason) noexcept;
    std::shared_ptr<State> state_;
    friend class EtcdResourceLock;
};

/// Reusable handle for the shared draft-1 native etcd lock name.
class EtcdResourceLock final {
public:
    EtcdResourceLock() noexcept = default;

    /// Throws unsupported_capability: etcd's native Lock RPC cannot prove a
    /// nonblocking try result, so this profile has no safe try-acquire API.
    [[nodiscard]] std::optional<EtcdLease> try_acquire(
        std::chrono::milliseconds ttl, std::stop_token stop = {}) const;
    /// Throws unsupported_capability, preserving etcd's native Lock limitation.
    [[nodiscard]] std::optional<EtcdLease> try_acquire(std::stop_token stop = {}) const;
    /// Throws unsupported_capability regardless of the requested policy.
    [[nodiscard]] std::optional<EtcdLease> try_acquire(
        const AcquireOptions& options, std::stop_token stop = {}) const;
    /// Perform one blocking native Lock call with this explicit wait budget;
    /// setup and cleanup RPCs use provider timeouts. The stop token wakes
    /// initial keepalive confirmation and cancels the native Lock RPC; cleanup
    /// then uses an independent budget to revoke the attempt lease. Concurrent
    /// calls are independent attempts with unique leases. Throws typed `Error` values
    /// for timeout, cancellation, mismatch, backend failure, or uncertain
    /// outcomes; backend errors are never contention.
    [[nodiscard]] EtcdLease acquire(
        std::chrono::milliseconds wait, std::chrono::milliseconds ttl,
        std::stop_token stop = {}) const;
    /// Acquire using provider defaults while preserving native queue and keepalive behavior.
    [[nodiscard]] EtcdLease acquire(std::stop_token stop = {}) const;
    /// Acquire using a complete immutable wait and lease policy.
    [[nodiscard]] EtcdLease acquire(const AcquireOptions& options,
                                    std::stop_token stop = {}) const;

private:
    struct State;
    explicit EtcdResourceLock(std::shared_ptr<State> state) noexcept;
    [[nodiscard]] EtcdLease attempt(
        std::chrono::milliseconds ttl, std::chrono::milliseconds wait_budget,
        std::stop_token stop) const;
    std::shared_ptr<State> state_;
    friend class EtcdProvider;
};

/// Owns one gRPC channel; its locks and leases keep the channel alive as needed.
/// The default credentials are insecure; pass TLS credentials for remote use.
/// Handles from one provider may be used concurrently from multiple threads.
class EtcdProvider final {
public:
    explicit EtcdProvider(EtcdOptions options);
    [[nodiscard]] EtcdResourceLock lock(std::string_view resource) const;

private:
    struct State;
    std::shared_ptr<State> state_;
    friend class EtcdResourceLock;
    friend class EtcdLease;
};

} // namespace dilock
