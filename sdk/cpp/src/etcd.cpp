#include "dilock/etcd.hpp"
#include "etcd_deadline.hpp"
#include "metadata.hpp"

#include "rpc.grpc.pb.h"
#include "v3lock.grpc.pb.h"

#include <nlohmann/json.hpp>
#include <openssl/evp.h>

#include <algorithm>
#include <array>
#include <condition_variable>
#include <cstdint>
#include <limits>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

namespace dilock {
namespace {

using Clock = std::chrono::steady_clock;
using Json = nlohmann::json;
using LockStub = v3lockpb::Lock::Stub;
using LeaseStub = etcdserverpb::Lease::Stub;
using KvStub = etcdserverpb::KV::Stub;
constexpr std::string_view identity_prefix{"dilock.lock.identity\0", 21};
constexpr std::string_view identity_scheme{"sha256-framed-utf8-v1"};
constexpr std::string_view profile{"etcd-native-lock-v1"};

[[noreturn]] void fail(ErrorCode code, const char* message) {
    throw Error(code, message);
}

void validate_utf8(std::string_view value) {
    if (value.empty() || value.size() > 1024) fail(ErrorCode::invalid_argument, "resource must contain 1 to 1024 UTF-8 bytes");
    std::size_t i = 0;
    while (i < value.size()) {
        const auto first = static_cast<unsigned char>(value[i]);
        std::uint32_t scalar{};
        std::size_t count{};
        if (first <= 0x7f) { scalar = first; count = 1; }
        else if (first >= 0xc2 && first <= 0xdf) { scalar = first & 0x1f; count = 2; }
        else if (first >= 0xe0 && first <= 0xef) { scalar = first & 0x0f; count = 3; }
        else if (first >= 0xf0 && first <= 0xf4) { scalar = first & 0x07; count = 4; }
        else fail(ErrorCode::invalid_argument, "resource is not strict UTF-8");
        if (i + count > value.size()) fail(ErrorCode::invalid_argument, "resource is not strict UTF-8");
        for (std::size_t j = 1; j < count; ++j) {
            const auto byte = static_cast<unsigned char>(value[i + j]);
            if ((byte & 0xc0) != 0x80) fail(ErrorCode::invalid_argument, "resource is not strict UTF-8");
            scalar = (scalar << 6) | (byte & 0x3f);
        }
        if ((count == 2 && scalar < 0x80) || (count == 3 && scalar < 0x800) ||
            (count == 4 && scalar < 0x10000) || scalar > 0x10ffff ||
            (scalar >= 0xd800 && scalar <= 0xdfff) || scalar == 0) {
            fail(ErrorCode::invalid_argument, "resource contains an invalid Unicode scalar");
        }
        i += count;
    }
}

void validate_namespace(std::string_view value) {
    if (value.empty() || value.size() > 64) fail(ErrorCode::invalid_argument, "namespace must contain 1 to 64 ASCII characters");
    const auto first = value.front();
    if (!((first >= 'a' && first <= 'z') || (first >= '0' && first <= '9'))) {
        fail(ErrorCode::invalid_argument, "namespace must begin with a lowercase ASCII letter or digit");
    }
    for (const char c : value) {
        if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '.' || c == '-')) {
            fail(ErrorCode::invalid_argument, "namespace contains a character outside the draft 1 alphabet");
        }
    }
}

void append_u32_be(std::string& out, std::uint32_t value) {
    out.push_back(static_cast<char>((value >> 24) & 0xff));
    out.push_back(static_cast<char>((value >> 16) & 0xff));
    out.push_back(static_cast<char>((value >> 8) & 0xff));
    out.push_back(static_cast<char>(value & 0xff));
}

std::string digest_hex(std::string_view input) {
    std::array<unsigned char, 32> digest{};
    unsigned int size{};
    if (EVP_Digest(input.data(), input.size(), digest.data(), &size, EVP_sha256(), nullptr) != 1 || size != digest.size()) {
        fail(ErrorCode::backend_unavailable, "SHA-256 implementation failed");
    }
    constexpr char digits[] = "0123456789abcdef";
    std::string result;
    result.reserve(digest.size() * 2);
    for (const auto byte : digest) {
        result.push_back(digits[byte >> 4]);
        result.push_back(digits[byte & 0x0f]);
    }
    return result;
}

std::string etcd_name(std::string_view ns, std::string_view resource) {
    validate_namespace(ns);
    validate_utf8(resource);
    std::string bytes(identity_prefix);
    append_u32_be(bytes, static_cast<std::uint32_t>(ns.size()));
    bytes.append(ns);
    append_u32_be(bytes, static_cast<std::uint32_t>(resource.size()));
    bytes.append(resource);
    return "/dilock/locks/" + digest_hex(bytes);
}

std::chrono::milliseconds checked_ttl(std::chrono::milliseconds ttl, const EtcdOptions& options);

void validate_options(const EtcdOptions& options) {
    validate_namespace(options.name_space);
    validate_utf8(options.coordination_epoch);
    constexpr auto max_duration = std::chrono::hours(24);
    if (options.endpoint.empty() || !options.credentials || options.rpc_timeout <= std::chrono::milliseconds::zero() ||
        options.rpc_timeout > max_duration || options.cleanup_timeout <= std::chrono::milliseconds::zero() ||
        options.cleanup_timeout > max_duration || options.safety_margin < std::chrono::milliseconds::zero() ||
        options.safety_margin > max_duration || options.default_acquisition.wait <= std::chrono::milliseconds::zero() ||
        options.default_acquisition.wait > max_duration) {
        fail(ErrorCode::invalid_argument, "invalid etcd endpoint, credentials or timeout option");
    }
    (void)checked_ttl(options.default_acquisition.lease, options);
}

std::chrono::milliseconds checked_ttl(std::chrono::milliseconds ttl, const EtcdOptions& options) {
    constexpr auto max_ttl = std::chrono::hours(24);
    if (ttl <= std::chrono::milliseconds::zero() || ttl > max_ttl || ttl <= options.rpc_timeout + options.safety_margin) {
        fail(ErrorCode::invalid_argument, "lease TTL must be positive, at most 24 hours, and exceed RPC timeout plus safety margin");
    }
    return ttl;
}

std::int64_t ttl_seconds(std::chrono::milliseconds ttl) {
    return (ttl.count() + 999) / 1000;
}

bool validate_metadata(std::string_view raw, const EtcdOptions& options) {
    try {
        const auto json = detail::parse_metadata(raw);
        return detail::has_protocol_major_v1(json) &&
            json.value("identity_scheme", std::string{}) == identity_scheme &&
            json.value("profile", std::string{}) == profile &&
            json.value("coordination_epoch", std::string{}) == options.coordination_epoch &&
            json.contains("fixed_lease_ms") && json["fixed_lease_ms"].is_null();
    } catch (const std::exception&) {
        return false;
    }
}

} // namespace

struct EtcdProvider::State {
    EtcdOptions options;
    std::shared_ptr<grpc::Channel> channel;
    std::unique_ptr<KvStub> kv;
    std::unique_ptr<LeaseStub> leases;
    std::unique_ptr<LockStub> locks;

    explicit State(EtcdOptions value) : options(std::move(value)) {
        validate_options(options);
        channel = grpc::CreateChannel(options.endpoint, options.credentials);
        kv = etcdserverpb::KV::NewStub(channel);
        leases = etcdserverpb::Lease::NewStub(channel);
        locks = v3lockpb::Lock::NewStub(channel);
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + options.rpc_timeout);
        etcdserverpb::RangeRequest request;
        request.set_key("/dilock/namespaces/" + options.name_space + "/meta");
        etcdserverpb::RangeResponse response;
        const auto status = kv->Range(&context, request, &response);
        if (!status.ok()) fail(ErrorCode::backend_unavailable, "etcd namespace metadata could not be read");
        if (response.count() != 1 || response.kvs_size() != 1 ||
            response.kvs(0).key() != request.key() || response.kvs(0).value().empty() ||
            response.kvs(0).value().size() > 8192 || !validate_metadata(response.kvs(0).value(), options)) {
            fail(ErrorCode::protocol_mismatch, "etcd namespace metadata is missing or incompatible");
        }
    }
};

struct EtcdResourceLock::State {
    std::shared_ptr<EtcdProvider::State> provider;
    std::string name;
};

struct EtcdLease::State : std::enable_shared_from_this<EtcdLease::State> {
    enum class Phase { held, releasing, released, lost };
    std::shared_ptr<EtcdProvider::State> provider;
    std::int64_t id{};
    std::chrono::milliseconds ttl{};
    std::chrono::milliseconds margin{};
    std::string key;
    EtcdFence fence;
    bool verified{};

    mutable std::mutex mutex;
    std::condition_variable changed;
    Phase phase{Phase::held};
    detail::ConfirmedDeadline deadline;
    ReleaseResult terminal{ReleaseResult::unknown};
    bool terminal_set{};
    grpc::ClientContext* active_context{};
    std::thread keeper;
    bool stopping{};
    bool first_reply{};
    bool keeper_failed{};

    State(std::shared_ptr<EtcdProvider::State> p, std::int64_t lease_id,
          std::chrono::milliseconds effective, std::chrono::milliseconds safety,
          Clock::time_point sent)
        : provider(std::move(p)), id(lease_id), ttl(effective), margin(safety), deadline(sent + effective - safety) {}

    void start_keeper() {
        auto self = shared_from_this();
        keeper = std::thread([self = std::move(self)] { self->keepalive_loop(); });
    }

    void keepalive_loop() noexcept {
        grpc::ClientContext context;
        {
            std::lock_guard guard(mutex);
            active_context = &context;
        }
        auto stream = provider->leases->LeaseKeepAlive(&context);
        while (true) {
            {
                std::lock_guard guard(mutex);
                if (stopping || phase != Phase::held) break;
            }
            const auto sent = Clock::now();
            etcdserverpb::LeaseKeepAliveRequest request;
            request.set_id(id);
            if (!stream->Write(request)) { keeper_fail(); break; }
            etcdserverpb::LeaseKeepAliveResponse response;
            if (!stream->Read(&response) || response.id() != id || response.ttl() <= 0) { keeper_fail(); break; }
            {
                std::lock_guard guard(mutex);
                if (phase != Phase::held || stopping) break;
                const auto now = Clock::now();
                if (!deadline.confirm(sent, std::chrono::seconds(response.ttl()), margin, now)) {
                    phase = Phase::lost;
                    keeper_failed = true;
                    changed.notify_all();
                    break;
                }
                first_reply = true;
                changed.notify_all();
            }
            std::unique_lock guard(mutex);
            const auto interval = std::max(std::chrono::milliseconds(10), ttl / 3);
            const auto confirmed = deadline.snapshot(Clock::now());
            if (!confirmed) {
                if (phase == Phase::held) phase = Phase::lost;
                keeper_failed = true;
                changed.notify_all();
                break;
            }
            const auto wake = std::min(*confirmed, Clock::now() + interval);
            changed.wait_until(guard, wake, [this] { return stopping || phase != Phase::held; });
        }
        stream->WritesDone();
        (void)stream->Finish();
        {
            std::lock_guard guard(mutex);
            active_context = nullptr;
            changed.notify_all();
        }
    }

    void keeper_fail() noexcept {
        std::lock_guard guard(mutex);
        if (phase == Phase::held) phase = Phase::lost;
        keeper_failed = true;
        changed.notify_all();
    }

    bool await_initial(std::chrono::milliseconds budget, std::stop_token stop) {
        std::unique_lock guard(mutex);
        return detail::wait_until_ready_or_stopped(changed, guard, budget,
            [this] { return first_reply || keeper_failed || stopping; }, stop) && first_reply;
    }

    void stop_keeper() noexcept {
        {
            std::lock_guard guard(mutex);
            stopping = true;
            changed.notify_all();
            if (active_context) active_context->TryCancel();
        }
        if (keeper.joinable() && keeper.get_id() != std::this_thread::get_id()) keeper.join();
    }

    bool valid() noexcept {
        std::lock_guard guard(mutex);
        if (phase != Phase::held) return false;
        if (!deadline.valid(Clock::now())) {
            phase = Phase::lost;
            keeper_failed = true;
            changed.notify_all();
            return false;
        }
        return true;
    }

    std::optional<Clock::time_point> deadline_snapshot() noexcept {
        std::lock_guard guard(mutex);
        if (phase != Phase::held) return std::nullopt;
        auto result = deadline.snapshot(Clock::now());
        if (!result) {
            phase = Phase::lost;
            keeper_failed = true;
            changed.notify_all();
        }
        return result;
    }

    bool abandon() noexcept {
        stop_keeper();
        {
            std::lock_guard guard(mutex);
            if (phase == Phase::held) phase = Phase::lost;
        }
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + provider->options.cleanup_timeout);
        etcdserverpb::LeaseRevokeRequest request;
        request.set_id(id);
        etcdserverpb::LeaseRevokeResponse response;
        return provider->leases->LeaseRevoke(&context, request, &response).ok();
    }

    ReleaseResult release() noexcept {
        {
            std::unique_lock guard(mutex);
            if (terminal_set) return terminal;
            if (phase == Phase::releasing) {
                changed.wait(guard, [this] { return terminal_set; });
                return terminal;
            }
            if (!verified || key.empty()) {
                phase = Phase::lost;
                terminal_set = true;
                terminal = ReleaseResult::already_not_owned;
                return terminal;
            }
            phase = Phase::releasing;
        }
        const auto cleanup_deadline = Clock::now() + provider->options.cleanup_timeout;
        stop_keeper();
        const auto remaining_budget = [&] {
            const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(cleanup_deadline - Clock::now());
            return std::max(std::chrono::milliseconds::zero(), remaining);
        };
        detail::EtcdOwnershipObservation ownership = detail::EtcdOwnershipObservation::unknown;
        const auto range_budget = remaining_budget();
        if (range_budget > std::chrono::milliseconds::zero()) {
            grpc::ClientContext range_context;
            range_context.set_deadline(std::chrono::system_clock::now() + range_budget);
            etcdserverpb::RangeRequest range_request;
            range_request.set_key(key);
            etcdserverpb::RangeResponse range_response;
            const auto range_status = provider->kv->Range(&range_context, range_request, &range_response);
            if (range_status.ok()) {
                const auto exact_key = std::find_if(
                    range_response.kvs().begin(), range_response.kvs().end(),
                    [this](const auto& entry) { return entry.key() == key; });
                ownership = exact_key != range_response.kvs().end() && exact_key->lease() == id
                    ? detail::EtcdOwnershipObservation::owned
                    : detail::EtcdOwnershipObservation::not_owned;
            }
        }

        grpc::ClientContext unlock_context;
        const auto unlock_budget = remaining_budget();
        v3lockpb::UnlockRequest unlock_request;
        unlock_request.set_key(key);
        v3lockpb::UnlockResponse unlock_response;
        grpc::Status unlock_status;
        bool unlock_acknowledged = false;
        if (ownership != detail::EtcdOwnershipObservation::not_owned &&
            unlock_budget > std::chrono::milliseconds::zero()) {
            unlock_context.set_deadline(std::chrono::system_clock::now() + unlock_budget);
            unlock_status = provider->locks->Unlock(&unlock_context, unlock_request, &unlock_response);
            unlock_acknowledged = unlock_status.ok();
        }

        grpc::ClientContext revoke_context;
        const auto revoke_budget = remaining_budget();
        etcdserverpb::LeaseRevokeRequest revoke_request;
        revoke_request.set_id(id);
        etcdserverpb::LeaseRevokeResponse revoke_response;
        grpc::Status revoke_status;
        bool revoke_acknowledged = false;
        if (revoke_budget > std::chrono::milliseconds::zero()) {
            revoke_context.set_deadline(std::chrono::system_clock::now() + revoke_budget);
            revoke_status = provider->leases->LeaseRevoke(&revoke_context, revoke_request, &revoke_response);
            revoke_acknowledged = revoke_status.ok();
        }
        // etcd Unlock may acknowledge a no-op after the lease expires between
        // Range and Unlock. Require the unique lease Revoke ACK as well before
        // reporting Released; if it is already gone, or only revoke is known,
        // classify conservatively as AlreadyNotOwned.
        const auto outcome = detail::classify_etcd_release(
            unlock_acknowledged, ownership, revoke_acknowledged,
            revoke_status.error_code() == grpc::StatusCode::NOT_FOUND);
        {
            std::lock_guard guard(mutex);
            phase = outcome == ReleaseResult::released ? Phase::released : Phase::lost;
            terminal = outcome;
            terminal_set = true;
            changed.notify_all();
        }
        return outcome;
    }
};

EtcdProvider::EtcdProvider(EtcdOptions options) : state_(std::make_shared<State>(std::move(options))) {}

EtcdResourceLock EtcdProvider::lock(std::string_view resource) const {
    if (!state_) fail(ErrorCode::invalid_argument, "provider is empty");
    auto handle = std::make_shared<EtcdResourceLock::State>();
    handle->provider = state_;
    handle->name = etcd_name(state_->options.name_space, resource);
    return EtcdResourceLock(std::move(handle));
}

EtcdResourceLock::EtcdResourceLock(std::shared_ptr<State> state) noexcept : state_(std::move(state)) {}
EtcdLease::EtcdLease(std::shared_ptr<State> state) noexcept : state_(std::move(state)) {}
EtcdLease::~EtcdLease() noexcept { dispose_scope("lease-destructor"); }
EtcdLease::EtcdLease(EtcdLease&& other) noexcept : state_(std::move(other.state_)) {}
EtcdLease& EtcdLease::operator=(EtcdLease&& other) noexcept {
    if (this != &other) { dispose_scope("lease-move-assignment"); state_ = std::move(other.state_); }
    return *this;
}
bool EtcdLease::valid() const noexcept { return state_ && state_->valid(); }
std::chrono::milliseconds EtcdLease::effective_ttl() const noexcept { return state_ ? state_->ttl : std::chrono::milliseconds::zero(); }
EtcdFence EtcdLease::fence() const {
    if (!state_) fail(ErrorCode::lease_lost, "lease is empty");
    std::lock_guard guard(state_->mutex);
    if (!state_->verified) fail(ErrorCode::lease_lost, "lease has no verified fence");
    return state_->fence;
}
ReleaseResult EtcdLease::release() noexcept { return state_ ? state_->release() : ReleaseResult::already_not_owned; }

void EtcdLease::dispose_scope(const char* reason) noexcept {
    if (!state_) return;
    const auto result = state_->release();
    const auto& observer = state_->provider->options.cleanup_observer;
    if (result != ReleaseResult::released && observer) {
        try { observer(CleanupObservation{"etcd-native-lock-v1", result, reason}); }
        catch (...) { /* Diagnostics cannot escape noexcept scope cleanup. */ }
    }
}

std::optional<EtcdLease> EtcdResourceLock::try_acquire(std::chrono::milliseconds ttl, std::stop_token stop) const {
    if (!state_) fail(ErrorCode::invalid_argument, "resource handle is empty");
    (void)ttl;
    (void)stop;
    fail(ErrorCode::unsupported_capability, "etcd native Lock does not provide a safe try-acquire operation");
}

std::optional<EtcdLease> EtcdResourceLock::try_acquire(std::stop_token stop) const {
    if (!state_) fail(ErrorCode::invalid_argument, "resource handle is empty");
    return try_acquire(state_->provider->options.default_acquisition.lease, stop);
}

std::optional<EtcdLease> EtcdResourceLock::try_acquire(const AcquireOptions& options,
                                                        std::stop_token stop) const {
    if (!state_) fail(ErrorCode::invalid_argument, "resource handle is empty");
    return try_acquire(options.lease, stop);
}

EtcdLease EtcdResourceLock::attempt(std::chrono::milliseconds ttl,
                                    std::chrono::milliseconds wait_budget,
                                    std::stop_token stop) const {
    if (!state_) fail(ErrorCode::invalid_argument, "resource handle is empty");
    const auto& provider = state_->provider;
    const auto& options = provider->options;
    ttl = checked_ttl(ttl, options);
    if (wait_budget <= std::chrono::milliseconds::zero()) fail(ErrorCode::wait_timeout, "etcd attempt budget expired before sending");
    if (stop.stop_requested()) fail(ErrorCode::cancelled, "acquisition cancelled before sending");
    const auto operation_deadline = Clock::now() + wait_budget;
    const auto make_deadline = [&](std::chrono::milliseconds maximum) {
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(operation_deadline - Clock::now());
        if (remaining <= std::chrono::milliseconds::zero()) fail(ErrorCode::wait_timeout, "etcd acquisition budget expired");
        return std::chrono::system_clock::now() + std::min(maximum, remaining);
    };

    const auto metadata_key = "/dilock/namespaces/" + options.name_space + "/meta";
    etcdserverpb::RangeRequest metadata_request;
    metadata_request.set_key(metadata_key);
    etcdserverpb::RangeResponse metadata_response;
    grpc::ClientContext metadata_context;
    metadata_context.set_deadline(make_deadline(options.rpc_timeout));
    const auto metadata_status = provider->kv->Range(&metadata_context, metadata_request, &metadata_response);
    if (!metadata_status.ok()) {
        if (Clock::now() >= operation_deadline) fail(ErrorCode::wait_timeout, "etcd acquisition budget expired during metadata validation");
        fail(ErrorCode::backend_unavailable, "etcd namespace metadata could not be read");
    }
    if (metadata_response.count() != 1 || metadata_response.kvs_size() != 1 ||
        metadata_response.kvs(0).key() != metadata_key || metadata_response.kvs(0).value().empty() ||
        metadata_response.kvs(0).value().size() > 8192 || !validate_metadata(metadata_response.kvs(0).value(), options)) {
        fail(ErrorCode::protocol_mismatch, "etcd namespace metadata is missing or incompatible");
    }
    if (stop.stop_requested()) fail(ErrorCode::cancelled, "acquisition cancelled during metadata validation");

    const auto requested_seconds = ttl_seconds(ttl);
    auto sent = Clock::now();
    grpc::ClientContext grant_context;
    grant_context.set_deadline(make_deadline(options.rpc_timeout));
    std::stop_callback cancel_grant(stop, [&grant_context] { grant_context.TryCancel(); });
    etcdserverpb::LeaseGrantRequest grant_request;
    grant_request.set_ttl(requested_seconds);
    etcdserverpb::LeaseGrantResponse grant;
    const auto grant_status = provider->leases->LeaseGrant(&grant_context, grant_request, &grant);
    if (!grant_status.ok()) {
        if (stop.stop_requested()) fail(ErrorCode::acquisition_outcome_unknown, "cancelled LeaseGrant acknowledgement and lease cleanup are unknown");
        fail(ErrorCode::acquisition_outcome_unknown, "etcd LeaseGrant outcome was not acknowledged");
    }
    if (grant.id() == 0) {
        fail(ErrorCode::acquisition_outcome_unknown, "etcd returned an invalid lease ID; attempt cleanup cannot be addressed safely");
    }
    if (grant.ttl() <= 0) {
        grpc::ClientContext cleanup;
        cleanup.set_deadline(std::chrono::system_clock::now() + options.cleanup_timeout);
        etcdserverpb::LeaseRevokeRequest revoke_request;
        revoke_request.set_id(grant.id());
        etcdserverpb::LeaseRevokeResponse revoke_response;
        const bool revoked = provider->leases->LeaseRevoke(&cleanup, revoke_request, &revoke_response).ok();
        const auto failure = detail::classify_abandoned_acquisition(ErrorCode::protocol_mismatch, revoked);
        fail(failure, revoked ? "etcd returned a non-positive LeaseGrant TTL; the lease was revoked"
                              : "etcd returned a non-positive LeaseGrant TTL and revocation was not acknowledged");
    }
    if (stop.stop_requested()) {
        grpc::ClientContext cleanup;
        cleanup.set_deadline(std::chrono::system_clock::now() + options.cleanup_timeout);
        etcdserverpb::LeaseRevokeRequest revoke_request;
        revoke_request.set_id(grant.id());
        etcdserverpb::LeaseRevokeResponse revoke_response;
        if (!provider->leases->LeaseRevoke(&cleanup, revoke_request, &revoke_response).ok()) {
            fail(ErrorCode::acquisition_outcome_unknown, "cancelled etcd LeaseGrant could not be revoked");
        }
        fail(ErrorCode::cancelled, "acquisition cancelled after LeaseGrant; its lease was revoked");
    }
    const auto effective = std::chrono::seconds(grant.ttl());
    const auto margin = std::max(options.safety_margin, std::chrono::milliseconds(10));
    if (std::chrono::duration_cast<std::chrono::milliseconds>(effective) <= margin + options.rpc_timeout) {
        grpc::ClientContext cleanup;
        cleanup.set_deadline(std::chrono::system_clock::now() + options.cleanup_timeout);
        etcdserverpb::LeaseRevokeRequest revoke_request;
        revoke_request.set_id(grant.id());
        etcdserverpb::LeaseRevokeResponse revoke_response;
        const bool revoked = provider->leases->LeaseRevoke(&cleanup, revoke_request, &revoke_response).ok();
        const auto failure = detail::classify_abandoned_acquisition(ErrorCode::invalid_argument, revoked);
        fail(failure, revoked ? "effective etcd TTL does not leave enough time for RPCs and safety margin; the lease was revoked"
                              : "effective etcd TTL is too short and lease revocation was not acknowledged");
    }
    auto lease = std::make_shared<EtcdLease::State>(provider, grant.id(), effective, margin, sent);
    lease->start_keeper();
    const auto after_grant_budget = std::chrono::duration_cast<std::chrono::milliseconds>(operation_deadline - Clock::now());
    if (after_grant_budget <= std::chrono::milliseconds::zero()) {
        const auto failure = detail::classify_abandoned_acquisition(
            ErrorCode::wait_timeout, lease->abandon());
        fail(failure, failure == ErrorCode::wait_timeout
            ? "etcd acquisition budget expired after LeaseGrant; its lease was revoked"
            : "etcd acquisition budget expired after LeaseGrant and lease revocation was not acknowledged");
    }
    if (!lease->await_initial(std::min({options.rpc_timeout, after_grant_budget,
            std::chrono::duration_cast<std::chrono::milliseconds>(effective) - margin}), stop)) {
        const bool cancelled = stop.stop_requested();
        const bool wait_expired = Clock::now() >= operation_deadline;
        const bool revoked = lease->abandon();
        const auto failure = detail::classify_initial_keepalive_failure(cancelled, wait_expired, revoked);
        if (failure == ErrorCode::cancelled)
            fail(failure, "acquisition cancelled while confirming etcd keepalive; its lease was revoked");
        if (failure == ErrorCode::wait_timeout)
            fail(failure, "acquisition wait budget expired while confirming etcd keepalive; its lease was revoked");
        if (failure == ErrorCode::acquisition_outcome_unknown)
            fail(failure, "initial etcd keepalive was not confirmed and its lease revoke was not acknowledged");
        fail(failure, "initial etcd keepalive was not confirmed; its lease was revoked");
    }

    grpc::ClientContext lock_context;
    const auto lock_budget = std::chrono::duration_cast<std::chrono::milliseconds>(operation_deadline - Clock::now());
    if (lock_budget <= std::chrono::milliseconds::zero()) {
        const auto failure = detail::classify_abandoned_acquisition(
            ErrorCode::wait_timeout, lease->abandon());
        fail(failure, failure == ErrorCode::wait_timeout
            ? "etcd acquisition budget expired before native Lock; its lease was revoked"
            : "etcd acquisition budget expired before native Lock and lease revocation was not acknowledged");
    }
    // Native Lock is a long-poll: its deadline is the caller's acquisition
    // budget, not the shorter setup-RPC timeout. Cutting it off at rpc_timeout
    // would turn a valid queued wait into an ambiguous backend failure.
    lock_context.set_deadline(std::chrono::system_clock::now() + lock_budget);
    std::stop_callback cancel_lock(stop, [&lock_context] { lock_context.TryCancel(); });
    v3lockpb::LockRequest lock_request;
    lock_request.set_name(state_->name);
    lock_request.set_lease(grant.id());
    v3lockpb::LockResponse locked;
    const auto lock_status = provider->locks->Lock(&lock_context, lock_request, &locked);
    if (!lock_status.ok()) {
        const bool revoked = lease->abandon();
        if (!revoked) fail(ErrorCode::acquisition_outcome_unknown, "etcd Lock failed and its attempt lease revoke was not acknowledged");
        const bool caller_deadline_expired = lock_status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED &&
            Clock::now() >= operation_deadline;
        const auto failure = detail::classify_lock_failure(stop.stop_requested(), caller_deadline_expired);
        if (failure == ErrorCode::cancelled)
            fail(failure, "etcd Lock was cancelled and its lease revoke was attempted");
        if (failure == ErrorCode::wait_timeout)
            fail(failure, "etcd acquisition wait budget expired and its attempt lease was revoked");
        fail(ErrorCode::backend_unavailable, "etcd native Lock RPC failed; the attempt lease was revoked");
    }
    if (stop.stop_requested()) {
        if (!lease->abandon()) fail(ErrorCode::acquisition_outcome_unknown, "cancelled etcd Lock acquired a key but its lease revoke was not acknowledged");
        fail(ErrorCode::cancelled, "acquisition cancelled after etcd Lock; its lease was revoked");
    }
    if (locked.key().empty()) {
        const auto failure = detail::classify_abandoned_acquisition(
            ErrorCode::protocol_mismatch, lease->abandon());
        fail(failure, failure == ErrorCode::protocol_mismatch
            ? "etcd Lock returned an empty ownership key; its lease was revoked"
            : "etcd Lock returned an empty ownership key and lease revocation was not acknowledged");
    }
    lease->key = locked.key();

    etcdserverpb::RangeRequest range_request;
    range_request.set_key(locked.key());
    etcdserverpb::RangeResponse ranged;
    grpc::ClientContext range_context;
    const auto confirmed_deadline = lease->deadline_snapshot();
    if (!confirmed_deadline) {
        (void)lease->abandon();
        fail(ErrorCode::acquisition_outcome_unknown, "etcd lease expired before ownership verification could be budgeted");
    }
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(*confirmed_deadline - Clock::now());
    range_context.set_deadline(std::chrono::system_clock::now() + std::max(std::chrono::milliseconds::zero(), std::min(options.rpc_timeout, remaining)));
    const auto range_status = provider->kv->Range(&range_context, range_request, &ranged);
    if (!range_status.ok()) {
        (void)lease->abandon();
        fail(ErrorCode::acquisition_outcome_unknown, "linearizable Range on the returned etcd Lock key was not acknowledged");
    }
    if (ranged.count() != 1 || ranged.kvs_size() != 1 || ranged.kvs(0).key() != locked.key() ||
        ranged.kvs(0).lease() != grant.id() || ranged.kvs(0).create_revision() <= 0) {
        const auto failure = detail::classify_abandoned_acquisition(
            ErrorCode::protocol_mismatch, lease->abandon());
        fail(failure, failure == ErrorCode::protocol_mismatch
            ? "etcd Lock key is missing or attached to a different lease; its lease was revoked"
            : "etcd Lock key verification failed and lease revocation was not acknowledged");
    }
    if (!lease->valid()) {
        (void)lease->abandon();
        fail(ErrorCode::acquisition_outcome_unknown, "etcd ownership verification exceeded conservative lease validity");
    }
    if (stop.stop_requested()) {
        if (!lease->abandon()) fail(ErrorCode::acquisition_outcome_unknown, "cancelled etcd verification could not revoke its lease");
        fail(ErrorCode::cancelled, "acquisition cancelled during etcd ownership verification; its lease was revoked");
    }
    lease->fence = EtcdFence{options.coordination_epoch, ranged.kvs(0).create_revision()};
    lease->verified = true;
    if (!options.auto_renew) lease->stop_keeper();
    return EtcdLease(std::move(lease));
}

EtcdLease EtcdResourceLock::acquire(std::chrono::milliseconds wait, std::chrono::milliseconds ttl,
                                    std::stop_token stop) const {
    if (wait <= std::chrono::milliseconds::zero() || wait > std::chrono::hours(24)) {
        fail(ErrorCode::invalid_argument, "wait must be positive and at most 24 hours");
    }
    if (!state_) fail(ErrorCode::invalid_argument, "resource handle is empty");
    if (stop.stop_requested()) fail(ErrorCode::cancelled, "acquisition cancelled before sending");
    return attempt(ttl, wait, stop);
}

EtcdLease EtcdResourceLock::acquire(std::stop_token stop) const {
    if (!state_) fail(ErrorCode::invalid_argument, "resource handle is empty");
    return acquire(state_->provider->options.default_acquisition, stop);
}

EtcdLease EtcdResourceLock::acquire(const AcquireOptions& options, std::stop_token stop) const {
    if (options.wait <= std::chrono::milliseconds::zero() || options.wait > std::chrono::hours(24)) {
        fail(ErrorCode::invalid_argument, "wait must be positive and at most 24 hours");
    }
    return acquire(options.wait, options.lease, stop);
}

} // namespace dilock
