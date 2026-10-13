#include "dilock/redis.hpp"

#include "metadata.hpp"
#include "redis_wait.hpp"
#include "redis_scripts.hpp"

#include <nlohmann/json.hpp>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <sw/redis++/redis++.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <mutex>
#include <random>
#include <thread>
#include <utility>

namespace dilock {
namespace {

using Json = nlohmann::json;
using Clock = std::chrono::steady_clock;
constexpr std::string_view identity_prefix{"dilock.lock.identity\0", 21};
constexpr std::string_view redis_profile{"redis-single-authority-v1"};
constexpr std::string_view identity_scheme{"sha256-framed-utf8-v1"};

void validate_utf8(std::string_view value) {
    if (value.empty() || value.size() > 1024) {
        throw Error(ErrorCode::invalid_argument, "resource must contain 1 to 1024 UTF-8 bytes");
    }
    std::size_t i = 0;
    while (i < value.size()) {
        const auto lead = static_cast<unsigned char>(value[i]);
        std::uint32_t scalar{};
        std::size_t count{};
        if (lead <= 0x7f) { scalar = lead; count = 1; }
        else if (lead >= 0xc2 && lead <= 0xdf) { scalar = lead & 0x1f; count = 2; }
        else if (lead >= 0xe0 && lead <= 0xef) { scalar = lead & 0x0f; count = 3; }
        else if (lead >= 0xf0 && lead <= 0xf4) { scalar = lead & 0x07; count = 4; }
        else { throw Error(ErrorCode::invalid_argument, "resource is not strict UTF-8"); }
        if (i + count > value.size()) {
            throw Error(ErrorCode::invalid_argument, "resource is not strict UTF-8");
        }
        for (std::size_t j = 1; j < count; ++j) {
            const auto byte = static_cast<unsigned char>(value[i + j]);
            if ((byte & 0xc0) != 0x80) {
                throw Error(ErrorCode::invalid_argument, "resource is not strict UTF-8");
            }
            scalar = (scalar << 6) | (byte & 0x3f);
        }
        if ((count == 2 && scalar < 0x80) || (count == 3 && scalar < 0x800) ||
            (count == 4 && scalar < 0x10000) || scalar > 0x10ffff ||
            (scalar >= 0xd800 && scalar <= 0xdfff) || scalar == 0) {
            throw Error(ErrorCode::invalid_argument, "resource contains an invalid Unicode scalar");
        }
        i += count;
    }
}

void validate_namespace(std::string_view name_space) {
    if (name_space.empty() || name_space.size() > 64) {
        throw Error(ErrorCode::invalid_argument, "namespace must contain 1 to 64 ASCII characters");
    }
    const auto first = name_space.front();
    if (!((first >= 'a' && first <= 'z') || (first >= '0' && first <= '9'))) {
        throw Error(ErrorCode::invalid_argument, "namespace must begin with a lowercase ASCII letter or digit");
    }
    for (const char c : name_space) {
        if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '.' || c == '-')) {
            throw Error(ErrorCode::invalid_argument, "namespace contains a character outside the draft 1 alphabet");
        }
    }
}

void append_u32_be(std::string& bytes, std::uint32_t value) {
    bytes.push_back(static_cast<char>((value >> 24) & 0xff));
    bytes.push_back(static_cast<char>((value >> 16) & 0xff));
    bytes.push_back(static_cast<char>((value >> 8) & 0xff));
    bytes.push_back(static_cast<char>(value & 0xff));
}

std::string hex_lower(const unsigned char* bytes, std::size_t size) {
    constexpr char digits[] = "0123456789abcdef";
    std::string output;
    output.reserve(size * 2);
    for (std::size_t i = 0; i < size; ++i) {
        output.push_back(digits[bytes[i] >> 4]);
        output.push_back(digits[bytes[i] & 0x0f]);
    }
    return output;
}

std::string new_owner() {
    std::array<unsigned char, 32> bytes{};
    if (RAND_bytes(bytes.data(), static_cast<int>(bytes.size())) != 1) {
        throw Error(ErrorCode::backend_unavailable, "platform CSPRNG failed to generate an owner token");
    }
    return "dilock/1/" + hex_lower(bytes.data(), bytes.size());
}

std::chrono::milliseconds checked_ttl(std::chrono::milliseconds ttl,
                                     const RedisOptions& options) {
    constexpr auto max_ttl = std::chrono::hours(24);
    if (ttl <= std::chrono::milliseconds::zero() || ttl > max_ttl ||
        ttl <= options.rpc_timeout + options.safety_margin) {
        throw Error(ErrorCode::invalid_argument,
                    "lease TTL must be positive, at most 24 hours, and exceed RPC timeout plus safety margin");
    }
    return ttl;
}

void validate_options(const RedisOptions& options) {
    validate_namespace(options.name_space);
    if (options.coordination_epoch.empty()) {
        throw Error(ErrorCode::invalid_argument, "coordination epoch must be provisioned and nonempty");
    }
    constexpr auto max_timeout = std::chrono::hours(24);
    if (options.host.empty() || options.port < 1 || options.port > 65535 || options.database < 0 ||
        options.rpc_timeout <= std::chrono::milliseconds::zero() || options.rpc_timeout > max_timeout ||
        options.cleanup_timeout <= std::chrono::milliseconds::zero() || options.cleanup_timeout > max_timeout ||
        options.safety_margin < std::chrono::milliseconds::zero() || options.safety_margin > max_timeout ||
        options.poll_interval <= std::chrono::milliseconds::zero() || options.poll_interval > max_timeout ||
        options.default_acquisition.wait <= std::chrono::milliseconds::zero() || options.default_acquisition.wait > max_timeout) {
        throw Error(ErrorCode::invalid_argument, "invalid Redis endpoint or timeout option");
    }
    (void)checked_ttl(options.default_acquisition.lease, options);
}

Json metadata_for(const RedisOptions& options) {
    return Json{{"protocol_major", 1},
                {"identity_scheme", std::string(identity_scheme)},
                {"profile", std::string(redis_profile)},
                {"coordination_epoch", options.coordination_epoch},
                {"fixed_lease_ms", nullptr}};
}

void validate_metadata(std::string_view value, const RedisOptions& options) {
    Json parsed;
    try {
        parsed = detail::parse_metadata(value);
    } catch (const std::exception&) {
        throw Error(ErrorCode::protocol_mismatch, "Redis namespace metadata is not valid JSON");
    }
    const auto expected = metadata_for(options);
    if (!detail::has_protocol_major_v1(parsed)) {
        throw Error(ErrorCode::protocol_mismatch, "Redis namespace metadata does not match this client");
    }
    for (const auto* field : {"identity_scheme", "profile", "coordination_epoch", "fixed_lease_ms"}) {
        if (!parsed.is_object() || !parsed.contains(field) || parsed[field] != expected[field]) {
            throw Error(ErrorCode::protocol_mismatch, "Redis namespace metadata does not match this client");
        }
    }
}

std::shared_ptr<sw::redis::Redis> make_redis(const RedisOptions& options) {
    sw::redis::ConnectionOptions connection;
    connection.host = options.host;
    connection.port = options.port;
    connection.user = options.username.empty() ? "default" : options.username;
    connection.password = options.password;
    connection.db = options.database;
    connection.connect_timeout = options.rpc_timeout;
    // The synchronous client cannot cancel an in-flight call. Apply the tighter
    // budget to its shared pool so the destructor's release stays bounded too.
    connection.socket_timeout = std::min(options.rpc_timeout, options.cleanup_timeout);
    connection.keep_alive = true;
    sw::redis::ConnectionPoolOptions pool;
    pool.size = 4;
    pool.wait_timeout = std::min(options.rpc_timeout, options.cleanup_timeout);
    return std::make_shared<sw::redis::Redis>(connection, pool);
}

std::string metadata_key(const RedisOptions& options) {
    return "dilock:namespace:" + options.name_space + ":meta";
}

void ensure_metadata(const std::shared_ptr<sw::redis::Redis>& redis, const RedisOptions& options) {
    try {
        const auto current = redis->get(metadata_key(options));
        if (!current) {
            throw Error(ErrorCode::protocol_mismatch, "Redis namespace metadata is missing; provision it explicitly");
        }
        validate_metadata(*current, options);
    } catch (const Error&) {
        throw;
    } catch (const std::exception&) {
        throw Error(ErrorCode::backend_unavailable, "Redis namespace metadata could not be read");
    }
}

std::string redis_key(std::string_view id) {
    return "dilock:{" + std::string(id) + "}:lease";
}

std::chrono::milliseconds wait_slice(std::chrono::milliseconds remaining,
                                    std::chrono::milliseconds configured) {
    static thread_local std::minstd_rand jitter{std::random_device{}()};
    const auto base = std::min(configured, remaining);
    const auto spread = std::max<std::int64_t>(1, base.count() / 4);
    std::uniform_int_distribution<std::int64_t> delay(0, spread);
    return std::min(remaining, base + std::chrono::milliseconds(delay(jitter)));
}

} // namespace

struct Runtime {
    RedisOptions options;
    std::shared_ptr<sw::redis::Redis> redis;
};

struct Lease::State {
    enum class Phase { held, releasing, released, lost, released_unknown };
    std::shared_ptr<Runtime> runtime;
    std::string key;
    std::string owner;
    Clock::time_point deadline;
    Phase phase{Phase::held};
    ReleaseResult terminal{ReleaseResult::unknown};
    mutable std::mutex mutex;
};

struct ResourceLock::State {
    std::shared_ptr<Runtime> runtime;
    std::string key;
};

struct RedisProvider::State {
    std::shared_ptr<Runtime> runtime;
};

std::string resource_id(std::string_view name_space, std::string_view resource) {
    validate_namespace(name_space);
    validate_utf8(resource);
    std::string framed(identity_prefix);
    append_u32_be(framed, static_cast<std::uint32_t>(name_space.size()));
    framed.append(name_space);
    append_u32_be(framed, static_cast<std::uint32_t>(resource.size()));
    framed.append(resource);
    std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
    unsigned int digest_size{};
    if (EVP_Digest(framed.data(), framed.size(), digest.data(), &digest_size, EVP_sha256(), nullptr) != 1) {
        throw Error(ErrorCode::backend_unavailable, "SHA-256 identity computation failed");
    }
    return hex_lower(digest.data(), digest_size);
}

RedisProvider::RedisProvider(RedisOptions options) {
    validate_options(options);
    auto runtime = std::make_shared<Runtime>();
    runtime->options = std::move(options);
    runtime->redis = make_redis(runtime->options);
    ensure_metadata(runtime->redis, runtime->options);
    state_ = std::make_shared<State>(State{std::move(runtime)});
}

ResourceLock RedisProvider::lock(std::string_view resource) const {
    if (!state_) throw Error(ErrorCode::invalid_argument, "provider is empty");
    auto handle = std::make_shared<ResourceLock::State>();
    handle->runtime = state_->runtime;
    handle->key = redis_key(resource_id(handle->runtime->options.name_space, resource));
    return ResourceLock(std::move(handle));
}

ResourceLock::ResourceLock(std::shared_ptr<State> state) noexcept : state_(std::move(state)) {}

std::optional<Lease> ResourceLock::try_acquire(std::chrono::milliseconds ttl,
                                                std::stop_token stop) const {
    if (!state_) throw Error(ErrorCode::invalid_argument, "resource handle is empty");
    const auto& options = state_->runtime->options;
    ttl = checked_ttl(ttl, options);
    detail::throw_if_cancelled(stop, "acquisition cancelled before sending");
    ensure_metadata(state_->runtime->redis, options);
    // Metadata I/O is synchronous and cannot be interrupted by stop_token. Do
    // not dispatch the mutating SET if cancellation arrived during that read.
    detail::throw_if_cancelled(stop, "acquisition cancelled before SET dispatch");
    const auto sent = Clock::now();
    const auto owner = new_owner();
    auto lease_state = std::make_shared<Lease::State>();
    lease_state->runtime = state_->runtime;
    lease_state->key = state_->key;
    lease_state->owner = owner;
    try {
        if (!state_->runtime->redis->set(state_->key, owner, ttl, sw::redis::UpdateType::NOT_EXIST)) {
            // SET NX reports that some key exists, regardless of its Redis
            // data type. A bounded, read-only TYPE distinguishes protocol
            // misuse from ordinary string-key contention without mutation.
            std::string key_type;
            try {
                key_type = state_->runtime->redis->type(state_->key);
            } catch (const std::exception&) {
                throw Error(ErrorCode::backend_unavailable,
                            "Redis contention key type could not be read");
            }
            if (key_type != "string" && key_type != "none") {
                throw Error(ErrorCode::protocol_mismatch,
                            "Redis lock key has an unexpected type");
            }
            // A known NX miss is ordinary contention only while the caller is
            // still interested in the result. In particular, try_acquire must
            // not return null after cancellation during the classification RPC.
            detail::throw_if_cancelled(stop, "acquisition cancelled while classifying contention");
            return std::nullopt;
        }
    } catch (const Error&) {
        throw;
    } catch (const sw::redis::ReplyError&) {
        throw Error(ErrorCode::backend_unavailable, "Redis rejected the acquisition command");
    } catch (const std::exception&) {
        // A transport failure cannot reveal whether SET reached Redis. Cleanup is owner checked;
        // if it races ahead of a delayed command, TTL remains the bounded fallback.
        try {
            (void)state_->runtime->redis->eval<long long>(
                detail::release_script, {state_->key}, {owner});
        } catch (const std::exception&) {}
        throw Error(ErrorCode::acquisition_outcome_unknown, "Redis SET outcome is unknown");
    }
    const auto deadline = sent + ttl - options.safety_margin;
    lease_state->deadline = deadline;
    if (stop.stop_requested()) {
        long long removed{};
        try {
            removed = state_->runtime->redis->eval<long long>(detail::release_script, {state_->key}, {owner});
        } catch (const std::exception&) {
            throw Error(ErrorCode::acquisition_outcome_unknown, "cancelled after acquisition; owner cleanup outcome is unknown");
        }
        const auto cleanup = removed == 1 ? ReleaseResult::released : ReleaseResult::already_not_owned;
        throw Error(detail::cancelled_acquisition_error(cleanup), "cancelled after acquisition; owner cleanup was acknowledged");
    }
    if (Clock::now() >= deadline) {
        try { (void)state_->runtime->redis->eval<long long>(detail::release_script, {state_->key}, {owner}); }
        catch (const std::exception&) {}
        throw Error(ErrorCode::acquisition_outcome_unknown, "Redis confirmed SET after the conservative lease deadline");
    }
    return Lease(std::move(lease_state));
}

Lease ResourceLock::acquire(std::chrono::milliseconds wait,
                             std::chrono::milliseconds ttl,
                             std::stop_token stop) const {
    if (wait <= std::chrono::milliseconds::zero()) {
        throw Error(ErrorCode::invalid_argument, "wait budget must be positive");
    }
    const auto until = Clock::now() + wait;
    for (;;) {
        if (stop.stop_requested()) throw Error(ErrorCode::cancelled, "acquisition cancelled while waiting");
        if (auto lease = try_acquire(ttl, stop)) {
            if (Clock::now() < until) return std::move(*lease);
            const auto cleanup = lease->release();
            const auto failure = detail::late_acquisition_error(cleanup);
            if (failure == ErrorCode::acquisition_outcome_unknown) {
                throw Error(failure, "Redis acquired after the wait deadline and owner cleanup was not acknowledged");
            }
            throw Error(failure, "Redis acquired after the wait deadline; owner cleanup was acknowledged");
        }
        const auto now = Clock::now();
        if (now >= until) throw Error(ErrorCode::wait_timeout, "Redis resource remained busy until the wait deadline");
        const auto pause = wait_slice(std::chrono::duration_cast<std::chrono::milliseconds>(until - now),
                                      state_->runtime->options.poll_interval);
        const auto wake = now + pause;
        while (!stop.stop_requested() && Clock::now() < wake) {
            std::this_thread::sleep_for(std::min(std::chrono::milliseconds(5), pause));
        }
    }
}

Lease::Lease(std::shared_ptr<State> state) noexcept : state_(std::move(state)) {}
Lease::~Lease() noexcept { dispose_scope("lease-destructor"); }
Lease::Lease(Lease&& other) noexcept : state_(std::exchange(other.state_, {})) {}
Lease& Lease::operator=(Lease&& other) noexcept {
    if (this != &other) {
        dispose_scope("lease-move-assignment");
        state_ = std::exchange(other.state_, {});
    }
    return *this;
}

bool Lease::valid() const noexcept {
    if (!state_) return false;
    std::lock_guard lock(state_->mutex);
    if (state_->phase != State::Phase::held) return false;
    if (Clock::now() >= state_->deadline) {
        state_->phase = State::Phase::lost;
        return false;
    }
    return true;
}

void Lease::dispose_scope(const char* reason) noexcept {
    if (!state_) return;
    const auto result = release();
    const auto& observer = state_->runtime->options.cleanup_observer;
    if (result != ReleaseResult::released && observer) {
        try { observer(CleanupObservation{"redis-single-authority-v1", result, reason}); }
        catch (...) { /* Diagnostics cannot escape noexcept scope cleanup. */ }
    }
}

bool Lease::renew(std::chrono::milliseconds ttl) {
    if (!state_) throw Error(ErrorCode::lease_lost, "lease is empty");
    std::lock_guard lock(state_->mutex);
    if (state_->phase != State::Phase::held || Clock::now() >= state_->deadline) {
        state_->phase = State::Phase::lost;
        throw Error(ErrorCode::lease_lost, "lease is no longer locally valid");
    }
    ttl = checked_ttl(ttl, state_->runtime->options);
    const auto sent = Clock::now();
    try {
        const auto renewed = state_->runtime->redis->eval<long long>(
            detail::renew_script, {state_->key}, {state_->owner, std::to_string(ttl.count())});
        if (renewed != 1) {
            state_->phase = State::Phase::lost;
            throw Error(ErrorCode::lease_lost, "Redis no longer recognizes this lease owner");
        }
    } catch (const Error&) {
        throw;
    } catch (const std::exception&) {
        state_->phase = State::Phase::lost;
        throw Error(ErrorCode::backend_unavailable, "Redis renewal outcome is unknown; local lease is lost");
    }
    const auto next_deadline = sent + ttl - state_->runtime->options.safety_margin;
    const auto acknowledged = Clock::now();
    // A successful server renewal cannot restore local reliance after the
    // previous confirmed window elapsed while its reply was in flight.
    if (acknowledged >= state_->deadline || acknowledged >= next_deadline) {
        state_->phase = State::Phase::lost;
        throw Error(ErrorCode::lease_lost, "renewal response arrived after a confirmed validity deadline");
    }
    state_->deadline = next_deadline;
    return true;
}

ReleaseResult Lease::release() noexcept {
    if (!state_) return ReleaseResult::already_not_owned;
    std::lock_guard lock(state_->mutex);
    if (state_->phase == State::Phase::released || state_->phase == State::Phase::released_unknown) {
        return state_->terminal;
    }
    state_->phase = State::Phase::releasing;
    try {
        const auto removed = state_->runtime->redis->eval<long long>(
            detail::release_script, {state_->key}, {state_->owner});
        state_->terminal = removed == 1 ? ReleaseResult::released : ReleaseResult::already_not_owned;
        state_->phase = State::Phase::released;
    } catch (const std::exception&) {
        state_->terminal = ReleaseResult::unknown;
        state_->phase = State::Phase::released_unknown;
    }
    return state_->terminal;
}

std::optional<Lease> ResourceLock::try_acquire(std::stop_token stop) const {
    if (!state_) throw Error(ErrorCode::invalid_argument, "resource handle is empty");
    return try_acquire(state_->runtime->options.default_acquisition, stop);
}

std::optional<Lease> ResourceLock::try_acquire(const AcquireOptions& options,
                                                std::stop_token stop) const {
    return try_acquire(options.lease, stop);
}

Lease ResourceLock::acquire(std::stop_token stop) const {
    if (!state_) throw Error(ErrorCode::invalid_argument, "resource handle is empty");
    return acquire(state_->runtime->options.default_acquisition, stop);
}

Lease ResourceLock::acquire(const AcquireOptions& options, std::stop_token stop) const {
    return acquire(options.wait, options.lease, stop);
}

void provision_redis_namespace(const RedisOptions& options) {
    validate_options(options);
    const auto redis = make_redis(options);
    const auto encoded = metadata_for(options).dump();
    try {
        if (redis->set(metadata_key(options), encoded, std::chrono::milliseconds::zero(),
                       sw::redis::UpdateType::NOT_EXIST)) {
            return;
        }
        const auto current = redis->get(metadata_key(options));
        if (!current) throw Error(ErrorCode::protocol_mismatch, "namespace metadata disappeared during provisioning");
        validate_metadata(*current, options);
    } catch (const Error&) {
        throw;
    } catch (const std::exception&) {
        throw Error(ErrorCode::backend_unavailable, "Redis namespace metadata provisioning failed");
    }
}

} // namespace dilock
