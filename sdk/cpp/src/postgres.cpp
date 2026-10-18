#include "dilock/postgres.hpp"

#include <libpq-fe.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdint>
#include <limits>
#include <mutex>
#include <random>
#include <string>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

namespace dilock {
namespace {

constexpr std::string_view profile{"postgres-advisory-session-v1"};
constexpr std::string_view scheme{"sha256-framed-utf8-v1"};

void validate_namespace(std::string_view value) {
    if (value.empty() || value.size() > 64 ||
        (!std::islower(static_cast<unsigned char>(value.front())) &&
        !std::isdigit(static_cast<unsigned char>(value.front())))
    )
        throw Error(ErrorCode::invalid_argument, "namespace is outside the draft 1 alphabet");
    for (const char c : value) {
        if (!(std::islower(static_cast<unsigned char>(c)) || std::isdigit(static_cast<unsigned char>(c)) || c == '_' || c == '.' || c == '-'))
            throw Error(ErrorCode::invalid_argument, "namespace is outside the draft 1 alphabet");
    }
}

void check_options(const PostgresOptions& options) {
    validate_namespace(options.name_space);
    if (options.connection_string.empty() || options.coordination_epoch.empty() || options.coordination_epoch.size() > 1024)
        throw Error(ErrorCode::invalid_argument, "PostgreSQL connection string and coordination epoch are required");
    if (options.rpc_timeout <= std::chrono::milliseconds::zero() || options.rpc_timeout > std::chrono::hours(24) ||
        options.cleanup_timeout <= std::chrono::milliseconds::zero() || options.cleanup_timeout > std::chrono::hours(24) ||
        options.poll_interval <= std::chrono::milliseconds::zero() || options.poll_interval > std::chrono::hours(24) ||
        options.default_acquisition.wait <= std::chrono::milliseconds::zero() || options.default_acquisition.wait > std::chrono::hours(24))
        throw Error(ErrorCode::invalid_argument, "invalid PostgreSQL timeout policy");
    if (options.default_acquisition.lease <= std::chrono::milliseconds::zero() || options.default_acquisition.lease > std::chrono::hours(24))
        throw Error(ErrorCode::invalid_argument, "invalid PostgreSQL acquisition policy");
}

PGconn* connect_session(const PostgresOptions& options) {
    PGconn* connection = PQconnectdb(options.connection_string.c_str());
    if (connection == nullptr || PQstatus(connection) != CONNECTION_OK) {
        const std::string message = connection == nullptr ? "PostgreSQL connection allocation failed" : PQerrorMessage(connection);
        if (connection != nullptr) PQfinish(connection);
        throw Error(ErrorCode::backend_unavailable, message);
    }
    const std::string timeout = "SET statement_timeout = " + std::to_string(options.rpc_timeout.count());
    PGresult* result = PQexec(connection, timeout.c_str());
    if (result == nullptr || PQresultStatus(result) != PGRES_COMMAND_OK) {
        const std::string message = PQerrorMessage(connection);
        if (result != nullptr) PQclear(result);
        PQfinish(connection);
        throw Error(ErrorCode::backend_unavailable, message.empty() ? "PostgreSQL session setup failed" : message);
    }
    PQclear(result);
    return connection;
}

void finish(PGconn* connection) noexcept { if (connection != nullptr) PQfinish(connection); }

void validate_metadata(PGconn* connection, const PostgresOptions& options) {
    const char* values[] = {options.name_space.c_str()};
    std::vector<const char*> params{values, values + 1};
    PGresult* result = PQexecParams(connection,
        "SELECT protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms FROM dilock_namespace_meta WHERE namespace = $1",
        1, nullptr, params.data(), nullptr, nullptr, 0);
    if (result == nullptr || PQresultStatus(result) != PGRES_TUPLES_OK) {
        const std::string message = PQerrorMessage(connection);
        if (result != nullptr) PQclear(result);
        throw Error(ErrorCode::backend_unavailable, message.empty() ? "PostgreSQL metadata query failed" : message);
    }
    if (PQntuples(result) != 1 || PQnfields(result) != 5 || std::string(PQgetvalue(result, 0, 0)) != "1" ||
        std::string(PQgetvalue(result, 0, 1)) != scheme || std::string(PQgetvalue(result, 0, 2)) != profile ||
        std::string(PQgetvalue(result, 0, 3)) != options.coordination_epoch || !PQgetisnull(result, 0, 4)) {
        PQclear(result);
        throw Error(ErrorCode::protocol_mismatch, "PostgreSQL namespace metadata is missing or incompatible");
    }
    PQclear(result);
}

std::chrono::milliseconds pause_for(std::chrono::milliseconds remaining, std::chrono::milliseconds configured) {
    static thread_local std::minstd_rand jitter{std::random_device{}()};
    std::uniform_int_distribution<std::int64_t> spread(0, std::max<std::int64_t>(1, configured.count() / 4));
    return std::min(remaining, configured + std::chrono::milliseconds(spread(jitter)));
}

} // namespace

struct PostgresProvider::State {
    explicit State(PostgresOptions value) : options(std::move(value)) {}
    PostgresOptions options;
    std::mutex mutex;
    bool closed{false};
};
struct PostgresResourceLock::State { std::shared_ptr<PostgresProvider::State> provider; std::int64_t key{}; };
struct PostgresLease::State {
    State(std::shared_ptr<PostgresProvider::State> provider_value, PGconn* connection_value,
        std::int64_t key_value)
        : provider(std::move(provider_value)), connection(connection_value), key(key_value) {}
    std::shared_ptr<PostgresProvider::State> provider;
    PGconn* connection{};
    std::int64_t key{};
    bool held{true};
    bool release_unknown{false};
    std::mutex mutex;
};

std::int64_t postgres_advisory_key(std::string_view resource_id) {
    if (resource_id.size() != 64 || !std::all_of(resource_id.begin(), resource_id.end(), [](char c) { return std::isxdigit(static_cast<unsigned char>(c)) && std::tolower(static_cast<unsigned char>(c)) == c; }))
        throw Error(ErrorCode::invalid_argument, "resource id must be a lowercase SHA-256 digest");
    std::uint64_t value{};
    for (const char c : resource_id.substr(0, 16)) {
        value = (value << 4) | static_cast<std::uint64_t>(std::isdigit(static_cast<unsigned char>(c)) ? c - '0' : c - 'a' + 10);
    }
    if (value <= static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()))
        return static_cast<std::int64_t>(value);
    // Avoid an out-of-range unsigned-to-signed cast: the profile defines the
    // hexadecimal prefix as a signed two's-complement int64 value.
    const auto magnitude = std::numeric_limits<std::uint64_t>::max() - value;
    return -static_cast<std::int64_t>(magnitude) - 1;
}

PostgresProvider::PostgresProvider(PostgresOptions options) {
    check_options(options);
    auto state = std::make_shared<State>(std::move(options));
    PGconn* connection = connect_session(state->options);
    try { validate_metadata(connection, state->options); }
    catch (...) { finish(connection); throw; }
    finish(connection);
    state_ = std::move(state);
}

const PostgresOptions& PostgresProvider::options() const noexcept { return state_->options; }

PostgresResourceLock PostgresProvider::lock(std::string_view resource) const {
    if (!state_) throw Error(ErrorCode::invalid_argument, "PostgreSQL provider is empty");
    { std::lock_guard guard(state_->mutex); if (state_->closed) throw Error(ErrorCode::invalid_argument, "PostgreSQL provider is closed"); }
    auto handle = std::make_shared<PostgresResourceLock::State>();
    handle->provider = state_;
    handle->key = postgres_advisory_key(resource_id(state_->options.name_space, resource));
    return PostgresResourceLock{std::move(handle)};
}

PostgresResourceLock::PostgresResourceLock(std::shared_ptr<State> state) noexcept : state_(std::move(state)) {}
PostgresLease::PostgresLease(std::shared_ptr<State> state) noexcept : state_(std::move(state)) {}
PostgresLease::~PostgresLease() noexcept { if (state_) (void)release(); }
PostgresLease::PostgresLease(PostgresLease&& other) noexcept : state_(std::move(other.state_)) {}
PostgresLease& PostgresLease::operator=(PostgresLease&& other) noexcept { if (this != &other) { if (state_) (void)release(); state_ = std::move(other.state_); } return *this; }

std::optional<PostgresLease> PostgresResourceLock::try_acquire(std::stop_token stop) const {
    if (!state_) throw Error(ErrorCode::invalid_argument, "PostgreSQL lock is empty");
    return try_acquire(state_->provider->options.default_acquisition, stop);
}

std::optional<PostgresLease> PostgresResourceLock::try_acquire(const AcquireOptions&, std::stop_token stop) const {
    if (!state_) throw Error(ErrorCode::invalid_argument, "PostgreSQL lock is empty");
    if (stop.stop_requested()) throw Error(ErrorCode::cancelled, "PostgreSQL acquisition cancelled before send");
    PGconn* connection = connect_session(state_->provider->options);
    bool ownership_transferred = false;
    try {
        const std::string key = std::to_string(state_->key);
        const char* values[] = {key.c_str()};
        std::vector<const char*> params{values, values + 1};
        PGresult* result = PQexecParams(connection, "SELECT pg_try_advisory_lock($1::bigint)", 1, nullptr, params.data(), nullptr, nullptr, 0);
        if (result == nullptr || PQresultStatus(result) != PGRES_TUPLES_OK || PQntuples(result) != 1) {
            if (result != nullptr) PQclear(result);
            finish(connection);
            throw Error(ErrorCode::acquisition_outcome_unknown, "PostgreSQL advisory-lock response is unknown");
        }
        const bool acquired = std::string(PQgetvalue(result, 0, 0)) == "t";
        PQclear(result);
        if (!acquired) { finish(connection); return std::nullopt; }
        auto lease = std::make_shared<PostgresLease::State>(state_->provider, connection, state_->key);
        ownership_transferred = true;
        return PostgresLease{std::move(lease)};
    } catch (...) { if (!ownership_transferred) finish(connection); throw; }
}

PostgresLease PostgresResourceLock::acquire(std::stop_token stop) const { return acquire(state_->provider->options.default_acquisition, stop); }
PostgresLease PostgresResourceLock::acquire(const AcquireOptions& options, std::stop_token stop) const {
    const auto deadline = std::chrono::steady_clock::now() + options.wait;
    for (;;) {
        if (stop.stop_requested()) throw Error(ErrorCode::cancelled, "PostgreSQL acquisition cancelled");
        if (auto lease = try_acquire(options, stop)) return std::move(*lease);
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
        if (remaining <= std::chrono::milliseconds::zero()) throw Error(ErrorCode::wait_timeout, "PostgreSQL advisory-lock wait exhausted");
        std::this_thread::sleep_for(pause_for(remaining, state_->provider->options.poll_interval));
    }
}

bool PostgresLease::valid() const noexcept { if (!state_) return false; std::lock_guard guard(state_->mutex); return state_->held && !state_->release_unknown; }

bool PostgresLease::renew() {
    if (!state_ || !valid()) throw Error(ErrorCode::lease_lost, "PostgreSQL advisory lease is no longer locally valid");
    try { PGresult* result = PQexec(state_->connection, "SELECT 1"); const bool ok = result != nullptr && PQresultStatus(result) == PGRES_TUPLES_OK; if (result != nullptr) PQclear(result); if (!ok) throw std::runtime_error("session liveness failed"); return true; }
    catch (...) { std::lock_guard guard(state_->mutex); state_->held = false; throw Error(ErrorCode::backend_unavailable, "PostgreSQL advisory session liveness failed"); }
}

ReleaseResult PostgresLease::release() noexcept {
    if (!state_) return ReleaseResult::already_not_owned;
    std::lock_guard guard(state_->mutex);
    if (!state_->held && !state_->release_unknown) return ReleaseResult::already_not_owned;
    if (state_->release_unknown) return ReleaseResult::unknown;
    const std::string key = std::to_string(state_->key);
    const char* values[] = {key.c_str()};
    PGresult* result = PQexecParams(state_->connection, "SELECT pg_advisory_unlock($1::bigint)", 1, nullptr, values, nullptr, nullptr, 0);
    if (result == nullptr || PQresultStatus(result) != PGRES_TUPLES_OK || PQntuples(result) != 1) {
        if (result != nullptr) PQclear(result);
        state_->release_unknown = true;
        return ReleaseResult::unknown;
    }
    const bool released = std::string(PQgetvalue(result, 0, 0)) == "t"; PQclear(result); finish(state_->connection); state_->connection = nullptr; state_->held = false;
    return released ? ReleaseResult::released : ReleaseResult::already_not_owned;
}

void provision_postgres_namespace(const PostgresOptions& options) {
    check_options(options);
    PGconn* connection = connect_session(options);
    bool transaction_started = false;
    try {
        PGresult* began = PQexec(connection, "BEGIN");
        if (began == nullptr || PQresultStatus(began) != PGRES_COMMAND_OK) { if (began) PQclear(began); throw Error(ErrorCode::backend_unavailable, "PostgreSQL metadata transaction start failed"); }
        PQclear(began);
        transaction_started = true;
        PGresult* serialized = PQexec(connection, "SELECT pg_advisory_xact_lock(hashtextextended('dilock:namespace-meta:v1', 0))");
        if (serialized == nullptr || PQresultStatus(serialized) != PGRES_TUPLES_OK) { if (serialized) PQclear(serialized); throw Error(ErrorCode::backend_unavailable, "PostgreSQL metadata provisioning serialization failed"); }
        PQclear(serialized);
        const char* ddl = "CREATE TABLE IF NOT EXISTS dilock_namespace_meta (namespace text PRIMARY KEY, protocol_major integer NOT NULL, identity_scheme text NOT NULL, profile text NOT NULL, coordination_epoch text NOT NULL, fixed_lease_ms bigint NULL)";
        PGresult* created = PQexec(connection, ddl); if (created == nullptr || PQresultStatus(created) != PGRES_COMMAND_OK) { if (created) PQclear(created); throw Error(ErrorCode::backend_unavailable, "PostgreSQL metadata table creation failed"); } PQclear(created);
        const std::string scheme_string{scheme};
        const std::string profile_string{profile};
        const char* values[] = {options.name_space.c_str(), scheme_string.c_str(), profile_string.c_str(), options.coordination_epoch.c_str()};
        PGresult* inserted = PQexecParams(connection, "INSERT INTO dilock_namespace_meta (namespace, protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms) VALUES ($1, 1, $2, $3, $4, NULL) ON CONFLICT (namespace) DO NOTHING", 4, nullptr, values, nullptr, nullptr, 0);
        if (inserted == nullptr || PQresultStatus(inserted) != PGRES_COMMAND_OK) { if (inserted) PQclear(inserted); throw Error(ErrorCode::backend_unavailable, "PostgreSQL metadata provisioning failed"); } PQclear(inserted);
        PGresult* committed = PQexec(connection, "COMMIT");
        if (committed == nullptr || PQresultStatus(committed) != PGRES_COMMAND_OK) { if (committed) PQclear(committed); throw Error(ErrorCode::backend_unavailable, "PostgreSQL metadata transaction commit failed"); }
        PQclear(committed);
        transaction_started = false;
        validate_metadata(connection, options);
    } catch (...) {
        if (transaction_started) {
            PGresult* rolled_back = PQexec(connection, "ROLLBACK");
            if (rolled_back != nullptr) PQclear(rolled_back);
        }
        finish(connection);
        throw;
    }
    finish(connection);
}

} // namespace dilock
