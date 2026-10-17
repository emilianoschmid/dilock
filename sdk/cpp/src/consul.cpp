#include "dilock/consul.hpp"

#include "metadata.hpp"
#include "consul_release.hpp"

#include <curl/curl.h>
#include <nlohmann/json.hpp>
#include <openssl/evp.h>

#include <algorithm>
#include <cctype>
#include <condition_variable>
#include <limits>
#include <mutex>
#include <random>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

namespace dilock {
namespace {

using Json = nlohmann::json;
using Clock = std::chrono::steady_clock;
using TimePoint = Clock::time_point;
constexpr std::string_view consul_profile{"consul-kv-session-v1"};
constexpr std::string_view identity_scheme{"sha256-framed-utf8-v1"};
constexpr std::size_t max_response_bytes = 65'536;
constexpr auto minimum_ttl = std::chrono::seconds(10);
constexpr auto maximum_ttl = std::chrono::hours(24);

struct HttpResponse {
    long status{};
    std::string body;
};

struct CurlDeleter { void operator()(CURL* value) const noexcept { if (value) curl_easy_cleanup(value); } };
struct CurlUrlDeleter { void operator()(CURLU* value) const noexcept { if (value) curl_url_cleanup(value); } };
struct CurlHeadersDeleter { void operator()(curl_slist* value) const noexcept { if (value) curl_slist_free_all(value); } };

std::once_flag curl_initialized;
void initialize_curl() {
    std::call_once(curl_initialized, [] {
        if (curl_global_init(CURL_GLOBAL_DEFAULT) != CURLE_OK) {
            throw Error(ErrorCode::backend_unavailable, "Consul HTTP transport initialization failed");
        }
    });
}

std::size_t append_response(char* data, std::size_t size, std::size_t count, void* context) {
    try {
        auto* output = static_cast<std::string*>(context);
        if (size != 0 && count > (max_response_bytes - std::min(output->size(), max_response_bytes)) / size) {
            return 0;
        }
        output->append(data, size * count);
        return size * count;
    } catch (...) { return 0; }
}

std::string curl_error(CURLcode code) {
    // CURLOPT_ERRORBUFFER and response bodies may contain peer-controlled data;
    // keep public errors categorical and never include endpoints or ACL tokens.
    (void)code;
    return "Consul HTTP request failed";
}

std::chrono::milliseconds effective_ttl(std::chrono::milliseconds requested);
std::chrono::milliseconds lease_margin(std::chrono::milliseconds ttl);

void set_option(CURL* handle, CURLoption option, long value) {
    if (curl_easy_setopt(handle, option, value) != CURLE_OK) {
        throw Error(ErrorCode::backend_unavailable, "Consul HTTP transport configuration failed");
    }
}

void set_option(CURL* handle, CURLoption option, const char* value) {
    if (curl_easy_setopt(handle, option, value) != CURLE_OK) {
        throw Error(ErrorCode::backend_unavailable, "Consul HTTP transport configuration failed");
    }
}

std::string normalized_endpoint(std::string_view endpoint) {
    initialize_curl();
    std::unique_ptr<CURLU, CurlUrlDeleter> parsed(curl_url());
    if (!parsed || curl_url_set(parsed.get(), CURLUPART_URL, std::string(endpoint).c_str(), 0) != CURLUE_OK) {
        throw Error(ErrorCode::invalid_argument, "Consul endpoint must be an HTTP(S) authority");
    }
    auto part = [&](CURLUPart key) {
        char* raw{};
        const auto result = curl_url_get(parsed.get(), key, &raw, 0);
        std::string value = result == CURLUE_OK && raw ? raw : "";
        if (raw) curl_free(raw);
        return value;
    };
    const auto scheme = part(CURLUPART_SCHEME);
    const auto host = part(CURLUPART_HOST);
    const auto path = part(CURLUPART_PATH);
    if ((scheme != "http" && scheme != "https") || host.empty() ||
        !part(CURLUPART_USER).empty() || !part(CURLUPART_PASSWORD).empty() ||
        !part(CURLUPART_QUERY).empty() || !part(CURLUPART_FRAGMENT).empty() ||
        (!path.empty() && path != "/")) {
        throw Error(ErrorCode::invalid_argument,
                    "Consul endpoint must not contain credentials, a path, query, or fragment");
    }
    char* raw{};
    if (curl_url_get(parsed.get(), CURLUPART_URL, &raw, CURLU_NO_DEFAULT_PORT) != CURLUE_OK || raw == nullptr) {
        throw Error(ErrorCode::invalid_argument, "Consul endpoint is malformed");
    }
    std::string result(raw);
    curl_free(raw);
    if (!result.empty() && result.back() == '/') result.pop_back();
    return result;
}

void validate_options(const ConsulOptions& options) {
    (void)resource_id(options.name_space, "validation");
    (void)resource_id(options.name_space, options.coordination_epoch);
    (void)normalized_endpoint(options.endpoint);
    if (options.acl_token.size() > 4096 ||
        std::any_of(options.acl_token.begin(), options.acl_token.end(), [](unsigned char c) {
            return c < 0x21 || c > 0x7e;
        })) {
        throw Error(ErrorCode::invalid_argument, "Consul ACL token must be printable non-space ASCII");
    }
    if (options.rpc_timeout <= std::chrono::milliseconds::zero() ||
        options.cleanup_timeout <= std::chrono::milliseconds::zero() ||
        options.poll_interval <= std::chrono::milliseconds::zero() ||
        options.rpc_timeout.count() > std::numeric_limits<long>::max() ||
        options.cleanup_timeout.count() > std::numeric_limits<long>::max() ||
        options.default_acquisition.wait <= std::chrono::milliseconds::zero() ||
        options.default_acquisition.wait > maximum_ttl ||
        options.default_acquisition.lease < minimum_ttl ||
        options.default_acquisition.lease > maximum_ttl) {
        throw Error(ErrorCode::invalid_argument, "Consul timeouts, wait, or session TTL are outside supported bounds");
    }
    const auto ttl = effective_ttl(options.default_acquisition.lease);
    const auto margin = lease_margin(ttl);
    if (options.rpc_timeout >= ttl - margin) {
        throw Error(ErrorCode::invalid_argument, "Consul RPC timeout and safety margin must fit within the session TTL");
    }
}

std::chrono::milliseconds effective_ttl(std::chrono::milliseconds requested) {
    if (requested < minimum_ttl || requested > maximum_ttl) {
        throw Error(ErrorCode::invalid_argument, "Consul session TTL must be 10 seconds through 24 hours");
    }
    const auto seconds = (requested.count() + 999) / 1000;
    const auto effective = std::chrono::seconds(seconds);
    if (effective > maximum_ttl) {
        throw Error(ErrorCode::invalid_argument, "rounded Consul session TTL exceeds 24 hours");
    }
    return std::chrono::duration_cast<std::chrono::milliseconds>(effective);
}

std::chrono::milliseconds lease_margin(std::chrono::milliseconds ttl) {
    return std::max(std::chrono::milliseconds(100), ttl / 100);
}

std::chrono::milliseconds remaining_timeout(TimePoint deadline, std::chrono::milliseconds maximum) {
    const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now());
    if (left <= std::chrono::milliseconds::zero()) return std::chrono::milliseconds::zero();
    return std::min(left, maximum);
}

Json parse_response(const HttpResponse& response, bool mutation) {
    if (response.status < 200 || response.status >= 300) {
        if (response.status == 404 && !mutation) {
            throw Error(ErrorCode::protocol_mismatch, "Consul metadata or lock key is missing");
        }
        if (mutation && response.status >= 500) {
            throw Error(ErrorCode::acquisition_outcome_unknown, "Consul mutation acknowledgement is unknown");
        }
        throw Error(ErrorCode::backend_unavailable, "Consul rejected an HTTP request");
    }
    if (response.body.empty()) return nullptr;
    try {
        return Json::parse(response.body.begin(), response.body.end());
    } catch (const std::exception&) {
        throw Error(mutation ? ErrorCode::acquisition_outcome_unknown : ErrorCode::protocol_mismatch,
                    mutation ? "Consul mutation response was malformed" : "Consul response was malformed");
    }
}

HttpResponse http_request(const ConsulOptions& options, std::string_view method,
                          std::string_view path_and_query, std::string_view body,
                          std::chrono::milliseconds timeout, bool mutation) {
    initialize_curl();
    std::unique_ptr<CURL, CurlDeleter> handle(curl_easy_init());
    if (!handle) throw Error(ErrorCode::backend_unavailable, "Consul HTTP handle allocation failed");
    const auto endpoint = normalized_endpoint(options.endpoint);
    const auto url = endpoint + "/" + std::string(path_and_query);
    std::unique_ptr<curl_slist, CurlHeadersDeleter> headers;
    headers.reset(curl_slist_append(nullptr, "Accept: application/json"));
    if (!headers) throw Error(ErrorCode::backend_unavailable, "Consul HTTP header allocation failed");
    if (!options.acl_token.empty()) {
        const auto token_header = "X-Consul-Token: " + options.acl_token;
        auto* next = curl_slist_append(headers.get(), token_header.c_str());
        if (!next) throw Error(ErrorCode::backend_unavailable, "Consul HTTP header allocation failed");
        headers.release();
        headers.reset(next);
    }
    if (method != "GET") {
        auto* next = curl_slist_append(headers.get(), "Content-Type: application/json");
        if (!next) throw Error(ErrorCode::backend_unavailable, "Consul HTTP header allocation failed");
        headers.release();
        headers.reset(next);
    }
    HttpResponse result;
    set_option(handle.get(), CURLOPT_URL, url.c_str());
    const std::string method_string(method);
    set_option(handle.get(), CURLOPT_CUSTOMREQUEST, method_string.c_str());
    if (curl_easy_setopt(handle.get(), CURLOPT_HTTPHEADER, headers.get()) != CURLE_OK ||
        curl_easy_setopt(handle.get(), CURLOPT_WRITEFUNCTION, append_response) != CURLE_OK ||
        curl_easy_setopt(handle.get(), CURLOPT_WRITEDATA, &result.body) != CURLE_OK) {
        throw Error(ErrorCode::backend_unavailable, "Consul HTTP transport configuration failed");
    }
    set_option(handle.get(), CURLOPT_FOLLOWLOCATION, 0L);
    set_option(handle.get(), CURLOPT_MAXREDIRS, 0L);
    set_option(handle.get(), CURLOPT_NOSIGNAL, 1L);
    set_option(handle.get(), CURLOPT_SSL_VERIFYPEER, 1L);
    set_option(handle.get(), CURLOPT_SSL_VERIFYHOST, 2L);
    const auto bounded = std::max<std::int64_t>(1, timeout.count());
    set_option(handle.get(), CURLOPT_TIMEOUT_MS, static_cast<long>(bounded));
    set_option(handle.get(), CURLOPT_CONNECTTIMEOUT_MS, static_cast<long>(bounded));
    if (method != "GET") {
        const char* request_body = body.empty() ? "" : body.data();
        if (curl_easy_setopt(handle.get(), CURLOPT_POSTFIELDS, request_body) != CURLE_OK ||
            curl_easy_setopt(handle.get(), CURLOPT_POSTFIELDSIZE_LARGE, static_cast<curl_off_t>(body.size())) != CURLE_OK) {
            throw Error(mutation ? ErrorCode::acquisition_outcome_unknown : ErrorCode::backend_unavailable,
                        "Consul HTTP request configuration failed");
        }
    }
    const auto code = curl_easy_perform(handle.get());
    if (code != CURLE_OK) {
        throw Error(mutation ? ErrorCode::acquisition_outcome_unknown : ErrorCode::backend_unavailable,
                    curl_error(code));
    }
    if (curl_easy_getinfo(handle.get(), CURLINFO_RESPONSE_CODE, &result.status) != CURLE_OK) {
        throw Error(mutation ? ErrorCode::acquisition_outcome_unknown : ErrorCode::backend_unavailable,
                    "Consul HTTP response status was unavailable");
    }
    return result;
}

Json request(const ConsulOptions& options, std::string_view method,
             std::string_view path, std::string_view body,
             std::chrono::milliseconds timeout, bool mutation) {
    return parse_response(http_request(options, method, path, body, timeout, mutation), mutation);
}

Json metadata_value(const ConsulOptions& options) {
    return Json{{"protocol_major", 1}, {"identity_scheme", std::string(identity_scheme)},
                {"profile", std::string(consul_profile)}, {"coordination_epoch", options.coordination_epoch},
                {"fixed_lease_ms", nullptr}};
}

std::string metadata_key(const ConsulOptions& options) {
    return "dilock/namespaces/" + options.name_space + "/meta";
}

void validate_metadata(const ConsulOptions& options, const Json& rows) {
    const auto key = metadata_key(options);
    if (!rows.is_array() || rows.size() != 1 || !rows[0].is_object() ||
        !rows[0].contains("Key") || !rows[0]["Key"].is_string() || rows[0]["Key"] != key ||
        !rows[0].contains("Value") || !rows[0]["Value"].is_string()) {
        throw Error(ErrorCode::protocol_mismatch, "Consul namespace metadata key is absent or malformed");
    }
    const auto encoded = rows[0]["Value"].get<std::string>();
    if (encoded.empty() || encoded.size() > 16'384 || encoded.size() % 4 != 0 ||
        encoded.size() > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
        throw Error(ErrorCode::protocol_mismatch, "Consul metadata base64 size is invalid");
    }
    std::vector<unsigned char> bytes((encoded.size() * 3) / 4 + 4);
    const auto decoded = EVP_DecodeBlock(bytes.data(),
        reinterpret_cast<const unsigned char*>(encoded.data()), static_cast<int>(encoded.size()));
    if (decoded < 0) throw Error(ErrorCode::protocol_mismatch, "Consul metadata is not valid base64");
    std::size_t actual = static_cast<std::size_t>(decoded);
    if (!encoded.empty() && encoded.back() == '=') --actual;
    if (encoded.size() > 1 && encoded[encoded.size() - 2] == '=') --actual;
    if (actual == 0 || actual > 8192) throw Error(ErrorCode::protocol_mismatch, "Consul metadata size is invalid");
    const std::string raw(reinterpret_cast<const char*>(bytes.data()), actual);
    Json value;
    try { value = detail::parse_metadata(raw); }
    catch (const std::exception&) { throw Error(ErrorCode::protocol_mismatch, "Consul metadata JSON is malformed or ambiguous"); }
    if (!detail::has_protocol_major_v1(value) || value.size() != 5 ||
        !value.contains("identity_scheme") || value["identity_scheme"] != std::string(identity_scheme) ||
        !value.contains("profile") || value["profile"] != std::string(consul_profile) ||
        !value.contains("coordination_epoch") || value["coordination_epoch"] != options.coordination_epoch ||
        !value.contains("fixed_lease_ms") || !value["fixed_lease_ms"].is_null()) {
        throw Error(ErrorCode::protocol_mismatch, "Consul namespace metadata does not match this authority");
    }
}

Json linearizable_kv(const ConsulOptions& options, std::string_view key,
                     std::chrono::milliseconds timeout) {
    return request(options, "GET", "v1/kv/" + std::string(key) + "?consistent", {}, timeout, false);
}

std::pair<std::string, std::uint64_t> read_owner(const ConsulOptions& options,
                                                 std::string_view key,
                                                 std::chrono::milliseconds timeout) {
    const auto rows = linearizable_kv(options, key, timeout);
    if (!rows.is_array() || rows.size() != 1 || !rows[0].is_object() ||
        !rows[0].contains("Key") || !rows[0]["Key"].is_string() || rows[0]["Key"].get<std::string>() != key ||
        !rows[0].contains("LockIndex") ||
        (!rows[0]["LockIndex"].is_number_unsigned() && !rows[0]["LockIndex"].is_number_integer()) ||
        !rows[0].contains("Session") ||
        (!rows[0]["Session"].is_null() && !rows[0]["Session"].is_string())) {
        throw Error(ErrorCode::protocol_mismatch, "Consul lock ownership response is malformed");
    }
    std::uint64_t index{};
    try {
        if (rows[0]["LockIndex"].is_number_integer() && rows[0]["LockIndex"].get<std::int64_t>() < 0) {
            throw Error(ErrorCode::protocol_mismatch, "Consul LockIndex was negative");
        }
        index = rows[0]["LockIndex"].get<std::uint64_t>();
    } catch (const Error&) { throw; }
    catch (const std::exception&) { throw Error(ErrorCode::protocol_mismatch, "Consul LockIndex was invalid"); }
    const auto owner = rows[0]["Session"].is_string() ? rows[0]["Session"].get<std::string>() : std::string{};
    return {owner, index};
}

struct Runtime {
    ConsulOptions options;
    std::string endpoint;
};

std::pair<std::string, TimePoint> create_session(const Runtime& runtime,
                                                 std::chrono::milliseconds ttl,
                                                 std::chrono::milliseconds timeout) {
    const auto sent = Clock::now();
    const auto body = Json{{"Name", "dilock lease"}, {"Behavior", "release"},
                           {"LockDelay", "0s"}, {"TTL", std::to_string(ttl.count() / 1000) + "s"},
                           {"Checks", Json::array()}}.dump();
    const auto response = request(runtime.options, "PUT", "v1/session/create", body, timeout, true);
    if (!response.is_object() || !response.contains("ID") || !response["ID"].is_string() ||
        response["ID"].get_ref<const std::string&>().empty()) {
        throw Error(ErrorCode::acquisition_outcome_unknown,
                    "Consul may have created a session without returning its identifier");
    }
    return {response["ID"].get<std::string>(), sent};
}

TimePoint renew_session(const Runtime& runtime, std::string_view session,
                        std::chrono::milliseconds timeout) {
    const auto sent = Clock::now();
    const auto response = request(runtime.options, "PUT", "v1/session/renew/" + std::string(session), {}, timeout, true);
    if (!response.is_array() || response.size() != 1 || !response[0].is_object() ||
        !response[0].contains("ID") || response[0]["ID"] != session) {
        throw Error(ErrorCode::protocol_mismatch, "Consul session renewal response was malformed");
    }
    return sent;
}

bool destroy_session(const Runtime& runtime, std::string_view session,
                     std::chrono::milliseconds timeout) noexcept {
    try {
        const auto response = request(runtime.options, "PUT",
            "v1/session/destroy/" + std::string(session), {}, timeout, true);
        return response.is_boolean() && response.get<bool>();
    } catch (...) { return false; }
}

bool abandon_attempt(const Runtime& runtime, std::string_view key, std::string_view session,
                     std::chrono::milliseconds timeout) noexcept {
    try {
        (void)request(runtime.options, "PUT",
            "v1/kv/" + std::string(key) + "?release=" + std::string(session), {}, timeout, true);
    } catch (...) {}
    // This attempt owns a unique session. A confirmed destroy prevents that
    // session from owning any key even when the release was false or unknown.
    return destroy_session(runtime, session, timeout);
}

} // namespace

struct ConsulLease::State {
    enum class Phase { held, releasing, released, lost, released_unknown };
    std::shared_ptr<Runtime> runtime;
    std::string key;
    std::string session;
    std::uint64_t lock_index{};
    std::chrono::milliseconds ttl{};
    std::chrono::milliseconds margin{};
    TimePoint deadline{};
    Phase phase{Phase::held};
    ReleaseResult terminal{ReleaseResult::unknown};
    mutable std::mutex mutex;
};

struct ConsulResourceLock::State {
    std::shared_ptr<Runtime> runtime;
    std::string key;
};

struct ConsulProvider::State {
    std::shared_ptr<Runtime> runtime;
};

void provision_consul_namespace(const ConsulOptions& options) {
    validate_options(options);
    const auto key = metadata_key(options);
    const auto result = request(options, "PUT", "v1/kv/" + key + "?cas=0",
                                metadata_value(options).dump(), options.rpc_timeout, true);
    if (!result.is_boolean()) throw Error(ErrorCode::protocol_mismatch, "Consul metadata CAS response was not boolean");
    if (result.get<bool>()) return;
    validate_metadata(options, linearizable_kv(options, key, options.rpc_timeout));
}

ConsulProvider::ConsulProvider(ConsulOptions options) {
    validate_options(options);
    auto runtime = std::make_shared<Runtime>();
    runtime->endpoint = normalized_endpoint(options.endpoint);
    runtime->options = std::move(options);
    validate_metadata(runtime->options,
        linearizable_kv(runtime->options, metadata_key(runtime->options), runtime->options.rpc_timeout));
    state_ = std::make_shared<State>(State{std::move(runtime)});
}

ConsulResourceLock ConsulProvider::lock(std::string_view resource) const {
    if (!state_) throw Error(ErrorCode::invalid_argument, "Consul provider is empty");
    auto value = std::make_shared<ConsulResourceLock::State>();
    value->runtime = state_->runtime;
    value->key = "dilock/locks/" + resource_id(value->runtime->options.name_space, resource);
    return ConsulResourceLock(std::move(value));
}

ConsulResourceLock::ConsulResourceLock(std::shared_ptr<State> state) noexcept : state_(std::move(state)) {}
ConsulLease::ConsulLease(std::shared_ptr<State> state) noexcept : state_(std::move(state)) {}
ConsulLease::~ConsulLease() noexcept { dispose_scope("lease-destructor"); }
ConsulLease::ConsulLease(ConsulLease&& other) noexcept : state_(std::exchange(other.state_, {})) {}
ConsulLease& ConsulLease::operator=(ConsulLease&& other) noexcept {
    if (this != &other) { dispose_scope("lease-move-assignment"); state_ = std::exchange(other.state_, {}); }
    return *this;
}

std::optional<ConsulLease> ConsulResourceLock::attempt(
    std::chrono::milliseconds wait_budget, std::chrono::milliseconds requested_ttl,
    bool should_wait, std::stop_token stop) const {
    if (!state_) throw Error(ErrorCode::invalid_argument, "Consul resource handle is empty");
    const auto& runtime = *state_->runtime;
    const auto ttl = effective_ttl(requested_ttl);
    const auto margin = lease_margin(ttl);
    if (runtime.options.rpc_timeout >= ttl - margin) {
        throw Error(ErrorCode::invalid_argument, "Consul RPC timeout and safety margin must fit within the session TTL");
    }
    if (should_wait && (wait_budget <= std::chrono::milliseconds::zero() || wait_budget > maximum_ttl)) {
        throw Error(ErrorCode::invalid_argument, "Consul acquisition wait must be positive and at most 24 hours");
    }
    if (stop.stop_requested()) throw Error(ErrorCode::cancelled, "Consul acquisition cancelled before session creation");
    const auto wait_started = Clock::now();
    const auto wait_deadline = wait_started + wait_budget;
    const auto create_timeout = should_wait ? remaining_timeout(wait_deadline, runtime.options.rpc_timeout)
                                           : runtime.options.rpc_timeout;
    if (should_wait && create_timeout <= std::chrono::milliseconds::zero()) {
        throw Error(ErrorCode::wait_timeout, "Consul acquisition wait expired before session creation");
    }
    const auto [session, session_sent] = create_session(runtime, ttl, create_timeout);
    auto lease_deadline = session_sent + ttl - margin;
    auto last_renewal = session_sent;

    auto clean_and_throw = [&](ErrorCode intended, const char* message) -> void {
        const bool cleaned = abandon_attempt(runtime, state_->key, session, runtime.options.cleanup_timeout);
        if (!cleaned) throw Error(ErrorCode::acquisition_outcome_unknown, "Consul acquisition cleanup was not confirmed");
        throw Error(intended, message);
    };

    for (;;) {
        if (stop.stop_requested()) clean_and_throw(ErrorCode::cancelled, "Consul acquisition cancelled after session creation");
        if (Clock::now() >= lease_deadline) clean_and_throw(ErrorCode::acquisition_outcome_unknown,
            "Consul attempt session validity elapsed before ownership was confirmed");
        if (should_wait && Clock::now() >= wait_deadline) clean_and_throw(ErrorCode::wait_timeout,
            "Consul resource remained busy until the acquisition deadline");

        auto rpc_timeout = runtime.options.rpc_timeout;
        if (should_wait) {
            rpc_timeout = remaining_timeout(wait_deadline, rpc_timeout);
            if (rpc_timeout <= std::chrono::milliseconds::zero()) continue;
        }
        Json acquired;
        try {
            acquired = request(runtime.options, "PUT", "v1/kv/" + state_->key + "?acquire=" + session,
                               {}, rpc_timeout, true);
        } catch (const Error& error) {
            const bool cleaned = abandon_attempt(runtime, state_->key, session, runtime.options.cleanup_timeout);
            if (!cleaned) throw Error(ErrorCode::acquisition_outcome_unknown,
                "Consul acquire acknowledgement and attempt cleanup are unknown");
            throw;
        }
        if (!acquired.is_boolean()) clean_and_throw(ErrorCode::protocol_mismatch,
            "Consul KV acquire response was not a boolean");
        if (acquired.get<bool>()) {
            auto verify_timeout = runtime.options.rpc_timeout;
            if (should_wait) verify_timeout = remaining_timeout(wait_deadline, verify_timeout);
            if (verify_timeout <= std::chrono::milliseconds::zero()) {
                clean_and_throw(ErrorCode::wait_timeout, "Consul ownership verification exceeded the wait deadline");
            }
            std::string owner;
            std::uint64_t index{};
            try {
                const auto verified = read_owner(runtime.options, state_->key, verify_timeout);
                owner = verified.first;
                index = verified.second;
            } catch (const Error&) {
                const bool cleaned = abandon_attempt(runtime, state_->key, session, runtime.options.cleanup_timeout);
                if (!cleaned) throw Error(ErrorCode::acquisition_outcome_unknown,
                    "Consul owner verification failed and session cleanup is unknown");
                throw Error(ErrorCode::acquisition_outcome_unknown,
                    "Consul owner verification failed after acquisition");
            }
            if (owner != session || index == 0 || Clock::now() >= lease_deadline) {
                clean_and_throw(ErrorCode::acquisition_outcome_unknown,
                    "Consul ownership or LockIndex verification failed");
            }
            if (should_wait && Clock::now() >= wait_deadline) {
                clean_and_throw(ErrorCode::wait_timeout, "Consul ownership was confirmed after the wait deadline");
            }
            if (stop.stop_requested()) clean_and_throw(ErrorCode::cancelled,
                "Consul acquisition was cancelled after ownership confirmation");
            auto lease = std::make_shared<ConsulLease::State>();
            lease->runtime = state_->runtime;
            lease->key = state_->key;
            lease->session = session;
            lease->lock_index = index;
            lease->ttl = ttl;
            lease->margin = margin;
            lease->deadline = lease_deadline;
            return ConsulLease(std::move(lease));
        }
        if (!should_wait) {
            if (!abandon_attempt(runtime, state_->key, session, runtime.options.cleanup_timeout)) {
                throw Error(ErrorCode::acquisition_outcome_unknown, "Consul contention session cleanup was not confirmed");
            }
            return std::nullopt;
        }

        const auto now = Clock::now();
        if (now >= wait_deadline) continue;
        const auto wake = std::min(wait_deadline, now + runtime.options.poll_interval);
        std::mutex pause_mutex;
        std::condition_variable changed;
        std::unique_lock pause_lock(pause_mutex);
        std::stop_callback wake_on_stop(stop, [&changed] { changed.notify_all(); });
        changed.wait_until(pause_lock, wake, [&] { return stop.stop_requested(); });
        if (stop.stop_requested()) continue;

        if (Clock::now() - last_renewal >= ttl * 2 / 3) {
            const auto renewed_at = Clock::now();
            try {
                (void)renew_session(runtime, session, runtime.options.rpc_timeout);
                last_renewal = renewed_at;
                lease_deadline = renewed_at + ttl - margin;
            } catch (const Error&) {
                clean_and_throw(ErrorCode::acquisition_outcome_unknown,
                    "Consul waiting session renewal was not confirmed");
            }
        }
    }
}

std::optional<ConsulLease> ConsulResourceLock::try_acquire(
    std::chrono::milliseconds ttl, std::stop_token stop) const {
    return attempt(std::chrono::milliseconds::zero(), ttl, false, stop);
}

std::optional<ConsulLease> ConsulResourceLock::try_acquire(std::stop_token stop) const {
    if (!state_) throw Error(ErrorCode::invalid_argument, "Consul resource handle is empty");
    return try_acquire(state_->runtime->options.default_acquisition.lease, stop);
}

std::optional<ConsulLease> ConsulResourceLock::try_acquire(
    const AcquireOptions& options, std::stop_token stop) const {
    return try_acquire(options.lease, stop);
}

ConsulLease ConsulResourceLock::acquire(std::chrono::milliseconds wait,
                                         std::chrono::milliseconds ttl,
                                         std::stop_token stop) const {
    auto lease = attempt(wait, ttl, true, stop);
    if (!lease) throw Error(ErrorCode::wait_timeout, "Consul acquisition returned no lease while waiting");
    return std::move(*lease);
}

ConsulLease ConsulResourceLock::acquire(std::stop_token stop) const {
    if (!state_) throw Error(ErrorCode::invalid_argument, "Consul resource handle is empty");
    return acquire(state_->runtime->options.default_acquisition, stop);
}

ConsulLease ConsulResourceLock::acquire(const AcquireOptions& options, std::stop_token stop) const {
    return acquire(options.wait, options.lease, stop);
}

bool ConsulLease::valid() const noexcept {
    if (!state_) return false;
    std::lock_guard guard(state_->mutex);
    if (state_->phase != State::Phase::held) return false;
    if (Clock::now() >= state_->deadline) { state_->phase = State::Phase::lost; return false; }
    return true;
}

std::chrono::milliseconds ConsulLease::effective_ttl() const noexcept {
    return state_ ? state_->ttl : std::chrono::milliseconds::zero();
}

ConsulFence ConsulLease::fence() const {
    if (!state_) throw Error(ErrorCode::lease_lost, "Consul lease is empty");
    return ConsulFence{state_->runtime->options.coordination_epoch, state_->lock_index};
}

bool ConsulLease::renew() {
    if (!state_) throw Error(ErrorCode::lease_lost, "Consul lease is empty");
    std::lock_guard guard(state_->mutex);
    if (state_->phase != State::Phase::held || Clock::now() >= state_->deadline) {
        state_->phase = State::Phase::lost;
        throw Error(ErrorCode::lease_lost, "Consul lease is no longer locally valid");
    }
    const auto old_deadline = state_->deadline;
    TimePoint sent{};
    std::string owner;
    std::uint64_t index{};
    try {
        sent = renew_session(*state_->runtime, state_->session, state_->runtime->options.rpc_timeout);
        const auto verified = read_owner(state_->runtime->options, state_->key,
                                         state_->runtime->options.rpc_timeout);
        owner = verified.first;
        index = verified.second;
    } catch (...) {
        state_->phase = State::Phase::lost;
        throw;
    }
    if (owner != state_->session || index != state_->lock_index || Clock::now() >= old_deadline) {
        state_->phase = State::Phase::lost;
        throw Error(ErrorCode::lease_lost, "Consul no longer confirms this session and LockIndex");
    }
    const auto next_deadline = sent + state_->ttl - state_->margin;
    if (Clock::now() >= next_deadline) {
        state_->phase = State::Phase::lost;
        throw Error(ErrorCode::lease_lost, "Consul renewal response arrived outside its validity budget");
    }
    state_->deadline = next_deadline;
    return true;
}

ReleaseResult ConsulLease::release() noexcept {
    if (!state_) return ReleaseResult::already_not_owned;
    std::lock_guard guard(state_->mutex);
    if (state_->phase == State::Phase::released || state_->phase == State::Phase::released_unknown) return state_->terminal;
    state_->phase = State::Phase::releasing;
    const auto operation_deadline = Clock::now() + state_->runtime->options.cleanup_timeout;
    auto operation_budget = [&] {
        return remaining_timeout(operation_deadline, state_->runtime->options.cleanup_timeout);
    };
    const auto outcome = detail::release_with_cleanup(
        [&]() -> ReleaseResult {
            const auto [owner, index] = read_owner(state_->runtime->options, state_->key, operation_budget());
            if (owner != state_->session || index != state_->lock_index) {
                return ReleaseResult::already_not_owned;
            }
            const auto response = request(state_->runtime->options, "PUT",
                "v1/kv/" + state_->key + "?release=" + state_->session, {}, operation_budget(), true);
            if (!response.is_boolean()) {
                throw Error(ErrorCode::protocol_mismatch, "Consul release response was not boolean");
            }
            if (response.get<bool>()) return ReleaseResult::released;
            const auto [after_owner, after_index] = read_owner(
                state_->runtime->options, state_->key, operation_budget());
            return (after_owner != state_->session || after_index != state_->lock_index)
                ? ReleaseResult::already_not_owned : ReleaseResult::unknown;
        },
        [&] {
            const auto cleanup_deadline = Clock::now() + state_->runtime->options.cleanup_timeout;
            const auto cleanup_budget = remaining_timeout(
                cleanup_deadline, state_->runtime->options.cleanup_timeout);
            return cleanup_budget > std::chrono::milliseconds::zero() &&
                destroy_session(*state_->runtime, state_->session, cleanup_budget);
        });
    state_->terminal = outcome;
    state_->phase = outcome == ReleaseResult::unknown ? State::Phase::released_unknown : State::Phase::released;
    return outcome;
}

void ConsulLease::dispose_scope(const char* reason) noexcept {
    if (!state_) return;
    const auto outcome = release();
    const auto& observer = state_->runtime->options.cleanup_observer;
    if (outcome != ReleaseResult::released && observer) {
        try { observer(CleanupObservation{"consul-kv-session-v1", outcome, reason}); }
        catch (...) {}
    }
}

} // namespace dilock
