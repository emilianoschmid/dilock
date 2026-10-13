#include <dilock/dilock.h>

#include <nlohmann/json.hpp>

#include <atomic>
#include <charconv>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace {
using Json = nlohmann::json;
constexpr std::size_t max_line_bytes = 64U * 1024U;

enum class Backend { redis, etcd };

struct Options {
    Backend backend{Backend::redis};
    std::string redis_host{"127.0.0.1"};
    std::string redis_username;
    std::string redis_password;
    std::string name_space{"default"};
    std::string epoch;
    std::string etcd_endpoint{"127.0.0.1:2379"};
    int32_t redis_port{6379};
    int32_t redis_database{};
    int64_t rpc_timeout_ms{2000};
    int64_t cleanup_timeout_ms{2000};
    int64_t safety_margin_ms{25};
    int64_t poll_interval_ms{25};
};

struct Cancellation final {
    explicit Cancellation(dilock_cancellation* value) : handle(value) {}
    ~Cancellation() { dilock_cancellation_destroy(handle); }
    Cancellation(const Cancellation&) = delete;
    Cancellation& operator=(const Cancellation&) = delete;
    dilock_cancellation* handle;
};

struct Lease final {
    Backend backend;
    dilock_redis_lease* redis{};
    dilock_etcd_lease* etcd{};
    mutable std::mutex operation_mutex;

    ~Lease() {
        if (backend == Backend::redis) dilock_redis_lease_destroy(redis);
        else dilock_etcd_lease_destroy(etcd);
    }
    Lease(const Lease&) = delete;
    Lease& operator=(const Lease&) = delete;
    explicit Lease(Backend selected) : backend(selected) {}

    [[nodiscard]] bool valid() const {
        std::lock_guard guard(operation_mutex);
        return backend == Backend::redis
            ? dilock_redis_lease_is_valid(redis) != 0
            : dilock_etcd_lease_is_valid(etcd) != 0;
    }

    [[nodiscard]] dilock_status release(dilock_release_outcome* result,
                                        dilock_error* error) {
        std::lock_guard guard(operation_mutex);
        return backend == Backend::redis
            ? dilock_redis_lease_release(redis, result, error)
            : dilock_etcd_lease_release(etcd, result, error);
    }

    [[nodiscard]] dilock_status renew(int64_t lease_ms, int32_t* renewed,
                                      dilock_error* error) {
        std::lock_guard guard(operation_mutex);
        return backend == Backend::redis
            ? dilock_redis_lease_renew(redis, lease_ms, renewed, error)
            : DILOCK_STATUS_UNSUPPORTED_CAPABILITY;
    }
};

struct Task final {
    std::shared_ptr<std::atomic_bool> finished;
    std::thread thread;
};

[[nodiscard]] std::string status_name(dilock_status status) {
    switch (status) {
    case DILOCK_STATUS_OK: return "ok";
    case DILOCK_STATUS_BUSY: return "busy";
    case DILOCK_STATUS_INVALID_ARGUMENT: return "invalid_argument";
    case DILOCK_STATUS_CANCELLED: return "cancelled";
    case DILOCK_STATUS_WAIT_TIMEOUT: return "timeout";
    case DILOCK_STATUS_BACKEND_UNAVAILABLE: return "backend_unavailable";
    case DILOCK_STATUS_ACQUISITION_UNKNOWN: return "acquisition_unknown";
    case DILOCK_STATUS_PROTOCOL_MISMATCH: return "protocol_mismatch";
    case DILOCK_STATUS_LEASE_LOST: return "backend_unavailable";
    case DILOCK_STATUS_UNSUPPORTED_CAPABILITY: return "unsupported_capability";
    case DILOCK_STATUS_INTERNAL: return "internal_error";
    }
    return "internal_error";
}

[[nodiscard]] Json failure(std::string_view id, dilock_status status,
                           const dilock_error& error) {
    Json response{{"id", id}, {"status", status_name(status)}};
    if (status == DILOCK_STATUS_CANCELLED) {
        response["remote_outcome_unknown"] =
            std::string_view(error.message).find("unknown") != std::string_view::npos;
    }
    return response;
}

[[nodiscard]] bool read_bounded_line(std::istream& input, std::string& line,
                                     bool& too_large) {
    line.clear();
    too_large = false;
    bool saw_input = false;
    char ch = '\0';
    while (input.get(ch)) {
        saw_input = true;
        if (ch == '\n') return true;
        if (line.size() < max_line_bytes) line.push_back(ch);
        else too_large = true;
    }
    return saw_input;
}

[[nodiscard]] Options read_options(const char* path) {
    std::ifstream input(path, std::ios::binary);
    if (!input) throw std::runtime_error("worker config is unavailable");
    const auto config = Json::parse(input);
    if (!config.is_object()) throw std::runtime_error("worker config must be an object");

    Options result;
    const auto backend = config.value("backend", std::string{"redis"});
    if (backend == "redis") {
        result.backend = Backend::redis;
        result.name_space = config.value("namespace", std::string{"default"});
        result.epoch = config.value("epoch", std::string{});
        if (config.contains("redis")) {
            const auto& redis = config.at("redis");
            if (!redis.is_object()) throw std::runtime_error("Redis config must be an object");
            result.redis_host = redis.value("host", std::string{"127.0.0.1"});
            result.redis_port = redis.value("port", 6379);
            result.redis_username = redis.value("username", std::string{});
            result.redis_password = redis.value("password", std::string{});
            result.redis_database = redis.value("database", 0);
            result.name_space = redis.value("namespace", result.name_space);
            result.epoch = redis.value("coordination_epoch", result.epoch);
        } else if (config.contains("endpoint")) {
            const auto endpoint = config.at("endpoint").get<std::string>();
            constexpr std::string_view scheme{"redis://"};
            if (!std::string_view(endpoint).starts_with(scheme)) {
                throw std::runtime_error("Redis endpoint must use redis://");
            }
            const auto authority = std::string_view(endpoint).substr(scheme.size());
            const auto colon = authority.rfind(':');
            if (colon == std::string_view::npos || colon == 0U || colon + 1U >= authority.size()) {
                throw std::runtime_error("Redis endpoint requires host and port");
            }
            const auto host = std::string(authority.substr(0, colon));
            int port = 0;
            const auto port_text = authority.substr(colon + 1U);
            const auto parsed = std::from_chars(port_text.data(), port_text.data() + port_text.size(), port);
            if (parsed.ec != std::errc{} || parsed.ptr != port_text.data() + port_text.size() ||
                port < 1 || port > 65535) {
                throw std::runtime_error("Redis endpoint port is invalid");
            }
            result.redis_host = host;
            result.redis_port = port;
        }
        result.rpc_timeout_ms = config.value("rpc_timeout_ms", result.rpc_timeout_ms);
        result.cleanup_timeout_ms = config.value("cleanup_timeout_ms", result.cleanup_timeout_ms);
        result.safety_margin_ms = config.value("safety_margin_ms", result.safety_margin_ms);
        result.poll_interval_ms = config.value("poll_interval_ms", result.poll_interval_ms);
        return result;
    }
    if (backend != "etcd") throw std::runtime_error("backend must be redis or etcd");

    result.backend = Backend::etcd;
    const auto endpoint = config.at("endpoint").get<std::string>();
    constexpr std::string_view http_scheme{"http://"};
    if (!std::string_view(endpoint).starts_with(http_scheme)) {
        throw std::runtime_error("C test worker requires an http:// etcd endpoint");
    }
    result.etcd_endpoint = endpoint.substr(http_scheme.size());
    result.name_space = config.at("namespace").get<std::string>();
    result.epoch = config.at("epoch").get<std::string>();
    result.rpc_timeout_ms = config.value("rpc_timeout_ms", result.rpc_timeout_ms);
    result.cleanup_timeout_ms = config.value("cleanup_timeout_ms", result.cleanup_timeout_ms);
    result.safety_margin_ms = config.value("safety_margin_ms", result.safety_margin_ms);
    return result;
}

class Worker final {
public:
    explicit Worker(Options options) : options_(std::move(options)) {}

    void run() {
        std::string line;
        bool too_large = false;
        while (read_bounded_line(std::cin, line, too_large)) {
            if (too_large) {
                write({{"status", "invalid_argument"}, {"error", "input line exceeds 64 KiB"}});
                continue;
            }
            Json request;
            try {
                request = Json::parse(line);
                if (!request.is_object() || !request.contains("id") || !request.at("id").is_string() ||
                    !request.contains("op") || !request.at("op").is_string()) {
                    throw std::runtime_error("invalid envelope");
                }
            } catch (const std::exception&) {
                write({{"status", "invalid_argument"}, {"error", "malformed request"}});
                continue;
            }
            const auto id = request.at("id").get<std::string>();
            const auto op = request.at("op").get<std::string>();
            if (id.empty() || id.size() > 128U) {
                write({{"id", id}, {"status", "invalid_argument"}, {"error", "invalid request id"}});
                continue;
            }
            if (!known_fields(request)) {
                write({{"id", id}, {"status", "invalid_argument"}, {"error", "unknown request field"}});
                continue;
            }
            reap_finished();
            if (!register_id(id)) {
                write({{"id", id}, {"status", "invalid_argument"}, {"error", "request id is already active"}});
                continue;
            }
            if (op == "shutdown") {
                shutdown(id);
                return;
            }
            if (op == "cancel_request") {
                cancel(id, request);
                unregister_id(id);
                continue;
            }

            dilock_error error{};
            auto* cancellation_handle = dilock_cancellation_create(&error);
            if (cancellation_handle == nullptr) {
                write(failure(id, static_cast<dilock_status>(error.code), error));
                unregister_id(id);
                continue;
            }
            auto cancellation = std::make_shared<Cancellation>(cancellation_handle);
            {
                std::lock_guard guard(mutex_);
                pending_.emplace(id, cancellation);
            }
            auto finished = std::make_shared<std::atomic_bool>(false);
            tasks_.push_back(Task{finished, std::thread([this, id, request = std::move(request), cancellation,
                                                          finished]() mutable {
                try {
                    dispatch(id, request, cancellation);
                } catch (const std::exception&) {
                    write({{"id", id}, {"status", "invalid_argument"}, {"error", "invalid request fields"}});
                } catch (...) {
                    write({{"id", id}, {"status", "internal_error"}, {"error", "worker operation failed"}});
                }
                {
                    std::lock_guard guard(mutex_);
                    pending_.erase(id);
                    active_ids_.erase(id);
                }
                finished->store(true, std::memory_order_release);
            })});
        }
        shutdown("eof");
    }

private:
    [[nodiscard]] bool known_fields(const Json& request) const {
        static const std::unordered_set<std::string> allowed{
            "id", "op", "resource", "lease_id", "target_id", "wait_ms", "lease_ms", "rpc_ms", "auto_renew"};
        for (auto field = request.begin(); field != request.end(); ++field) {
            if (!allowed.contains(field.key())) return false;
        }
        return true;
    }

    void write(Json response) {
        std::lock_guard guard(output_mutex_);
        std::cout << response.dump() << '\n' << std::flush;
    }

    [[nodiscard]] bool register_id(const std::string& id) {
        std::lock_guard guard(mutex_);
        return active_ids_.insert(id).second;
    }

    void unregister_id(const std::string& id) {
        std::lock_guard guard(mutex_);
        active_ids_.erase(id);
    }

    void reap_finished() {
        for (auto task = tasks_.begin(); task != tasks_.end();) {
            if (task->finished->load(std::memory_order_acquire)) {
                if (task->thread.joinable()) task->thread.join();
                task = tasks_.erase(task);
            } else ++task;
        }
    }

    void cancel(const std::string& id, const Json& request) {
        if (!request.contains("target_id") || !request.at("target_id").is_string()) {
            write({{"id", id}, {"status", "invalid_argument"}, {"error", "cancel_request requires target_id"}});
            return;
        }
        const auto target = request.at("target_id").get<std::string>();
        std::lock_guard guard(mutex_);
        const auto found = pending_.find(target);
        if (found == pending_.end()) {
            write({{"id", id}, {"status", "cancel_requested"}, {"found", false}});
        } else {
            dilock_cancellation_request(found->second->handle);
            write({{"id", id}, {"status", "cancel_requested"}, {"found", true}});
        }
    }

    void dispatch(const std::string& id, const Json& request,
                  const std::shared_ptr<Cancellation>& cancellation) {
        const auto op = request.at("op").get<std::string>();
        if (op == "hello") {
            Json capabilities = options_.backend == Backend::redis
                ? Json{"redis", "renewal"} : Json{"etcd", "auto_renew", "fencing"};
            Json response{{"id", id}, {"status", "ok"}, {"sdk", "c"}, {"protocol", 1},
                          {"capabilities", std::move(capabilities)}};
            response["backend"] = options_.backend == Backend::redis ? "redis" : "etcd";
            response["client"] = "Dilock C ABI " + std::to_string(dilock_c_abi_version());
            if (options_.backend == Backend::etcd) response["server_version"] = "unknown";
            write(std::move(response));
            return;
        }
        if (op == "inspect") { inspect(id, request); return; }
        if (op == "release") { release(id, request); return; }
        if (op == "renew") { renew(id, request); return; }
        if (op != "acquire" && op != "try_acquire") {
            write({{"id", id}, {"status", "invalid_argument"}, {"error", "unknown operation"}});
            return;
        }
        if (!request.contains("resource") || !request.at("resource").is_string() ||
            request.at("resource").get<std::string>().empty() || !request.contains("lease_ms") ||
            !request.at("lease_ms").is_number_integer() ||
            (op == "acquire" && (!request.contains("wait_ms") || !request.at("wait_ms").is_number_integer()))) {
            write({{"id", id}, {"status", "invalid_argument"}, {"error", "invalid acquisition fields"}});
            return;
        }
        if (request.value("auto_renew", false) && options_.backend == Backend::redis) {
            write({{"id", id}, {"status", "unsupported_capability"}, {"capability", "auto_renew"}});
            return;
        }
        if (request.contains("auto_renew") && !request.at("auto_renew").is_boolean()) {
            write({{"id", id}, {"status", "invalid_argument"}, {"error", "auto_renew must be a boolean"}});
            return;
        }
        const auto lease_ms = request.at("lease_ms").get<std::int64_t>();
        const auto wait_ms = op == "acquire" ? request.at("wait_ms").get<std::int64_t>() : 0;
        if (lease_ms <= 0 || (op == "acquire" && wait_ms <= 0)) {
            write({{"id", id}, {"status", "invalid_argument"}, {"error", "lease and acquisition wait durations must be positive"}});
            return;
        }

        dilock_error error{};
        std::shared_ptr<Lease> lease;
        dilock_status status = DILOCK_STATUS_INTERNAL;
        const auto resource = request.at("resource").get<std::string>();
        if (options_.backend == Backend::redis) {
            dilock_redis_options provider_options{};
            dilock_redis_options_init(&provider_options);
            provider_options.host = options_.redis_host.c_str();
            provider_options.port = options_.redis_port;
            provider_options.username = options_.redis_username.c_str();
            provider_options.password = options_.redis_password.c_str();
            provider_options.database = options_.redis_database;
            provider_options.name_space = options_.name_space.c_str();
            provider_options.coordination_epoch = options_.epoch.c_str();
            provider_options.rpc_timeout_ms = options_.rpc_timeout_ms;
            provider_options.cleanup_timeout_ms = options_.cleanup_timeout_ms;
            provider_options.safety_margin_ms = options_.safety_margin_ms;
            provider_options.poll_interval_ms = options_.poll_interval_ms;
            if (request.contains("rpc_ms")) {
                if (!request.at("rpc_ms").is_number_integer() || request.at("rpc_ms").get<std::int64_t>() <= 0) {
                    write({{"id", id}, {"status", "invalid_argument"}, {"error", "rpc_ms must be positive"}});
                    return;
                }
                provider_options.rpc_timeout_ms = request.at("rpc_ms").get<std::int64_t>();
            }
            dilock_redis_provider* provider = nullptr;
            dilock_redis_lock* lock = nullptr;
            status = dilock_redis_provider_create(&provider_options, &provider, &error);
            if (status == DILOCK_STATUS_OK) status = dilock_redis_lock_create(provider, resource.c_str(), &lock, &error);
            if (status == DILOCK_STATUS_OK) {
                auto acquired = std::make_shared<Lease>(Backend::redis);
                if (op == "try_acquire") {
                    status = dilock_redis_try_acquire(lock, lease_ms, &acquired->redis, &error);
                } else {
                    status = dilock_redis_acquire_with_cancellation(lock, wait_ms, lease_ms,
                        cancellation->handle, &acquired->redis, &error);
                }
                if (status == DILOCK_STATUS_OK) lease = std::move(acquired);
            }
            dilock_redis_lock_destroy(lock);
            dilock_redis_provider_destroy(provider);
        } else {
            dilock_etcd_options provider_options{};
            dilock_etcd_options_init(&provider_options);
            provider_options.endpoint = options_.etcd_endpoint.c_str();
            provider_options.name_space = options_.name_space.c_str();
            provider_options.coordination_epoch = options_.epoch.c_str();
            provider_options.rpc_timeout_ms = options_.rpc_timeout_ms;
            provider_options.cleanup_timeout_ms = options_.cleanup_timeout_ms;
            provider_options.safety_margin_ms = options_.safety_margin_ms;
            provider_options.auto_renew = request.value("auto_renew", false) ? 1 : 0;
            if (request.contains("rpc_ms")) {
                if (!request.at("rpc_ms").is_number_integer() || request.at("rpc_ms").get<std::int64_t>() <= 0) {
                    write({{"id", id}, {"status", "invalid_argument"}, {"error", "rpc_ms must be positive"}});
                    return;
                }
                provider_options.rpc_timeout_ms = request.at("rpc_ms").get<std::int64_t>();
            }
            dilock_etcd_provider* provider = nullptr;
            dilock_etcd_lock* lock = nullptr;
            status = dilock_etcd_provider_create(&provider_options, &provider, &error);
            if (status == DILOCK_STATUS_OK) status = dilock_etcd_lock_create(provider, resource.c_str(), &lock, &error);
            if (status == DILOCK_STATUS_OK) {
                auto acquired = std::make_shared<Lease>(Backend::etcd);
                if (op == "try_acquire") {
                    status = dilock_etcd_try_acquire(lock, lease_ms, &acquired->etcd, &error);
                } else {
                    status = dilock_etcd_acquire_with_cancellation(lock, wait_ms, lease_ms,
                        cancellation->handle, &acquired->etcd, &error);
                }
                if (status == DILOCK_STATUS_OK) lease = std::move(acquired);
            }
            dilock_etcd_lock_destroy(lock);
            dilock_etcd_provider_destroy(provider);
        }
        if (status != DILOCK_STATUS_OK) { write(failure(id, status, error)); return; }

        const auto lease_id = "c-" + std::to_string(next_lease_.fetch_add(1, std::memory_order_relaxed));
        int64_t effective_lease_ms = lease_ms;
        if (options_.backend == Backend::etcd) {
            status = dilock_etcd_lease_effective_ttl_ms(
                lease->etcd, &effective_lease_ms, &error);
            if (status != DILOCK_STATUS_OK) {
                dilock_release_outcome ignored = DILOCK_RELEASE_UNKNOWN;
                (void)lease->release(&ignored, &error);
                write(failure(id, status, error));
                return;
            }
        }
        Json response{{"id", id}, {"status", "acquired"}, {"lease_id", lease_id},
                      {"effective_lease_ms", effective_lease_ms}};
        if (options_.backend == Backend::etcd) {
            dilock_etcd_fence fence{};
            char epoch[1024]{};
            status = dilock_etcd_lease_fence(lease->etcd, &fence, epoch, sizeof(epoch), &error);
            if (status != DILOCK_STATUS_OK) {
                dilock_release_outcome ignored = DILOCK_RELEASE_UNKNOWN;
                (void)lease->release(&ignored, &error);
                write(failure(id, status, error));
                return;
            }
            response["fence"] = {{"epoch", epoch}, {"value", std::to_string(fence.create_revision)}};
        }
        {
            std::lock_guard guard(mutex_);
            leases_.emplace(lease_id, std::move(lease));
        }
        write(std::move(response));
    }

    void inspect(const std::string& id, const Json& request) {
        if (!request.contains("lease_id") || !request.at("lease_id").is_string()) {
            write({{"id", id}, {"status", "invalid_argument"}, {"error", "inspect requires lease_id"}});
            return;
        }
        std::shared_ptr<Lease> lease;
        {
            std::lock_guard guard(mutex_);
            const auto found = leases_.find(request.at("lease_id").get<std::string>());
            if (found != leases_.end()) lease = found->second;
        }
        if (!lease) { write({{"id", id}, {"status", "invalid_argument"}, {"error", "unknown lease_id"}}); return; }
        write({{"id", id}, {"status", lease->valid() ? "held" : "lost"}});
    }

    void release(const std::string& id, const Json& request) {
        if (!request.contains("lease_id") || !request.at("lease_id").is_string()) {
            write({{"id", id}, {"status", "invalid_argument"}, {"error", "release requires lease_id"}});
            return;
        }
        std::shared_ptr<Lease> lease;
        {
            std::lock_guard guard(mutex_);
            const auto found = leases_.find(request.at("lease_id").get<std::string>());
            if (found != leases_.end()) lease = found->second;
        }
        if (!lease) { write({{"id", id}, {"status", "invalid_argument"}, {"error", "unknown lease_id"}}); return; }
        dilock_error error{};
        dilock_release_outcome outcome = DILOCK_RELEASE_UNKNOWN;
        const auto status = lease->release(&outcome, &error);
        if (status != DILOCK_STATUS_OK) { write(failure(id, status, error)); return; }
        const char* result = outcome == DILOCK_RELEASED ? "released" :
            outcome == DILOCK_ALREADY_NOT_OWNED ? "already_not_owned" : "release_unknown";
        write({{"id", id}, {"status", result}});
    }

    void renew(const std::string& id, const Json& request) {
        if (!request.contains("lease_id") || !request.at("lease_id").is_string() ||
            !request.contains("lease_ms") || !request.at("lease_ms").is_number_integer() ||
            request.at("lease_ms").get<std::int64_t>() <= 0) {
            write({{"id", id}, {"status", "invalid_argument"}, {"error", "renew requires lease_id and positive lease_ms"}});
            return;
        }
        std::shared_ptr<Lease> lease;
        {
            std::lock_guard guard(mutex_);
            const auto found = leases_.find(request.at("lease_id").get<std::string>());
            if (found != leases_.end()) lease = found->second;
        }
        if (!lease) { write({{"id", id}, {"status", "invalid_argument"}, {"error", "unknown lease_id"}}); return; }
        if (lease->backend == Backend::etcd) {
            write({{"id", id}, {"status", "unsupported_capability"}, {"capability", "manual_renewal"}});
            return;
        }
        int32_t renewed = 0;
        dilock_error error{};
        const auto status = lease->renew(request.at("lease_ms").get<std::int64_t>(), &renewed, &error);
        if (status != DILOCK_STATUS_OK) { write(failure(id, status, error)); return; }
        write({{"id", id}, {"status", renewed != 0 ? "renewed" : "lost"}});
    }

    void shutdown(const std::string& id) {
        {
            std::lock_guard guard(mutex_);
            for (const auto& [_, cancellation] : pending_) dilock_cancellation_request(cancellation->handle);
        }
        for (auto& task : tasks_) if (task.thread.joinable()) task.thread.join();
        tasks_.clear();
        std::size_t outstanding = 0U;
        std::vector<std::shared_ptr<Lease>> leases;
        {
            std::lock_guard guard(mutex_);
            for (const auto& [_, lease] : leases_) leases.push_back(lease);
        }
        for (const auto& lease : leases) {
            dilock_error error{};
            dilock_release_outcome outcome = DILOCK_RELEASE_UNKNOWN;
            if (lease->release(&outcome, &error) != DILOCK_STATUS_OK || outcome == DILOCK_RELEASE_UNKNOWN) ++outstanding;
        }
        leases_.clear();
        if (id != "eof") write({{"id", id}, {"status", "ok"}, {"outstanding_cleanup_attempts", outstanding}});
    }

    Options options_;
    std::mutex mutex_;
    std::mutex output_mutex_;
    std::unordered_map<std::string, std::shared_ptr<Cancellation>> pending_;
    std::unordered_map<std::string, std::shared_ptr<Lease>> leases_;
    std::unordered_set<std::string> active_ids_;
    std::vector<Task> tasks_;
    std::atomic_uint64_t next_lease_{1};
};
} // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "usage: dilock-c-conformance-worker <config.json>\n";
        return 2;
    }
    try {
        if (dilock_c_abi_version() != DILOCK_C_ABI_VERSION) return 3;
        Worker(read_options(argv[1])).run();
        return 0;
    } catch (const std::exception&) {
        std::cerr << "dilock C conformance worker initialization failed\n";
        return 2;
    }
}
