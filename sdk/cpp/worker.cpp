#include "dilock/redis.hpp"
#ifdef DILOCK_WORKER_ETCD
#include "dilock/etcd.hpp"
#endif

#include <nlohmann/json.hpp>

#include <atomic>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <stop_token>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

namespace {
using Json = nlohmann::json;

struct WorkerOptions {
    std::string backend{"redis"};
    dilock::RedisOptions redis;
#ifdef DILOCK_WORKER_ETCD
    dilock::EtcdOptions etcd;
#endif
};

#ifdef DILOCK_WORKER_ETCD
using ManagedLease = std::variant<dilock::Lease, dilock::EtcdLease>;
bool lease_valid(const ManagedLease& lease) {
    return std::visit([](const auto& value) { return value.valid(); }, lease);
}
dilock::ReleaseResult lease_release(ManagedLease& lease) noexcept {
    return std::visit([](auto& value) { return value.release(); }, lease);
}
#else
using ManagedLease = dilock::Lease;
bool lease_valid(const ManagedLease& lease) { return lease.valid(); }
dilock::ReleaseResult lease_release(ManagedLease& lease) noexcept { return lease.release(); }
#endif

class Worker final {
public:
    explicit Worker(WorkerOptions options) : options_(std::move(options)) {}

    void run() {
        std::string line;
        while (std::getline(std::cin, line)) {
            if (line.size() > 64 * 1024) {
                write({{"status", "invalid_argument"}, {"error", "input line exceeds 64 KiB"}});
                continue;
            }
            Json request;
            try {
                request = Json::parse(line);
                if (!request.is_object() || !request.contains("id") || !request.at("id").is_string() ||
                    !request.contains("op") || !request.at("op").is_string()) {
                    throw std::runtime_error("request requires string id and op");
                }
            } catch (const std::exception&) {
                write({{"status", "invalid_argument"}, {"error", "malformed request"}});
                continue;
            }
            const auto id = request.at("id").get<std::string>();
            const auto op = request.at("op").get<std::string>();
            if (id.empty() || id.size() > 128) {
                write({{"id", id}, {"status", "invalid_argument"}, {"error", "request id length must be 1..128"}});
                continue;
            }
            static const std::unordered_set<std::string> request_fields{
                "id", "op", "resource", "lease_id", "target_id", "wait_ms", "lease_ms", "rpc_ms", "auto_renew"};
            bool unknown_field = false;
            for (auto field = request.begin(); field != request.end(); ++field) {
                if (!request_fields.contains(field.key())) unknown_field = true;
            }
            if (unknown_field) {
                write({{"id", id}, {"status", "invalid_argument"}, {"error", "unknown request field"}});
                continue;
            }
            if (op == "shutdown") {
                shutdown(id);
                return;
            }
            if (op == "cancel_request") {
                cancel(id, request);
                continue;
            }
            {
                std::lock_guard guard(mutex_);
                if (!active_ids_.insert(id).second) {
                    write({{"id", id}, {"status", "invalid_argument"}, {"error", "request id is already active"}});
                    continue;
                }
            }
            auto cancellation = std::make_shared<std::stop_source>();
            {
                std::lock_guard guard(mutex_);
                pending_.emplace(id, cancellation);
            }
            threads_.emplace_back([this, id, request = std::move(request), cancellation]() mutable {
                dispatch(id, request, cancellation->get_token());
                std::lock_guard guard(mutex_);
                pending_.erase(id);
                active_ids_.erase(id);
            });
        }
        shutdown("eof");
    }

private:
    void write(Json response) {
        std::lock_guard guard(output_mutex_);
        std::cout << response.dump() << '\n' << std::flush;
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
            found->second->request_stop();
            write({{"id", id}, {"status", "cancel_requested"}, {"found", true}});
        }
    }

    void dispatch(const std::string& id, const Json& request, std::stop_token stop) {
        try {
            const auto& op = request.at("op").get_ref<const std::string&>();
            if (op == "hello") {
                Json capabilities = options_.backend == "etcd"
                    ? Json{"etcd", "renewal", "auto_renew", "fencing"}
                    : Json{"redis", "renewal"};
                Json hello{{"id", id}, {"status", "ok"}, {"sdk", "cpp"}, {"protocol", 1},
                           {"capabilities", std::move(capabilities)}};
                if (options_.backend == "etcd") {
                    hello["backend"] = "etcd";
                    hello["client"] = "grpc++ with official etcd v3.7.2 bindings";
                    hello["server_version"] = "unknown";
                } else {
                    hello["client"] = "redis-plus-plus/1.3.15";
                }
                write(std::move(hello));
                return;
            }
            if (op == "inspect") {
                const auto lease_id = request.at("lease_id").get<std::string>();
                std::lock_guard guard(mutex_);
                const auto found = leases_.find(lease_id);
                if (found == leases_.end()) {
                    write({{"id", id}, {"status", "invalid_argument"}, {"error", "unknown lease_id"}});
                } else {
                    write({{"id", id}, {"status", lease_valid(found->second) ? "held" : "lost"}});
                }
                return;
            }
            if (op == "release") {
                release(id, request);
                return;
            }
            if (op == "renew") {
                renew(id, request);
                return;
            }
            if (op != "acquire" && op != "try_acquire") {
                write({{"id", id}, {"status", "invalid_argument"}, {"error", "unknown operation"}});
                return;
            }
            if (!request.contains("resource") || !request.at("resource").is_string() ||
                !request.contains("lease_ms") || !request.at("lease_ms").is_number_integer() ||
                (op == "acquire" && (!request.contains("wait_ms") || !request.at("wait_ms").is_number_integer()))) {
                write({{"id", id}, {"status", "invalid_argument"}, {"error", "acquisition fields are missing or invalid"}});
                return;
            }
            if (request.value("auto_renew", false) && options_.backend == "redis") {
                write({{"id", id}, {"status", "unsupported_capability"}, {"capability", "auto_renew"}});
                return;
            }
            const auto resource = request.at("resource").get<std::string>();
            const auto ttl = std::chrono::milliseconds(request.at("lease_ms").get<std::int64_t>());
            auto rpc_timeout = options_.backend == "redis" ? options_.redis.rpc_timeout : std::chrono::milliseconds(2000);
            if (request.contains("rpc_ms")) {
                if (!request.at("rpc_ms").is_number_integer()) {
                    write({{"id", id}, {"status", "invalid_argument"}, {"error", "rpc_ms must be an integer"}});
                    return;
                }
                rpc_timeout = std::chrono::milliseconds(request.at("rpc_ms").get<std::int64_t>());
            }
            const auto wait = op == "acquire"
                ? std::chrono::milliseconds(request.at("wait_ms").get<std::int64_t>())
                : std::chrono::milliseconds::zero();
            std::optional<ManagedLease> acquired;
            std::chrono::milliseconds effective_ttl = ttl;
            Json fence = nullptr;
            if (options_.backend == "etcd") {
#ifdef DILOCK_WORKER_ETCD
                auto options = options_.etcd;
                options.rpc_timeout = rpc_timeout;
                options.auto_renew = request.value("auto_renew", false);
                dilock::EtcdProvider provider(std::move(options));
                auto lock = provider.lock(resource);
                if (op == "try_acquire") {
                    auto lease = lock.try_acquire(ttl, stop);
                    if (lease) acquired.emplace(std::move(*lease));
                } else {
                    acquired.emplace(lock.acquire(wait, ttl, stop));
                }
                if (acquired && std::holds_alternative<dilock::EtcdLease>(*acquired)) {
                    const auto& lease = std::get<dilock::EtcdLease>(*acquired);
                    effective_ttl = lease.effective_ttl();
                    const auto value = lease.fence();
                    fence = {{"epoch", value.coordination_epoch}, {"value", std::to_string(value.create_revision)}};
                }
#else
                write({{"id", id}, {"status", "unsupported_capability"}, {"capability", "etcd"}});
                return;
#endif
            } else {
                auto options = options_.redis;
                options.rpc_timeout = rpc_timeout;
                dilock::RedisProvider provider(std::move(options));
                auto lock = provider.lock(resource);
                if (op == "try_acquire") {
                    auto lease = lock.try_acquire(ttl, stop);
                    if (lease) acquired.emplace(std::move(*lease));
                } else {
                    acquired.emplace(lock.acquire(wait, ttl, stop));
                }
            }
            if (!acquired) {
                write({{"id", id}, {"status", "busy"}});
                return;
            }
            const auto lease_id = "cpp-" + std::to_string(next_lease_.fetch_add(1));
            {
                std::lock_guard guard(mutex_);
                leases_.emplace(lease_id, std::move(*acquired));
            }
            Json response{{"id", id}, {"status", "acquired"}, {"lease_id", lease_id},
                          {"effective_lease_ms", effective_ttl.count()}};
            if (!fence.is_null()) response["fence"] = std::move(fence);
            write(std::move(response));
        } catch (const dilock::Error& error) {
            write(map_error(id, error));
        } catch (const nlohmann::json::exception&) {
            write({{"id", id}, {"status", "invalid_argument"}, {"error", "invalid request fields"}});
        } catch (const std::exception&) {
            write({{"id", id}, {"status", "internal_error"}, {"error", "worker operation failed"}});
        }
    }

    static Json map_error(const std::string& id, const dilock::Error& error) {
        std::string status;
        switch (error.code()) {
            case dilock::ErrorCode::invalid_argument: status = "invalid_argument"; break;
            case dilock::ErrorCode::cancelled:
                return {{"id", id}, {"status", "cancelled"},
                        {"remote_outcome_unknown", std::string(error.what()).find("unknown") != std::string::npos}};
            case dilock::ErrorCode::wait_timeout: status = "timeout"; break;
            case dilock::ErrorCode::backend_unavailable: status = "backend_unavailable"; break;
            case dilock::ErrorCode::acquisition_outcome_unknown: status = "acquisition_unknown"; break;
            case dilock::ErrorCode::protocol_mismatch: status = "protocol_mismatch"; break;
            case dilock::ErrorCode::lease_lost: status = "backend_unavailable"; break;
            case dilock::ErrorCode::unsupported_capability: status = "unsupported_capability"; break;
        }
        return {{"id", id}, {"status", status}};
    }

    void release(const std::string& id, const Json& request) {
        const auto lease_id = request.at("lease_id").get<std::string>();
        std::lock_guard guard(mutex_);
        const auto found = leases_.find(lease_id);
        if (found == leases_.end()) {
            write({{"id", id}, {"status", "invalid_argument"}, {"error", "unknown lease_id"}});
            return;
        }
        const auto result = lease_release(found->second);
        const char* status = result == dilock::ReleaseResult::released ? "released" :
            result == dilock::ReleaseResult::already_not_owned ? "already_not_owned" : "release_unknown";
        write({{"id", id}, {"status", status}});
    }

    void renew(const std::string& id, const Json& request) {
        const auto lease_id = request.at("lease_id").get<std::string>();
        const auto ttl = std::chrono::milliseconds(request.at("lease_ms").get<std::int64_t>());
        std::lock_guard guard(mutex_);
        const auto found = leases_.find(lease_id);
        if (found == leases_.end()) {
            write({{"id", id}, {"status", "invalid_argument"}, {"error", "unknown lease_id"}});
            return;
        }
#ifdef DILOCK_WORKER_ETCD
        auto* redis_lease = std::get_if<dilock::Lease>(&found->second);
        if (redis_lease == nullptr) {
            write({{"id", id}, {"status", "unsupported_capability"}, {"capability", "manual_renewal"}});
            return;
        }
        write({{"id", id}, {"status", redis_lease->renew(ttl) ? "renewed" : "lost"}});
#else
        write({{"id", id}, {"status", found->second.renew(ttl) ? "renewed" : "lost"}});
#endif
    }

    void shutdown(const std::string& id) {
        {
            std::lock_guard guard(mutex_);
            for (const auto& [_, source] : pending_) source->request_stop();
        }
        for (auto& thread : threads_) if (thread.joinable()) thread.join();
        threads_.clear();
        std::size_t outstanding = 0;
        {
            std::lock_guard guard(mutex_);
            for (auto& [_, lease] : leases_) {
                if (lease_release(lease) == dilock::ReleaseResult::unknown) ++outstanding;
            }
            leases_.clear();
        }
        if (id != "eof") {
            write({{"id", id}, {"status", "ok"}, {"outstanding_cleanup_attempts", outstanding}});
        }
    }

    WorkerOptions options_;
    std::mutex mutex_;
    std::mutex output_mutex_;
    std::unordered_map<std::string, std::shared_ptr<std::stop_source>> pending_;
    std::unordered_map<std::string, ManagedLease> leases_;
    std::unordered_set<std::string> active_ids_;
    std::vector<std::jthread> threads_;
    std::atomic<unsigned long long> next_lease_{1};
};

WorkerOptions read_options(const char* path) {
    std::ifstream input(path);
    if (!input) throw std::runtime_error("could not open worker config file");
    const auto config = Json::parse(input);
    WorkerOptions options;
    options.backend = config.value("backend", std::string{"redis"});
    if (options.backend != "redis" && options.backend != "etcd") {
        throw std::runtime_error("worker backend must be redis or etcd");
    }
    if (options.backend == "etcd") {
#ifdef DILOCK_WORKER_ETCD
        auto endpoint = config.at("endpoint").get<std::string>();
        constexpr std::string_view http_scheme{"http://"};
        if (!endpoint.starts_with(http_scheme)) {
            throw std::runtime_error("C++ etcd worker requires an http:// endpoint and uses insecure gRPC credentials");
        }
        endpoint.erase(0, http_scheme.size());
        options.etcd.endpoint = std::move(endpoint);
        options.etcd.name_space = config.at("namespace").get<std::string>();
        options.etcd.coordination_epoch = config.at("epoch").get<std::string>();
        options.etcd.rpc_timeout = std::chrono::milliseconds(config.value("rpc_timeout_ms", 2000));
        options.etcd.cleanup_timeout = std::chrono::milliseconds(config.value("cleanup_timeout_ms", 2000));
#else
        throw std::runtime_error("this C++ worker build has no etcd support; configure DILOCK_BUILD_ETCD=ON");
#endif
        return options;
    }
    if (config.contains("redis")) {
        const auto& redis = config.at("redis");
        options.redis.host = redis.value("host", options.redis.host);
        options.redis.port = redis.value("port", options.redis.port);
        options.redis.username = redis.value("username", options.redis.username);
        options.redis.password = redis.value("password", options.redis.password);
        options.redis.database = redis.value("database", options.redis.database);
        options.redis.name_space = redis.at("namespace").get<std::string>();
        options.redis.coordination_epoch = redis.at("coordination_epoch").get<std::string>();
        options.redis.rpc_timeout = std::chrono::milliseconds(redis.value("rpc_timeout_ms", options.redis.rpc_timeout.count()));
        options.redis.safety_margin = std::chrono::milliseconds(redis.value("safety_margin_ms", options.redis.safety_margin.count()));
        options.redis.poll_interval = std::chrono::milliseconds(redis.value("poll_interval_ms", options.redis.poll_interval.count()));
    } else {
        const auto endpoint = config.at("endpoint").get<std::string>();
        constexpr std::string_view scheme{"redis://"};
        if (!endpoint.starts_with(scheme)) throw std::runtime_error("worker endpoint must use redis://");
        const auto authority = endpoint.substr(scheme.size());
        const auto colon = authority.rfind(':');
        if (colon == std::string::npos) throw std::runtime_error("worker endpoint requires an explicit port");
        options.redis.host = authority.substr(0, colon);
        options.redis.port = std::stoi(authority.substr(colon + 1));
        options.redis.name_space = config.at("namespace").get<std::string>();
        options.redis.coordination_epoch = config.at("epoch").get<std::string>();
        options.redis.rpc_timeout = std::chrono::milliseconds(200);
    }
    return options;
}
} // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "usage: dilock-cpp-worker <config.json>\n";
        return 2;
    }
    try {
        Worker(read_options(argv[1])).run();
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "dilock-cpp-worker initialization failed: " << error.what() << '\n';
        return 2;
    }
}
