#include "dilock/dilock.h"
#include "c_api_error.hpp"

#ifdef DILOCK_C_HAS_ETCD
#include "dilock/etcd.hpp"
#endif
#ifdef DILOCK_C_HAS_CONSUL
#include "dilock/consul.hpp"
#endif
#include "dilock/redis.hpp"
#ifdef DILOCK_C_HAS_POSTGRES
#include "dilock/postgres.hpp"
#endif

#include <algorithm>
#include <cstring>
#include <exception>
#include <memory>
#include <new>
#include <atomic>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>

struct dilock_redis_provider { std::unique_ptr<dilock::RedisProvider> value; };
struct dilock_redis_lock { dilock::ResourceLock value; };
struct dilock_redis_lease { dilock::Lease value; };
#ifdef DILOCK_C_HAS_ETCD
struct dilock_etcd_provider { std::unique_ptr<dilock::EtcdProvider> value; };
struct dilock_etcd_lock { dilock::EtcdResourceLock value; };
struct dilock_etcd_lease { dilock::EtcdLease value; };
#endif
#ifdef DILOCK_C_HAS_CONSUL
struct dilock_consul_provider { std::unique_ptr<dilock::ConsulProvider> value; };
struct dilock_consul_lock { dilock::ConsulResourceLock value; };
struct dilock_consul_lease { dilock::ConsulLease value; };
#endif
#ifdef DILOCK_C_HAS_POSTGRES
struct dilock_postgres_provider { std::unique_ptr<dilock::PostgresProvider> value; };
struct dilock_postgres_lock { dilock::PostgresResourceLock value; };
struct dilock_postgres_lease { dilock::PostgresLease value; };
#endif
struct dilock_cancellation { std::atomic_bool requested{false}; };

namespace {

void clear_error(dilock_error* error) noexcept {
    if (error == nullptr) return;
    error->code = DILOCK_STATUS_OK;
    error->message[0] = '\0';
}

void set_error(dilock_error* error, dilock_status status, const char* message) noexcept {
    if (error == nullptr) return;
    error->code = static_cast<int32_t>(status);
    const char* source = message == nullptr ? "Dilock operation failed" : message;
    const auto length = std::min<std::size_t>(std::strlen(source), DILOCK_C_ERROR_MESSAGE_CAPACITY - 1u);
    std::memcpy(error->message, source, length);
    error->message[length] = '\0';
}

dilock_status translate(dilock::ErrorCode code) noexcept {
    using dilock::ErrorCode;
    switch (code) {
    case ErrorCode::invalid_argument: return DILOCK_STATUS_INVALID_ARGUMENT;
    case ErrorCode::cancelled: return DILOCK_STATUS_CANCELLED;
    case ErrorCode::wait_timeout: return DILOCK_STATUS_WAIT_TIMEOUT;
    case ErrorCode::backend_unavailable: return DILOCK_STATUS_BACKEND_UNAVAILABLE;
    case ErrorCode::acquisition_outcome_unknown: return DILOCK_STATUS_ACQUISITION_UNKNOWN;
    case ErrorCode::protocol_mismatch: return DILOCK_STATUS_PROTOCOL_MISMATCH;
    case ErrorCode::lease_lost: return DILOCK_STATUS_LEASE_LOST;
    case ErrorCode::unsupported_capability: return DILOCK_STATUS_UNSUPPORTED_CAPABILITY;
    }
    return DILOCK_STATUS_INTERNAL;
}

template<class Function>
dilock_status invoke(dilock_error* error, Function&& function) noexcept {
    clear_error(error);
    try {
        function();
        return DILOCK_STATUS_OK;
    } catch (const dilock::Error& failure) {
        const auto status = translate(failure.code());
        set_error(error, status, failure.what());
        return status;
    } catch (const std::invalid_argument& failure) {
        set_error(error, DILOCK_STATUS_INVALID_ARGUMENT, failure.what());
        return DILOCK_STATUS_INVALID_ARGUMENT;
    } catch (const std::exception& failure) {
        const auto status = dilock::c_api_detail::standard_exception_status(failure);
        set_error(error, status, failure.what());
        return status;
    } catch (...) {
        set_error(error, DILOCK_STATUS_INTERNAL, "unknown native exception");
        return DILOCK_STATUS_INTERNAL;
    }
}

template<class Function>
dilock_status invoke_cancellable(dilock_error* error, dilock_cancellation* cancellation,
    Function&& function) noexcept {
    if (cancellation == nullptr) return invoke(error, [&] { function(std::stop_token{}); });
    return invoke(error, [&] {
        std::stop_source source;
        if (cancellation->requested.load(std::memory_order_acquire)) source.request_stop();
        std::jthread watcher([&](std::stop_token stop) {
            while (!stop.stop_requested() && !cancellation->requested.load(std::memory_order_acquire)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
            }
            if (cancellation->requested.load(std::memory_order_acquire)) source.request_stop();
        });
        function(source.get_token());
        watcher.request_stop();
    });
}

template<class T>
bool require_output(T** output, dilock_error* error) noexcept {
    if (output == nullptr) {
        set_error(error, DILOCK_STATUS_INVALID_ARGUMENT, "output pointer is null");
        return false;
    }
    *output = nullptr;
    return true;
}

const char* optional_string(const char* value, const char* fallback) noexcept {
    return value == nullptr ? fallback : value;
}

dilock_status public_release(dilock::ReleaseResult result, dilock_release_outcome* outcome) noexcept {
    if (outcome == nullptr) return DILOCK_STATUS_INVALID_ARGUMENT;
    switch (result) {
    case dilock::ReleaseResult::released: *outcome = DILOCK_RELEASED; break;
    case dilock::ReleaseResult::already_not_owned: *outcome = DILOCK_ALREADY_NOT_OWNED; break;
    case dilock::ReleaseResult::unknown: *outcome = DILOCK_RELEASE_UNKNOWN; break;
    }
    return DILOCK_STATUS_OK;
}

dilock::RedisOptions redis_options(const dilock_redis_options* source) {
    if (source == nullptr) throw std::invalid_argument("Redis options pointer is null");
    dilock::RedisOptions value;
    value.host = optional_string(source->host, "127.0.0.1");
    value.port = source->port == 0 ? 6379 : source->port;
    value.username = optional_string(source->username, "");
    value.password = optional_string(source->password, "");
    value.database = source->database;
    value.name_space = optional_string(source->name_space, "default");
    value.coordination_epoch = optional_string(source->coordination_epoch, "");
    if (source->cleanup_callback != nullptr) {
        const auto callback = source->cleanup_callback;
        void* const user_data = source->cleanup_user_data;
        value.cleanup_observer = [callback, user_data](const dilock::CleanupObservation& observation) {
            const auto outcome = observation.outcome == dilock::ReleaseResult::unknown ? DILOCK_RELEASE_UNKNOWN :
                observation.outcome == dilock::ReleaseResult::already_not_owned ? DILOCK_ALREADY_NOT_OWNED : DILOCK_RELEASED;
            callback(DILOCK_BACKEND_REDIS, outcome, observation.reason.c_str(), user_data);
        };
    }
    value.rpc_timeout = std::chrono::milliseconds(source->rpc_timeout_ms);
    value.cleanup_timeout = std::chrono::milliseconds(source->cleanup_timeout_ms);
    value.safety_margin = std::chrono::milliseconds(source->safety_margin_ms);
    value.poll_interval = std::chrono::milliseconds(source->poll_interval_ms);
    value.default_acquisition.wait = std::chrono::milliseconds(source->default_wait_ms);
    value.default_acquisition.lease = std::chrono::milliseconds(source->default_lease_ms);
    return value;
}

#ifdef DILOCK_C_HAS_ETCD
dilock::EtcdOptions etcd_options(const dilock_etcd_options* source) {
    if (source == nullptr) throw std::invalid_argument("etcd options pointer is null");
    dilock::EtcdOptions value;
    value.endpoint = optional_string(source->endpoint, "127.0.0.1:2379");
    value.name_space = optional_string(source->name_space, "default");
    value.coordination_epoch = optional_string(source->coordination_epoch, "");
    const std::string roots = optional_string(source->tls_root_certificates, "");
    const std::string certificate = optional_string(source->tls_client_certificate, "");
    const std::string private_key = optional_string(source->tls_client_private_key, "");
    if (!roots.empty() || !certificate.empty() || !private_key.empty()) {
        grpc::SslCredentialsOptions tls;
        tls.pem_root_certs = roots;
        tls.pem_cert_chain = certificate;
        tls.pem_private_key = private_key;
        value.credentials = grpc::SslCredentials(tls);
    }
    if (source->cleanup_callback != nullptr) {
        const auto callback = source->cleanup_callback;
        void* const user_data = source->cleanup_user_data;
        value.cleanup_observer = [callback, user_data](const dilock::CleanupObservation& observation) {
            const auto outcome = observation.outcome == dilock::ReleaseResult::unknown ? DILOCK_RELEASE_UNKNOWN :
                observation.outcome == dilock::ReleaseResult::already_not_owned ? DILOCK_ALREADY_NOT_OWNED : DILOCK_RELEASED;
            callback(DILOCK_BACKEND_ETCD, outcome, observation.reason.c_str(), user_data);
        };
    }
    value.rpc_timeout = std::chrono::milliseconds(source->rpc_timeout_ms);
    value.cleanup_timeout = std::chrono::milliseconds(source->cleanup_timeout_ms);
    value.safety_margin = std::chrono::milliseconds(source->safety_margin_ms);
    value.default_acquisition.wait = std::chrono::milliseconds(source->default_wait_ms);
    value.default_acquisition.lease = std::chrono::milliseconds(source->default_lease_ms);
    value.auto_renew = source->auto_renew != 0;
    return value;
}
#endif

#ifdef DILOCK_C_HAS_CONSUL
dilock::ConsulOptions consul_options(const dilock_consul_options* source) {
    if (source == nullptr) throw std::invalid_argument("Consul options pointer is null");
    dilock::ConsulOptions value;
    value.endpoint = optional_string(source->endpoint, "http://127.0.0.1:8500");
    value.name_space = optional_string(source->name_space, "default");
    value.coordination_epoch = optional_string(source->coordination_epoch, "");
    value.acl_token = optional_string(source->acl_token, "");
    value.rpc_timeout = std::chrono::milliseconds(source->rpc_timeout_ms);
    value.cleanup_timeout = std::chrono::milliseconds(source->cleanup_timeout_ms);
    value.poll_interval = std::chrono::milliseconds(source->poll_interval_ms);
    value.default_acquisition.wait = std::chrono::milliseconds(source->default_wait_ms);
    value.default_acquisition.lease = std::chrono::milliseconds(source->default_lease_ms);
    if (source->cleanup_callback != nullptr) {
        const auto callback = source->cleanup_callback;
        void* const user_data = source->cleanup_user_data;
        value.cleanup_observer = [callback, user_data](const dilock::CleanupObservation& observation) {
            const auto outcome = observation.outcome == dilock::ReleaseResult::unknown ? DILOCK_RELEASE_UNKNOWN :
                observation.outcome == dilock::ReleaseResult::already_not_owned ? DILOCK_ALREADY_NOT_OWNED : DILOCK_RELEASED;
            callback(DILOCK_BACKEND_CONSUL, outcome, observation.reason.c_str(), user_data);
        };
    }
    return value;
}
#endif

#ifdef DILOCK_C_HAS_POSTGRES
dilock::PostgresOptions postgres_options(const dilock_postgres_options* source) {
    if (source == nullptr) throw std::invalid_argument("PostgreSQL options pointer is null");
    dilock::PostgresOptions value;
    value.connection_string = optional_string(source->connection_string, "");
    value.name_space = optional_string(source->name_space, "default");
    value.coordination_epoch = optional_string(source->coordination_epoch, "");
    value.rpc_timeout = std::chrono::milliseconds(source->rpc_timeout_ms);
    value.cleanup_timeout = std::chrono::milliseconds(source->cleanup_timeout_ms);
    value.poll_interval = std::chrono::milliseconds(source->poll_interval_ms);
    value.default_acquisition.wait = std::chrono::milliseconds(source->default_wait_ms);
    value.default_acquisition.lease = std::chrono::milliseconds(source->default_lease_ms);
    if (source->cleanup_callback != nullptr) {
        const auto callback = source->cleanup_callback;
        void* const user_data = source->cleanup_user_data;
        value.cleanup_observer = [callback, user_data](const dilock::CleanupObservation& observation) {
            const auto outcome = observation.outcome == dilock::ReleaseResult::unknown ? DILOCK_RELEASE_UNKNOWN :
                observation.outcome == dilock::ReleaseResult::already_not_owned ? DILOCK_ALREADY_NOT_OWNED : DILOCK_RELEASED;
            callback(DILOCK_BACKEND_POSTGRES, outcome, observation.reason.c_str(), user_data);
        };
    }
    return value;
}
#endif

} // namespace

extern "C" {

uint32_t dilock_c_abi_version(void) { return DILOCK_C_ABI_VERSION; }

void dilock_redis_options_init(dilock_redis_options* options) {
    if (options == nullptr) return;
    *options = dilock_redis_options{"127.0.0.1", 6379, "", "", 0, "default", "",
        nullptr, nullptr, 2000, 2000, 25, 25, 5000, 30000};
}

void dilock_etcd_options_init(dilock_etcd_options* options) {
    if (options == nullptr) return;
    *options = dilock_etcd_options{"127.0.0.1:2379", "default", "", nullptr, nullptr, nullptr, nullptr, nullptr,
        2000, 2000, 25, 5000, 30000, 0};
}
void dilock_consul_options_init(dilock_consul_options* options) {
    if (options == nullptr) return;
    *options = dilock_consul_options{"http://127.0.0.1:8500", "default", "", "", nullptr, nullptr,
        2000, 2000, 50, 5000, 30000};
}
void dilock_postgres_options_init(dilock_postgres_options* options) {
    if (options == nullptr) return;
    *options = dilock_postgres_options{"", "default", "", nullptr, nullptr, 2000, 2000, 25, 5000, 30000};
}
dilock_cancellation* dilock_cancellation_create(dilock_error* error) {
    dilock_cancellation* result = nullptr;
    (void)invoke(error, [&] { result = new dilock_cancellation(); });
    return result;
}
void dilock_cancellation_request(dilock_cancellation* cancellation) {
    if (cancellation != nullptr) cancellation->requested.store(true, std::memory_order_release);
}
void dilock_cancellation_destroy(dilock_cancellation* cancellation) { delete cancellation; }

dilock_status dilock_redis_provider_create(const dilock_redis_options* options,
    dilock_redis_provider** out, dilock_error* error) {
    if (!require_output(out, error)) return DILOCK_STATUS_INVALID_ARGUMENT;
    return invoke(error, [&] { auto value = std::make_unique<dilock_redis_provider>(); value->value = std::make_unique<dilock::RedisProvider>(redis_options(options)); *out = value.release(); });
}
void dilock_redis_provider_destroy(dilock_redis_provider* provider) { delete provider; }
dilock_status dilock_redis_lock_create(dilock_redis_provider* provider, const char* resource,
    dilock_redis_lock** out, dilock_error* error) {
    if (!require_output(out, error)) return DILOCK_STATUS_INVALID_ARGUMENT;
    return invoke(error, [&] { if (provider == nullptr || resource == nullptr) throw std::invalid_argument("provider and resource are required"); auto value = std::make_unique<dilock_redis_lock>(); value->value = provider->value->lock(resource); *out = value.release(); });
}
void dilock_redis_lock_destroy(dilock_redis_lock* lock) { delete lock; }
dilock_status dilock_redis_acquire(dilock_redis_lock* lock, int64_t wait_ms, int64_t lease_ms,
    dilock_redis_lease** out, dilock_error* error) {
    if (!require_output(out, error)) return DILOCK_STATUS_INVALID_ARGUMENT;
    return invoke(error, [&] { if (lock == nullptr) throw std::invalid_argument("lock is null"); auto value = std::make_unique<dilock_redis_lease>(); value->value = lock->value.acquire(std::chrono::milliseconds(wait_ms), std::chrono::milliseconds(lease_ms)); *out = value.release(); });
}
dilock_status dilock_redis_acquire_default(dilock_redis_lock* lock,
    dilock_redis_lease** out, dilock_error* error) {
    if (!require_output(out, error)) return DILOCK_STATUS_INVALID_ARGUMENT;
    return invoke(error, [&] { if (lock == nullptr) throw std::invalid_argument("lock is null"); auto value = std::make_unique<dilock_redis_lease>(); value->value = lock->value.acquire(); *out = value.release(); });
}
dilock_status dilock_redis_acquire_with_cancellation(dilock_redis_lock* lock,
    int64_t wait_ms, int64_t lease_ms, dilock_cancellation* cancellation,
    dilock_redis_lease** out, dilock_error* error) {
    if (!require_output(out, error)) return DILOCK_STATUS_INVALID_ARGUMENT;
    return invoke_cancellable(error, cancellation, [&](std::stop_token stop) { if (lock == nullptr) throw std::invalid_argument("lock is null"); auto value = std::make_unique<dilock_redis_lease>(); value->value = lock->value.acquire(std::chrono::milliseconds(wait_ms), std::chrono::milliseconds(lease_ms), stop); *out = value.release(); });
}
dilock_status dilock_redis_acquire_default_with_cancellation(dilock_redis_lock* lock,
    dilock_cancellation* cancellation, dilock_redis_lease** out, dilock_error* error) {
    if (!require_output(out, error)) return DILOCK_STATUS_INVALID_ARGUMENT;
    return invoke_cancellable(error, cancellation, [&](std::stop_token stop) { if (lock == nullptr) throw std::invalid_argument("lock is null"); auto value = std::make_unique<dilock_redis_lease>(); value->value = lock->value.acquire(stop); *out = value.release(); });
}
dilock_status dilock_redis_try_acquire(dilock_redis_lock* lock, int64_t lease_ms,
    dilock_redis_lease** out, dilock_error* error) {
    if (!require_output(out, error)) return DILOCK_STATUS_INVALID_ARGUMENT;
    bool busy = false;
    const auto status = invoke(error, [&] { if (lock == nullptr) throw std::invalid_argument("lock is null"); auto result = lock->value.try_acquire(std::chrono::milliseconds(lease_ms)); if (!result) { busy = true; set_error(error, DILOCK_STATUS_BUSY, "resource is currently held"); return; } auto value = std::make_unique<dilock_redis_lease>(); value->value = std::move(*result); *out = value.release(); });
    return status == DILOCK_STATUS_OK && busy ? DILOCK_STATUS_BUSY : status;
}
int32_t dilock_redis_lease_is_valid(const dilock_redis_lease* lease) { return lease != nullptr && lease->value.valid() ? 1 : 0; }
dilock_status dilock_redis_lease_release(dilock_redis_lease* lease, dilock_release_outcome* outcome,
    dilock_error* error) {
    clear_error(error);
    if (outcome != nullptr) *outcome = DILOCK_RELEASE_UNKNOWN;
    if (lease == nullptr || outcome == nullptr) { set_error(error, DILOCK_STATUS_INVALID_ARGUMENT, "lease and outcome are required"); return DILOCK_STATUS_INVALID_ARGUMENT; }
    return public_release(lease->value.release(), outcome);
}
dilock_status dilock_redis_lease_renew(dilock_redis_lease* lease, int64_t lease_ms,
    int32_t* renewed, dilock_error* error) {
    if (renewed != nullptr) *renewed = 0;
    if (lease == nullptr || renewed == nullptr) {
        set_error(error, DILOCK_STATUS_INVALID_ARGUMENT, "lease and renewed output are required");
        return DILOCK_STATUS_INVALID_ARGUMENT;
    }
    return invoke(error, [&] { *renewed = lease->value.renew(std::chrono::milliseconds(lease_ms)) ? 1 : 0; });
}
void dilock_redis_lease_destroy(dilock_redis_lease* lease) { delete lease; }
dilock_status dilock_redis_provision_namespace(const dilock_redis_options* options, dilock_error* error) {
    return invoke(error, [&] { dilock::provision_redis_namespace(redis_options(options)); });
}

#ifdef DILOCK_C_HAS_ETCD
dilock_status dilock_etcd_provider_create(const dilock_etcd_options* options,
    dilock_etcd_provider** out, dilock_error* error) {
    if (!require_output(out, error)) return DILOCK_STATUS_INVALID_ARGUMENT;
    return invoke(error, [&] { auto value = std::make_unique<dilock_etcd_provider>(); value->value = std::make_unique<dilock::EtcdProvider>(etcd_options(options)); *out = value.release(); });
}
void dilock_etcd_provider_destroy(dilock_etcd_provider* provider) { delete provider; }
dilock_status dilock_etcd_lock_create(dilock_etcd_provider* provider, const char* resource,
    dilock_etcd_lock** out, dilock_error* error) {
    if (!require_output(out, error)) return DILOCK_STATUS_INVALID_ARGUMENT;
    return invoke(error, [&] { if (provider == nullptr || resource == nullptr) throw std::invalid_argument("provider and resource are required"); auto value = std::make_unique<dilock_etcd_lock>(); value->value = provider->value->lock(resource); *out = value.release(); });
}
void dilock_etcd_lock_destroy(dilock_etcd_lock* lock) { delete lock; }
dilock_status dilock_etcd_acquire(dilock_etcd_lock* lock, int64_t wait_ms, int64_t lease_ms,
    dilock_etcd_lease** out, dilock_error* error) {
    if (!require_output(out, error)) return DILOCK_STATUS_INVALID_ARGUMENT;
    return invoke(error, [&] { if (lock == nullptr) throw std::invalid_argument("lock is null"); auto value = std::make_unique<dilock_etcd_lease>(); value->value = lock->value.acquire(std::chrono::milliseconds(wait_ms), std::chrono::milliseconds(lease_ms)); *out = value.release(); });
}
dilock_status dilock_etcd_acquire_default(dilock_etcd_lock* lock,
    dilock_etcd_lease** out, dilock_error* error) {
    if (!require_output(out, error)) return DILOCK_STATUS_INVALID_ARGUMENT;
    return invoke(error, [&] { if (lock == nullptr) throw std::invalid_argument("lock is null"); auto value = std::make_unique<dilock_etcd_lease>(); value->value = lock->value.acquire(); *out = value.release(); });
}
dilock_status dilock_etcd_acquire_with_cancellation(dilock_etcd_lock* lock,
    int64_t wait_ms, int64_t lease_ms, dilock_cancellation* cancellation,
    dilock_etcd_lease** out, dilock_error* error) {
    if (!require_output(out, error)) return DILOCK_STATUS_INVALID_ARGUMENT;
    return invoke_cancellable(error, cancellation, [&](std::stop_token stop) { if (lock == nullptr) throw std::invalid_argument("lock is null"); auto value = std::make_unique<dilock_etcd_lease>(); value->value = lock->value.acquire(std::chrono::milliseconds(wait_ms), std::chrono::milliseconds(lease_ms), stop); *out = value.release(); });
}
dilock_status dilock_etcd_acquire_default_with_cancellation(dilock_etcd_lock* lock,
    dilock_cancellation* cancellation, dilock_etcd_lease** out, dilock_error* error) {
    if (!require_output(out, error)) return DILOCK_STATUS_INVALID_ARGUMENT;
    return invoke_cancellable(error, cancellation, [&](std::stop_token stop) { if (lock == nullptr) throw std::invalid_argument("lock is null"); auto value = std::make_unique<dilock_etcd_lease>(); value->value = lock->value.acquire(stop); *out = value.release(); });
}
int32_t dilock_etcd_lease_is_valid(const dilock_etcd_lease* lease) { return lease != nullptr && lease->value.valid() ? 1 : 0; }
dilock_status dilock_etcd_lease_effective_ttl_ms(const dilock_etcd_lease* lease,
    int64_t* effective_ttl_ms, dilock_error* error) {
    clear_error(error);
    if (lease == nullptr || effective_ttl_ms == nullptr) {
        set_error(error, DILOCK_STATUS_INVALID_ARGUMENT,
            "lease and effective TTL output are required");
        return DILOCK_STATUS_INVALID_ARGUMENT;
    }
    return invoke(error, [&] {
        *effective_ttl_ms = lease->value.effective_ttl().count();
    });
}
dilock_status dilock_etcd_lease_fence(const dilock_etcd_lease* lease, dilock_etcd_fence* fence,
    char* epoch, size_t epoch_capacity, dilock_error* error) {
    clear_error(error);
    if (lease == nullptr || fence == nullptr) {
        set_error(error, DILOCK_STATUS_INVALID_ARGUMENT, "lease and fence output are required");
        return DILOCK_STATUS_INVALID_ARGUMENT;
    }
    return invoke(error, [&] {
        const auto value = lease->value.fence();
        fence->create_revision = value.create_revision;
        fence->coordination_epoch_bytes = value.coordination_epoch.size();
        if (epoch == nullptr || epoch_capacity <= value.coordination_epoch.size()) {
            throw std::invalid_argument("coordination epoch output buffer is null or too small");
        }
        std::memcpy(epoch, value.coordination_epoch.data(), value.coordination_epoch.size());
        epoch[value.coordination_epoch.size()] = '\0';
    });
}
dilock_status dilock_etcd_lease_release(dilock_etcd_lease* lease, dilock_release_outcome* outcome,
    dilock_error* error) {
    clear_error(error);
    if (outcome != nullptr) *outcome = DILOCK_RELEASE_UNKNOWN;
    if (lease == nullptr || outcome == nullptr) { set_error(error, DILOCK_STATUS_INVALID_ARGUMENT, "lease and outcome are required"); return DILOCK_STATUS_INVALID_ARGUMENT; }
    return public_release(lease->value.release(), outcome);
}
void dilock_etcd_lease_destroy(dilock_etcd_lease* lease) { delete lease; }
dilock_status dilock_etcd_try_acquire(dilock_etcd_lock* lock, int64_t lease_ms,
    dilock_etcd_lease** out, dilock_error* error) {
    if (!require_output(out, error)) return DILOCK_STATUS_INVALID_ARGUMENT;
    if (lock == nullptr || lease_ms <= 0) {
        set_error(error, DILOCK_STATUS_INVALID_ARGUMENT, "lock is null or lease duration is not positive");
        return DILOCK_STATUS_INVALID_ARGUMENT;
    }
    set_error(error, DILOCK_STATUS_UNSUPPORTED_CAPABILITY,
        "etcd native Lock has no safe try-acquire operation");
    return DILOCK_STATUS_UNSUPPORTED_CAPABILITY;
}
#endif

#ifdef DILOCK_C_HAS_CONSUL
dilock_status dilock_consul_provider_create(const dilock_consul_options* options,
    dilock_consul_provider** out, dilock_error* error) {
    if (!require_output(out, error)) return DILOCK_STATUS_INVALID_ARGUMENT;
    return invoke(error, [&] { auto value = std::make_unique<dilock_consul_provider>(); value->value = std::make_unique<dilock::ConsulProvider>(consul_options(options)); *out = value.release(); });
}
void dilock_consul_provider_destroy(dilock_consul_provider* provider) { delete provider; }
dilock_status dilock_consul_lock_create(dilock_consul_provider* provider, const char* resource,
    dilock_consul_lock** out, dilock_error* error) {
    if (!require_output(out, error)) return DILOCK_STATUS_INVALID_ARGUMENT;
    return invoke(error, [&] { if (provider == nullptr || resource == nullptr) throw std::invalid_argument("provider and resource are required"); auto value = std::make_unique<dilock_consul_lock>(); value->value = provider->value->lock(resource); *out = value.release(); });
}
void dilock_consul_lock_destroy(dilock_consul_lock* lock) { delete lock; }
dilock_status dilock_consul_acquire(dilock_consul_lock* lock, int64_t wait_ms, int64_t lease_ms,
    dilock_consul_lease** out, dilock_error* error) {
    if (!require_output(out, error)) return DILOCK_STATUS_INVALID_ARGUMENT;
    return invoke(error, [&] { if (lock == nullptr) throw std::invalid_argument("lock is null"); auto value = std::make_unique<dilock_consul_lease>(); value->value = lock->value.acquire(std::chrono::milliseconds(wait_ms), std::chrono::milliseconds(lease_ms)); *out = value.release(); });
}
dilock_status dilock_consul_acquire_default(dilock_consul_lock* lock,
    dilock_consul_lease** out, dilock_error* error) {
    if (!require_output(out, error)) return DILOCK_STATUS_INVALID_ARGUMENT;
    return invoke(error, [&] { if (lock == nullptr) throw std::invalid_argument("lock is null"); auto value = std::make_unique<dilock_consul_lease>(); value->value = lock->value.acquire(); *out = value.release(); });
}
dilock_status dilock_consul_acquire_with_cancellation(dilock_consul_lock* lock,
    int64_t wait_ms, int64_t lease_ms, dilock_cancellation* cancellation,
    dilock_consul_lease** out, dilock_error* error) {
    if (!require_output(out, error)) return DILOCK_STATUS_INVALID_ARGUMENT;
    return invoke_cancellable(error, cancellation, [&](std::stop_token stop) { if (lock == nullptr) throw std::invalid_argument("lock is null"); auto value = std::make_unique<dilock_consul_lease>(); value->value = lock->value.acquire(std::chrono::milliseconds(wait_ms), std::chrono::milliseconds(lease_ms), stop); *out = value.release(); });
}
dilock_status dilock_consul_acquire_default_with_cancellation(dilock_consul_lock* lock,
    dilock_cancellation* cancellation, dilock_consul_lease** out, dilock_error* error) {
    if (!require_output(out, error)) return DILOCK_STATUS_INVALID_ARGUMENT;
    return invoke_cancellable(error, cancellation, [&](std::stop_token stop) { if (lock == nullptr) throw std::invalid_argument("lock is null"); auto value = std::make_unique<dilock_consul_lease>(); value->value = lock->value.acquire(stop); *out = value.release(); });
}
dilock_status dilock_consul_try_acquire(dilock_consul_lock* lock, int64_t lease_ms,
    dilock_consul_lease** out, dilock_error* error) {
    if (!require_output(out, error)) return DILOCK_STATUS_INVALID_ARGUMENT;
    bool busy = false;
    const auto status = invoke(error, [&] { if (lock == nullptr) throw std::invalid_argument("lock is null"); auto result = lock->value.try_acquire(std::chrono::milliseconds(lease_ms)); if (!result) { busy = true; set_error(error, DILOCK_STATUS_BUSY, "resource is currently held"); return; } auto value = std::make_unique<dilock_consul_lease>(); value->value = std::move(*result); *out = value.release(); });
    return status == DILOCK_STATUS_OK && busy ? DILOCK_STATUS_BUSY : status;
}
int32_t dilock_consul_lease_is_valid(const dilock_consul_lease* lease) { return lease != nullptr && lease->value.valid() ? 1 : 0; }
dilock_status dilock_consul_lease_effective_ttl_ms(const dilock_consul_lease* lease,
    int64_t* effective_ttl_ms, dilock_error* error) {
    clear_error(error);
    if (lease == nullptr || effective_ttl_ms == nullptr) { set_error(error, DILOCK_STATUS_INVALID_ARGUMENT, "lease and effective TTL output are required"); return DILOCK_STATUS_INVALID_ARGUMENT; }
    return invoke(error, [&] { *effective_ttl_ms = lease->value.effective_ttl().count(); });
}
dilock_status dilock_consul_lease_fence(const dilock_consul_lease* lease, dilock_consul_fence* fence,
    char* epoch, size_t epoch_capacity, dilock_error* error) {
    clear_error(error);
    if (lease == nullptr || fence == nullptr) { set_error(error, DILOCK_STATUS_INVALID_ARGUMENT, "lease and fence output are required"); return DILOCK_STATUS_INVALID_ARGUMENT; }
    return invoke(error, [&] { const auto value = lease->value.fence(); fence->lock_index = value.lock_index; fence->coordination_epoch_bytes = value.coordination_epoch.size(); if (epoch == nullptr || epoch_capacity <= value.coordination_epoch.size()) throw std::invalid_argument("coordination epoch output buffer is null or too small"); std::memcpy(epoch, value.coordination_epoch.data(), value.coordination_epoch.size()); epoch[value.coordination_epoch.size()] = '\0'; });
}
dilock_status dilock_consul_lease_renew(dilock_consul_lease* lease, int32_t* renewed, dilock_error* error) {
    if (renewed != nullptr) *renewed = 0;
    if (lease == nullptr || renewed == nullptr) { set_error(error, DILOCK_STATUS_INVALID_ARGUMENT, "lease and renewed output are required"); return DILOCK_STATUS_INVALID_ARGUMENT; }
    return invoke(error, [&] { *renewed = lease->value.renew() ? 1 : 0; });
}
dilock_status dilock_consul_lease_release(dilock_consul_lease* lease,
    dilock_release_outcome* outcome, dilock_error* error) {
    clear_error(error);
    if (outcome != nullptr) *outcome = DILOCK_RELEASE_UNKNOWN;
    if (lease == nullptr || outcome == nullptr) { set_error(error, DILOCK_STATUS_INVALID_ARGUMENT, "lease and outcome are required"); return DILOCK_STATUS_INVALID_ARGUMENT; }
    return public_release(lease->value.release(), outcome);
}
void dilock_consul_lease_destroy(dilock_consul_lease* lease) { delete lease; }
dilock_status dilock_consul_provision_namespace(const dilock_consul_options* options, dilock_error* error) {
    return invoke(error, [&] { dilock::provision_consul_namespace(consul_options(options)); });
}
#endif

#ifdef DILOCK_C_HAS_POSTGRES
dilock_status dilock_postgres_provider_create(const dilock_postgres_options* options,
    dilock_postgres_provider** out, dilock_error* error) {
    if (!require_output(out, error)) return DILOCK_STATUS_INVALID_ARGUMENT;
    return invoke(error, [&] { auto value = std::make_unique<dilock_postgres_provider>(); value->value = std::make_unique<dilock::PostgresProvider>(postgres_options(options)); *out = value.release(); });
}
void dilock_postgres_provider_destroy(dilock_postgres_provider* provider) { delete provider; }
dilock_status dilock_postgres_lock_create(dilock_postgres_provider* provider, const char* resource,
    dilock_postgres_lock** out, dilock_error* error) {
    if (!require_output(out, error)) return DILOCK_STATUS_INVALID_ARGUMENT;
    return invoke(error, [&] { if (provider == nullptr || resource == nullptr) throw std::invalid_argument("provider and resource are required"); auto value = std::make_unique<dilock_postgres_lock>(); value->value = provider->value->lock(resource); *out = value.release(); });
}
void dilock_postgres_lock_destroy(dilock_postgres_lock* lock) { delete lock; }
dilock_status dilock_postgres_acquire(dilock_postgres_lock* lock, int64_t wait_ms, int64_t lease_ms,
    dilock_postgres_lease** out, dilock_error* error) {
    if (!require_output(out, error)) return DILOCK_STATUS_INVALID_ARGUMENT;
    return invoke(error, [&] { if (lock == nullptr) throw std::invalid_argument("lock is null"); dilock::AcquireOptions options; options.wait = std::chrono::milliseconds(wait_ms); options.lease = std::chrono::milliseconds(lease_ms); auto value = std::make_unique<dilock_postgres_lease>(); value->value = lock->value.acquire(options); *out = value.release(); });
}
dilock_status dilock_postgres_acquire_default(dilock_postgres_lock* lock,
    dilock_postgres_lease** out, dilock_error* error) {
    if (!require_output(out, error)) return DILOCK_STATUS_INVALID_ARGUMENT;
    return invoke(error, [&] { if (lock == nullptr) throw std::invalid_argument("lock is null"); auto result = lock->value.acquire(); auto value = std::make_unique<dilock_postgres_lease>(); value->value = std::move(result); *out = value.release(); });
}
dilock_status dilock_postgres_acquire_with_cancellation(dilock_postgres_lock* lock, int64_t wait_ms, int64_t lease_ms,
    dilock_cancellation* cancellation, dilock_postgres_lease** out, dilock_error* error) {
    if (!require_output(out, error)) return DILOCK_STATUS_INVALID_ARGUMENT;
    return invoke_cancellable(error, cancellation, [&](std::stop_token stop) { if (lock == nullptr) throw std::invalid_argument("lock is null"); dilock::AcquireOptions options; options.wait = std::chrono::milliseconds(wait_ms); options.lease = std::chrono::milliseconds(lease_ms); auto value = std::make_unique<dilock_postgres_lease>(); value->value = lock->value.acquire(options, stop); *out = value.release(); });
}
dilock_status dilock_postgres_acquire_default_with_cancellation(dilock_postgres_lock* lock,
    dilock_cancellation* cancellation, dilock_postgres_lease** out, dilock_error* error) {
    if (!require_output(out, error)) return DILOCK_STATUS_INVALID_ARGUMENT;
    return invoke_cancellable(error, cancellation, [&](std::stop_token stop) { if (lock == nullptr) throw std::invalid_argument("lock is null"); auto value = std::make_unique<dilock_postgres_lease>(); value->value = lock->value.acquire(stop); *out = value.release(); });
}
dilock_status dilock_postgres_try_acquire(dilock_postgres_lock* lock, dilock_postgres_lease** out, dilock_error* error) {
    if (!require_output(out, error)) return DILOCK_STATUS_INVALID_ARGUMENT;
    bool busy = false;
    const auto status = invoke(error, [&] { if (lock == nullptr) throw std::invalid_argument("lock is null"); auto result = lock->value.try_acquire(); if (!result) { busy = true; set_error(error, DILOCK_STATUS_BUSY, "resource is currently held"); return; } auto value = std::make_unique<dilock_postgres_lease>(); value->value = std::move(*result); *out = value.release(); });
    return status == DILOCK_STATUS_OK && busy ? DILOCK_STATUS_BUSY : status;
}
int32_t dilock_postgres_lease_is_valid(const dilock_postgres_lease* lease) { return lease != nullptr && lease->value.valid() ? 1 : 0; }
dilock_status dilock_postgres_lease_renew(dilock_postgres_lease* lease, int32_t* renewed, dilock_error* error) {
    if (renewed != nullptr) *renewed = 0;
    if (lease == nullptr || renewed == nullptr) { set_error(error, DILOCK_STATUS_INVALID_ARGUMENT, "lease and renewed output are required"); return DILOCK_STATUS_INVALID_ARGUMENT; }
    return invoke(error, [&] { *renewed = lease->value.renew() ? 1 : 0; });
}
dilock_status dilock_postgres_lease_release(dilock_postgres_lease* lease, dilock_release_outcome* outcome, dilock_error* error) {
    clear_error(error); if (outcome != nullptr) *outcome = DILOCK_RELEASE_UNKNOWN;
    if (lease == nullptr || outcome == nullptr) { set_error(error, DILOCK_STATUS_INVALID_ARGUMENT, "lease and outcome are required"); return DILOCK_STATUS_INVALID_ARGUMENT; }
    return public_release(lease->value.release(), outcome);
}
void dilock_postgres_lease_destroy(dilock_postgres_lease* lease) { delete lease; }
dilock_status dilock_postgres_provision_namespace(const dilock_postgres_options* options, dilock_error* error) {
    return invoke(error, [&] { dilock::provision_postgres_namespace(postgres_options(options)); });
}
#endif

} // extern "C"
