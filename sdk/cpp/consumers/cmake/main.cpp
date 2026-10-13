#include <dilock/etcd.hpp>
#include <dilock/redis.hpp>

#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>
#include <utility>

namespace {

std::string environment_or(const char* name, const char* fallback) {
    const auto* value = std::getenv(name);
    return value == nullptr || *value == '\0' ? fallback : value;
}

void require_released(dilock::ReleaseResult result, const char* backend) {
    if (result != dilock::ReleaseResult::released) {
        throw std::runtime_error(std::string(backend) + " package consumer release was not acknowledged");
    }
}

} // namespace

int main() {
    try {
        dilock::RedisOptions options;
        options.host = environment_or("DILOCK_REDIS_HOST", "127.0.0.1");
        options.name_space = environment_or("DILOCK_NAMESPACE", "consumer");
        options.coordination_epoch = environment_or("DILOCK_EPOCH", "local-consumer");
        dilock::RedisProvider redis(std::move(options));
        auto redis_lease = redis.lock("artifact-consumer/redis").acquire();
        require_released(redis_lease.release(), "Redis");

        dilock::EtcdOptions etcd_options;
        etcd_options.endpoint = environment_or("DILOCK_ETCD_ENDPOINT", "127.0.0.1:2379");
        etcd_options.name_space = environment_or("DILOCK_NAMESPACE", "consumer");
        etcd_options.coordination_epoch = environment_or("DILOCK_EPOCH", "local-consumer");
        dilock::EtcdProvider etcd(std::move(etcd_options));
        auto etcd_lease = etcd.lock("artifact-consumer/etcd").acquire();
        require_released(etcd_lease.release(), "etcd");

        std::cout << "Exact installed Dilock::redis and Dilock::etcd package consumer passed.\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
