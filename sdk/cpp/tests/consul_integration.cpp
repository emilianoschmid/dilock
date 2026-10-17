#include "dilock/consul.hpp"

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <stdexcept>

namespace {

using namespace std::chrono_literals;

const char* env(const char* name) {
    const auto* value = std::getenv(name);
    return value && *value ? value : nullptr;
}

void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

} // namespace

int main() {
    const auto* endpoint = env("DILOCK_CONSUL_ENDPOINT");
    const auto* name_space = env("DILOCK_NAMESPACE");
    const auto* epoch = env("DILOCK_EPOCH");
    if (!endpoint || !name_space || !epoch) {
        std::cout << "SKIP: set DILOCK_CONSUL_ENDPOINT, DILOCK_NAMESPACE, and DILOCK_EPOCH\n";
        return 77;
    }

    try {
        dilock::ConsulOptions options;
        options.endpoint = endpoint;
        options.name_space = name_space;
        options.coordination_epoch = epoch;
        options.rpc_timeout = 1s;
        options.cleanup_timeout = 1s;
        options.poll_interval = 25ms;

        dilock::ConsulProvider first_provider(options);
        dilock::ConsulProvider second_provider(options);
        auto first_lock = first_provider.lock("cpp/live-consul-lifecycle");
        auto second_lock = second_provider.lock("cpp/live-consul-lifecycle");
        auto first = first_lock.try_acquire(10s);
        require(first.has_value(), "first Consul acquisition did not succeed");
        const auto first_fence = first->fence();
        require(first_fence.coordination_epoch == epoch && first_fence.lock_index > 0,
                "first Consul fence did not match the configured epoch");
        require(!second_lock.try_acquire(10s).has_value(), "contending Consul provider acquired the held key");
        require(first->renew(), "Consul session renewal was not confirmed");
        require(first->release() == dilock::ReleaseResult::released,
                "Consul release was not confirmed");

        auto successor = second_lock.try_acquire(10s);
        require(successor.has_value(), "Consul successor did not acquire after release");
        const auto successor_fence = successor->fence();
        require(successor_fence.coordination_epoch == epoch &&
                    successor_fence.lock_index > first_fence.lock_index,
                "Consul LockIndex did not increase across handoff");
        require(successor->release() == dilock::ReleaseResult::released,
                "Consul successor release was not confirmed");

        std::cout << "PASS: Consul contention, renewal, release, handoff, and LockIndex fencing\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
