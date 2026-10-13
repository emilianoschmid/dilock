#include <dilock/etcd.hpp>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <random>
#include <stdexcept>
#include <string>

namespace {
std::string required_env(const char* name) {
    const char* value = std::getenv(name);
    if (value == nullptr || *value == '\0') {
        throw std::runtime_error(std::string("set ") + name + " for the pre-provisioned etcd namespace");
    }
    return value;
}

std::string unique_suffix() {
    std::random_device random;
    constexpr char digits[] = "0123456789abcdef";
    std::string suffix(16, '0');
    for (auto& digit : suffix) digit = digits[random() & 0x0fU];
    return suffix;
}
} // namespace

int main() {
    using namespace std::chrono_literals;
    try {
        dilock::EtcdOptions options;
        options.endpoint = required_env("DILOCK_ETCD_ENDPOINT");
        options.name_space = required_env("DILOCK_NAMESPACE");
        options.coordination_epoch = required_env("DILOCK_EPOCH");
        options.rpc_timeout = 250ms;
        options.cleanup_timeout = 1s;

        dilock::EtcdProvider owner_provider(options);
        dilock::EtcdProvider contender_provider(options);
        const auto resource = "docs-example/cpp-etcd/" + unique_suffix();
        const auto owner_lock = owner_provider.lock(resource);
        const auto contender_lock = contender_provider.lock(resource);

        auto owner = owner_lock.acquire(3s, 5s);
        const auto fence = owner.fence();
        std::cout << "owner: acquired with epoch-scoped create_revision "
                  << fence.coordination_epoch << '/' << fence.create_revision << '\n';

        try {
            (void)contender_lock.acquire(250ms, 5s);
            throw std::runtime_error("contender acquired while the owner still held the resource");
        } catch (const dilock::Error& error) {
            if (error.code() != dilock::ErrorCode::wait_timeout) throw;
            std::cout << "contender: bounded queue wait timed out under contention\n";
        }

        if (owner.release() != dilock::ReleaseResult::released) {
            throw std::runtime_error("owner release was not acknowledged");
        }
        auto successor = contender_lock.acquire(3s, 5s);
        const auto next_fence = successor.fence();
        if (next_fence.coordination_epoch != fence.coordination_epoch ||
            next_fence.create_revision <= fence.create_revision) {
            throw std::runtime_error("successor did not receive a later fence in the same epoch");
        }
        std::cout << "contender: acquired after release with create_revision "
                  << next_fence.create_revision << '\n';
        if (successor.release() != dilock::ReleaseResult::released) {
            throw std::runtime_error("successor release was not acknowledged");
        }
        return 0;
    } catch (const dilock::Error& error) {
        std::cerr << "Dilock error (code " << static_cast<int>(error.code()) << "): " << error.what() << '\n';
        return 1;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
