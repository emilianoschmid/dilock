#include <dilock/redis.hpp>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>

namespace {

std::string required_env(const char* name) {
    const char* value = std::getenv(name);
    if (value == nullptr || *value == '\0') {
        throw std::runtime_error(std::string("set ") + name + " for a pre-provisioned Redis namespace");
    }
    return value;
}

int redis_port() {
    const char* value = std::getenv("DILOCK_REDIS_PORT");
    if (value == nullptr || *value == '\0') return 6379;

    std::size_t consumed = 0;
    const std::string port_text(value);
    const long parsed = std::stol(port_text, &consumed);
    if (consumed != port_text.size() || parsed < 1 || parsed > 65535) {
        throw std::runtime_error("DILOCK_REDIS_PORT must be an integer from 1 through 65535");
    }
    return static_cast<int>(parsed);
}

std::string run_suffix() {
    std::random_device random;
    constexpr char digits[] = "0123456789abcdef";
    std::string suffix(16, '0');
    for (char& digit : suffix) digit = digits[random() & 0x0fU];
    return suffix;
}

} // namespace

int main() {
    using namespace std::chrono_literals;

    try {
        dilock::RedisOptions options;
        if (const char* host = std::getenv("DILOCK_REDIS_HOST"); host != nullptr && *host != '\0') {
            options.host = host;
        }
        options.port = redis_port();
        if (const char* username = std::getenv("DILOCK_REDIS_USERNAME"); username != nullptr) {
            options.username = username;
        }
        if (const char* password = std::getenv("DILOCK_REDIS_PASSWORD"); password != nullptr) {
            options.password = password;
        }
        options.name_space = required_env("DILOCK_NAMESPACE");
        options.coordination_epoch = required_env("DILOCK_EPOCH");

        // Separate providers demonstrate that contention crosses client pools.
        // Construction validates existing metadata; this example never provisions it.
        dilock::RedisProvider owner_provider(options);
        dilock::RedisProvider contender_provider(options);
        const auto resource = std::string("docs-example/cpp/") + run_suffix();
        const auto owner_lock = owner_provider.lock(resource);
        const auto contender_lock = contender_provider.lock(resource);

        auto owner = owner_lock.try_acquire(10s);
        if (!owner) throw std::runtime_error("unique example resource was unexpectedly busy");

        auto unexpected = contender_lock.try_acquire(10s);
        if (unexpected) {
            (void)unexpected->release();
            (void)owner->release();
            throw std::runtime_error("contender acquired while the owner lease was active");
        }
        std::cout << "contender: acknowledged busy\n";

        const auto released = owner->release();
        if (released != dilock::ReleaseResult::released) {
            throw std::runtime_error("owner release was not acknowledged");
        }

        auto successor = contender_lock.try_acquire(10s);
        if (!successor) throw std::runtime_error("contender remained busy after acknowledged release");
        if (!successor->valid()) throw std::runtime_error("successor lease is no longer locally valid");
        std::cout << "contender: acquired after owner release\n";

        if (successor->release() != dilock::ReleaseResult::released) {
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
