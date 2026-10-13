#include <dilock/redis.hpp>

#include <chrono>
#include <charconv>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace {

std::string environment_or(const char* name, const char* fallback) {
    std::string result;
#ifdef _MSC_VER
    char* buffer = nullptr;
    size_t length = 0;
    if (_dupenv_s(&buffer, &length, name) != 0 || buffer == nullptr) return fallback;
    result.assign(buffer);
    std::free(buffer);
#else
    const char* value = std::getenv(name);
    if (value == nullptr) return fallback;
    result.assign(value);
#endif
    return result.empty() ? fallback : result;
}

int integer_environment(const char* name, int fallback) {
    const std::string text = environment_or(name, "");
    if (text.empty()) return fallback;
    int value{};
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (error != std::errc{} || end != text.data() + text.size()) {
        throw std::invalid_argument(std::string(name) + " must be a decimal integer");
    }
    return value;
}

void require_released(dilock::Lease& lease) {
    if (lease.release() != dilock::ReleaseResult::released) {
        throw std::runtime_error("Redis release was not acknowledged as released");
    }
}

bool configured_mode(int argc, char** argv) {
    return argc > 1 && std::string(argv[1]) == "--configured";
}

bool shared_provider_mode(int argc, char** argv) {
    return argc > 1 && std::string(argv[1]) == "--shared-provider";
}

} // namespace

int main(int argc, char** argv) {
    using namespace std::chrono_literals;

    dilock::RedisOptions options;
    options.coordination_epoch = environment_or("DILOCK_EPOCH", "");
    if (options.coordination_epoch.empty()) {
        std::cerr << "Set DILOCK_EPOCH to the approved namespace epoch.\n";
        return 2;
    }
    options.host = environment_or("DILOCK_REDIS_HOST", "127.0.0.1");
    if (configured_mode(argc, argv)) {
        options.name_space = environment_or("DILOCK_NAMESPACE", "default");
        options.port = integer_environment("DILOCK_REDIS_PORT", 6379);
        options.username = environment_or("DILOCK_REDIS_USERNAME", "");
        options.password = environment_or("DILOCK_REDIS_PASSWORD", "");
        options.database = integer_environment("DILOCK_REDIS_DATABASE", 0);
        options.rpc_timeout = 2s;
        options.cleanup_timeout = 2s;
        options.safety_margin = 25ms;
        options.poll_interval = 25ms;
        options.default_acquisition = {.wait = 10s, .lease = 60s};
        options.cleanup_observer = [](const dilock::CleanupObservation& observation) {
            if (observation.outcome == dilock::ReleaseResult::unknown) {
                std::cerr << "cleanup outcome=unknown; reason=" << observation.reason << '\n';
            }
        };
    }

    // The provider owns one reusable client for every resource handle.
    dilock::RedisProvider provider(std::move(options));
    auto daily = provider.lock("reports/daily");
    if (shared_provider_mode(argc, argv)) {
        auto weekly = provider.lock("reports/weekly");
        auto daily_lease = daily.acquire();
        auto weekly_lease = weekly.acquire();
        require_released(weekly_lease);
        require_released(daily_lease);
        std::cout << "Two distinct resources share one Redis provider and connection pool.\n";
    } else {
        auto lease = configured_mode(argc, argv)
            ? daily.acquire({.wait = 2s, .lease = 15s})
            : daily.acquire();
        require_released(lease);
        std::cout << "One short protected Redis operation completed.\n";
    } // RAII remains a bounded best-effort fallback if work or release throws.
}
