#include <dilock/etcd.hpp>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>
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

bool configured_mode(int argc, char** argv) {
    return argc > 1 && std::string(argv[1]) == "--configured";
}

bool shared_provider_mode(int argc, char** argv) {
    return argc > 1 && std::string(argv[1]) == "--shared-provider";
}

void require_released(dilock::EtcdLease& lease) {
    if (lease.release() != dilock::ReleaseResult::released) {
        throw std::runtime_error("etcd release was not acknowledged as released");
    }
}

} // namespace

int main(int argc, char** argv) {
    using namespace std::chrono_literals;

    dilock::EtcdOptions options;
    options.coordination_epoch = environment_or("DILOCK_EPOCH", "");
    if (options.coordination_epoch.empty()) {
        std::cerr << "Set DILOCK_EPOCH to the approved namespace epoch.\n";
        return 2;
    }
    options.endpoint = environment_or("DILOCK_ETCD_ENDPOINT", "127.0.0.1:2379");
    if (configured_mode(argc, argv)) {
        options.name_space = environment_or("DILOCK_NAMESPACE", "default");
        options.credentials = grpc::InsecureChannelCredentials();
        options.rpc_timeout = 2s;
        options.cleanup_timeout = 2s;
        options.safety_margin = 25ms;
        options.default_acquisition = {.wait = 10s, .lease = 60s};
        options.auto_renew = false;
        options.cleanup_observer = [](const dilock::CleanupObservation& observation) {
            if (observation.outcome == dilock::ReleaseResult::unknown) {
                std::cerr << "cleanup outcome=unknown; reason=" << observation.reason << '\n';
            }
        };
    }

    // The provider owns one reusable gRPC channel for every resource handle.
    dilock::EtcdProvider provider(std::move(options));
    auto daily = provider.lock("reports/daily");
    if (shared_provider_mode(argc, argv)) {
        auto weekly = provider.lock("reports/weekly");
        auto daily_lease = daily.acquire();
        auto weekly_lease = weekly.acquire();
        const auto daily_revision = daily_lease.fence().create_revision;
        const auto weekly_revision = weekly_lease.fence().create_revision;
        require_released(weekly_lease);
        require_released(daily_lease);
        std::cout << "Two distinct resources share one etcd provider and gRPC channel.\n"
                  << "create_revision=" << daily_revision << ','
                  << weekly_revision << '\n';
    } else {
        auto lease = configured_mode(argc, argv)
            ? daily.acquire({.wait = 2s, .lease = 15s})
            : daily.acquire();
        const auto revision = lease.fence().create_revision;
        require_released(lease);
        std::cout << "One short protected etcd operation completed at revision "
                  << revision << ".\n";
    } // RAII remains a bounded best-effort fallback if work or release throws.
}
