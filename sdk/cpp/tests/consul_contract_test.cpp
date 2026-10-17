#include "dilock/consul.hpp"
#include "../src/consul_release.hpp"

#include <chrono>
#include <stdexcept>

int main() {
    using namespace std::chrono_literals;
    dilock::ConsulOptions options;
    if (options.endpoint != "http://127.0.0.1:8500" || options.name_space != "default" ||
        options.rpc_timeout != 2s || options.default_acquisition.wait != 5s ||
        options.default_acquisition.lease != 30s) return 1;

    bool rejected = false;
    options.endpoint = "http://127.0.0.1:8500/path";
    try {
        dilock::ConsulProvider provider(options);
    } catch (const dilock::Error& error) {
        rejected = error.code() == dilock::ErrorCode::invalid_argument;
    }
    if (!rejected) return 1;

    bool destroy_attempted = false;
    const auto ambiguous_release = dilock::detail::release_with_cleanup(
        []() -> dilock::ReleaseResult { throw std::runtime_error("owner read failed"); },
        [&] { destroy_attempted = true; return true; });
    if (!destroy_attempted || ambiguous_release != dilock::ReleaseResult::unknown) return 1;

    const auto unconfirmed_destroy = dilock::detail::release_with_cleanup(
        [] { return dilock::ReleaseResult::released; }, [] { return false; });
    if (unconfirmed_destroy != dilock::ReleaseResult::unknown) return 1;
}
