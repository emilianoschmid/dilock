// Local-only C++20 API example. No TTL, network, renewal or wire protocol.
#include <cassert>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

namespace dilock::style {
enum class release_outcome { released, already_not_owned };
namespace detail {
// A retained shared identity cannot alias a newer acquisition's identity.
using owner_id = std::shared_ptr<const int>;
struct memory_state {
    std::mutex gate;
    std::unordered_map<std::string, owner_id> owners;

    bool claim(const std::string& key, const owner_id& owner) {
        const std::lock_guard guard{gate};
        return owners.emplace(key, owner).second;
    }
    release_outcome release(const std::string& key, const owner_id& owner) {
        const std::lock_guard guard{gate};
        const auto current = owners.find(key);
        if (current == owners.end() || current->second != owner)
            return release_outcome::already_not_owned;
        owners.erase(current);
        return release_outcome::released;
    }
    void expire_for_test(const std::string& key) {
        const std::lock_guard guard{gate};
        owners.erase(key);
    }
};
}

class resource_lock;
/// Move-only acquisition; local cleanup is synchronous, remote cleanup is not.
class lease final {
    std::shared_ptr<detail::memory_state> state_;
    std::string resource_;
    detail::owner_id owner_;
    friend class resource_lock;
    lease(std::shared_ptr<detail::memory_state> state, std::string resource,
          detail::owner_id owner)
        : state_(std::move(state)), resource_(std::move(resource)), owner_(std::move(owner)) {}
public:
    lease(const lease&) = delete;
    lease& operator=(const lease&) = delete;
    lease(lease&& other) noexcept = default;
    // Simpler ownership contract: move construction allowed, reassignment omitted.
    lease& operator=(lease&&) = delete;
    ~lease() noexcept {
        try { (void)release(); }
        catch (...) { /* Local demo fallback only; remote implementation needs diagnostics. */ }
    }
    release_outcome release() {
        if (!owner_) return release_outcome::already_not_owned;
        auto owner = std::exchange(owner_, {});
        return state_->release(resource_, owner);
    }
};

/// Copyable handle; copying it does not grant or duplicate ownership.
class resource_lock final {
    std::shared_ptr<detail::memory_state> state_;
    std::string resource_;
public:
    resource_lock(std::shared_ptr<detail::memory_state> state, std::string resource)
        : state_(std::move(state)), resource_(std::move(resource)) {
        if (resource_.empty()) throw std::invalid_argument("empty resource");
    }
    [[nodiscard]] std::optional<lease> try_acquire() const {
        auto owner = std::make_shared<const int>(0);
        if (!state_->claim(resource_, owner)) return std::nullopt;
        return lease{state_, resource_, std::move(owner)};
    }
};

class provider final {
    std::shared_ptr<detail::memory_state> state_;
public:
    explicit provider(std::shared_ptr<detail::memory_state> state) : state_(std::move(state)) {
        if (!state_) throw std::invalid_argument("null store");
    }
    [[nodiscard]] resource_lock get_lock(std::string resource) const {
        return resource_lock{state_, std::move(resource)};
    }
};
}

int main() {
    using namespace dilock::style;
    auto state = std::make_shared<detail::memory_state>();
    const auto lock = provider{state}.get_lock("example/resource");
    auto first = lock.try_acquire();
    assert(first && !lock.try_acquire());
    state->expire_for_test("example/resource");
    auto second = lock.try_acquire();
    assert(second);
    assert(first->release() == release_outcome::already_not_owned);
    assert(!lock.try_acquire());
    assert(second->release() == release_outcome::released);
    assert(second->release() == release_outcome::already_not_owned);
    try {
        auto held = lock.try_acquire();
        assert(held);
        auto moved = std::move(*held);
        assert(held->release() == release_outcome::already_not_owned);
        throw std::runtime_error("consumer failure");
    } catch (const std::runtime_error&) {}
    assert(lock.try_acquire()); // Temporary is cleaned up at the end of this statement.
    bool rejected = false;
    try { (void)provider{state}.get_lock(""); }
    catch (const std::invalid_argument&) { rejected = true; }
    assert(rejected);
    std::cout << "PASS cpp: busy, stale owner, idempotence, RAII, move, validation\n";
}
