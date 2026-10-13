#include "dilock/redis.hpp"

#include <type_traits>

using RedisDefaultAcquire = dilock::Lease (dilock::ResourceLock::*)(std::stop_token) const;
using RedisPolicyAcquire = dilock::Lease (dilock::ResourceLock::*)(
    const dilock::AcquireOptions&, std::stop_token) const;
using RedisDefaultTry = std::optional<dilock::Lease> (dilock::ResourceLock::*)(std::stop_token) const;

static_assert(std::is_same_v<
    decltype(static_cast<RedisDefaultAcquire>(&dilock::ResourceLock::acquire)),
    RedisDefaultAcquire>);
static_assert(std::is_same_v<
    decltype(static_cast<RedisPolicyAcquire>(&dilock::ResourceLock::acquire)),
    RedisPolicyAcquire>);
static_assert(std::is_same_v<
    decltype(static_cast<RedisDefaultTry>(&dilock::ResourceLock::try_acquire)),
    RedisDefaultTry>);

int main() {
    const dilock::AcquireOptions policy{};
    return policy.wait == std::chrono::seconds(5) && policy.lease == std::chrono::seconds(30) ? 0 : 1;
}
