#include "dilock/etcd.hpp"

#include <type_traits>

using EtcdDefaultAcquire = dilock::EtcdLease (dilock::EtcdResourceLock::*)(std::stop_token) const;
using EtcdPolicyAcquire = dilock::EtcdLease (dilock::EtcdResourceLock::*)(
    const dilock::AcquireOptions&, std::stop_token) const;
using EtcdDefaultTry = std::optional<dilock::EtcdLease> (dilock::EtcdResourceLock::*)(std::stop_token) const;

static_assert(std::is_same_v<
    decltype(static_cast<EtcdDefaultAcquire>(&dilock::EtcdResourceLock::acquire)),
    EtcdDefaultAcquire>);
static_assert(std::is_same_v<
    decltype(static_cast<EtcdPolicyAcquire>(&dilock::EtcdResourceLock::acquire)),
    EtcdPolicyAcquire>);
static_assert(std::is_same_v<
    decltype(static_cast<EtcdDefaultTry>(&dilock::EtcdResourceLock::try_acquire)),
    EtcdDefaultTry>);
