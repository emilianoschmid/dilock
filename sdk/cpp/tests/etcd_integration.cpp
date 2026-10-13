#include "dilock/etcd.hpp"

#include "rpc.grpc.pb.h"

#include <nlohmann/json.hpp>

#include <chrono>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <string>
#include <thread>
#include <utility>

namespace {

using namespace std::chrono_literals;

void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

const char* env(const char* key) {
    const auto* value = std::getenv(key);
    return value && *value ? value : nullptr;
}

void provision_local_test_namespace(const std::string& endpoint, const std::string& name_space,
                                    const std::string& epoch) {
    auto channel = grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials());
    auto kv = etcdserverpb::KV::NewStub(channel);
    const auto key = "/dilock/namespaces/" + name_space + "/meta";
    etcdserverpb::RangeRequest range_request;
    range_request.set_key(key);
    etcdserverpb::RangeResponse range_response;
    grpc::ClientContext range_context;
    range_context.set_deadline(std::chrono::system_clock::now() + 2s);
    const auto read = kv->Range(&range_context, range_request, &range_response);
    if (!read.ok()) throw std::runtime_error("test namespace metadata read failed: " + read.error_message());
    if (range_response.count() != 0) return; // Existing metadata is validated, never overwritten.

    const auto metadata = nlohmann::json{{"protocol_major", 1},
        {"identity_scheme", "sha256-framed-utf8-v1"},
        {"profile", "etcd-native-lock-v1"},
        {"coordination_epoch", epoch},
        {"fixed_lease_ms", nullptr}}.dump();
    etcdserverpb::PutRequest put_request;
    put_request.set_key(key);
    put_request.set_value(metadata);
    etcdserverpb::PutResponse put_response;
    grpc::ClientContext put_context;
    put_context.set_deadline(std::chrono::system_clock::now() + 2s);
    const auto put = kv->Put(&put_context, put_request, &put_response);
    if (!put.ok()) throw std::runtime_error("test namespace metadata provision failed: " + put.error_message());
}

} // namespace

int main() {
    const auto* endpoint = env("DILOCK_ETCD_ENDPOINT");
    if (!endpoint) {
        std::cout << "SKIP: set DILOCK_ETCD_ENDPOINT to an explicitly disposable etcd instance\n";
        return 77;
    }
    try {
        const std::string name_space = env("DILOCK_ETCD_NAMESPACE") ? env("DILOCK_ETCD_NAMESPACE") : "dilock-cpp-test";
        const std::string epoch = env("DILOCK_ETCD_EPOCH") ? env("DILOCK_ETCD_EPOCH") : "local-functional-test-v1";
        provision_local_test_namespace(endpoint, name_space, epoch);

        dilock::EtcdOptions options;
        options.endpoint = endpoint;
        options.name_space = name_space;
        options.coordination_epoch = epoch;
        options.rpc_timeout = 300ms;
        options.cleanup_timeout = 1s;
        options.safety_margin = 25ms;
        options.auto_renew = true;
        options.default_acquisition = dilock::AcquireOptions{2s, 2500ms};
        dilock::EtcdProvider first(options);
        dilock::EtcdProvider second(options);
        const auto suffix = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        {
            auto daily = first.lock("cpp-etcd-default-scope/daily/" + suffix);
            auto weekly = first.lock("cpp-etcd-default-scope/weekly/" + suffix);
            auto daily_lease = daily.acquire();
            auto weekly_lease = weekly.acquire(dilock::AcquireOptions{2s, 2500ms});
            require(daily_lease.valid() && weekly_lease.valid(),
                    "one etcd provider did not hold distinct default and configured resources");
            require(daily_lease.fence().create_revision > 0 && weekly_lease.fence().create_revision > 0,
                    "scoped leases did not preserve their etcd fences");
        }

        const auto exception_owner = first.lock("cpp-etcd-scope-exception/" + suffix);
        const auto exception_contender = second.lock("cpp-etcd-scope-exception/" + suffix);
        const std::runtime_error body_failure("preserve this etcd work failure");
        bool body_failure_preserved = false;
        try {
            auto scoped = exception_owner.acquire();
            (void)scoped;
            throw body_failure;
        } catch (const std::runtime_error& error) {
            body_failure_preserved = std::string(error.what()) == body_failure.what();
        }
        require(body_failure_preserved, "RAII scope replaced the primary etcd work exception");
        auto after_exception = exception_contender.acquire(2s, 2500ms);
        require(after_exception.valid(), "RAII exception exit did not release for an independent etcd acquisition");
        require(after_exception.release() == dilock::ReleaseResult::released,
                "post-exception etcd successor release was not acknowledged");

        const auto moved_resource = first.lock("cpp-etcd-move-source/" + suffix);
        const auto moved_contender = second.lock("cpp-etcd-move-source/" + suffix);
        const auto replaced_resource = first.lock("cpp-etcd-move-destination/" + suffix);
        const auto replaced_contender = second.lock("cpp-etcd-move-destination/" + suffix);
        auto destination = replaced_resource.acquire(2s, 2500ms);
        {
            auto source = moved_resource.acquire(2s, 2500ms);
            destination = std::move(source);
            require(!source.valid(), "moved-from etcd lease retained local validity");
        }
        auto replaced_successor = replaced_contender.acquire(2s, 2500ms);
        require(replaced_successor.valid(), "etcd move assignment did not clean up the overwritten destination");
        require(replaced_successor.release() == dilock::ReleaseResult::released,
                "overwritten etcd destination successor release failed");
        bool transferred_owner_remains = false;
        try { (void)moved_contender.acquire(100ms, 1s); }
        catch (const dilock::Error& error) { transferred_owner_remains = error.code() == dilock::ErrorCode::wait_timeout; }
        require(transferred_owner_remains,
                "moved-from etcd lease destructor released the transferred owner");
        require(destination.valid(), "etcd move destination lost the transferred lease when its source left scope");
        require(destination.release() == dilock::ReleaseResult::released,
                "transferred etcd owner release was not acknowledged");
        auto moved_successor = moved_contender.acquire(2s, 2500ms);
        require(moved_successor.valid(), "etcd successor could not acquire after moved-to lease release");
        require(moved_successor.release() == dilock::ReleaseResult::released,
                "moved etcd successor release was not acknowledged");

        const auto lock = first.lock("cpp-etcd-lifecycle/" + suffix);
        const auto queued_handle = second.lock("cpp-etcd-lifecycle/" + suffix);

        bool unsupported_try = false;
        try { (void)lock.try_acquire(1s); }
        catch (const dilock::Error& error) { unsupported_try = error.code() == dilock::ErrorCode::unsupported_capability; }
        require(unsupported_try, "native etcd try-acquire must be explicitly unsupported");

        auto owner = lock.acquire(2s, 2500ms);
        require(owner.valid(), "new etcd lease is not locally valid");
        require(owner.fence().create_revision > 0, "verified etcd create_revision is missing");
        require(owner.fence().coordination_epoch == epoch, "fence epoch differs from configured epoch");
        require(owner.effective_ttl() >= 1s, "effective TTL is not the server-granted duration");
        const auto owner_fence = owner.fence();
        bool timed_out = false;
        try { (void)queued_handle.acquire(150ms, 1s); }
        catch (const dilock::Error& error) { timed_out = error.code() == dilock::ErrorCode::wait_timeout; }
        require(timed_out, "a bounded native Lock wait must report timeout rather than ordinary contention");

        std::optional<dilock::EtcdLease> successor;
        std::exception_ptr wait_error;
        std::thread waiter([&] {
            try { successor.emplace(queued_handle.acquire(8s, 2500ms)); }
            catch (...) { wait_error = std::current_exception(); }
        });
        std::this_thread::sleep_for(3500ms); // etcd rounds 2500ms up; keep the queued lease alive past that TTL.
        require(owner.valid(), "owner lost local validity while auto-renewing");
        require(owner.release() == dilock::ReleaseResult::released, "native Unlock was not acknowledged");
        waiter.join();
        if (wait_error) std::rethrow_exception(wait_error);
        require(successor.has_value(), "queued successor did not acquire after handoff");
        require(successor->fence().create_revision > owner_fence.create_revision,
                "successor create_revision did not advance");
        require(owner.release() == dilock::ReleaseResult::released, "repeated owner release was not cached");
        bool successor_still_owned = false;
        try { (void)lock.acquire(150ms, 1s); }
        catch (const dilock::Error& error) { successor_still_owned = error.code() == dilock::ErrorCode::wait_timeout; }
        require(successor_still_owned, "stale repeat release removed successor ownership");
        require(successor->release() == dilock::ReleaseResult::released, "successor release failed");

        auto cancellation_owner = lock.acquire(2s, 2s);
        std::stop_source stop;
        bool cancelled = false;
        std::thread cancelled_waiter([&] {
            try { (void)queued_handle.acquire(5s, 2s, stop.get_token()); }
            catch (const dilock::Error& error) { cancelled = error.code() == dilock::ErrorCode::cancelled; }
        });
        std::this_thread::sleep_for(200ms);
        stop.request_stop();
        cancelled_waiter.join();
        require(cancelled, "cancelled waiter did not surface cancellation");
        require(cancellation_owner.release() == dilock::ReleaseResult::released,
                "owner release after waiter cancellation failed");
        auto after_cancel = lock.acquire(2s, 2s);
        require(after_cancel.release() == dilock::ReleaseResult::released, "post-cancellation successor release failed");

        auto expiry_options = options;
        expiry_options.auto_renew = false;
        dilock::EtcdProvider expiry_provider(expiry_options);
        auto expired_owner = expiry_provider.lock("cpp-etcd-natural-expiry/" + suffix).acquire(2s, 1s);
        std::this_thread::sleep_for(3s); // etcd grants a minimum two-second TTL.
        require(!expired_owner.valid(), "expired etcd lease remained locally valid");
        require(expired_owner.release() == dilock::ReleaseResult::already_not_owned,
                "expired etcd owner was reported released after Unlock acknowledged a no-op");
        auto after_expiry = lock.acquire(2s, 2s);
        require(after_expiry.release() == dilock::ReleaseResult::released,
                "successor could not acquire and release after natural owner expiry");

        std::cout << "PASS: native lock handoff, auto-keepalive past TTL, contention, create_revision fencing, repeat release, cancellation cleanup, natural expiry outcome\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
