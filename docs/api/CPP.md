# C++ API design

Baseline: portable C++20. Use C++23 features only behind validated support or a deliberate baseline change. Do not mandate C++ modules, preview compilers, `std::execution` support that is not actually available, or an invented coroutine task framework.

## Values and ownership

A `distributed_lock` is a reusable resource value bound to shared provider implementation state. A `lease` is move-only with a `noexcept` destructor; moved-from leases are inert. `lock_name`, `acquire_options`, `lease_options`, `fencing_token` and error codes are ordinary native values. Use `std::chrono` durations and `std::stop_token`; never expose raw millisecond `int` parameters without a named type.

```cpp
// Design example. Final API is validated with installed-package consumer tests.
auto lock = provider.get_lock(dilock::lock_name{"camera/123/reindex"});
auto lease = lock.acquire(dilock::acquire_options{}, stop_token);
rebuild_derived_index(lease.loss_token());
auto outcome = lease.release(); // Bounded, observable remote acknowledgement.
```

Destructor cleanup is a fallback, not proof that Redis/etcd acknowledged removal. Exceptions during work must not lead to exceptions escaping the destructor. No indefinite blocking in destructors or detached tasks accessing destroyed state.

## Async policy

Ship a correctly bounded synchronous C++ API first if the selected backend clients naturally support it. A sync C++ SDK is interoperable with async .NET/Python because interoperability is a wire protocol, not an async syntax.

The provider's renewal scheduler is shared/bounded, not one thread per lease. If asynchronous consumer operations are added, expose a separate optional Asio integration using established completion-token/cancellation conventions, not `std::async` per call and not a universal custom `task<T>`. Document that initial async adapters are not yet supported rather than faking them.

## Errors and ABI

Use `std::optional<lease>` for try-acquire contention; typed exceptions/error codes distinguish backend errors, timeout and unknown acquisition. Because C++20 does not include `std::expected`, do not use it unconditionally in the public baseline. An error-code overload can be added if it has real consumer value, not by duplicating every method preemptively.

Prefer non-template implementation behind PImpl where this limits exposed dependencies. Smart-pointer ownership must express reality; a borrowed raw client cannot outlive its owner. No public `void*` or mandatory inheritance merely to use a backend. C ABI/FFI is not the v0.1 strategy. Document ABI policy: source compatibility is tracked; binary compatibility across compilers/STL combinations is not promised.

## Build and package

One CMake project with options `DILOCK_WITH_REDIS`, `DILOCK_WITH_ETCD`, future NATS; build only requested components. `find_package(dilock CONFIG REQUIRED COMPONENTS redis)` and `target_link_libraries(app PRIVATE dilock::redis)` must work after install. Export namespaced targets, transitive dependencies and version config; avoid global include/link directories and build-tree paths in install exports.

Use `CMakePresets.json`, Ninja where suitable, clang-format/clang-tidy, ASan/UBSan and TSan in appropriate separate jobs. Test MSVC, GCC and Clang; minimum-version and latest-supported compiler jobs are different responsibilities. CTest and a normal test framework drive tests.

Evaluate redis-plus-plus/hiredis for Redis; generated official gRPC bindings for etcd where appropriate; official NATS C client behind a C++ adapter is acceptable later. Client selection must include license, deadline/cancellation support, queue/retry behavior, activity and supported toolchains.

Conan 2 recipe plus `test_package`; vcpkg overlay port for local consumption; upstream submission is a separate owner/maintainer action. No promise of universal prebuilt binaries. Build/install and consume from a clean project before calling packaging complete.
