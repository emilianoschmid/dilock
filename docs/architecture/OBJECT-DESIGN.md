# Object and API design

## Design intent

Use mature idioms rather than inventing a universal syntax. Similar responsibilities and behavior matter more than identical method names. Microsoft framework guidance, C++ Core Guidelines, Python context-manager conventions and existing .NET distributed-lock APIs are design references, not code to copy. Consult the official documentation for each language and framework when making implementation choices.

## Three lifetimes

| Concept | Responsibility | Lifetime and concurrency |
|---|---|---|
| Provider | Backend configuration, capabilities, connections, renewal scheduling, diagnostics. | Long-lived. Thread-safe in .NET/C++; event-loop-confined in Python unless documented otherwise. |
| Named lock | Validated resource identity and a provider binding; creates acquisitions. | Immutable/reusable. Calling acquire twice is two independent attempts, not reentrancy. |
| Lease | Ownership of one successful acquisition, terminal state, optional fencing token, bounded release. | One acquisition generation. Never reused or resurrected. C++ move-only. |

A `lock` object is not proof of ownership. Only a successfully returned lease represents an acquisition. A provider should not cache one mutable lease per resource. Do not identify owners by process ID, thread ID, task ID, connection ID or resource name.

## Values

Use a validated `LockName`/`lock_name` or equivalent, immutable options, a discriminated set of error categories and an immutable fencing value. The ownership secret is internal. Do not expose a constructor that lets consumers forge an acquired lease. Keep provider creation distinct from remote capability validation; constructors must not hide network I/O.

`FencingToken` carries a monotonic value and a coordination epoch/domain. Its value alone is not comparable across clusters, resets or backend types. Diagnostic lease IDs may be public but must not be the secret stored as ownership proof.

## Extensibility without a framework

A narrow provider/lock/lease contract is justified by backend substitution and testing. Internal adapter ports express acquisition/release and lease-lifecycle events, while allowing native etcd waiting. Do not reduce all backends to `get/set/delete` or force a FIFO etcd queue into a fake Redis algorithm.

Composition is the default. Avoid deep base classes, service locators, mandatory DI containers, reflection discovery, universal “manager” objects and a factory for every object. Use interfaces/protocols for actual extension seams, not as decoration on each internal class. Public interfaces are compatibility commitments.

Split consumer-facing APIs from adapter-maintainer APIs. The internal adapter API may remain internal in 0.x; third-party drivers should not force premature public freezing.

## Lifetimes and disposal

Borrowing an existing connection is the default when one is supplied; the caller owns it. Explicit owning factories may create/close clients and must say so in their names/options. Leases retain the state needed for safe cleanup and must never retain a dangling provider pointer.

Provider shutdown rejects new acquisition, cancels pending waiters, invalidates local live leases, stops renewal with bounded cleanup and releases owned resources. Test close racing with acquire/renew/release. No unobserved task exceptions, detached renewal loops or one OS thread per lock.

Explicit `release` returns an observable release outcome. Convenience cleanup (`await using`, `async with`, destructor) is idempotent and bounded. It must preserve the primary user exception: backend cleanup failures become diagnostics and an inspectable terminal outcome, not a replacement exception. Explicit release is the path for callers needing acknowledgement. C++ destructor is `noexcept`, cannot block indefinitely and cannot guarantee remote release.

## Renewal and lost ownership

Auto-renewal is managed, not detached. A single scheduler per provider or equivalent bounded mechanism tracks active leases. At most one renewal is in flight per lease. Stop renewal when explicit release begins. Late responses cannot revive a terminal lease.

Loss notification has a reason and is monotonic. It is an instruction to stop/cooperate, not remote preemption. `IsHeld`/`is_valid` may only be a local observation; name it `State`/`state` and document that it is not a remote authority check. Omit a “safe to write now” boolean.

Do not execute arbitrary consumer callbacks while holding an internal mutex. Diagnostics failures must not break ownership bookkeeping; user exceptions must not be swallowed.

## API review before implementation expansion

Write consumer tests first: basic acquisition, contention, cancellation, lease loss, exception inside scope, explicit release acknowledgement and use with a borrowed connection. Reject an API requiring four factories, a DI host or inheritance to acquire one resource.

Compare against native alternatives and record why conventions were adopted or intentionally not adopted. Review byte encoding, time units, ownership, cancellation and async/sync semantics across SDKs before adding another backend.

Modern means supported, clear and idiomatic, not “use every newest feature.” .NET 10 is the proposed initial target; C++20 is the portability floor with optional C++23 ergonomics; Python 3.12+ is proposed. Reverify supported runtimes and dependency compatibility in M0. Do not use preview language features for production API design merely because they exist.

## Expanded SDK design

The core target now includes seven SDKs. See `../api/LANGUAGE-DESIGN.md`. Provider/resource/lease are responsibilities, not mandatory identical objects in functional languages. Kotlin and Scala reuse JVM engine state; .NET/JS ecosystems reuse their native engines. Style samples intentionally use local state; their disposal implementation must not be extrapolated to network I/O.
