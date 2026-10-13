# dilock

Shared identity, timing policy, and failure types for the Dilock distributed
lock SDKs. This package does not connect to a backend by itself; install the
Redis or etcd adapter for a trusted server or CLI process.

The identity implementation follows the cross-language vectors and does not
normalize namespace or resource strings. Leases represent time-bounded remote
ownership; they do not stop stale processes, and a backend lease is not a
substitute for fencing at the protected resource.

See the [Dart SDK guide](../README.md), [shared semantics](../../../spec/SEMANTICS.md),
and the per-backend adapter documentation.
